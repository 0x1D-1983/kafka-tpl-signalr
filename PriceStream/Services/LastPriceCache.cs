using System.Collections.Concurrent;
using System.Text.Json;
using PriceStream.Models;
using StackExchange.Redis;

namespace PriceStream.Services;

/// <summary>
/// Latest price per symbol, sent to a connection when it subscribes so it does not wait for the next tick.
/// A value never replaces one with a newer source timestamp.
/// </summary>
public interface ILastPriceCache
{
    /// <param name="symbol">Normalized (trimmed, upper-case) symbol.</param>
    Task SetAsync(string symbol, EpexMarketData value);

    /// <param name="symbols">Normalized symbols. Symbols with no cached value are omitted.</param>
    Task<IReadOnlyList<EpexMarketData>> GetAsync(IReadOnlyCollection<string> symbols);
}

/// <summary>
/// Single-pod cache. With several pods each one only sees the symbols from its own Kafka partitions,
/// so use <see cref="RedisLastPriceCache"/> whenever the backplane is enabled.
/// </summary>
public sealed class InMemoryLastPriceCache : ILastPriceCache
{
    private readonly ConcurrentDictionary<string, EpexMarketData> _values = new(StringComparer.Ordinal);

    public Task SetAsync(string symbol, EpexMarketData value)
    {
        _values.AddOrUpdate(
            symbol,
            static (_, incoming) => incoming,
            static (_, current, incoming) => incoming.Timestamp >= current.Timestamp ? incoming : current,
            value);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<EpexMarketData>> GetAsync(IReadOnlyCollection<string> symbols)
    {
        var found = new List<EpexMarketData>(symbols.Count);
        foreach (var symbol in symbols)
        {
            if (_values.TryGetValue(symbol, out var value))
                found.Add(value);
        }

        return Task.FromResult<IReadOnlyList<EpexMarketData>>(found);
    }
}

/// <summary>
/// Shared across pods: whichever pod consumes a symbol writes it, and any pod can serve the snapshot.
/// </summary>
public sealed class RedisLastPriceCache : ILastPriceCache
{
    // Compare-and-set on the source timestamp. After a Kafka rebalance two pods can briefly
    // write the same symbol, and the older tick must not win.
    private const string SetIfNewerScript = """
        local current = redis.call('HGET', KEYS[2], ARGV[1])
        if current and tonumber(current) > tonumber(ARGV[2]) then
          return 0
        end
        redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
        return 1
        """;

    private readonly IConnectionMultiplexer _redis;
    private readonly RedisKey[] _keys;
    private readonly ILogger<RedisLastPriceCache> _logger;

    public RedisLastPriceCache(IConnectionMultiplexer redis, string? instanceName, ILogger<RedisLastPriceCache> logger)
    {
        _redis = redis;
        _logger = logger;
        // The hash tag keeps both keys in one slot so the script also runs on Redis Cluster.
        var prefix = string.IsNullOrWhiteSpace(instanceName) ? "pricestream" : instanceName;
        _keys = [$"{{{prefix}}}:last-price", $"{{{prefix}}}:last-price-ts"];
    }

    public Task SetAsync(string symbol, EpexMarketData value)
    {
        // Fire-and-forget keeps Redis latency off the broadcast path. Commands on one
        // multiplexer stay in order, and the script rejects older timestamps regardless.
        _ = _redis.GetDatabase().ScriptEvaluate(
            SetIfNewerScript,
            _keys,
            [symbol, value.Timestamp.ToUnixTimeMilliseconds(), JsonSerializer.Serialize(value)],
            CommandFlags.FireAndForget);
        return Task.CompletedTask;
    }

    public async Task<IReadOnlyList<EpexMarketData>> GetAsync(IReadOnlyCollection<string> symbols)
    {
        if (symbols.Count == 0)
            return [];

        var fields = symbols.Select(symbol => (RedisValue)symbol).ToArray();
        var values = await _redis.GetDatabase().HashGetAsync(_keys[0], fields);

        var found = new List<EpexMarketData>(values.Length);
        foreach (var value in values)
        {
            if (value.IsNullOrEmpty)
                continue;
            try
            {
                if (JsonSerializer.Deserialize<EpexMarketData>(value.ToString()) is { } price)
                    found.Add(price);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Ignoring unreadable cached price");
            }
        }

        return found;
    }
}
