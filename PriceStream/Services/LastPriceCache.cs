using System.Collections.Concurrent;
using PriceStream.Models;

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
/// Per-pod cache. Every pod reads the whole topic, so each one holds every symbol.
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
