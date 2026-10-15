using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PriceStream.Models;
using PriceStream.Services;
using StackExchange.Redis;
using Testcontainers.Redis;
using Xunit;

namespace PriceStream.Tests;

public sealed class SnapshotTests
{
    [Fact]
    public async Task In_memory_cache_keeps_the_newest_timestamp()
    {
        var cache = new InMemoryLastPriceCache();
        await cache.SetAsync("DE", Price("DE", 2m, "2026-10-13T12:00:00Z"));
        await cache.SetAsync("DE", Price("DE", 1m, "2026-10-13T11:00:00Z"));

        var cached = Assert.Single(await cache.GetAsync(["DE", "FR"]));
        Assert.Equal(2m, cached.Price);
    }

    [Fact(Timeout = 60_000)]
    public async Task Subscriber_gets_the_last_price_without_waiting_for_a_tick()
    {
        await using var pod = await Pod.StartAsync();
        pod.Publish(Price("DE", 55m, "2026-10-13T12:00:00Z"));
        await pod.WaitUntilCachedAsync("DE", 55m);

        await using var client = await SnapshotClient.ConnectAsync(pod.Url);
        await client.SubscribeAsync("DE", "FR");

        var update = await client.NextAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("DE", update.Symbol);
        Assert.Equal(55m, update.Price);
        await client.ExpectNothingAsync(TimeSpan.FromMilliseconds(300));
    }

    [Fact(Timeout = 60_000)]
    public async Task Snapshot_is_only_sent_for_newly_added_symbols()
    {
        await using var pod = await Pod.StartAsync();
        pod.Publish(Price("DE", 55m, "2026-10-13T12:00:00Z"));
        await pod.WaitUntilCachedAsync("DE", 55m);

        await using var client = await SnapshotClient.ConnectAsync(pod.Url);
        await client.SubscribeAsync("DE");
        await client.NextAsync(TimeSpan.FromSeconds(5));

        await client.SubscribeAsync("DE");
        await client.ExpectNothingAsync(TimeSpan.FromMilliseconds(300));
    }

    [Fact(Timeout = 180_000)]
    public async Task Snapshot_comes_from_redis_when_another_pod_consumed_the_symbol()
    {
        var redis = new RedisBuilder("redis:7-alpine").Build();
        await using var redisLifetime = redis;
        await redis.StartAsync();
        var configuration = RedisConfiguration(redis.GetConnectionString());
        var instance = $"snapshot-{Guid.NewGuid():N}";

        await using var publisher = await Pod.StartAsync(configuration, instance);
        await using var reader = await Pod.StartAsync(configuration, instance);

        publisher.Publish(Price("FR", 77m, "2026-10-13T12:00:00Z"));
        await reader.WaitUntilCachedAsync("FR", 77m);

        await using var client = await SnapshotClient.ConnectAsync(reader.Url);
        await client.SubscribeAsync("FR");

        var update = await client.NextAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("FR", update.Symbol);
        Assert.Equal(77m, update.Price);
    }

    [Fact(Timeout = 180_000)]
    public async Task Redis_cache_ignores_an_older_tick()
    {
        var redis = new RedisBuilder("redis:7-alpine").Build();
        await using var redisLifetime = redis;
        await redis.StartAsync();

        using var multiplexer = await ConnectionMultiplexer.ConnectAsync(RedisConfiguration(redis.GetConnectionString()));
        var cache = new RedisLastPriceCache(multiplexer, $"snapshot-{Guid.NewGuid():N}", NullLogger<RedisLastPriceCache>.Instance);

        await cache.SetAsync("DE", Price("DE", 2m, "2026-10-13T12:00:00Z"));
        await cache.SetAsync("DE", Price("DE", 1m, "2026-10-13T11:00:00Z"));
        await multiplexer.GetDatabase().PingAsync();

        var cached = Assert.Single(await cache.GetAsync(["DE"]));
        Assert.Equal(2m, cached.Price);
    }

    private static EpexMarketData Price(string symbol, decimal price, string timestamp) => new()
    {
        Symbol = symbol,
        Market = "Day-Ahead",
        DeliveryArea = symbol,
        Timestamp = DateTimeOffset.Parse(timestamp),
        Price = price,
        Unit = "EUR/MWh",
        Contract = "P60",
    };

    private static string RedisConfiguration(string connectionString)
    {
        if (Uri.TryCreate(connectionString, UriKind.Absolute, out var uri) && uri.Port > 0)
            return $"{uri.Host}:{uri.Port}";
        return connectionString;
    }

    private sealed class Pod : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private Pod(WebApplication app, string url)
        {
            _app = app;
            Url = url;
        }

        public string Url { get; }

        public static async Task<Pod> StartAsync(string? redisConfiguration = null, string? instanceName = null)
        {
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Development");
            var args = new List<string>
            {
                "--urls", "http://127.0.0.1:0",
                "--Kafka:Enabled=false",
                "--ShutdownTimeoutSeconds=5",
                "--Logging:LogLevel:Default=Warning",
            };
            if (redisConfiguration is not null)
            {
                args.Add($"--Redis:Configuration={redisConfiguration}");
                args.Add($"--Redis:InstanceName={instanceName}");
            }

            var app = Program.Create([.. args]);
            await app.StartAsync();
            var url = app.Urls.First(address => address.StartsWith("http://", StringComparison.OrdinalIgnoreCase)).TrimEnd('/');
            return new Pod(app, url);
        }

        public void Publish(EpexMarketData price)
        {
            var pipeline = _app.Services.GetRequiredService<EpexDataflowPipeline>();
            if (!pipeline.Post(JsonSerializer.Serialize(price)))
                throw new InvalidOperationException($"Pipeline dropped {price.Symbol}.");
        }

        public async Task WaitUntilCachedAsync(string symbol, decimal price)
        {
            var cache = _app.Services.GetRequiredService<ILastPriceCache>();
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                var cached = await cache.GetAsync([symbol]);
                if (cached.Any(value => value.Price == price))
                    return;
                await Task.Delay(50);
            }

            throw new TimeoutException($"{symbol} at {price} was not cached.");
        }

        public async ValueTask DisposeAsync()
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }

    private sealed class SnapshotClient : IAsyncDisposable
    {
        private readonly HubConnection _connection;
        private readonly Channel<EpexMarketData> _updates = Channel.CreateUnbounded<EpexMarketData>();

        private SnapshotClient(HubConnection connection) => _connection = connection;

        public static async Task<SnapshotClient> ConnectAsync(string url)
        {
            var connection = new HubConnectionBuilder()
                .WithUrl($"{url}/hubs/pricestream", options => options.Transports = HttpTransportType.WebSockets)
                .Build();
            var client = new SnapshotClient(connection);
            connection.On<EpexMarketData>("PriceUpdate", update => client._updates.Writer.TryWrite(update));
            await connection.StartAsync();
            return client;
        }

        public Task SubscribeAsync(params string[] symbols) => _connection.InvokeAsync("Subscribe", symbols);

        public async Task<EpexMarketData> NextAsync(TimeSpan timeout)
        {
            using var cts = new CancellationTokenSource(timeout);
            return await _updates.Reader.ReadAsync(cts.Token);
        }

        public async Task ExpectNothingAsync(TimeSpan wait)
        {
            await Task.Delay(wait);
            Assert.False(_updates.Reader.TryRead(out var unexpected), $"Unexpected update {unexpected?.Symbol}={unexpected?.Price}.");
        }

        public ValueTask DisposeAsync() => _connection.DisposeAsync();
    }
}
