using System.Net;
using System.Text.Json;
using System.Threading.Channels;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using PriceStream.Hubs;
using PriceStream.Models;
using PriceStream.Services;
using Testcontainers.Kafka;
using Xunit;

namespace PriceStream.Tests;

[Collection(MetricsCollection.Name)]
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
        Assert.Equal(1, await pod.MetricAsync("pricestream_subscriptions", "symbol=\"DE\""));

        await client.DisposeAsync();
        await pod.WaitUntilMetricGoneAsync("pricestream_subscriptions", "symbol=\"DE\"", TimeSpan.FromSeconds(10));
    }

    [Fact(Timeout = 240_000)]
    public async Task New_pod_replays_the_window_into_its_cache_before_it_is_ready()
    {
        var kafka = new KafkaBuilder("confluentinc/cp-kafka:7.5.0").Build();
        await using var kafkaLifetime = kafka;
        await kafka.StartAsync();
        var bootstrap = kafka.GetBootstrapAddress();
        var topic = $"epex-{Guid.NewGuid():N}";

        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }]);

        using var producer = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = bootstrap }).Build();
        var now = DateTimeOffset.UtcNow;
        // Kafka time before the window: not replayed. Partition 2 stays empty and counts as caught up.
        await Produce(producer, topic, 0, Price("AT", 9m, "2026-10-13T08:00:00Z"), now.AddHours(-2));
        await Produce(producer, topic, 0, Price("DE", 41m, "2026-10-13T12:00:00Z"), now.AddMinutes(-1));
        await Produce(producer, topic, 1, Price("FR", 42m, "2026-10-13T12:00:00Z"), now.AddMinutes(-1));
        await producer.ProduceAsync(
            new TopicPartition(topic, 1),
            new Message<Null, string> { Value = "not json", Timestamp = new Timestamp(now.AddMinutes(-1)) });

        await using var pod = await Pod.StartAsync(
            "--Kafka:Enabled=true",
            $"--Kafka:BootstrapServers={bootstrap}",
            $"--Kafka:Topic={topic}",
            "--Kafka:ReplayWindowSeconds=600");
        await pod.WaitUntilReadyAsync(TimeSpan.FromSeconds(60));

        await pod.WaitUntilCachedAsync("DE", 41m);
        await pod.WaitUntilCachedAsync("FR", 42m);
        Assert.Empty(await pod.CachedAsync("AT"));

        // Prometheus compares this across pods to find one that stopped receiving data.
        var lastMessage = await pod.MetricAsync("pricestream_kafka_last_message_time_seconds");
        Assert.InRange(lastMessage, DateTimeOffset.UtcNow.AddMinutes(-1).ToUnixTimeSeconds(), DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 1);
        Assert.Equal(1, await pod.MetricAsync("pricestream_kafka_connected"));
        Assert.Equal(3, await pod.MetricAsync("pricestream_kafka_messages_total"));
        Assert.Equal(2, await pod.MetricAsync("pricestream_kafka_messages_parsed_total"));
        Assert.Equal(1, await pod.MetricAsync("pricestream_kafka_messages_failed_total"));

        await using var client = await SnapshotClient.ConnectAsync(pod.Url);
        await client.SubscribeAsync("DE");
        Assert.Equal(41m, (await client.NextAsync(TimeSpan.FromSeconds(5))).Price);
        Assert.Equal(1, await pod.MetricAsync("pricestream_subscriptions", "symbol=\"DE\""));
        Assert.Equal(1, await pod.MetricAsync("signalr_server_active_connections"));

        // After replay, new messages are broadcast.
        await Produce(producer, topic, 2, Price("DE", 43m, "2026-10-13T12:15:00Z"), DateTimeOffset.UtcNow);
        Assert.Equal(43m, (await client.NextAsync(TimeSpan.FromSeconds(20))).Price);

        // Replayed messages are cached, not sent, so only the live tick has a publish latency.
        Assert.Equal(1, await pod.MetricAsync("pricestream_publish_latency_seconds_count"));
        await pod.WaitForMetricAsync("pricestream_kafka_consumer_lag", "partition=\"2\"", 0, TimeSpan.FromSeconds(10));
        Assert.Equal(0, await pod.MetricAsync("pricestream_pipeline_queue_depth", "stage=\"queue\""));
    }

    [Fact(Timeout = 240_000)]
    public async Task Kafka_outage_is_reported_but_the_pod_stays_ready()
    {
        var kafka = new KafkaBuilder("confluentinc/cp-kafka:7.5.0").Build();
        await using var kafkaLifetime = kafka;
        await kafka.StartAsync();
        var bootstrap = kafka.GetBootstrapAddress();
        var topic = $"epex-{Guid.NewGuid():N}";
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);

        await using var pod = await Pod.StartAsync(
            "--Kafka:Enabled=true",
            $"--Kafka:BootstrapServers={bootstrap}",
            $"--Kafka:Topic={topic}");
        await pod.WaitUntilReadyAsync(TimeSpan.FromSeconds(60));
        await pod.WaitForStatusAsync("/health/kafka", HttpStatusCode.OK, TimeSpan.FromSeconds(30));

        await kafka.StopAsync();

        await pod.WaitForStatusAsync("/health/kafka", HttpStatusCode.ServiceUnavailable, TimeSpan.FromSeconds(60));
        Assert.Equal(HttpStatusCode.OK, await pod.StatusAsync("/health/ready"));
        Assert.Equal(HttpStatusCode.OK, await pod.StatusAsync("/health/live"));
    }

    private static Task Produce(IProducer<Null, string> producer, string topic, int partition, EpexMarketData price, DateTimeOffset kafkaTime) =>
        producer.ProduceAsync(
            new TopicPartition(topic, partition),
            new Message<Null, string> { Value = JsonSerializer.Serialize(price), Timestamp = new Timestamp(kafkaTime) });

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

    private sealed class Pod : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private Pod(WebApplication app, string url)
        {
            _app = app;
            Url = url;
        }

        public string Url { get; }

        public static async Task<Pod> StartAsync(params string[] extraArgs)
        {
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Development");
            var app = Program.Create(
            [
                "--urls", "http://127.0.0.1:0",
                "--Kafka:Enabled=false",
                "--ShutdownTimeoutSeconds=5",
                "--Logging:LogLevel:Default=Warning",
                .. extraArgs,
            ]);
            await app.StartAsync();
            var url = app.Urls.First(address => address.StartsWith("http://", StringComparison.OrdinalIgnoreCase)).TrimEnd('/');
            return new Pod(app, url);
        }

        public void Publish(EpexMarketData price)
        {
            var pipeline = _app.Services.GetRequiredService<EpexDataflowPipeline>();
            if (!pipeline.Post(price))
                throw new InvalidOperationException($"Pipeline dropped {price.Symbol}.");
        }

        public Task<IReadOnlyList<EpexMarketData>> CachedAsync(string symbol) =>
            _app.Services.GetRequiredService<ILastPriceCache>().GetAsync([symbol]);

        public async Task WaitUntilCachedAsync(string symbol, decimal price)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                if ((await CachedAsync(symbol)).Any(value => value.Price == price))
                    return;
                await Task.Delay(50);
            }

            throw new TimeoutException($"{symbol} at {price} was not cached.");
        }

        public async Task WaitUntilReadyAsync(TimeSpan timeout)
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
            var deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                var response = await http.GetAsync($"{Url}/health/ready");
                if (response.StatusCode == HttpStatusCode.OK)
                    return;
                await Task.Delay(200);
            }

            throw new TimeoutException("Pod did not become ready.");
        }

        public async Task<HttpStatusCode> StatusAsync(string path)
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
            using var response = await http.GetAsync($"{Url}{path}");
            return response.StatusCode;
        }

        public async Task<double> MetricAsync(string name, string label = "")
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
            var text = await http.GetStringAsync($"{Url}/metrics");
            var line = text.Split('\n').FirstOrDefault(l =>
                    (l.StartsWith(name + " ") || l.StartsWith(name + "{")) && l.Contains(label))
                ?? throw new InvalidOperationException($"{name}{{{label}}} not in /metrics:\n{text}");
            // Value is followed by an optional timestamp.
            var parts = line[(line.LastIndexOf('}') + 1)..].Trim().Split(' ');
            if (!line.Contains('{'))
                parts = line.Split(' ')[1..];
            return double.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture);
        }

        public async Task WaitUntilMetricGoneAsync(string name, string label, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            var value = double.NaN;
            while (DateTime.UtcNow < deadline)
            {
                try
                {
                    value = await MetricAsync(name, label);
                }
                catch (InvalidOperationException)
                {
                    return;
                }

                await Task.Delay(200);
            }

            throw new TimeoutException($"{name}{{{label}}} is still reported ({value}).");
        }

        public async Task WaitForMetricAsync(string name, string label, double expected, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (true)
            {
                try
                {
                    if (await MetricAsync(name, label) == expected)
                        return;
                }
                catch (InvalidOperationException) when (DateTime.UtcNow < deadline)
                {
                }

                if (DateTime.UtcNow >= deadline)
                    throw new TimeoutException($"{name}{{{label}}} did not reach {expected}.");
                await Task.Delay(200);
            }
        }

        public async Task WaitForStatusAsync(string path, HttpStatusCode expected, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < deadline)
            {
                if (await StatusAsync(path) == expected)
                    return;
                await Task.Delay(200);
            }

            throw new TimeoutException($"{path} did not return {expected}.");
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
            connection.On<EpexMarketData>(nameof(IPriceClient.PriceUpdate), update => client._updates.Writer.TryWrite(update));
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
