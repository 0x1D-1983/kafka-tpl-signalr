using System.Net;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using PriceStream.Health;
using PriceStream.Hubs;
using PriceStream.Models;
using PriceStream.Services;
using Xunit;

namespace PriceStream.Tests;

public sealed class PipelineFailureTests
{
    [Fact]
    public async Task Complete_drains_linked_targets()
    {
        var pipeline = new EpexDataflowPipeline();
        var target = new ActionBlock<EpexMarketData>(_ => { });
        using var link = pipeline.LinkTo(target);

        Assert.True(pipeline.Post(Sample("DE", 10m)));
        pipeline.Complete();
        pipeline.Complete();

        await target.Completion.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(TaskStatus.RanToCompletion, target.Completion.Status);
        Assert.Empty(pipeline.FaultedBlocks);
    }

    [Fact]
    public async Task Full_target_keeps_every_symbol()
    {
        var pipeline = new EpexDataflowPipeline();
        var processingFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new List<string>();
        var target = new ActionBlock<EpexMarketData>(async message =>
        {
            lock (received) received.Add(message.Symbol);
            if (message.Symbol == "DE")
            {
                processingFirst.TrySetResult();
                await release.Task;
            }
        }, new ExecutionDataflowBlockOptions { BoundedCapacity = 1, MaxDegreeOfParallelism = 1 });
        using var link = pipeline.LinkTo(target);

        Assert.True(pipeline.Post(Sample("DE", 1)));
        await processingFirst.Task.WaitAsync(TimeSpan.FromSeconds(2));
        foreach (var symbol in new[] { "FR", "AT", "NL", "CH" })
            Assert.True(pipeline.Post(Sample(symbol, 1)));

        release.TrySetResult();
        pipeline.Complete();
        await target.Completion.WaitAsync(TimeSpan.FromSeconds(3));

        lock (received)
            Assert.Equal(new[] { "AT", "CH", "DE", "FR", "NL" }, received.OrderBy(symbol => symbol).ToArray());
    }

    [Fact]
    public async Task Messages_posted_before_the_target_links_are_delivered()
    {
        var pipeline = new EpexDataflowPipeline();
        foreach (var symbol in new[] { "DE", "FR", "AT" })
            Assert.True(pipeline.Post(Sample(symbol, 1)));

        var received = new List<string>();
        var target = new ActionBlock<EpexMarketData>(message =>
        {
            lock (received) received.Add(message.Symbol);
        });
        using var link = pipeline.LinkTo(target);
        pipeline.Complete();
        await target.Completion.WaitAsync(TimeSpan.FromSeconds(3));

        lock (received)
            Assert.Equal(new[] { "AT", "DE", "FR" }, received.OrderBy(symbol => symbol).ToArray());
    }

    [Fact]
    public async Task Faulted_queue_is_named()
    {
        var pipeline = new EpexDataflowPipeline();
        Assert.Empty(pipeline.FaultedBlocks);

        Fault(pipeline, "_queue");
        try
        {
            await pipeline.Completion.WaitAsync(TimeSpan.FromSeconds(2));
        }
        catch (Exception) when (pipeline.Completion.IsFaulted)
        {
        }

        Assert.Contains("queue", pipeline.FaultedBlocks);
    }

    [Fact]
    public async Task Faulted_block_fails_liveness_and_readiness_and_stops_the_host()
    {
        await using var running = await RunningApp.StartAsync();
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };

        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync($"{running.Url}/health/live")).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await http.GetAsync($"{running.Url}/health/ready")).StatusCode);

        var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        running.App.Lifetime.ApplicationStopping.Register(() => stopping.TrySetResult());

        Fault(running.App.Services.GetRequiredService<EpexDataflowPipeline>(), "_queue");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, await StatusAsync(http, $"{running.Url}/health/live"));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, await StatusAsync(http, $"{running.Url}/health/ready"));

        var result = await ActivatorUtilities.CreateInstance<PipelineHealthCheck>(running.App.Services)
            .CheckHealthAsync(new HealthCheckContext());
        Assert.Contains("queue", result.Description);

        await stopping.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Faulted_signalr_block_fails_health_when_the_pipeline_is_still_running()
    {
        await using var running = await RunningApp.StartAsync();
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        running.App.Lifetime.ApplicationStopping.Register(() => stopping.TrySetResult());

        Fault(running.App.Services.GetRequiredService<SignalRPriceBroadcaster>(), "_targetBlock");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, await StatusAsync(http, $"{running.Url}/health/live"));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, await StatusAsync(http, $"{running.Url}/health/ready"));

        var result = await ActivatorUtilities.CreateInstance<PipelineHealthCheck>(running.App.Services)
            .CheckHealthAsync(new HealthCheckContext());
        Assert.Equal("Dataflow block faulted: signalr", result.Description);
        await stopping.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Stop_completes_the_pipeline_and_drains_the_broadcaster()
    {
        await using var running = await RunningApp.StartAsync();
        var pipeline = running.App.Services.GetRequiredService<EpexDataflowPipeline>();
        var broadcaster = running.App.Services.GetRequiredService<SignalRPriceBroadcaster>();
        var targetCompletion = Block(broadcaster, "_targetBlock").Completion;

        Assert.True(pipeline.Post(Sample("DE", 42m)));
        Assert.False(targetCompletion.IsCompleted);

        await running.App.StopAsync().WaitAsync(TimeSpan.FromSeconds(3));

        Assert.Equal(TaskStatus.RanToCompletion, targetCompletion.Status);
        Assert.True(pipeline.Completion.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Null_symbol_does_not_fault_the_broadcaster()
    {
        await using var running = await RunningApp.StartAsync();
        var pipeline = running.App.Services.GetRequiredService<EpexDataflowPipeline>();
        var broadcaster = running.App.Services.GetRequiredService<SignalRPriceBroadcaster>();
        var targetCompletion = Block(broadcaster, "_targetBlock").Completion;

        Assert.True(pipeline.Post(new EpexMarketData { Symbol = null!, Market = "Day-Ahead", DeliveryArea = "DE", Price = 1 }));
        Assert.True(pipeline.Post(Sample("DE", 42m)));

        await running.App.StopAsync().WaitAsync(TimeSpan.FromSeconds(3));

        Assert.False(broadcaster.IsFaulted);
        Assert.False(targetCompletion.IsFaulted, targetCompletion.Exception?.GetBaseException().ToString());
        Assert.Equal(TaskStatus.RanToCompletion, targetCompletion.Status);
    }

    [Fact]
    public async Task Same_symbol_is_sent_one_at_a_time_while_other_symbols_overlap()
    {
        var pipeline = new EpexDataflowPipeline();
        var proxy = new RecordingProxy();
        using var broadcaster = new SignalRPriceBroadcaster(
            new RecordingHubContext(proxy),
            pipeline,
            new InMemoryLastPriceCache(),
            new SubscriptionPolicy(Microsoft.Extensions.Options.Options.Create(new SubscriptionOptions())),
            NullLogger<SignalRPriceBroadcaster>.Instance);

        var symbols = new[] { "DE", "FR", "AT", "NL", "CH", "BE", "DK", "NO" };
        foreach (var symbol in symbols)
        {
            for (var price = 1; price <= 3; price++)
                Assert.True(pipeline.Post(Sample(symbol, price)));
        }

        await broadcaster.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));

        foreach (var symbol in symbols)
            Assert.Equal(1, proxy.MaxInFlight(symbol));
        Assert.True(proxy.MaxInFlightOverall > 1, "Expected different symbols to send in parallel.");
    }

    private static async Task<HttpStatusCode> StatusAsync(HttpClient http, string url)
    {
        Exception? last = null;
        for (var attempt = 0; attempt < 20; attempt++)
        {
            try
            {
                return (await http.GetAsync(url)).StatusCode;
            }
            catch (HttpRequestException ex)
            {
                last = ex;
                await Task.Delay(50);
            }
        }

        throw new InvalidOperationException($"Health endpoint {url} did not respond.", last);
    }

    private static EpexMarketData Sample(string symbol, decimal price) =>
        new()
        {
            Symbol = symbol,
            Market = "Day-Ahead",
            DeliveryArea = symbol,
            Timestamp = DateTimeOffset.Parse("2026-10-13T12:00:00Z"),
            Price = price,
            Unit = "EUR/MWh",
            Contract = "P60",
        };

    private static void Fault(object owner, string fieldName) =>
        Block(owner, fieldName).Fault(new InvalidOperationException($"test fault: {fieldName}"));

    private static IDataflowBlock Block(object owner, string fieldName)
    {
        var field = owner.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field {fieldName} was not found on {owner.GetType().Name}.");
        return (IDataflowBlock)field.GetValue(owner)!;
    }

    private sealed class RunningApp : IAsyncDisposable
    {
        private RunningApp(WebApplication app, string url)
        {
            App = app;
            Url = url;
        }

        public WebApplication App { get; }
        public string Url { get; }

        public static async Task<RunningApp> StartAsync()
        {
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Development");
            var app = Program.Create(
            [
                "--urls", "http://127.0.0.1:0",
                "--Kafka:Enabled=false",
                "--ShutdownTimeoutSeconds=5",
                "--Logging:LogLevel:Default=Warning",
                "--Logging:LogLevel:Microsoft.AspNetCore=Warning",
            ]);
            await app.StartAsync();

            var url = app.Urls.FirstOrDefault(address => address.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
                ?? throw new InvalidOperationException($"App did not listen on HTTP. Addresses: {string.Join(", ", app.Urls)}.");
            return new RunningApp(app, url.TrimEnd('/'));
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await App.StopAsync();
            }
            catch (Exception)
            {
            }

            await App.DisposeAsync();
        }
    }
}

sealed class RecordingHubContext : IHubContext<PriceStreamHub, IPriceClient>
{
    public RecordingHubContext(RecordingProxy proxy) => Clients = new RecordingClients(proxy);

    public IHubClients<IPriceClient> Clients { get; }
    public IGroupManager Groups => throw new NotSupportedException();
}

sealed class RecordingClients : IHubClients<IPriceClient>
{
    private readonly RecordingProxy _proxy;

    public RecordingClients(RecordingProxy proxy) => _proxy = proxy;

    public IPriceClient Group(string groupName) => _proxy;
    public IPriceClient All => throw new NotSupportedException();
    public IPriceClient AllExcept(IReadOnlyList<string> excludedConnectionIds) => throw new NotSupportedException();
    public IPriceClient Client(string connectionId) => throw new NotSupportedException();
    public IPriceClient Clients(IReadOnlyList<string> connectionIds) => throw new NotSupportedException();
    public IPriceClient GroupExcept(string groupName, IReadOnlyList<string> excludedConnectionIds) => throw new NotSupportedException();
    public IPriceClient Groups(IReadOnlyList<string> groupNames) => throw new NotSupportedException();
    public IPriceClient User(string userId) => throw new NotSupportedException();
    public IPriceClient Users(IReadOnlyList<string> userIds) => throw new NotSupportedException();
}

sealed class RecordingProxy : IPriceClient
{
    private readonly object _gate = new();
    private readonly Dictionary<string, int> _inFlight = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> _maxInFlight = new(StringComparer.OrdinalIgnoreCase);
    private int _overall;
    private int _maxOverall;

    public int MaxInFlightOverall
    {
        get { lock (_gate) return _maxOverall; }
    }

    public int MaxInFlight(string symbol)
    {
        lock (_gate) return _maxInFlight.GetValueOrDefault(symbol);
    }

    public async Task PriceUpdate(EpexMarketData message)
    {
        var symbol = message.Symbol;
        lock (_gate)
        {
            var inFlight = _inFlight.GetValueOrDefault(symbol) + 1;
            _inFlight[symbol] = inFlight;
            _maxInFlight[symbol] = Math.Max(_maxInFlight.GetValueOrDefault(symbol), inFlight);
            _overall++;
            _maxOverall = Math.Max(_maxOverall, _overall);
        }

        await Task.Delay(40);

        lock (_gate)
        {
            _inFlight[symbol]--;
            _overall--;
        }
    }
}
