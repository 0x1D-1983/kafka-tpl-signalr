using System.Diagnostics;
using System.Net.WebSockets;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using PriceStream.Models;
using PriceStream.Services;
using Xunit;
using Xunit.Abstractions;

namespace PriceStream.Tests;

/// <summary>Runs only when <c>PRICESTREAM_LOAD_TEST=1</c>; too slow and machine-dependent for every build.</summary>
public sealed class LoadFactAttribute : FactAttribute
{
    public LoadFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("PRICESTREAM_LOAD_TEST") != "1")
            Skip = "Load test: set PRICESTREAM_LOAD_TEST=1 to run.";
    }
}

/// <summary>
/// Fan-out cost with every connection subscribed to one symbol: CPU and allocations per delivered
/// message, and what that means for a pod's CPU limit. Use it to compare changes to the send path
/// (for example batching several ticks per frame) against a baseline run.
/// Clients are raw WebSockets that count <c>PriceUpdate</c> frames without parsing them. CPU is the
/// whole test process, clients included, so the numbers overstate what a pod alone would spend.
/// Tuning: PRICESTREAM_LOAD_CONNECTIONS (1000), PRICESTREAM_LOAD_TICKS_PER_SECOND (50),
/// PRICESTREAM_LOAD_SECONDS (10).
/// </summary>
[Collection(MetricsCollection.Name)]
public sealed class FanOutLoadTests(ITestOutputHelper output)
{
    private const double PodCpuLimitCores = 0.5;
    private static readonly string[] Contracts = Enumerable.Range(0, 10).Select(i => $"C{i}").ToArray();

    [LoadFact(Timeout = 900_000)]
    public async Task Fan_out_cost_per_delivered_message()
    {
        var connections = Env("PRICESTREAM_LOAD_CONNECTIONS", 1000);
        var ticksPerSecond = Env("PRICESTREAM_LOAD_TICKS_PER_SECOND", 50);
        var duration = TimeSpan.FromSeconds(Env("PRICESTREAM_LOAD_SECONDS", 10));

        // Two runs show how much the machine itself varies.
        var results = new List<Result>();
        for (var run = 0; run < 2; run++)
            results.Add(await RunAsync(connections, ticksPerSecond, duration));

        output.WriteLine($"{connections} connections, {ticksPerSecond} ticks/s on one symbol, {duration.TotalSeconds:0}s per run");
        output.WriteLine("run   delivered   of expected   CPU s   CPU µs/msg   alloc B/msg   gen0 GCs");
        for (var i = 0; i < results.Count; i++)
        {
            var r = results[i];
            output.WriteLine(
                $"{i + 1,3} {r.Delivered,11:N0} {r.DeliveredRatio,13:P1} {r.CpuSeconds,7:0.00} {r.CpuMicrosPerMessage,12:0.00} {r.AllocatedBytesPerMessage,13:N0} {r.Gen0,10}");
        }

        var best = results.MinBy(r => r.CpuMicrosPerMessage)!;
        output.WriteLine(
            $"messages/s within {PodCpuLimitCores} cores (upper bound on cost, clients included): " +
            $"{PodCpuLimitCores / best.CpuMicrosPerMessage * 1e6:N0}");

        // A run that could not keep up conflates ticks, which makes its per-message numbers meaningless.
        Assert.All(results, r => Assert.True(r.DeliveredRatio > 0.95, $"Delivered only {r.DeliveredRatio:P1}; lower the load."));
    }

    private static async Task<Result> RunAsync(int connections, int ticksPerSecond, TimeSpan duration)
    {
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Development");
        await using var app = Program.Create(
        [
            "--urls", "http://127.0.0.1:0",
            "--Kafka:Enabled=false",
            "--ShutdownTimeoutSeconds=5",
            "--Logging:LogLevel:Default=Error",
        ]);
        await app.StartAsync();
        var pipeline = app.Services.GetRequiredService<EpexDataflowPipeline>();
        var url = new Uri(app.Urls.First().Replace("http://", "ws://") + "/hubs/pricestream");

        var clients = new List<CountingClient>(connections);
        using var stop = new CancellationTokenSource();
        for (var i = 0; i < connections; i += 50)
        {
            clients.AddRange(await Task.WhenAll(
                Enumerable.Range(0, Math.Min(50, connections - i)).Select(_ => CountingClient.ConnectAsync(url, "DE", stop.Token))));
        }

        var sequence = 0;
        // Warm-up: JIT, pools and buffers.
        await PostAsync(pipeline, ticksPerSecond, TimeSpan.FromSeconds(2), () => sequence++);
        await WaitForQuietAsync(clients);

        var before = Delivered(clients);
        var process = Process.GetCurrentProcess();
        process.Refresh();
        var cpuBefore = process.TotalProcessorTime;
        var allocatedBefore = GC.GetTotalAllocatedBytes(precise: false);
        var gen0Before = GC.CollectionCount(0);

        var posted = await PostAsync(pipeline, ticksPerSecond, duration, () => sequence++);
        await WaitForQuietAsync(clients);

        process.Refresh();
        var cpu = (process.TotalProcessorTime - cpuBefore).TotalSeconds;
        var allocated = GC.GetTotalAllocatedBytes(precise: false) - allocatedBefore;
        var gen0 = GC.CollectionCount(0) - gen0Before;
        var delivered = Delivered(clients) - before;

        stop.Cancel();
        foreach (var client in clients)
            client.Abort();
        // Lets SignalR run OnDisconnectedAsync before the service provider is disposed.
        await app.StopAsync();

        return new Result(
            delivered,
            (double)delivered / ((long)posted * connections),
            cpu,
            cpu / Math.Max(1, delivered) * 1e6,
            (double)allocated / Math.Max(1, delivered),
            gen0);
    }

    private static async Task<int> PostAsync(EpexDataflowPipeline pipeline, int ticksPerSecond, TimeSpan duration, Func<int> next)
    {
        var clock = Stopwatch.StartNew();
        var posted = 0;
        while (clock.Elapsed < duration)
        {
            var due = (int)(clock.Elapsed.TotalSeconds * ticksPerSecond);
            while (posted < due)
            {
                var n = next();
                var tick = new EpexMarketData
                {
                    Symbol = "DE",
                    Market = "Intraday",
                    DeliveryArea = "DE-LU",
                    Timestamp = DateTimeOffset.UtcNow,
                    Price = 40m + n % 1000 / 100m,
                    Unit = "EUR/MWh",
                    Contract = Contracts[n % Contracts.Length],
                };
                if (!pipeline.Post(tick, DateTimeOffset.UtcNow))
                    throw new InvalidOperationException("Pipeline dropped a tick; the load is too high for this machine.");
                posted++;
            }

            await Task.Delay(1);
        }

        return posted;
    }

    // Until no client has received anything for 300ms.
    private static async Task WaitForQuietAsync(IReadOnlyList<CountingClient> clients)
    {
        var last = -1L;
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var now = Delivered(clients);
            if (now == last)
                return;
            last = now;
            await Task.Delay(300);
        }
    }

    private static long Delivered(IReadOnlyList<CountingClient> clients) => clients.Sum(client => client.Received);

    private static int Env(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), out var value) && value > 0 ? value : fallback;

    private sealed record Result(
        long Delivered,
        double DeliveredRatio,
        double CpuSeconds,
        double CpuMicrosPerMessage,
        double AllocatedBytesPerMessage,
        int Gen0);

    /// <summary>SignalR JSON over a raw WebSocket. Counts PriceUpdate invocations without deserializing them.</summary>
    private sealed class CountingClient
    {
        private const byte RecordSeparator = 0x1e;
        private static readonly byte[] Target = "\"target\":\"PriceUpdate\""u8.ToArray();
        private readonly ClientWebSocket _socket;
        private long _received;

        private CountingClient(ClientWebSocket socket) => _socket = socket;

        public long Received => Interlocked.Read(ref _received);

        public static async Task<CountingClient> ConnectAsync(Uri url, string symbol, CancellationToken stop)
        {
            var socket = new ClientWebSocket();
            await socket.ConnectAsync(url, CancellationToken.None);
            var client = new CountingClient(socket);
            await client.SendAsync("""{"protocol":"json","version":1}""");
            await client.ReadUntilAsync("{}"u8.ToArray());
            await client.SendAsync($$"""{"type":1,"invocationId":"1","target":"Subscribe","arguments":[["{{symbol}}"]]}""");
            await client.ReadUntilAsync("\"invocationId\":\"1\""u8.ToArray());
            _ = client.ReceiveLoopAsync(stop);
            _ = client.PingLoopAsync(stop);
            return client;
        }

        public void Abort() => _socket.Abort();

        private async Task ReceiveLoopAsync(CancellationToken stop)
        {
            var buffer = new byte[16 * 1024];
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    var result = await _socket.ReceiveAsync(buffer, stop);
                    if (result.MessageType == WebSocketMessageType.Close)
                        return;
                    var span = buffer.AsSpan(0, result.Count);
                    var count = 0;
                    for (var at = span.IndexOf(Target); at >= 0; at = span.IndexOf(Target))
                    {
                        count++;
                        span = span[(at + Target.Length)..];
                    }

                    if (count > 0)
                        Interlocked.Add(ref _received, count);
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or WebSocketException or ObjectDisposedException)
            {
            }
        }

        // The server closes connections it has not heard from within ClientTimeoutInterval (30s).
        private async Task PingLoopAsync(CancellationToken stop)
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(10), stop);
                    await SendAsync("""{"type":6}""");
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or WebSocketException or ObjectDisposedException)
            {
            }
        }

        private Task SendAsync(string json) =>
            _socket.SendAsync(Encoding.UTF8.GetBytes(json + (char)RecordSeparator), WebSocketMessageType.Text, true, CancellationToken.None);

        private async Task ReadUntilAsync(byte[] marker)
        {
            var buffer = new byte[4096];
            var received = new List<byte>();
            while (received.ToArray().AsSpan().IndexOf(marker) < 0)
            {
                var result = await _socket.ReceiveAsync(buffer, CancellationToken.None);
                received.AddRange(buffer.AsSpan(0, result.Count));
            }
        }
    }
}
