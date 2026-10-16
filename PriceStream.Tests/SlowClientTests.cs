using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using PriceStream.Hubs;
using PriceStream.Models;
using PriceStream.Services;
using Xunit;
using Xunit.Abstractions;

namespace PriceStream.Tests;

/// <summary>
/// A client that stops reading its socket must not stall delivery to everyone else.
/// </summary>
[Collection(MetricsCollection.Name)]
public sealed class SlowClientTests(ITestOutputHelper output)
{
    private static readonly TimeSpan LoadDuration = TimeSpan.FromSeconds(8);
    // Large payloads fill the stalled client's socket and pipe buffers within a second or two.
    private static readonly string Padding = new('x', 4096);

    [Fact(Timeout = 60_000)]
    public async Task Client_that_stops_reading_does_not_stall_other_symbols_or_drop_ticks()
    {
        await using var app = await StartAsync();
        var pipeline = app.Services.GetRequiredService<EpexDataflowPipeline>();
        var port = new Uri(app.Urls.First()).Port;

        // Subscribes to DE, then never reads again. Pings keep the server's client timeout from closing it,
        // as a paused tab or a stuck mobile client on a live TCP connection would.
        await using var stalled = await StalledClient.ConnectAsync(port, "DE");

        var frReceived = 0;
        await using var healthy = new HubConnectionBuilder()
            .WithUrl($"http://127.0.0.1:{port}/hubs/pricestream", HttpTransportType.WebSockets)
            .Build();
        healthy.On<EpexMarketData>("PriceUpdate", _ => Interlocked.Increment(ref frReceived));
        await healthy.StartAsync();
        await healthy.InvokeAsync<SubscriptionResult>("Subscribe", new[] { "FR" });

        var posted = 0;
        var dropped = 0;
        var clock = Stopwatch.StartNew();
        var nextReport = TimeSpan.FromSeconds(1);
        var lastFr = 0;
        var lastDropped = 0;
        output.WriteLine("t(s)  FR received/s  dropped/s  queue depth");
        while (clock.Elapsed < LoadDuration)
        {
            // About 500 ticks/s per symbol.
            foreach (var symbol in new[] { "DE", "FR" })
            {
                posted++;
                if (!pipeline.Post(Tick(symbol), DateTimeOffset.UtcNow))
                    dropped++;
            }

            if (clock.Elapsed >= nextReport)
            {
                var fr = Volatile.Read(ref frReceived);
                output.WriteLine($"{nextReport.TotalSeconds,4:0}  {fr - lastFr,13}  {dropped - lastDropped,9}  {pipeline.Count,11}");
                lastFr = fr;
                lastDropped = dropped;
                nextReport += TimeSpan.FromSeconds(1);
            }

            await Task.Delay(2);
        }

        await Task.Delay(500);
        var frTotal = Volatile.Read(ref frReceived);
        output.WriteLine($"posted {posted}, dropped {dropped}, FR posted {posted / 2}, FR received {frTotal}");

        Assert.Equal(0, dropped);
        Assert.True(frTotal >= posted / 2 * 0.95, $"Healthy FR client received {frTotal} of {posted / 2} ticks.");
    }

    private static EpexMarketData Tick(string symbol) => new()
    {
        Symbol = symbol,
        Market = Padding,
        DeliveryArea = symbol,
        Timestamp = DateTimeOffset.UtcNow,
        Price = 42m,
        Unit = "EUR/MWh",
        Contract = "P60",
    };

    private static async Task<WebApplication> StartAsync()
    {
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Development");
        var app = Program.Create(
        [
            "--urls", "http://127.0.0.1:0",
            "--Kafka:Enabled=false",
            "--ShutdownTimeoutSeconds=5",
            "--Logging:LogLevel:Default=Error",
        ]);
        await app.StartAsync();
        return app;
    }

    /// <summary>
    /// SignalR over a raw WebSocket on a socket with a tiny receive buffer, so the TCP window closes
    /// as soon as the client stops calling Receive.
    /// </summary>
    private sealed class StalledClient : IAsyncDisposable
    {
        private const char RecordSeparator = '\u001e';
        private readonly WebSocket _socket;
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _pings;

        private StalledClient(WebSocket socket)
        {
            _socket = socket;
            _pings = PingAsync();
        }

        public static async Task<StalledClient> ConnectAsync(int port, string symbol)
        {
            var tcp = new Socket(SocketType.Stream, ProtocolType.Tcp) { ReceiveBufferSize = 1024 };
            await tcp.ConnectAsync(IPAddress.Loopback, port);
            var stream = new NetworkStream(tcp, ownsSocket: true);

            var key = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16));
            await stream.WriteAsync(Encoding.ASCII.GetBytes(
                $"GET /hubs/pricestream HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                $"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"));
            await ReadHttpHeadersAsync(stream);

            var socket = WebSocket.CreateFromStream(stream, new WebSocketCreationOptions { KeepAliveInterval = TimeSpan.Zero });
            await SendAsync(socket, """{"protocol":"json","version":1}""");
            await ReceiveRecordAsync(socket);
            await SendAsync(socket, $$"""{"type":1,"invocationId":"1","target":"Subscribe","arguments":[["{{symbol}}"]]}""");
            // Completion of Subscribe. Nothing is read after this.
            await ReceiveRecordAsync(socket);
            return new StalledClient(socket);
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            await _pings;
            _socket.Abort();
            _socket.Dispose();
        }

        private async Task PingAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    await SendAsync(_socket, """{"type":6}""");
                    await Task.Delay(TimeSpan.FromSeconds(5), _stop.Token);
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or WebSocketException)
            {
            }
        }

        private static Task SendAsync(WebSocket socket, string json) =>
            socket.SendAsync(Encoding.UTF8.GetBytes(json + RecordSeparator), WebSocketMessageType.Text, true, CancellationToken.None);

        private static async Task ReceiveRecordAsync(WebSocket socket)
        {
            var buffer = new byte[4096];
            var text = new StringBuilder();
            while (!text.ToString().Contains(RecordSeparator))
            {
                var result = await socket.ReceiveAsync(buffer, CancellationToken.None);
                text.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
            }
        }

        private static async Task ReadHttpHeadersAsync(NetworkStream stream)
        {
            var headers = new StringBuilder();
            var one = new byte[1];
            while (!headers.ToString().EndsWith("\r\n\r\n", StringComparison.Ordinal))
            {
                if (await stream.ReadAsync(one) == 0)
                    throw new IOException("Connection closed during the WebSocket upgrade.");
                headers.Append((char)one[0]);
            }

            if (!headers.ToString().StartsWith("HTTP/1.1 101", StringComparison.Ordinal))
                throw new IOException($"WebSocket upgrade failed: {headers}");
        }
    }
}
