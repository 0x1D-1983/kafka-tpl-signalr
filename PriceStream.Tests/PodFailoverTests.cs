using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PriceStream.Models;
using PriceStream.Services;
using Testcontainers.Redis;
using Xunit;

namespace PriceStream.Tests;

/// <summary>
/// When the pod a client is stuck to goes down, the edge sends the reconnect to a healthy pod.
/// The client resubscribes to the symbols it already asked for and keeps receiving them.
/// </summary>
public sealed class PodFailoverTests
{
    internal static readonly string[] Symbols = ["DE", "FR"];

    [Fact(Timeout = 180_000)]
    public async Task Client_is_rerouted_to_a_healthy_pod_and_keeps_receiving_its_subscription()
    {
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Development");

        var redis = new RedisBuilder("redis:7-alpine").Build();
        await using var redisLifetime = redis;
        await redis.StartAsync();

        await using var fleet = await PodFleet.StartAsync(RedisConfiguration(redis.GetConnectionString()));
        await using var edge = await StickyPodProxy.StartAsync(fleet.Pods);
        await using var client = await SubscribingClient.ConnectAsync(edge.Url);

        var connected = edge.CurrentPod;
        Assert.Contains(connected, fleet.Pods.Select(pod => pod.Name));

        var publisher = fleet.Pods.First(pod => pod.Name != connected);
        await client.ExpectAsync("DE", 101m, () => publisher.Publish("DE", 101m));
        await client.ExpectAsync("FR", 102m, () => publisher.Publish("FR", 102m));

        edge.RemovePod(connected);
        await fleet.Pods.Single(pod => pod.Name == connected).DisposeAsync();
        await client.WaitUntilReroutedAsync(edge);

        var landed = edge.CurrentPod;
        Assert.NotEqual(connected, landed);
        Assert.Contains(landed, fleet.Pods.Where(pod => pod.Name != connected).Select(pod => pod.Name));

        var survivor = fleet.Pods.Single(pod => pod.Name != connected && pod.Name != landed);
        await client.ExpectAsync("DE", 201m, () => survivor.Publish("DE", 201m));
        await client.ExpectAsync("FR", 202m, () => survivor.Publish("FR", 202m));
        await client.ExpectAbsentAsync("AT", 999m, () => survivor.Publish("AT", 999m));
    }

    private static string RedisConfiguration(string connectionString)
    {
        if (Uri.TryCreate(connectionString, UriKind.Absolute, out var uri) && uri.Port > 0)
            return $"{uri.Host}:{uri.Port}";
        return connectionString;
    }
}

/// <summary>
/// Same retry schedule as the React client in <c>usePriceStream.js</c>: try immediately, then back off, and keep trying.
/// </summary>
sealed class PriceStreamRetryPolicy : IRetryPolicy
{
    private static readonly int[] DelaysMs = [0, 2000, 5000, 10000];

    public TimeSpan? NextRetryDelay(RetryContext retryContext)
    {
        var index = (int)Math.Min(retryContext.PreviousRetryCount, DelaysMs.Length - 1);
        return TimeSpan.FromMilliseconds(DelaysMs[index]);
    }
}

/// <summary>
/// Mirrors the React client: subscribe to the caller's symbols, and on reconnect subscribe to that same list again.
/// </summary>
sealed class SubscribingClient : IAsyncDisposable
{
    private readonly HubConnection _connection;
    private readonly Channel<EpexMarketData> _updates = Channel.CreateUnbounded<EpexMarketData>();
    private readonly ConcurrentQueue<string> _seen = new();
    private readonly TaskCompletionSource _rerouted = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private SubscribingClient(HubConnection connection) => _connection = connection;

    public static async Task<SubscribingClient> ConnectAsync(string edgeUrl)
    {
        var cookies = new CookieContainer();
        var connection = new HubConnectionBuilder()
            .WithUrl($"{edgeUrl}/hubs/pricestream", options =>
            {
                options.Cookies = cookies;
                options.Transports = HttpTransportType.WebSockets;
            })
            .WithAutomaticReconnect(new PriceStreamRetryPolicy())
            .Build();
        connection.HandshakeTimeout = TimeSpan.FromSeconds(10);

        var client = new SubscribingClient(connection);
        connection.On<EpexMarketData>("PriceUpdate", update =>
        {
            client._seen.Enqueue($"{update.Symbol}={update.Price}");
            client._updates.Writer.TryWrite(update);
        });
        connection.Reconnected += async _ =>
        {
            try
            {
                await connection.InvokeAsync("Subscribe", PodFailoverTests.Symbols);
                client._rerouted.TrySetResult();
            }
            catch (Exception ex)
            {
                client._rerouted.TrySetException(ex);
            }
        };

        await connection.StartAsync();
        await connection.InvokeAsync("Subscribe", PodFailoverTests.Symbols);
        return client;
    }

    public async Task WaitUntilReroutedAsync(StickyPodProxy edge)
    {
        try
        {
            await _rerouted.Task.WaitAsync(TimeSpan.FromSeconds(20));
        }
        catch (TimeoutException)
        {
            Assert.Fail(
                $"Client did not reconnect onto a healthy pod. State={_connection.State}. " +
                $"Negotiations={string.Join(" -> ", edge.Negotiations)}. Seen=[{string.Join(", ", _seen)}].");
        }
    }

    public Task ExpectAsync(string symbol, decimal price, Action publish) =>
        WaitForAsync(symbol, price, publish, shouldArrive: true, TimeSpan.FromSeconds(20));

    public Task ExpectAbsentAsync(string symbol, decimal price, Action publish) =>
        WaitForAsync(symbol, price, publish, shouldArrive: false, TimeSpan.FromSeconds(2));

    public async ValueTask DisposeAsync() => await _connection.DisposeAsync();

    private async Task WaitForAsync(string symbol, decimal price, Action publish, bool shouldArrive, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var publishing = PublishRepeatedlyAsync(publish, cts.Token);
        try
        {
            while (true)
            {
                var update = await _updates.Reader.ReadAsync(cts.Token);
                if (!update.Symbol.Equals(symbol, StringComparison.OrdinalIgnoreCase) || update.Price != price)
                    continue;

                if (!shouldArrive)
                    Assert.Fail($"{symbol} was not in the original subscription but it arrived after the client was rerouted.");
                return;
            }
        }
        catch (OperationCanceledException) when (!shouldArrive)
        {
        }
        catch (OperationCanceledException)
        {
            Assert.Fail(
                $"Expected {symbol} at {price} on the original subscription ({string.Join(", ", PodFailoverTests.Symbols)}), " +
                $"but it did not arrive. Seen=[{string.Join(", ", _seen)}].");
        }
        finally
        {
            await cts.CancelAsync();
            try { await publishing; }
            catch (OperationCanceledException) { }
        }
    }

    private static async Task PublishRepeatedlyAsync(Action publish, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            publish();
            try { await Task.Delay(200, cancellationToken); }
            catch (OperationCanceledException) { break; }
        }
    }
}

sealed class PodFleet : IAsyncDisposable
{
    private PodFleet(PriceStreamPod[] pods) => Pods = pods;

    public IReadOnlyList<PriceStreamPod> Pods { get; }

    public static async Task<PodFleet> StartAsync(string redisConfiguration)
    {
        var pods = await Task.WhenAll(
            PriceStreamPod.StartAsync("pod-a", redisConfiguration),
            PriceStreamPod.StartAsync("pod-b", redisConfiguration),
            PriceStreamPod.StartAsync("pod-c", redisConfiguration));
        return new PodFleet(pods);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var pod in Pods)
            await pod.DisposeAsync();
    }
}

sealed class PriceStreamPod : IAsyncDisposable
{
    private readonly WebApplication _app;
    private int _disposed;

    private PriceStreamPod(string name, WebApplication app, string url)
    {
        Name = name;
        _app = app;
        Url = url;
    }

    public string Name { get; }
    public string Url { get; }

    public static async Task<PriceStreamPod> StartAsync(string name, string redisConfiguration)
    {
        var app = Program.Create(
        [
            "--urls", "http://127.0.0.1:0",
            "--Kafka:Enabled=false",
            $"--Redis:Configuration={redisConfiguration}",
            "--Redis:InstanceName=pricestream-failover",
            "--ShutdownTimeoutSeconds=5",
            "--Logging:LogLevel:Default=Warning",
            "--Logging:LogLevel:Microsoft.AspNetCore=Warning",
            "--Logging:LogLevel:Microsoft.AspNetCore.HttpsPolicy=Error",
        ]);
        await app.StartAsync();

        var url = app.Urls.FirstOrDefault(address => address.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException($"Pod {name} did not listen on HTTP. Addresses: {string.Join(", ", app.Urls)}.");
        if (url.EndsWith(":0", StringComparison.Ordinal))
            throw new InvalidOperationException($"Pod {name} did not receive a port: {url}.");

        return new PriceStreamPod(name, app, url.TrimEnd('/'));
    }

    public void Publish(string symbol, decimal price)
    {
        var pipeline = _app.Services.GetRequiredService<EpexDataflowPipeline>();
        var json = JsonSerializer.Serialize(new EpexMarketData
        {
            Symbol = symbol,
            Market = "Day-Ahead",
            DeliveryArea = symbol,
            Timestamp = DateTimeOffset.Parse("2026-10-13T12:00:00Z"),
            Price = price,
            Unit = "EUR/MWh",
            Contract = "P60",
        });
        if (!pipeline.Post(json))
            throw new InvalidOperationException($"Pod {Name} dropped the {symbol} update.");
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try { await _app.StopAsync(); }
        finally { await _app.DisposeAsync(); }
    }
}

/// <summary>
/// Stands in for the edge Envoy. The affinity cookie pins a client to one pod.
/// Once that pod leaves the healthy set, the next connection is sent to a pod that is still up.
/// </summary>
sealed class StickyPodProxy : IAsyncDisposable
{
    public const string AffinityCookie = "pricestream-affinity";

    private static readonly HashSet<string> HopByHop = new(StringComparer.OrdinalIgnoreCase)
    {
        "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
        "TE", "Trailer", "Transfer-Encoding", "Upgrade", "Host", "Content-Length",
    };

    private readonly ConcurrentDictionary<string, PriceStreamPod> _pods = new();
    private readonly ConcurrentDictionary<WebSocket, string> _clientSockets = new();
    private readonly ConcurrentQueue<string> _negotiated = new();
    private readonly HttpClient _http = new(new SocketsHttpHandler
    {
        UseCookies = false,
        AllowAutoRedirect = false,
        AutomaticDecompression = DecompressionMethods.None,
    })
    { Timeout = TimeSpan.FromSeconds(30) };

    private readonly WebApplication _app;
    private readonly ILogger _logger;

    private StickyPodProxy(IEnumerable<PriceStreamPod> pods)
    {
        foreach (var pod in pods)
            _pods[pod.Name] = pod;

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            Args = ["--urls", "http://127.0.0.1:0"],
        });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        _app = builder.Build();
        _logger = _app.Logger;
        _app.UseWebSockets();
        _app.Use(async (HttpContext context, RequestDelegate next) =>
        {
            _ = next;
            try
            {
                await HandleAsync(context);
            }
            catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Edge proxy failed for {Path}", context.Request.Path);
                if (!context.Response.HasStarted)
                    context.Response.StatusCode = StatusCodes.Status502BadGateway;
            }
        });
    }

    public string Url { get; private set; } = "";

    public IReadOnlyList<string> Negotiations => _negotiated.ToArray();

    public string CurrentPod
    {
        get
        {
            if (_negotiated.TryPeekLast(out var pod) && !string.IsNullOrEmpty(pod))
                return pod;
            throw new InvalidOperationException("The client has not negotiated with a pod.");
        }
    }

    public static async Task<StickyPodProxy> StartAsync(IEnumerable<PriceStreamPod> pods)
    {
        var proxy = new StickyPodProxy(pods);
        await proxy._app.StartAsync();
        proxy.Url = proxy._app.Urls
            .FirstOrDefault(address => address.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            ?.TrimEnd('/')
            ?? throw new InvalidOperationException($"Edge proxy did not listen on HTTP. Addresses: {string.Join(", ", proxy._app.Urls)}.");
        return proxy;
    }

    /// <summary>
    /// Drops the pod from the healthy set and resets sockets still pinned to it, the way a crashed process does.
    /// </summary>
    public void RemovePod(string name)
    {
        _pods.TryRemove(name, out _);
        foreach (var (socket, pod) in _clientSockets)
        {
            if (pod == name)
                Abort(socket);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _app.StopAsync();
        await _app.DisposeAsync();
    }

    private async Task HandleAsync(HttpContext context)
    {
        if (!context.Request.Path.StartsWithSegments("/hubs"))
        {
            context.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        var pod = SelectPod(context, out var refreshCookie);
        if (context.Request.Path.Value?.EndsWith("/negotiate", StringComparison.OrdinalIgnoreCase) == true)
            _negotiated.Enqueue(pod.Name);

        if (context.WebSockets.IsWebSocketRequest)
            await ProxyWebSocketAsync(context, pod, refreshCookie);
        else
            await ProxyHttpAsync(context, pod, refreshCookie);
    }

    private PriceStreamPod SelectPod(HttpContext context, out bool refreshCookie)
    {
        if (context.Request.Cookies.TryGetValue(AffinityCookie, out var pinned)
            && _pods.TryGetValue(pinned, out var pod))
        {
            refreshCookie = false;
            return pod;
        }

        pod = _pods.Values.FirstOrDefault()
            ?? throw new InvalidOperationException("No healthy price stream pods remain.");
        refreshCookie = true;
        return pod;
    }

    private async Task ProxyHttpAsync(HttpContext context, PriceStreamPod pod, bool refreshCookie)
    {
        using var request = new HttpRequestMessage(new HttpMethod(context.Request.Method), $"{pod.Url}{context.Request.Path}{context.Request.QueryString}")
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };

        if (context.Request.ContentLength is > 0)
            request.Content = new StreamContent(context.Request.Body);

        foreach (var header in context.Request.Headers)
        {
            if (HopByHop.Contains(header.Key))
                continue;
            if (!request.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray()))
                request.Content?.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray());
        }

        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, context.RequestAborted);
        context.Response.StatusCode = (int)response.StatusCode;
        foreach (var header in response.Headers)
            CopyResponseHeader(context, header.Key, header.Value);
        foreach (var header in response.Content.Headers)
            CopyResponseHeader(context, header.Key, header.Value);

        if (refreshCookie)
            SetAffinityCookie(context, pod.Name);

        await response.Content.CopyToAsync(context.Response.Body, context.RequestAborted);
    }

    private async Task ProxyWebSocketAsync(HttpContext context, PriceStreamPod pod, bool refreshCookie)
    {
        using var backend = new ClientWebSocket();
        if (context.Request.Headers.TryGetValue("Cookie", out var cookie))
            backend.Options.SetRequestHeader("Cookie", cookie.ToString());

        foreach (var protocol in context.WebSockets.WebSocketRequestedProtocols)
            backend.Options.AddSubProtocol(protocol);

        var target = new Uri($"ws://{new Uri(pod.Url).Authority}{context.Request.Path}{context.Request.QueryString}");
        await backend.ConnectAsync(target, context.RequestAborted);

        if (refreshCookie)
            SetAffinityCookie(context, pod.Name);

        var client = await context.WebSockets.AcceptWebSocketAsync(backend.SubProtocol);
        _clientSockets[client] = pod.Name;
        try
        {
            using var done = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
            var inbound = PumpAsync(backend, client, done.Token);
            var outbound = PumpAsync(client, backend, done.Token);
            await Task.WhenAny(inbound, outbound);
            await done.CancelAsync();
            Abort(backend);
            Abort(client);
            await Task.WhenAll(SwallowAsync(inbound), SwallowAsync(outbound));
        }
        finally
        {
            _clientSockets.TryRemove(client, out _);
        }
    }

    private static async Task PumpAsync(WebSocket source, WebSocket destination, CancellationToken cancellationToken)
    {
        var buffer = new byte[64 * 1024];
        while (!cancellationToken.IsCancellationRequested
               && source.State == WebSocketState.Open
               && destination.State == WebSocketState.Open)
        {
            var result = await source.ReceiveAsync(buffer, cancellationToken);
            if (result.MessageType == WebSocketMessageType.Close)
                return;
            if (result.Count == 0 && !result.EndOfMessage)
                continue;
            await destination.SendAsync(buffer.AsMemory(0, result.Count), result.MessageType, result.EndOfMessage, cancellationToken);
        }
    }

    private static void SetAffinityCookie(HttpContext context, string podName)
    {
        context.Response.Cookies.Append(AffinityCookie, podName, new CookieOptions
        {
            Path = "/hubs",
            HttpOnly = true,
            IsEssential = true,
            SameSite = SameSiteMode.Lax,
        });
    }

    private static void CopyResponseHeader(HttpContext context, string key, IEnumerable<string> values)
    {
        if (!HopByHop.Contains(key))
            context.Response.Headers.Append(key, values.ToArray());
    }

    private static async Task SwallowAsync(Task task)
    {
        try { await task; }
        catch { }
    }

    private static void Abort(WebSocket socket)
    {
        try
        {
            if (socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
                socket.Abort();
        }
        catch (WebSocketException)
        {
        }
    }
}

static class ConcurrentQueueExtensions
{
    public static bool TryPeekLast<T>(this ConcurrentQueue<T> queue, out T? item)
    {
        var items = queue.ToArray();
        if (items.Length == 0)
        {
            item = default;
            return false;
        }

        item = items[^1];
        return true;
    }
}
