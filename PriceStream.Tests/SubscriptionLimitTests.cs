using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;
using PriceStream.Hubs;
using Xunit;

namespace PriceStream.Tests;

public sealed class SubscriptionLimitTests
{
    [Fact]
    public async Task Invalid_and_overlong_symbols_are_rejected_and_valid_ones_accepted()
    {
        await using var hub = await HubSession.StartAsync();

        var result = await hub.SubscribeAsync(" de ", "DE", "fr", "", "BAD SYMBOL", "x;y", new string('A', 500));

        Assert.Equal(["DE", "FR"], result.Accepted);
        Assert.Equal(4, result.Rejected.Count);
        Assert.All(result.Rejected, rejected => Assert.Equal(SubscriptionRejection.Invalid, rejected.Reason));
        Assert.All(result.Rejected, rejected => Assert.True(rejected.Symbol.Length <= 16));
    }

    [Fact]
    public async Task Allow_list_rejects_unknown_symbols()
    {
        await using var hub = await HubSession.StartAsync(
            "--Subscriptions:AllowedSymbols:0=DE",
            "--Subscriptions:AllowedSymbols:1=FR");

        var result = await hub.SubscribeAsync("DE", "AT");

        Assert.Equal(["DE"], result.Accepted);
        Assert.Equal([new RejectedSymbol("AT", SubscriptionRejection.NotAllowed)], result.Rejected);
    }

    [Fact]
    public async Task Connection_cap_counts_distinct_symbols_and_unsubscribe_frees_a_slot()
    {
        await using var hub = await HubSession.StartAsync("--Subscriptions:MaxSymbolsPerConnection=2");

        var first = await hub.SubscribeAsync("DE", "FR", "AT");
        Assert.Equal(["DE", "FR"], first.Accepted);
        Assert.Equal([new RejectedSymbol("AT", SubscriptionRejection.LimitReached)], first.Rejected);

        var again = await hub.SubscribeAsync("DE", "FR");
        Assert.Equal(["DE", "FR"], again.Accepted);
        Assert.Empty(again.Rejected);

        var removed = await hub.UnsubscribeAsync("FR", "NL");
        Assert.Equal(["FR"], removed.Accepted);
        Assert.Equal([new RejectedSymbol("NL", SubscriptionRejection.NotSubscribed)], removed.Rejected);

        var refill = await hub.SubscribeAsync("AT");
        Assert.Equal(["AT"], refill.Accepted);
    }

    [Fact]
    public async Task Calls_beyond_the_rate_limit_are_rejected()
    {
        await using var hub = await HubSession.StartAsync(
            "--Subscriptions:CallsPerWindow=2",
            "--Subscriptions:WindowSeconds=60");

        await hub.SubscribeAsync("DE");
        await hub.UnsubscribeAsync("DE");
        var limited = await hub.SubscribeAsync("FR");

        Assert.Empty(limited.Accepted);
        Assert.Equal([new RejectedSymbol("FR", SubscriptionRejection.RateLimited)], limited.Rejected);
    }

    private sealed class HubSession : IAsyncDisposable
    {
        private readonly WebApplication _app;
        private readonly HubConnection _connection;

        private HubSession(WebApplication app, HubConnection connection)
        {
            _app = app;
            _connection = connection;
        }

        public static async Task<HubSession> StartAsync(params string[] extraArgs)
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
            var connection = new HubConnectionBuilder()
                .WithUrl($"{url}/hubs/pricestream", options => options.Transports = HttpTransportType.WebSockets)
                .Build();
            await connection.StartAsync();
            return new HubSession(app, connection);
        }

        public Task<SubscriptionResult> SubscribeAsync(params string[] symbols) =>
            _connection.InvokeAsync<SubscriptionResult>("Subscribe", symbols);

        public Task<SubscriptionResult> UnsubscribeAsync(params string[] symbols) =>
            _connection.InvokeAsync<SubscriptionResult>("Unsubscribe", symbols);

        public async ValueTask DisposeAsync()
        {
            await _connection.DisposeAsync();
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }
}
