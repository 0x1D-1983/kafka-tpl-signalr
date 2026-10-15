using Microsoft.AspNetCore.SignalR;
using PriceStream.Services;

namespace PriceStream.Hubs;

public sealed class PriceStreamHub : Hub<IPriceClient>
{
    private static readonly object SubscriptionsKey = new();

    private readonly ILogger<PriceStreamHub> _logger;
    private readonly SubscriptionPolicy _policy;
    private readonly ILastPriceCache _lastPrices;
    private readonly PriceStreamMetrics _metrics;

    public PriceStreamHub(
        ILogger<PriceStreamHub> logger,
        SubscriptionPolicy policy,
        ILastPriceCache lastPrices,
        PriceStreamMetrics metrics)
    {
        _logger = logger;
        _policy = policy;
        _lastPrices = lastPrices;
        _metrics = metrics;
    }

    /// <summary>
    /// Subscribe to one or more symbols. Each accepted symbol is added to this connection's subscription set.
    /// Call again after reconnect to restore subscriptions (connection state is not shared across pods).
    /// Symbols already subscribed are accepted without another group operation.
    /// Newly added symbols with a cached price get it as a <c>PriceUpdate</c> right away.
    /// </summary>
    public async Task<SubscriptionResult> Subscribe(string[]? symbols)
    {
        symbols ??= [];
        if (!_policy.TryAcquireCall(Context.ConnectionId))
            return RateLimited(symbols);

        var subscriptions = Subscriptions();
        var accepted = new List<string>();
        var added = new List<string>();
        var rejected = new List<RejectedSymbol>();

        foreach (var raw in symbols)
        {
            var symbol = _policy.Normalize(raw, out var rejection);
            if (symbol is null)
            {
                rejected.Add(new RejectedSymbol(_policy.Echo(raw), rejection!));
                continue;
            }

            if (accepted.Contains(symbol))
                continue;

            if (subscriptions.Contains(symbol))
            {
                accepted.Add(symbol);
                continue;
            }

            if (subscriptions.Count >= _policy.MaxSymbolsPerConnection)
            {
                rejected.Add(new RejectedSymbol(symbol, SubscriptionRejection.LimitReached));
                continue;
            }

            subscriptions.Add(symbol);
            accepted.Add(symbol);
            added.Add(symbol);
        }

        try
        {
            await Task.WhenAll(added.Select(symbol =>
                Groups.AddToGroupAsync(Context.ConnectionId, SymbolGroup(symbol), Context.ConnectionAborted)));
        }
        catch
        {
            subscriptions.ExceptWith(added);
            throw;
        }

        foreach (var symbol in added)
            _metrics.SubscriptionAdded(symbol);

        // Read after joining the groups so no tick falls between snapshot and stream. A live tick can
        // still reach the client before an older snapshot; clients drop ticks older than the latest seen.
        await SendSnapshotAsync(added);

        _logger.LogDebug(
            "Connection {ConnectionId} subscribed to {Accepted}; rejected {RejectedCount}",
            Context.ConnectionId, string.Join(", ", accepted), rejected.Count);
        return new SubscriptionResult(accepted, rejected);
    }

    /// <summary>
    /// Unsubscribe from the given symbols. Symbols this connection is not subscribed to are rejected
    /// without a group operation.
    /// </summary>
    public async Task<SubscriptionResult> Unsubscribe(string[]? symbols)
    {
        symbols ??= [];
        if (!_policy.TryAcquireCall(Context.ConnectionId))
            return RateLimited(symbols);

        var subscriptions = Subscriptions();
        var removed = new List<string>();
        var rejected = new List<RejectedSymbol>();

        foreach (var raw in symbols)
        {
            var symbol = _policy.Normalize(raw, out var rejection);
            if (symbol is null)
            {
                rejected.Add(new RejectedSymbol(_policy.Echo(raw), rejection!));
                continue;
            }

            if (removed.Contains(symbol))
                continue;

            if (!subscriptions.Remove(symbol))
            {
                rejected.Add(new RejectedSymbol(symbol, SubscriptionRejection.NotSubscribed));
                continue;
            }

            removed.Add(symbol);
            _metrics.SubscriptionRemoved(symbol);
        }

        await Task.WhenAll(removed.Select(symbol =>
            Groups.RemoveFromGroupAsync(Context.ConnectionId, SymbolGroup(symbol), Context.ConnectionAborted)));

        return new SubscriptionResult(removed, rejected);
    }

    public override Task OnConnectedAsync()
    {
        _logger.LogInformation("Client connected: {ConnectionId}", Context.ConnectionId);
        return Task.CompletedTask;
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation("Client disconnected: {ConnectionId}, Reason: {Reason}", Context.ConnectionId, exception?.Message ?? "normal");
        // SignalR removes the connection from its groups; only the gauge needs updating.
        foreach (var symbol in Subscriptions())
            _metrics.SubscriptionRemoved(symbol);
        return Task.CompletedTask;
    }

    internal static string SymbolGroup(string symbol) => $"symbol:{symbol}";

    private async Task SendSnapshotAsync(IReadOnlyCollection<string> symbols)
    {
        if (symbols.Count == 0)
            return;

        try
        {
            var snapshot = await _lastPrices.GetAsync(symbols);
            foreach (var price in snapshot)
                await Clients.Caller.PriceUpdate(price);
        }
        catch (OperationCanceledException) when (Context.ConnectionAborted.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            // The subscription stands; the client gets the next live tick.
            _logger.LogWarning(ex, "Failed to send price snapshot to {ConnectionId}", Context.ConnectionId);
        }
    }

    // Not synchronized: SignalR runs one hub invocation per connection at a time
    // (MaximumParallelInvocationsPerClient defaults to 1).
    private HashSet<string> Subscriptions()
    {
        if (Context.Items.TryGetValue(SubscriptionsKey, out var existing) && existing is HashSet<string> set)
            return set;

        set = new HashSet<string>(StringComparer.Ordinal);
        Context.Items[SubscriptionsKey] = set;
        return set;
    }

    private SubscriptionResult RateLimited(string[] symbols)
    {
        _logger.LogWarning("Connection {ConnectionId} exceeded the subscription rate limit", Context.ConnectionId);
        var rejected = symbols
            .Take(_policy.MaxSymbolsPerConnection)
            .Select(raw => new RejectedSymbol(_policy.Echo(raw), SubscriptionRejection.RateLimited))
            .ToArray();
        return new SubscriptionResult([], rejected);
    }
}
