using Microsoft.AspNetCore.SignalR;
using PriceStream.Services;

namespace PriceStream.Hubs;

public sealed class PriceStreamHub : Hub<IPriceClient>
{
    private static readonly object SubscriptionsKey = new();

    private readonly ILogger<PriceStreamHub> _logger;
    private readonly SubscriptionPolicy _policy;
    private readonly ILastPriceCache _lastPrices;
    private readonly PriceSubscribers _subscribers;

    public PriceStreamHub(
        ILogger<PriceStreamHub> logger,
        SubscriptionPolicy policy,
        ILastPriceCache lastPrices,
        PriceSubscribers subscribers)
    {
        _logger = logger;
        _policy = policy;
        _lastPrices = lastPrices;
        _subscribers = subscribers;
    }

    /// <summary>
    /// Subscribe to one or more symbols. Each accepted symbol is added to this connection's subscription set.
    /// Call again after reconnect to restore subscriptions (connection state is not shared across pods).
    /// Symbols already subscribed are accepted again without another snapshot.
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

        foreach (var symbol in added)
            _subscribers.Subscribe(Context.ConnectionId, symbol);

        // Read after subscribing so no tick falls between snapshot and stream. The snapshot goes through
        // the same outbox as live ticks, so it never overtakes a newer one.
        await QueueSnapshotAsync(added);

        _logger.LogDebug(
            "Connection {ConnectionId} subscribed to {Accepted}; rejected {RejectedCount}",
            Context.ConnectionId, string.Join(", ", accepted), rejected.Count);
        return new SubscriptionResult(accepted, rejected);
    }

    /// <summary>
    /// Unsubscribe from the given symbols. Symbols this connection is not subscribed to are rejected.
    /// A tick already queued for a removed symbol may still arrive.
    /// </summary>
    public SubscriptionResult Unsubscribe(string[]? symbols)
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
            _subscribers.Unsubscribe(Context.ConnectionId, symbol);
        }

        return new SubscriptionResult(removed, rejected);
    }

    public override Task OnConnectedAsync()
    {
        _subscribers.Connect(Context.ConnectionId);
        _logger.LogInformation("Client connected: {ConnectionId}", Context.ConnectionId);
        return Task.CompletedTask;
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        _subscribers.Disconnect(Context.ConnectionId);
        _logger.LogInformation("Client disconnected: {ConnectionId}, Reason: {Reason}", Context.ConnectionId, exception?.Message ?? "normal");
        return Task.CompletedTask;
    }

    private async Task QueueSnapshotAsync(IReadOnlyCollection<string> symbols)
    {
        if (symbols.Count == 0)
            return;

        try
        {
            foreach (var price in await _lastPrices.GetAsync(symbols))
                _subscribers.SendSnapshot(Context.ConnectionId, new PriceTick(price, KafkaTimestamp: null));
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
