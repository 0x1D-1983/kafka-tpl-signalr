using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR;
using PriceStream.Hubs;

namespace PriceStream.Services;

/// <summary>
/// Which connections on this pod are subscribed to which symbol, and one <see cref="ClientOutbox"/>
/// per connection. Replaces SignalR groups: a group send completes only when every member's write
/// completes, so one client that stops reading would stall delivery to everyone.
/// </summary>
public sealed class PriceSubscribers
{
    private readonly IHubContext<PriceStreamHub, IPriceClient> _hubContext;
    private readonly PriceStreamMetrics _metrics;
    private readonly ILogger<ClientOutbox> _outboxLogger;
    private readonly ConcurrentDictionary<string, ClientOutbox> _outboxes = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, ClientOutbox>> _bySymbol = new(StringComparer.Ordinal);
    // Subscribe and unsubscribe are rare; the lock keeps "remove the symbol once empty" race-free.
    // Publish only reads.
    private readonly Lock _gate = new();

    public PriceSubscribers(
        IHubContext<PriceStreamHub, IPriceClient> hubContext,
        PriceStreamMetrics metrics,
        ILogger<ClientOutbox> outboxLogger)
    {
        _hubContext = hubContext;
        _metrics = metrics;
        _outboxLogger = outboxLogger;
        metrics.ObserveSubscriptions(() => _bySymbol.Select(s => KeyValuePair.Create(s.Key, s.Value.Count)));
        metrics.ObserveQueue("outboxes", () => _outboxes.Values.Sum(static outbox => outbox.Pending));
    }

    public void Connect(string connectionId) =>
        _outboxes.GetOrAdd(connectionId, id => new ClientOutbox(_hubContext.Clients.Client(id), _metrics, _outboxLogger, id));

    public void Disconnect(string connectionId)
    {
        ClientOutbox? outbox;
        lock (_gate)
        {
            if (!_outboxes.TryRemove(connectionId, out outbox))
                return;

            foreach (var (symbol, subscribers) in _bySymbol)
            {
                if (subscribers.TryRemove(connectionId, out _) && subscribers.IsEmpty)
                    _bySymbol.TryRemove(symbol, out _);
            }
        }

        outbox.Stop();
    }

    public void Subscribe(string connectionId, string symbol)
    {
        // Under the lock, so a concurrent Disconnect cannot leave a stopped outbox subscribed.
        lock (_gate)
        {
            if (!_outboxes.TryGetValue(connectionId, out var outbox))
                return;

            _bySymbol.GetOrAdd(symbol, static _ => new ConcurrentDictionary<string, ClientOutbox>(StringComparer.Ordinal))[connectionId] = outbox;
        }
    }

    public void Unsubscribe(string connectionId, string symbol)
    {
        lock (_gate)
        {
            if (_bySymbol.TryGetValue(symbol, out var subscribers)
                && subscribers.TryRemove(connectionId, out _)
                && subscribers.IsEmpty)
            {
                _bySymbol.TryRemove(symbol, out _);
            }
        }
    }

    /// <summary>Queues the tick for every subscriber of its symbol. Never waits on a socket.</summary>
    public void Publish(PriceTick tick)
    {
        if (!_bySymbol.TryGetValue(tick.Data.Symbol, out var subscribers))
            return;

        foreach (var outbox in subscribers.Values)
            outbox.Enqueue(tick);
    }

    /// <summary>Queues a cached price for one connection, behind or in place of any live tick already queued.</summary>
    public void SendSnapshot(string connectionId, PriceTick tick)
    {
        if (_outboxes.TryGetValue(connectionId, out var outbox))
            outbox.Enqueue(tick);
    }
}

/// <summary>
/// Pending ticks for one connection, at most one per symbol, sent by a single loop. While the client
/// is slow a newer tick replaces the queued one, so it falls behind only on its own prices and memory
/// stays bounded by its subscription count. A client that stops reading is disconnected by SignalR's
/// transport send timeout.
/// </summary>
public sealed class ClientOutbox
{
    private readonly IPriceClient _client;
    private readonly PriceStreamMetrics _metrics;
    private readonly ILogger<ClientOutbox> _logger;
    private readonly string _connectionId;
    private readonly Dictionary<string, PriceTick> _pending = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();
    private readonly Channel<bool> _signal = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite, SingleReader = true });
    private readonly CancellationTokenSource _stop = new();

    internal ClientOutbox(IPriceClient client, PriceStreamMetrics metrics, ILogger<ClientOutbox> logger, string connectionId)
    {
        _client = client;
        _metrics = metrics;
        _logger = logger;
        _connectionId = connectionId;
        _ = Task.Run(SendLoopAsync);
    }

    public int Pending
    {
        get { lock (_gate) return _pending.Count; }
    }

    public void Enqueue(PriceTick tick)
    {
        lock (_gate)
        {
            if (_pending.TryGetValue(tick.Data.Symbol, out var queued))
            {
                _metrics.TickSuperseded();
                // A snapshot read from the cache can be older than a live tick already queued.
                if (queued.Data.Timestamp > tick.Data.Timestamp)
                    return;
            }

            _pending[tick.Data.Symbol] = tick;
        }

        _signal.Writer.TryWrite(true);
    }

    public void Stop()
    {
        _stop.Cancel();
        _signal.Writer.TryComplete();
    }

    private async Task SendLoopAsync()
    {
        var batch = new List<PriceTick>();
        try
        {
            while (await _signal.Reader.WaitToReadAsync(_stop.Token))
            {
                _signal.Reader.TryRead(out _);
                lock (_gate)
                {
                    batch.AddRange(_pending.Values);
                    _pending.Clear();
                }

                foreach (var tick in batch)
                {
                    if (_stop.IsCancellationRequested)
                        return;
                    try
                    {
                        // Waits for this connection's write only. Completes (or throws) once SignalR
                        // closes a connection that stopped reading.
                        await _client.PriceUpdate(tick.Data);
                        if (tick.KafkaTimestamp is { } kafkaTimestamp)
                            _metrics.Published(kafkaTimestamp);
                    }
                    catch (Exception ex)
                    {
                        _metrics.MessageDropped(DropReason.SendFailed);
                        _logger.LogWarning(ex, "Failed to send {Symbol} to {ConnectionId}", tick.Data.Symbol, _connectionId);
                    }
                }

                batch.Clear();
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
        }
    }
}
