using System.Threading.Tasks.Dataflow;
using Microsoft.AspNetCore.SignalR;
using PriceStream.Hubs;

namespace PriceStream.Services;

/// <summary>
/// Consumes from the EPEX dataflow pipeline and broadcasts each message to SignalR groups by symbol.
/// Ticks for one symbol stay on a single lane so a slower send cannot finish after a newer one.
/// A faulted block fails this service so the host exits and Kubernetes restarts the pod.
/// Shutdown completes the pipeline and drains queued broadcasts.
/// </summary>
public sealed class SignalRPriceBroadcaster : BackgroundService
{
    /// <summary>
    /// How long shutdown waits for queued broadcasts to finish before dropping the rest.
    /// Bounded by the host shutdown token when that fires sooner.
    /// </summary>
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Parallel sends are split by symbol so one symbol is never in flight on two lanes.
    /// </summary>
    private const int LaneCount = 4;

    private readonly IHubContext<PriceStreamHub, IPriceClient> _hubContext;
    private readonly EpexDataflowPipeline _pipeline;
    private readonly ILastPriceCache _lastPrices;
    private readonly SubscriptionPolicy _policy;
    private readonly PriceStreamMetrics _metrics;
    private readonly ILogger<SignalRPriceBroadcaster> _logger;
    private readonly ActionBlock<PriceTick> _targetBlock;
    private readonly ActionBlock<PriceTick>[] _lanes;
    private readonly TaskCompletionSource _faulted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly IDisposable _link;

    public SignalRPriceBroadcaster(
        IHubContext<PriceStreamHub, IPriceClient> hubContext,
        EpexDataflowPipeline pipeline,
        ILastPriceCache lastPrices,
        SubscriptionPolicy policy,
        PriceStreamMetrics metrics,
        ILogger<SignalRPriceBroadcaster> logger)
    {
        _hubContext = hubContext;
        _pipeline = pipeline;
        _lastPrices = lastPrices;
        _policy = policy;
        _metrics = metrics;
        _logger = logger;
        _lanes = new ActionBlock<PriceTick>[LaneCount];
        for (var i = 0; i < _lanes.Length; i++)
        {
            var lane = new ActionBlock<PriceTick>(
                OnMessageAsync,
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 1,
                    BoundedCapacity = 500,
                    EnsureOrdered = true
                });
            _lanes[i] = lane;
            WatchFault(lane.Completion);
            metrics.ObserveQueue($"lane-{i}", () => lane.InputCount);
        }

        // Single reader into the lanes, so two ticks for one symbol are queued in arrival order.
        _targetBlock = new ActionBlock<PriceTick>(
            RouteAsync,
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 500,
                EnsureOrdered = true
            });
        _targetBlock.Completion.ContinueWith(
            static (completed, state) => CompleteLanes((ActionBlock<PriceTick>[])state!, completed),
            _lanes,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        WatchFault(_targetBlock.Completion);
        metrics.ObserveQueue("queue", () => _pipeline.Count);
        metrics.ObserveQueue("router", () => _targetBlock.InputCount);
        // Linked up front so shutdown can propagate completion into this block even if ExecuteAsync has not started.
        _link = _pipeline.LinkTo(_targetBlock);
    }

    /// <summary>
    /// True when the router or any symbol lane has faulted. A faulted block does not recover.
    /// </summary>
    public bool IsFaulted =>
        _targetBlock.Completion.IsFaulted || Array.Exists(_lanes, static lane => lane.Completion.IsFaulted);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // A faulted router or lane completes this task. The host stops, and Kubernetes restarts the pod.
            await _faulted.Task.WaitAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Price pipeline faulted; stopping the host so the pod restarts");
            throw;
        }
    }

    /// <summary>
    /// Completes the pipeline and waits for queued broadcasts to drain.
    /// </summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _pipeline.Complete();

        using var drain = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        drain.CancelAfter(DrainTimeout);
        try
        {
            await Task.WhenAll(_lanes.Append(_targetBlock).Select(static block => block.Completion)).WaitAsync(drain.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning(
                "Price pipeline did not drain within {DrainTimeout}; remaining messages will be dropped",
                DrainTimeout);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Price pipeline faulted while draining");
        }

        await base.StopAsync(cancellationToken);
    }

    public override void Dispose()
    {
        _link.Dispose();
        base.Dispose();
    }

    private void WatchFault(Task completion) =>
        _ = completion.ContinueWith(
            completed => _faulted.TrySetException(completed.Exception!.GetBaseException()),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    private static void CompleteLanes(ActionBlock<PriceTick>[] lanes, Task routerCompletion)
    {
        var error = routerCompletion.IsFaulted ? routerCompletion.Exception?.GetBaseException() : null;
        foreach (var lane in lanes)
        {
            try
            {
                if (error is null)
                    lane.Complete();
                else
                    ((IDataflowBlock)lane).Fault(error);
            }
            catch (InvalidOperationException)
            {
                // Lane already completed or faulted.
            }
        }
    }

    private static int LaneIndex(string symbol)
    {
        var hash = StringComparer.Ordinal.GetHashCode(symbol);
        return (hash & int.MaxValue) % LaneCount;
    }

    private Task RouteAsync(PriceTick tick)
    {
        try
        {
            // The parser rejects messages without a symbol. This guards code that posts objects
            // directly: a throw here would fault the router and stop broadcasting.
            if (string.IsNullOrEmpty(tick.Data.Symbol))
            {
                _logger.LogWarning("Dropping price update with no symbol");
                _metrics.MessageDropped(DropReason.NoSymbol);
                return Task.CompletedTask;
            }

            return _lanes[LaneIndex(tick.Data.Symbol)].SendAsync(tick);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to route price update");
            return Task.CompletedTask;
        }
    }

    private async Task OnMessageAsync(PriceTick tick)
    {
        string? group = null;
        try
        {
            // Nothing may escape this delegate: a thrown ActionBlock stays faulted and stops broadcasting.
            // Symbol is already trimmed and upper-case (EpexMessageParser) and non-empty (RouteAsync).
            var message = tick.Data;
            var symbol = message.Symbol;
            group = PriceStreamHub.SymbolGroup(symbol);

            // Only symbols a client could subscribe to are cached, so Kafka cannot grow it past the allow-list.
            if (_policy.IsAllowed(symbol))
            {
                try
                {
                    await _lastPrices.SetAsync(symbol, message);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to cache last price for {Symbol}", symbol);
                }
            }

            await _hubContext.Clients.Group(group).PriceUpdate(message);
            if (tick.KafkaTimestamp is { } kafkaTimestamp)
                _metrics.Published(kafkaTimestamp);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to broadcast to group {Group}", group);
            _metrics.MessageDropped(DropReason.SendFailed);
        }
    }
}
