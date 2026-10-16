using System.Threading.Tasks.Dataflow;
using PriceStream.Hubs;

namespace PriceStream.Services;

/// <summary>
/// Consumes from the EPEX dataflow pipeline, updates the last-price cache and hands each tick to the
/// subscribers of its symbol. Handing off only queues into per-connection outboxes
/// (<see cref="PriceSubscribers"/>), so a client that stops reading cannot hold up this block.
/// A faulted block fails this service so the host exits and Kubernetes restarts the pod.
/// Shutdown completes the pipeline and drains queued ticks.
/// </summary>
public sealed class SignalRPriceBroadcaster : BackgroundService
{
    /// <summary>
    /// How long shutdown waits for queued ticks to be handed off before dropping the rest.
    /// Bounded by the host shutdown token when that fires sooner.
    /// </summary>
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(10);

    private readonly EpexDataflowPipeline _pipeline;
    private readonly ILastPriceCache _lastPrices;
    private readonly SubscriptionPolicy _policy;
    private readonly PriceSubscribers _subscribers;
    private readonly PriceStreamMetrics _metrics;
    private readonly ILogger<SignalRPriceBroadcaster> _logger;
    private readonly ActionBlock<PriceTick> _targetBlock;
    private readonly TaskCompletionSource _faulted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly IDisposable _link;

    public SignalRPriceBroadcaster(
        EpexDataflowPipeline pipeline,
        ILastPriceCache lastPrices,
        SubscriptionPolicy policy,
        PriceSubscribers subscribers,
        PriceStreamMetrics metrics,
        ILogger<SignalRPriceBroadcaster> logger)
    {
        _pipeline = pipeline;
        _lastPrices = lastPrices;
        _policy = policy;
        _subscribers = subscribers;
        _metrics = metrics;
        _logger = logger;

        // One reader: ticks for a symbol reach the outboxes in arrival order.
        _targetBlock = new ActionBlock<PriceTick>(
            OnMessageAsync,
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 1,
                BoundedCapacity = 500,
                EnsureOrdered = true
            });
        _ = _targetBlock.Completion.ContinueWith(
            completed => _faulted.TrySetException(completed.Exception!.GetBaseException()),
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        metrics.ObserveQueue("queue", () => _pipeline.Count);
        metrics.ObserveQueue("fanout", () => _targetBlock.InputCount);
        // Linked up front so shutdown can propagate completion into this block even if ExecuteAsync has not started.
        _link = _pipeline.LinkTo(_targetBlock);
    }

    /// <summary>
    /// True when the fan-out block has faulted. A faulted block does not recover.
    /// </summary>
    public bool IsFaulted => _targetBlock.Completion.IsFaulted;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // A faulted block completes this task. The host stops, and Kubernetes restarts the pod.
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
    /// Completes the pipeline and waits for queued ticks to be handed off.
    /// </summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _pipeline.Complete();

        using var drain = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        drain.CancelAfter(DrainTimeout);
        try
        {
            await _targetBlock.Completion.WaitAsync(drain.Token);
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

    private async Task OnMessageAsync(PriceTick tick)
    {
        try
        {
            // Nothing may escape this delegate: a thrown ActionBlock stays faulted and stops broadcasting.
            // Symbol is already trimmed and upper-case (EpexMessageParser). This guards code that posts
            // objects directly.
            var message = tick.Data;
            var symbol = message.Symbol;
            if (string.IsNullOrEmpty(symbol))
            {
                _logger.LogWarning("Dropping price update with no symbol");
                _metrics.MessageDropped(DropReason.NoSymbol);
                return;
            }

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

            _subscribers.Publish(tick);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to hand off price update for {Symbol}", tick.Data.Symbol);
        }
    }
}
