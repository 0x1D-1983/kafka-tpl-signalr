using System.Threading.Tasks.Dataflow;
using Microsoft.AspNetCore.SignalR;
using PriceStream.Hubs;
using PriceStream.Models;

namespace PriceStream.Services;

/// <summary>
/// Consumes from the EPEX dataflow pipeline and broadcasts each message to SignalR groups by symbol.
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

    private readonly IHubContext<PriceStreamHub> _hubContext;
    private readonly EpexDataflowPipeline _pipeline;
    private readonly ILogger<SignalRPriceBroadcaster> _logger;
    private readonly ActionBlock<EpexMarketData> _targetBlock;
    private readonly IDisposable _link;

    public SignalRPriceBroadcaster(
        IHubContext<PriceStreamHub> hubContext,
        EpexDataflowPipeline pipeline,
        ILogger<SignalRPriceBroadcaster> logger)
    {
        _hubContext = hubContext;
        _pipeline = pipeline;
        _logger = logger;
        _targetBlock = new ActionBlock<EpexMarketData>(
            OnMessageAsync,
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = 4,
                BoundedCapacity = 500
            });
        // Linked up front so shutdown can propagate completion into this block even if ExecuteAsync has not started.
        _link = _pipeline.LinkTo(_targetBlock);
        ObserveFault(_targetBlock.Completion);
    }

    /// <summary>
    /// True when the SignalR action block has faulted. A faulted block does not recover.
    /// </summary>
    public bool IsFaulted => _targetBlock.Completion.IsFaulted;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // A faulted completion throws here. The host stops, and Kubernetes restarts the pod.
            await _targetBlock.Completion.WaitAsync(stoppingToken);
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

    private static void ObserveFault(Task completion) =>
        _ = completion.ContinueWith(
            static completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    private async Task OnMessageAsync(EpexMarketData message)
    {
        string? group = null;
        try
        {
            // System.Text.Json leaves Symbol null when the JSON value is null. That must not
            // escape this delegate: a thrown ActionBlock stays faulted and stops broadcasting.
            var symbol = message.Symbol;
            if (string.IsNullOrWhiteSpace(symbol))
            {
                _logger.LogWarning("Dropping price update with no symbol");
                return;
            }

            group = PriceStreamHub.SymbolGroup(symbol.ToUpperInvariant());
            await _hubContext.Clients.Group(group).SendAsync("PriceUpdate", message, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to broadcast to group {Group}", group);
        }
    }
}
