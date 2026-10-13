using System.Threading.Tasks.Dataflow;
using Microsoft.AspNetCore.SignalR;
using PriceStream.Hubs;
using PriceStream.Models;

namespace PriceStream.Services;

/// <summary>
/// Consumes from the EPEX dataflow pipeline and broadcasts each message to SignalR groups by symbol.
/// </summary>
public sealed class SignalRPriceBroadcaster : BackgroundService
{
    private readonly IHubContext<PriceStreamHub> _hubContext;
    private readonly EpexDataflowPipeline _pipeline;
    private readonly ILogger<SignalRPriceBroadcaster> _logger;
    private readonly ActionBlock<EpexMarketData> _targetBlock;
    private IDisposable? _link;

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
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _link = _pipeline.LinkTo(_targetBlock);
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    public override void Dispose()
    {
        _link?.Dispose();
        base.Dispose();
    }

    private async Task OnMessageAsync(EpexMarketData message)
    {
        var group = PriceStreamHub.SymbolGroup(message.Symbol.ToUpperInvariant());
        try
        {
            await _hubContext.Clients.Group(group).SendAsync("PriceUpdate", message, CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to broadcast to group {Group}", group);
        }
    }
}
