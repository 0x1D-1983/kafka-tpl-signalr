using Microsoft.Extensions.Diagnostics.HealthChecks;
using PriceStream.Services;

namespace PriceStream.Health;

/// <summary>
/// Liveness and readiness: a faulted dataflow block does not recover, so the pod must
/// leave the load balancer and be restarted.
/// </summary>
public sealed class PipelineHealthCheck : IHealthCheck
{
    private readonly EpexDataflowPipeline _pipeline;
    private readonly SignalRPriceBroadcaster _broadcaster;

    public PipelineHealthCheck(EpexDataflowPipeline pipeline, SignalRPriceBroadcaster broadcaster)
    {
        _pipeline = pipeline;
        _broadcaster = broadcaster;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var faulted = _pipeline.FaultedBlocks;
        var targetFaulted = _broadcaster.IsFaulted;
        if (faulted.Count == 0 && !targetFaulted)
            return Task.FromResult(HealthCheckResult.Healthy("Dataflow pipeline running"));

        var names = targetFaulted ? faulted.Concat(["signalr"]) : faulted;
        return Task.FromResult(HealthCheckResult.Unhealthy(
            $"Dataflow block faulted: {string.Join(", ", names)}"));
    }
}
