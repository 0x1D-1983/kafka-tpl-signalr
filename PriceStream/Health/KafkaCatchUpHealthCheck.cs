using Microsoft.Extensions.Diagnostics.HealthChecks;
using PriceStream.Services;

namespace PriceStream.Health;

/// <summary>
/// Readiness: the startup replay has filled the last-price cache, so a new subscriber gets a snapshot.
/// </summary>
public sealed class KafkaCatchUpHealthCheck : IHealthCheck
{
    private readonly KafkaCatchUpState _state;

    public KafkaCatchUpHealthCheck(KafkaCatchUpState state) => _state = state;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        Task.FromResult(_state.IsCaughtUp
            ? HealthCheckResult.Healthy("Caught up with Kafka")
            : HealthCheckResult.Unhealthy("Replaying Kafka into the last-price cache"));
}
