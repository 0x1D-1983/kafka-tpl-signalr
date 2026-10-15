using Microsoft.Extensions.Diagnostics.HealthChecks;
using PriceStream.Services;

namespace PriceStream.Health;

/// <summary>
/// Diagnostic only (served on /health/kafka, not part of readiness or liveness). Reports what the
/// consumer last saw instead of opening a new broker connection per probe.
/// </summary>
public sealed class KafkaHealthCheck : IHealthCheck
{
    private readonly KafkaConnectionState _state;

    public KafkaHealthCheck(KafkaConnectionState state) => _state = state;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var (lastContact, disconnectedSince, lastError) = _state.Snapshot();
        var data = new Dictionary<string, object>();
        if (lastContact is { } contact)
            data["lastContact"] = contact;
        if (disconnectedSince is { } since)
            data["disconnectedSince"] = since;
        if (lastError is not null)
            data["lastError"] = lastError;

        var result = (lastContact, disconnectedSince) switch
        {
            (null, _) => HealthCheckResult.Degraded("No contact with Kafka yet", data: data),
            (_, { } down) => HealthCheckResult.Degraded($"All Kafka brokers down since {down:O}", data: data),
            _ => HealthCheckResult.Healthy("Kafka reachable", data),
        };
        return Task.FromResult(result);
    }
}
