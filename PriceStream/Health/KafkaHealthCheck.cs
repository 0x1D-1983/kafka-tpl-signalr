using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using PriceStream.Services;

namespace PriceStream.Health;

/// <summary>
/// Readiness check: ensures Kafka brokers are reachable so the pod can receive traffic from the load balancer.
/// </summary>
public sealed class KafkaHealthCheck : IHealthCheck
{
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaHealthCheck> _logger;

    public KafkaHealthCheck(IOptions<KafkaOptions> options, ILogger<KafkaHealthCheck> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _options.BootstrapServers,
                SocketTimeoutMs = 5000
            }).Build();

            var meta = admin.GetMetadata(TimeSpan.FromSeconds(5));
            if (meta.Brokers.Count == 0)
            {
                _logger.LogWarning("Kafka health: no brokers in metadata");
                return Task.FromResult(HealthCheckResult.Unhealthy("No Kafka brokers in metadata"));
            }

            return Task.FromResult(HealthCheckResult.Healthy("Kafka reachable"));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Kafka health check failed");
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka unreachable", ex));
        }
    }
}
