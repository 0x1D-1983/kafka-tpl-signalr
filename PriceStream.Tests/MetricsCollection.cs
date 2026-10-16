using Xunit;

namespace PriceStream.Tests;

/// <summary>
/// OpenTelemetry listens to meters by name across the whole process, so a host's /metrics also
/// reports instruments from other hosts running in parallel. Tests that assert on metric values
/// run one at a time.
/// </summary>
[CollectionDefinition(Name)]
public sealed class MetricsCollection
{
    public const string Name = "Metrics";
}
