namespace PriceStream.Services;

public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    /// <summary>
    /// Redis connection string (e.g. "localhost:6379" or "redis-server:6379,password=..."). When set, SignalR uses Redis backplane.
    /// </summary>
    public string Configuration { get; set; } = string.Empty;

    /// <summary>
    /// Optional channel prefix for the backplane (isolates multiple apps sharing the same Redis instance).
    /// </summary>
    public string? InstanceName { get; set; }
}
