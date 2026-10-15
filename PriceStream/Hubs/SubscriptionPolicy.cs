using System.Text.RegularExpressions;
using System.Threading.RateLimiting;
using Microsoft.Extensions.Options;

namespace PriceStream.Hubs;

public sealed class SubscriptionOptions
{
    public const string SectionName = "Subscriptions";

    /// <summary>
    /// Known symbols. When non-empty, only these are accepted and <see cref="SymbolPattern"/> is still applied.
    /// </summary>
    public string[] AllowedSymbols { get; set; } = [];

    /// <summary>Applied to the trimmed, upper-cased symbol.</summary>
    public string SymbolPattern { get; set; } = "^[A-Z0-9][A-Z0-9_-]*$";

    public int MaxSymbolLength { get; set; } = 16;

    public int MaxSymbolsPerConnection { get; set; } = 50;

    /// <summary>Subscribe and Unsubscribe calls share this token bucket per connection.</summary>
    public int CallsPerWindow { get; set; } = 10;

    public int WindowSeconds { get; set; } = 10;
}

public static class SubscriptionRejection
{
    public const string Invalid = "invalid";
    public const string NotAllowed = "not_allowed";
    public const string LimitReached = "limit_reached";
    public const string RateLimited = "rate_limited";
    public const string NotSubscribed = "not_subscribed";
}

public sealed record RejectedSymbol(string Symbol, string Reason);

public sealed record SubscriptionResult(IReadOnlyList<string> Accepted, IReadOnlyList<RejectedSymbol> Rejected);

/// <summary>
/// Validates client-supplied symbols and rate limits subscription calls per connection.
/// </summary>
public sealed class SubscriptionPolicy : IDisposable
{
    private readonly Regex _pattern;
    private readonly HashSet<string> _allowed;
    private readonly PartitionedRateLimiter<string> _limiter;

    public SubscriptionPolicy(IOptions<SubscriptionOptions> options)
    {
        var value = options.Value;
        if (value.MaxSymbolLength <= 0)
            throw new OptionsValidationException(SubscriptionOptions.SectionName, typeof(SubscriptionOptions), ["MaxSymbolLength must be positive."]);
        if (value.MaxSymbolsPerConnection <= 0)
            throw new OptionsValidationException(SubscriptionOptions.SectionName, typeof(SubscriptionOptions), ["MaxSymbolsPerConnection must be positive."]);
        if (value.CallsPerWindow <= 0 || value.WindowSeconds <= 0)
            throw new OptionsValidationException(SubscriptionOptions.SectionName, typeof(SubscriptionOptions), ["CallsPerWindow and WindowSeconds must be positive."]);

        MaxSymbolLength = value.MaxSymbolLength;
        MaxSymbolsPerConnection = value.MaxSymbolsPerConnection;
        _pattern = new Regex(value.SymbolPattern, RegexOptions.CultureInvariant, TimeSpan.FromMilliseconds(50));
        _allowed = new HashSet<string>(
            value.AllowedSymbols.Where(s => !string.IsNullOrWhiteSpace(s)).Select(s => s.Trim().ToUpperInvariant()),
            StringComparer.Ordinal);

        var bucket = new TokenBucketRateLimiterOptions
        {
            TokenLimit = value.CallsPerWindow,
            TokensPerPeriod = value.CallsPerWindow,
            ReplenishmentPeriod = TimeSpan.FromSeconds(value.WindowSeconds),
            QueueLimit = 0,
            AutoReplenishment = true
        };
        // One limiter for all connections; idle partitions are evicted, so disconnected clients are not retained.
        _limiter = PartitionedRateLimiter.Create<string, string>(
            connectionId => RateLimitPartition.GetTokenBucketLimiter(connectionId, _ => bucket));
    }

    public int MaxSymbolLength { get; }

    public int MaxSymbolsPerConnection { get; }

    public bool TryAcquireCall(string connectionId)
    {
        using var lease = _limiter.AttemptAcquire(connectionId);
        return lease.IsAcquired;
    }

    /// <summary>
    /// Returns the normalized symbol, or null with a rejection reason.
    /// </summary>
    public string? Normalize(string? raw, out string? rejection)
    {
        var symbol = raw?.Trim().ToUpperInvariant() ?? string.Empty;
        if (symbol.Length == 0 || symbol.Length > MaxSymbolLength || !Matches(symbol))
        {
            rejection = SubscriptionRejection.Invalid;
            return null;
        }

        if (_allowed.Count > 0 && !_allowed.Contains(symbol))
        {
            rejection = SubscriptionRejection.NotAllowed;
            return null;
        }

        rejection = null;
        return symbol;
    }

    /// <summary>
    /// Echoed back in <see cref="RejectedSymbol"/>, cut to the maximum length so a rejection cannot be larger than the input limit allows.
    /// </summary>
    public string Echo(string? raw)
    {
        var value = raw ?? string.Empty;
        return value.Length <= MaxSymbolLength ? value : value[..MaxSymbolLength];
    }

    public void Dispose() => _limiter.Dispose();

    private bool Matches(string symbol)
    {
        try
        {
            return _pattern.IsMatch(symbol);
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
    }
}
