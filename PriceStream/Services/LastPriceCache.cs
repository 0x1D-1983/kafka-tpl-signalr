using System.Collections.Concurrent;
using PriceStream.Models;

namespace PriceStream.Services;

/// <summary>
/// Latest price per instrument (<see cref="InstrumentKey"/>), sent to a connection when it subscribes
/// so it does not wait for the next tick. A value never replaces one for the same instrument with a
/// newer source timestamp.
/// </summary>
public interface ILastPriceCache
{
    /// <param name="value">Symbol already normalized (trimmed, upper-case).</param>
    Task SetAsync(EpexMarketData value);

    /// <param name="symbols">Normalized symbols.</param>
    /// <returns>Every cached instrument of those symbols, oldest source timestamp first.</returns>
    Task<IReadOnlyList<EpexMarketData>> GetAsync(IReadOnlyCollection<string> symbols);
}

/// <summary>
/// Per-pod cache. Every pod reads the whole topic, so each one holds every instrument.
/// An instrument this pod has not received for <c>retention</c> is evicted, so contracts that
/// stopped trading do not accumulate. Measured on the pod's clock, not the payload timestamp.
/// </summary>
public sealed class InMemoryLastPriceCache(TimeSpan retention, TimeProvider timeProvider) : ILastPriceCache
{
    public static readonly TimeSpan DefaultRetention = TimeSpan.FromHours(48);
    private static readonly TimeSpan SweepInterval = TimeSpan.FromMinutes(1);

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<InstrumentKey, Entry>> _bySymbol = new(StringComparer.Ordinal);
    private long _nextSweepTicks;

    public InMemoryLastPriceCache()
        : this(DefaultRetention, TimeProvider.System)
    {
    }

    public Task SetAsync(EpexMarketData value)
    {
        var now = timeProvider.GetUtcNow();
        var instruments = _bySymbol.GetOrAdd(value.Symbol, static _ => new ConcurrentDictionary<InstrumentKey, Entry>());
        instruments.AddOrUpdate(
            InstrumentKey.Of(value),
            static (_, incoming) => incoming,
            // A late, older tick still counts as activity for the instrument.
            static (_, current, incoming) => incoming.Value.Timestamp >= current.Value.Timestamp ? incoming : current with { ReceivedAt = incoming.ReceivedAt },
            new Entry(value, now));
        SweepIfDue(now);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<EpexMarketData>> GetAsync(IReadOnlyCollection<string> symbols)
    {
        var cutoff = timeProvider.GetUtcNow() - retention;
        var found = new List<EpexMarketData>();
        foreach (var symbol in symbols)
        {
            if (!_bySymbol.TryGetValue(symbol, out var instruments))
                continue;

            foreach (var entry in instruments.Values)
            {
                if (entry.ReceivedAt >= cutoff)
                    found.Add(entry.Value);
            }
        }

        found.Sort(static (a, b) => a.Timestamp.CompareTo(b.Timestamp));
        return Task.FromResult<IReadOnlyList<EpexMarketData>>(found);
    }

    private void SweepIfDue(DateTimeOffset now)
    {
        var due = Interlocked.Read(ref _nextSweepTicks);
        if (now.UtcTicks < due || Interlocked.CompareExchange(ref _nextSweepTicks, (now + SweepInterval).UtcTicks, due) != due)
            return;

        var cutoff = now - retention;
        foreach (var (_, instruments) in _bySymbol)
        {
            foreach (var entry in instruments)
            {
                if (entry.Value.ReceivedAt < cutoff)
                    instruments.TryRemove(entry);
            }
        }
    }

    private sealed record Entry(EpexMarketData Value, DateTimeOffset ReceivedAt);
}
