using System.Threading.Tasks.Dataflow;
using PriceStream.Models;

namespace PriceStream.Services;

/// <summary>
/// A queued update and, when it came from Kafka, the Kafka message timestamp (for publish latency).
/// </summary>
public readonly record struct PriceTick(EpexMarketData Data, DateTimeOffset? KafkaTimestamp);

/// <summary>
/// Queues every accepted update for the linked target. Messages arrive already parsed (see
/// <see cref="EpexMessageParser"/>). Post returns false once the bounded queue is full. A full
/// SignalR target postpones and the queue retains the message, so one slow symbol cannot
/// overwrite another.
/// </summary>
public sealed class EpexDataflowPipeline
{
    // BroadcastBlock keeps a single value and overwrites it when the target postpones.
    // A buffer retains every message and applies backpressure instead.
    private readonly BufferBlock<PriceTick> _queue;
    private int _completed;

    public EpexDataflowPipeline()
    {
        _queue = new BufferBlock<PriceTick>(new DataflowBlockOptions { BoundedCapacity = 1000 });
        ObserveFault(_queue.Completion);
    }

    /// <summary>
    /// Post a parsed message. Returns true if accepted.
    /// </summary>
    public bool Post(EpexMarketData message, DateTimeOffset? kafkaTimestamp = null)
    {
        return _queue.Post(new PriceTick(message, kafkaTimestamp));
    }

    /// <summary>Messages queued and not yet taken by the linked target.</summary>
    public int Count => _queue.Count;

    /// <summary>
    /// Link the target that receives every queued update. Messages posted before the link
    /// stay in the queue. Completion and faults propagate to <paramref name="target"/>.
    /// </summary>
    public IDisposable LinkTo(ITargetBlock<PriceTick> target)
    {
        return _queue.LinkTo(target, new DataflowLinkOptions { PropagateCompletion = true });
    }

    /// <summary>
    /// Accepts no further messages. Messages already queued are still delivered, then completion
    /// propagates to linked targets so they can drain.
    /// </summary>
    public void Complete()
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
            return;

        try
        {
            _queue.Complete();
        }
        catch (InvalidOperationException)
        {
            // Already completed or faulted.
        }
    }

    /// <summary>
    /// Names of pipeline blocks whose completion has faulted. Empty while the pipeline is running
    /// or after a clean completion. A faulted block does not recover.
    /// </summary>
    public IReadOnlyList<string> FaultedBlocks =>
        _queue.Completion.IsFaulted ? ["queue"] : Array.Empty<string>();

    public Task Completion => _queue.Completion;

    private static void ObserveFault(Task completion) =>
        _ = completion.ContinueWith(
            static completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
}
