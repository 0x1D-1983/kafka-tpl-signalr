using System.Threading.Tasks.Dataflow;
using PriceStream.Models;

namespace PriceStream.Services;

/// <summary>
/// Parses Kafka payloads and queues every accepted update for the linked target.
/// Post returns false once the bounded queue is full. A full SignalR target postpones
/// and the queue retains the message, so one slow symbol cannot overwrite another.
/// </summary>
public sealed class EpexDataflowPipeline
{
    private readonly TransformManyBlock<string, EpexMarketData> _parseBlock;
    private readonly BufferBlock<EpexMarketData> _queue;
    private int _completed;

    public EpexDataflowPipeline()
    {
        _parseBlock = new TransformManyBlock<string, EpexMarketData>(
            ParseEpexMessages,
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded,
                BoundedCapacity = 1000
            });

        // BroadcastBlock keeps a single value and overwrites it when the target postpones.
        // A buffer retains every message and applies backpressure instead.
        _queue = new BufferBlock<EpexMarketData>(new DataflowBlockOptions { BoundedCapacity = 1000 });

        _parseBlock.LinkTo(_queue, new DataflowLinkOptions { PropagateCompletion = true });
        ObserveFault(_parseBlock.Completion);
        ObserveFault(_queue.Completion);
    }

    /// <summary>
    /// Post a raw message (e.g. JSON) from Kafka. Returns true if accepted.
    /// </summary>
    public bool Post(string rawMessage)
    {
        return _parseBlock.Post(rawMessage);
    }

    /// <summary>
    /// Link the target that receives every queued update. Messages posted before the link
    /// stay in the queue. Completion and faults propagate to <paramref name="target"/>.
    /// </summary>
    public IDisposable LinkTo(ITargetBlock<EpexMarketData> target)
    {
        return _queue.LinkTo(target, new DataflowLinkOptions { PropagateCompletion = true });
    }

    /// <summary>
    /// Accepts no further messages. Blocks already queued are still processed, then completion
    /// propagates to linked targets so they can drain.
    /// </summary>
    public void Complete()
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
            return;

        try
        {
            _parseBlock.Complete();
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
    public IReadOnlyList<string> FaultedBlocks
    {
        get
        {
            var parseFaulted = _parseBlock.Completion.IsFaulted;
            var queueFaulted = _queue.Completion.IsFaulted;
            if (!parseFaulted && !queueFaulted)
                return Array.Empty<string>();
            if (parseFaulted && queueFaulted)
                return ["parse", "queue"];
            return parseFaulted ? ["parse"] : ["queue"];
        }
    }

    public Task Completion => _parseBlock.Completion;

    private static void ObserveFault(Task completion) =>
        _ = completion.ContinueWith(
            static completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    private static IEnumerable<EpexMarketData> ParseEpexMessages(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return [];
        try
        {
            var item = System.Text.Json.JsonSerializer.Deserialize<EpexMarketData>(raw);
            return item is not null ? [item] : [];
        }
        catch
        {
            return [];
        }
    }
}
