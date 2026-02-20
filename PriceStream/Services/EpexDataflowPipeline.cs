using System.Threading.Tasks.Dataflow;
using PriceStream.Models;

namespace PriceStream.Services;

/// <summary>
/// TPL Dataflow pipeline: receives raw Kafka messages, parses to EPEX model, and broadcasts to linked blocks.
/// Downstream (SignalR) subscribes to the broadcast block and filters by symbol per connection.
/// </summary>
public sealed class EpexDataflowPipeline
{
    private readonly TransformManyBlock<string, EpexMarketData> _parseBlock;
    private readonly BroadcastBlock<EpexMarketData> _broadcastBlock;

    public EpexDataflowPipeline()
    {
        _parseBlock = new TransformManyBlock<string, EpexMarketData>(
            ParseEpexMessages,
            new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = DataflowBlockOptions.Unbounded,
                BoundedCapacity = 1000
            });

        _broadcastBlock = new BroadcastBlock<EpexMarketData>(m => m);

        _parseBlock.LinkTo(_broadcastBlock, new DataflowLinkOptions { PropagateCompletion = true });
    }

    /// <summary>
    /// Post a raw message (e.g. JSON) from Kafka. Returns true if accepted.
    /// </summary>
    public bool Post(string rawMessage)
    {
        return _parseBlock.Post(rawMessage);
    }

    /// <summary>
    /// Subscribe to the pipeline to receive all parsed EPEX messages. Used by SignalR broadcaster.
    /// </summary>
    public IDisposable LinkTo(ITargetBlock<EpexMarketData> target)
    {
        return _broadcastBlock.LinkTo(target, new DataflowLinkOptions { PropagateCompletion = true });
    }

    public Task Completion => _parseBlock.Completion;

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
