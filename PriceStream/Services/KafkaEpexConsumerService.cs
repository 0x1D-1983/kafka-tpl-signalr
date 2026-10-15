using Confluent.Kafka;
using Microsoft.Extensions.Options;
using PriceStream.Hubs;

namespace PriceStream.Services;

/// <summary>
/// Every pod reads every partition of the topic and fans out to its own connections only, so no
/// backplane is needed. Partitions are assigned manually instead of joining a consumer group:
/// scaling or restarting a pod never triggers a rebalance, and nothing is committed.
/// On startup the last <see cref="KafkaOptions.ReplayWindowSeconds"/> are read into the last-price
/// cache without broadcasting; readiness stays false until every partition reaches the offset it
/// had when the pod started.
/// </summary>
public sealed class KafkaEpexConsumerService : BackgroundService
{
    private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    private readonly ILogger<KafkaEpexConsumerService> _logger;
    private readonly KafkaOptions _options;
    private readonly EpexDataflowPipeline _pipeline;
    private readonly ILastPriceCache _lastPrices;
    private readonly SubscriptionPolicy _policy;
    private readonly KafkaCatchUpState _catchUp;

    public KafkaEpexConsumerService(
        ILogger<KafkaEpexConsumerService> logger,
        IOptions<KafkaOptions> options,
        EpexDataflowPipeline pipeline,
        ILastPriceCache lastPrices,
        SubscriptionPolicy policy,
        KafkaCatchUpState catchUp)
    {
        _logger = logger;
        _options = options.Value;
        _pipeline = pipeline;
        _lastPrices = lastPrices;
        _policy = policy;
        _catchUp = catchUp;
    }

    // Consume blocks the calling thread, so the loop runs off the host's startup path.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Factory.StartNew(
            () => RunAsync(stoppingToken),
            stoppingToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();

    private async Task RunAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            // Only identifies the pod in broker logs. Manual assignment never joins the group.
            GroupId = $"{_options.ConsumerGroupId}-{Environment.MachineName}",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            EnablePartitionEof = true
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        using var admin = new DependentAdminClientBuilder(consumer.Handle).Build();

        // Partition -> first offset that is live. Earlier offsets are replay.
        var liveFrom = new Dictionary<TopicPartition, long>();
        var pending = new HashSet<TopicPartition>();
        var nextRefresh = DateTime.MinValue;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                if (DateTime.UtcNow >= nextRefresh)
                {
                    if (!TryAssignNewPartitions(consumer, admin, liveFrom, pending))
                    {
                        await Task.Delay(RetryDelay, stoppingToken);
                        continue;
                    }

                    nextRefresh = DateTime.UtcNow.AddSeconds(_options.PartitionRefreshSeconds);
                    MarkCaughtUpIfDone(pending);
                }

                ConsumeResult<Ignore, string>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromSeconds(1));
                }
                catch (ConsumeException ex)
                {
                    _logger.LogWarning(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
                    continue;
                }

                if (result is not null && !result.IsPartitionEOF
                    && liveFrom.TryGetValue(result.TopicPartition, out var live) && result.Offset.Value < live)
                {
                    await ReplayAsync(result.Message.Value);
                }

                // Position, not the last message offset: a partition can end in transaction markers
                // that are never delivered as messages.
                if (pending.Count > 0)
                {
                    pending.RemoveWhere(tp => consumer.Position(tp).Value >= liveFrom[tp]);
                    MarkCaughtUpIfDone(pending);
                }

                if (result is null || result.IsPartitionEOF || result.Offset.Value < liveFrom.GetValueOrDefault(result.TopicPartition))
                    continue;

                var accepted = _pipeline.Post(result.Message.Value ?? string.Empty);
                if (!accepted)
                    _logger.LogWarning("Pipeline backpressure: message dropped for offset {Offset}", result.Offset.Value);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>
    /// Assigns partitions not yet assigned. At startup each starts at the replay window; partitions
    /// that appear later start at the beginning, because everything in them is new.
    /// </summary>
    private bool TryAssignNewPartitions(
        IConsumer<Ignore, string> consumer,
        IAdminClient admin,
        Dictionary<TopicPartition, long> liveFrom,
        HashSet<TopicPartition> pending)
    {
        try
        {
            var metadata = admin.GetMetadata(_options.Topic, MetadataTimeout);
            var topic = metadata.Topics.SingleOrDefault(t => t.Topic == _options.Topic);
            if (topic is null || topic.Error.IsError || topic.Partitions.Count == 0)
            {
                _logger.LogWarning("Kafka topic {Topic} has no partitions yet: {Reason}", _options.Topic, topic?.Error.Reason);
                return false;
            }

            var initial = liveFrom.Count == 0;
            var added = topic.Partitions
                .Select(p => new TopicPartition(_options.Topic, p.PartitionId))
                .Where(tp => !liveFrom.ContainsKey(tp))
                .ToList();
            if (added.Count == 0)
                return true;

            var addedLiveFrom = new Dictionary<TopicPartition, long>();
            var addedPending = new HashSet<TopicPartition>();
            var start = initial
                ? ReplayStart(consumer, added, addedLiveFrom, addedPending)
                : added.Select(tp =>
                {
                    addedLiveFrom[tp] = 0;
                    return new TopicPartitionOffset(tp, Offset.Beginning);
                }).ToList();

            consumer.IncrementalAssign(start);
            foreach (var (tp, offset) in addedLiveFrom)
                liveFrom[tp] = offset;
            pending.UnionWith(addedPending);

            _logger.LogInformation(
                "Assigned {Count} partition(s) of {Topic}; replaying {Pending} partition(s)",
                added.Count, _options.Topic, addedPending.Count);
            return true;
        }
        catch (KafkaException ex)
        {
            _logger.LogWarning(ex, "Kafka partition assignment failed: {Reason}", ex.Error.Reason);
            return false;
        }
    }

    private List<TopicPartitionOffset> ReplayStart(
        IConsumer<Ignore, string> consumer,
        List<TopicPartition> partitions,
        Dictionary<TopicPartition, long> liveFrom,
        HashSet<TopicPartition> pending)
    {
        foreach (var tp in partitions)
            liveFrom[tp] = consumer.QueryWatermarkOffsets(tp, MetadataTimeout).High.Value;

        if (_options.ReplayWindowSeconds <= 0)
            return partitions.Select(tp => new TopicPartitionOffset(tp, liveFrom[tp])).ToList();

        var since = new Timestamp(DateTimeOffset.UtcNow.AddSeconds(-_options.ReplayWindowSeconds));
        var found = consumer.OffsetsForTimes(
            partitions.Select(tp => new TopicPartitionTimestamp(tp, since)),
            MetadataTimeout);

        return found.Select(tpo =>
        {
            var end = liveFrom[tpo.TopicPartition];
            // No message at or after the window start: nothing to replay.
            var offset = tpo.Offset.Value < 0 ? end : tpo.Offset.Value;
            if (offset < end)
                pending.Add(tpo.TopicPartition);
            return new TopicPartitionOffset(tpo.TopicPartition, offset);
        }).ToList();
    }

    private async Task ReplayAsync(string? raw)
    {
        if (!EpexDataflowPipeline.TryParse(raw, out var message))
            return;
        var symbol = _policy.Normalize(message.Symbol, out _);
        if (symbol is null)
            return;

        try
        {
            await _lastPrices.SetAsync(symbol, message);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to cache replayed price for {Symbol}", symbol);
        }
    }

    private void MarkCaughtUpIfDone(HashSet<TopicPartition> pending)
    {
        if (pending.Count != 0 || _catchUp.IsCaughtUp)
            return;
        _catchUp.MarkCaughtUp();
        _logger.LogInformation("Kafka replay finished; pod is caught up with {Topic}", _options.Topic);
    }
}

/// <summary>
/// Set once the startup replay reaches the offsets the pod started at. Gates readiness.
/// </summary>
public sealed class KafkaCatchUpState
{
    private volatile bool _caughtUp;

    public bool IsCaughtUp => _caughtUp;

    public void MarkCaughtUp() => _caughtUp = true;
}

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    /// <summary>When false, this process does not consume Kafka and Kafka is omitted from readiness.</summary>
    public bool Enabled { get; set; } = true;

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "epex-market-data";

    /// <summary>Prefix for the per-pod group.id; the machine (pod) name is appended.</summary>
    public string ConsumerGroupId { get; set; } = "pricestream-signalr";

    /// <summary>How far back to read into the last-price cache at startup. 0 starts at the latest offset.</summary>
    public int ReplayWindowSeconds { get; set; } = 900;

    /// <summary>How often to check the topic for added partitions.</summary>
    public int PartitionRefreshSeconds { get; set; } = 60;
}
