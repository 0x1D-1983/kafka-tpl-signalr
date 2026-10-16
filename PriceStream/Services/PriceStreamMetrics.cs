using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace PriceStream.Services;

/// <summary>
/// Exported on /metrics. Every pod reads the whole topic, so Kafka-side series should look the same
/// on every pod; one pod diverging means a fault local to it.
/// </summary>
public sealed class PriceStreamMetrics
{
    public const string MeterName = "PriceStream";

    private readonly Counter<long> _consumed;
    private readonly Counter<long> _parsed;
    private readonly Counter<long> _failed;
    private readonly Counter<long> _dropped;
    private readonly Histogram<double> _publishLatency;
    private readonly ConcurrentDictionary<string, Func<int>> _queues = new(StringComparer.Ordinal);
    private readonly Counter<long> _superseded;
    private Func<IEnumerable<KeyValuePair<string, int>>> _subscriptions = static () => [];
    private readonly ConcurrentDictionary<(string Topic, int Partition), long> _consumerLag = new();
    // Unix seconds; 0 until the first message.
    private double _lastMessageTime;

    public PriceStreamMetrics(IMeterFactory meterFactory, KafkaConnectionState connection)
    {
        var meter = meterFactory.Create(MeterName);

        _consumed = meter.CreateCounter<long>(
            "pricestream.kafka.messages",
            description: "Kafka messages received, including startup replay.");
        _parsed = meter.CreateCounter<long>(
            "pricestream.kafka.messages.parsed",
            description: "Received messages that parsed as EPEX market data.");
        _failed = meter.CreateCounter<long>(
            "pricestream.kafka.messages.failed",
            description: "Received messages that were not valid EPEX market data.");
        _dropped = meter.CreateCounter<long>(
            "pricestream.messages.dropped",
            description: "Parsed messages that were not delivered, by reason.");
        _superseded = meter.CreateCounter<long>(
            "pricestream.outbox.superseded",
            description: "Ticks replaced by a newer tick for the same symbol before a slow client was sent them.");
        // Exported from 0 so alert rates see the first increment and have a baseline before any drop.
        foreach (var reason in DropReason.All)
            _dropped.Add(0, new KeyValuePair<string, object?>("reason", reason));
        _superseded.Add(0);
        _publishLatency = meter.CreateHistogram(
            "pricestream.publish.latency",
            unit: "s",
            description: "Kafka message timestamp to the send to one client completed, per delivered tick. Includes producer clock skew.",
            advice: new InstrumentAdvice<double>
            {
                HistogramBucketBoundaries = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
            });

        meter.CreateObservableGauge(
            "pricestream.pipeline.queue_depth",
            () => _queues.Select(queue => new Measurement<int>(queue.Value(), new KeyValuePair<string, object?>("stage", queue.Key))),
            description: "Messages waiting in each dataflow block.");
        meter.CreateObservableGauge(
            "pricestream.subscriptions",
            () => _subscriptions().Select(s => new Measurement<int>(s.Value, new KeyValuePair<string, object?>("symbol", s.Key))),
            description: "Connections on this pod subscribed to each symbol.");
        meter.CreateObservableGauge(
            "pricestream.kafka.consumer_lag",
            () => _consumerLag.Select(lag => new Measurement<long>(
                lag.Value,
                new KeyValuePair<string, object?>("topic", lag.Key.Topic),
                new KeyValuePair<string, object?>("partition", lag.Key.Partition))),
            description: "High watermark minus this pod's position, per partition.");
        meter.CreateObservableGauge(
            "pricestream.kafka.last_message_time",
            () => Volatile.Read(ref _lastMessageTime),
            unit: "s",
            description: "Unix time this pod last received a Kafka message.");
        meter.CreateObservableGauge(
            "pricestream.kafka.connected",
            () => connection.IsConnected ? 1 : 0,
            description: "1 while the consumer can reach a Kafka broker.");
    }

    public void MessageReceived(bool parsed)
    {
        _consumed.Add(1);
        (parsed ? _parsed : _failed).Add(1);
        Volatile.Write(ref _lastMessageTime, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0);
    }

    public void MessageDropped(string reason) =>
        _dropped.Add(1, new KeyValuePair<string, object?>("reason", reason));

    public void Published(DateTimeOffset kafkaTimestamp) =>
        _publishLatency.Record(Math.Max(0, (DateTimeOffset.UtcNow - kafkaTimestamp).TotalSeconds));

    /// <summary>Reports <paramref name="depth"/> on every collection, tagged with <paramref name="stage"/>.</summary>
    public void ObserveQueue(string stage, Func<int> depth) => _queues[stage] = depth;

    public void SetConsumerLag(string topic, int partition, long lag) => _consumerLag[(topic, partition)] = lag;

    /// <summary>Subscribers per symbol; symbols without subscribers should not be reported.</summary>
    public void ObserveSubscriptions(Func<IEnumerable<KeyValuePair<string, int>>> subscriptions) =>
        _subscriptions = subscriptions;

    public void TickSuperseded() => _superseded.Add(1);
}

public static class DropReason
{
    /// <summary>The pipeline queue was full when the consumer posted.</summary>
    public const string Backpressure = "backpressure";
    public const string NoSymbol = "no_symbol";
    public const string SendFailed = "send_failed";

    public static readonly string[] All = [Backpressure, NoSymbol, SendFailed];
}
