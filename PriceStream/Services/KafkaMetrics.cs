using System.Diagnostics.Metrics;

namespace PriceStream.Services;

/// <summary>
/// Exported on /metrics. Every pod reads the whole topic, so all pods should report roughly the
/// same last-message time; one pod falling behind the others means its own Kafka path is broken.
/// </summary>
public sealed class KafkaMetrics
{
    public const string MeterName = "PriceStream.Kafka";

    private readonly Counter<long> _messages;
    // Unix seconds; 0 until the first message.
    private double _lastMessageTime;

    public KafkaMetrics(IMeterFactory meterFactory, KafkaConnectionState connection)
    {
        var meter = meterFactory.Create(MeterName);
        _messages = meter.CreateCounter<long>(
            "pricestream.kafka.messages",
            description: "Kafka messages received, including startup replay and unparseable payloads.");
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

    public void MessageReceived()
    {
        _messages.Add(1);
        Volatile.Write(ref _lastMessageTime, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0);
    }
}
