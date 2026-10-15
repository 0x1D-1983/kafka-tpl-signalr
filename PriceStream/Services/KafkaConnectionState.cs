using Confluent.Kafka;

namespace PriceStream.Services;

/// <summary>
/// Broker connectivity as seen by the consumer. Kafka is shared by every pod, so this is reported
/// for monitoring only and never gates readiness: a Kafka outage would otherwise take every pod out
/// of the load balancer at once, although each can still serve cached prices.
/// </summary>
public sealed class KafkaConnectionState
{
    private readonly Lock _gate = new();
    private DateTimeOffset? _lastContact;
    private DateTimeOffset? _disconnectedSince;
    private string? _lastError;

    public bool IsConnected
    {
        get { lock (_gate) return _lastContact is not null && _disconnectedSince is null; }
    }

    public (DateTimeOffset? LastContact, DateTimeOffset? DisconnectedSince, string? LastError) Snapshot()
    {
        lock (_gate) return (_lastContact, _disconnectedSince, _lastError);
    }

    /// <summary>A broker answered: a message, partition EOF or metadata response.</summary>
    public void RecordContact()
    {
        lock (_gate)
        {
            _lastContact = DateTimeOffset.UtcNow;
            _disconnectedSince = null;
        }
    }

    public void RecordError(Error error)
    {
        lock (_gate)
        {
            _lastError = $"{error.Code}: {error.Reason}";
            if (error.IsFatal || error.Code == ErrorCode.Local_AllBrokersDown)
                _disconnectedSince ??= DateTimeOffset.UtcNow;
        }
    }
}
