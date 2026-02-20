namespace PriceStream.Models;

/// <summary>
/// EPEX spot market data message (e.g. day-ahead auction or intraday).
/// </summary>
public sealed class EpexMarketData
{
    public required string Symbol { get; init; }
    public required string Market { get; init; }
    public required string DeliveryArea { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public decimal Price { get; init; }
    public string? Unit { get; init; }
    public string? Contract { get; init; }
}
