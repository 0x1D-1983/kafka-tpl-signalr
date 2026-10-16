using System.Text.Json.Serialization;
using PriceStream.Services;

namespace PriceStream.Models;

/// <summary>
/// EPEX spot market data message (e.g. day-ahead auction or intraday).
/// </summary>
public sealed class EpexMarketData
{
    /// <summary>Trimmed and upper-case when parsed from JSON.</summary>
    [JsonConverter(typeof(SymbolJsonConverter))]
    public required string Symbol { get; init; }
    public required string Market { get; init; }
    public required string DeliveryArea { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required decimal Price { get; init; }
    public string? Unit { get; init; }
    public string? Contract { get; init; }
}
