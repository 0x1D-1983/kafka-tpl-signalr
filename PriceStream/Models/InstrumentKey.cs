namespace PriceStream.Models;

/// <summary>
/// One priced product. A symbol can carry many (day-ahead and intraday, hourly and quarter-hour
/// contracts), so "latest price" and "newer replaces older" apply per instrument, not per symbol.
/// Clients still subscribe by symbol.
/// </summary>
public readonly record struct InstrumentKey(string Symbol, string Market, string DeliveryArea, string? Contract)
{
    public static InstrumentKey Of(EpexMarketData data) =>
        new(data.Symbol, data.Market, data.DeliveryArea, data.Contract);
}
