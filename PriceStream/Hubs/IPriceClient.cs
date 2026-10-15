using PriceStream.Models;

namespace PriceStream.Hubs;

/// <summary>
/// Methods the server invokes on connected clients. The method name is the wire event name,
/// so renaming one breaks the JavaScript client (<c>connection.on('PriceUpdate', ...)</c>).
/// </summary>
public interface IPriceClient
{
    /// <summary>A live tick, or the cached last price sent right after <c>Subscribe</c>.</summary>
    Task PriceUpdate(EpexMarketData update);
}
