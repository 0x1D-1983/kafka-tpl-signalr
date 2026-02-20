using Microsoft.AspNetCore.SignalR;

namespace PriceStream.Hubs;

public sealed class PriceStreamHub : Hub
{
    private readonly ILogger<PriceStreamHub> _logger;

    public PriceStreamHub(ILogger<PriceStreamHub> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Subscribe to one or more symbols. Each symbol is added to this connection's subscription set.
    /// Call again after reconnect to restore subscriptions (connection state is not shared across pods).
    /// </summary>
    public Task Subscribe(string[] symbols)
    {
        if (symbols is null || symbols.Length == 0)
            return Task.CompletedTask;

        var normalized = symbols
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s.Trim().ToUpperInvariant())
            .Distinct()
            .ToArray();

        foreach (var symbol in normalized)
            Groups.AddToGroupAsync(Context.ConnectionId, SymbolGroup(symbol), Context.ConnectionAborted);

        _logger.LogDebug("Connection {ConnectionId} subscribed to {Symbols}", Context.ConnectionId, string.Join(", ", normalized));
        return Task.CompletedTask;
    }

    /// <summary>
    /// Unsubscribe from the given symbols.
    /// </summary>
    public Task Unsubscribe(string[] symbols)
    {
        if (symbols is null || symbols.Length == 0)
            return Task.CompletedTask;

        var tasks = symbols
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => Groups.RemoveFromGroupAsync(Context.ConnectionId, SymbolGroup(s.Trim().ToUpperInvariant()), Context.ConnectionAborted));

        return Task.WhenAll(tasks);
    }

    public override Task OnConnectedAsync()
    {
        _logger.LogInformation("Client connected: {ConnectionId}", Context.ConnectionId);
        return Task.CompletedTask;
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation("Client disconnected: {ConnectionId}, Reason: {Reason}", Context.ConnectionId, exception?.Message ?? "normal");
        return Task.CompletedTask;
    }

    internal static string SymbolGroup(string symbol) => $"symbol:{symbol}";
}
