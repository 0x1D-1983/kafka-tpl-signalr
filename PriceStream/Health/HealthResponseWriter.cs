using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace PriceStream.Health;

/// <summary>
/// Writes a minimal health response for load balancers (e.g. 200 + "Healthy").
/// </summary>
public static class HealthResponseWriter
{
    public static Task WriteMinimal(HttpContext context, HealthReport report)
    {
        context.Response.ContentType = "text/plain; charset=utf-8";
        context.Response.StatusCode = report.Status == HealthStatus.Healthy ? 200 : 503;
        var text = report.Status == HealthStatus.Healthy ? "Healthy" : "Unhealthy";
        return context.Response.WriteAsync(text, System.Text.Encoding.UTF8);
    }
}
