using System.Net;
using Microsoft.AspNetCore.Builder;
using Xunit;

namespace PriceStream.Tests;

public sealed class HttpsRedirectTests
{
    private const string Negotiate = "/hubs/pricestream/negotiate?negotiateVersion=1";

    [Fact]
    public async Task No_redirect_by_default()
    {
        await using var app = await StartAsync();
        using var http = Client();

        using var response = await http.PostAsync($"{app.Url}{Negotiate}", null);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Plain_http_is_redirected_when_enabled()
    {
        await using var app = await StartAsync("--Https:Redirect=true");
        using var http = Client();

        using var response = await http.PostAsync($"{app.Url}{Negotiate}", null);

        Assert.Equal(HttpStatusCode.TemporaryRedirect, response.StatusCode);
        Assert.Equal("https", response.Headers.Location?.Scheme);
    }

    [Fact]
    public async Task Tls_terminated_at_the_proxy_is_not_redirected_again()
    {
        await using var app = await StartAsync("--Https:Redirect=true");
        using var http = Client();
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{app.Url}{Negotiate}");
        request.Headers.Add("X-Forwarded-Proto", "https");

        using var response = await http.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Theory]
    [InlineData("/health/live")]
    [InlineData("/health/ready")]
    public async Task Probes_are_never_redirected(string path)
    {
        await using var app = await StartAsync("--Https:Redirect=true");
        using var http = Client();

        using var response = await http.GetAsync($"{app.Url}{path}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static HttpClient Client() =>
        new(new HttpClientHandler { AllowAutoRedirect = false }) { Timeout = TimeSpan.FromSeconds(10) };

    private static async Task<RunningApp> StartAsync(params string[] extraArgs)
    {
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Development");
        var app = Program.Create(
        [
            "--urls", "http://127.0.0.1:0",
            "--Kafka:Enabled=false",
            "--ShutdownTimeoutSeconds=5",
            "--Logging:LogLevel:Default=Warning",
            .. extraArgs,
        ]);
        await app.StartAsync();
        var url = app.Urls.First(address => address.StartsWith("http://", StringComparison.OrdinalIgnoreCase)).TrimEnd('/');
        return new RunningApp(app, url);
    }

    private sealed class RunningApp(WebApplication app, string url) : IAsyncDisposable
    {
        public string Url { get; } = url;

        public async ValueTask DisposeAsync()
        {
            await app.StopAsync();
            await app.DisposeAsync();
        }
    }
}
