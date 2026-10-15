using HealthChecks.UI.Client;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using PriceStream.Health;
using PriceStream.Hubs;
using PriceStream.Services;

var app = Create(args);
app.Run();

public partial class Program
{
    private static readonly string LiveTag = "live";
    private static readonly string ReadyTag = "ready";
    private static readonly string KafkaTag = "kafka";

    public static WebApplication Create(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        var services = builder.Services;
        var configuration = builder.Configuration;

        // Configuration
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));
        services.Configure<SubscriptionOptions>(configuration.GetSection(SubscriptionOptions.SectionName));
        services.AddSingleton<SubscriptionPolicy>();

        var shutdownSeconds = configuration.GetValue<int?>("ShutdownTimeoutSeconds");
        services.Configure<HostOptions>(options =>
        {
            // A faulted pipeline throws out of the broadcaster and stops the process.
            options.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.StopHost;
            if (shutdownSeconds is > 0)
                options.ShutdownTimeout = TimeSpan.FromSeconds(shutdownSeconds.Value);
        });

        // Kafka → TPL Dataflow → SignalR. Every pod reads the whole topic and sends only to its own
        // connections, so there is no backplane and the last-price cache can be local.
        services.AddSingleton<EpexDataflowPipeline>();
        services.AddSingleton<ILastPriceCache, InMemoryLastPriceCache>();
        services.AddSingleton<KafkaCatchUpState>();
        services.AddSingleton<KafkaConnectionState>();
        var kafkaEnabled = configuration.GetValue("Kafka:Enabled", true);
        if (kafkaEnabled)
        {
            services.AddHostedService<KafkaEpexConsumerService>();
        }
        services.AddSingleton<SignalRPriceBroadcaster>();
        services.AddHostedService(sp => sp.GetRequiredService<SignalRPriceBroadcaster>());

        services.AddSignalR();

        // Liveness: process is up and the dataflow pipeline has not faulted (restart if it fails).
        // Readiness: can serve traffic (pipeline, startup replay finished). Only conditions local to
        // this pod: Kafka is shared, so gating on it would make every pod unready at once.
        var healthChecks = services.AddHealthChecks()
            .AddCheck("self", () => HealthCheckResult.Healthy(), tags: [ LiveTag ])
            .AddCheck<PipelineHealthCheck>("pipeline", tags: [ LiveTag, ReadyTag ]);

        if (kafkaEnabled)
        {
            healthChecks.AddCheck<KafkaCatchUpHealthCheck>("kafka-catchup", tags: [ ReadyTag ]);
            healthChecks.AddCheck<KafkaHealthCheck>("kafka", tags: [ KafkaTag ]);
        }

        services.AddProblemDetails();

        // TLS terminates at the edge. Trust X-Forwarded-For/Proto only from configured proxy addresses
        // (loopback is always trusted), so Request.Scheme and RemoteIpAddress reflect the client.
        var knownNetworks = configuration.GetSection("ForwardedHeaders:KnownNetworks").Get<string[]>() ?? [];
        var knownProxies = configuration.GetSection("ForwardedHeaders:KnownProxies").Get<string[]>() ?? [];
        services.Configure<ForwardedHeadersOptions>(options =>
        {
            options.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
            // Envoy is the only hop in front of the pods.
            options.ForwardLimit = 1;
            foreach (var network in knownNetworks)
                options.KnownIPNetworks.Add(System.Net.IPNetwork.Parse(network));
            foreach (var proxy in knownProxies)
                options.KnownProxies.Add(System.Net.IPAddress.Parse(proxy));
        });

        // Off by default: the edge owns HTTP->HTTPS redirects. Enable only when clients can reach
        // the pods over plain HTTP without going through a TLS-terminating proxy.
        var httpsRedirect = configuration.GetValue("Https:Redirect", false);
        if (httpsRedirect)
            services.AddHttpsRedirection(options => options.HttpsPort = configuration.GetValue("Https:Port", 443));

        var app = builder.Build();

        app.UseForwardedHeaders();

        if (!app.Environment.IsDevelopment())
        {
            app.UseExceptionHandler();
            // Only emitted on HTTPS requests, which after UseForwardedHeaders includes TLS terminated at the edge.
            app.UseHsts();
        }

        if (httpsRedirect)
        {
            // Kubelet probes call the pod IP over plain HTTP and must never see a 307.
            app.UseWhen(
                context => !context.Request.Path.StartsWithSegments("/health"),
                branch => branch.UseHttpsRedirection());
        }

        app.UseRouting();
        app.UseAuthorization();

        // Liveness: process is up and the pipeline has not faulted (restart if it fails)
        app.MapHealthChecks("/health/live", new HealthCheckOptions
        {
            Predicate = reg => reg.Tags.Contains(LiveTag),
            ResponseWriter = HealthResponseWriter.WriteMinimal
        }).AllowAnonymous();

        // Readiness: pod can receive traffic (pipeline, replay done). LB stops sending if not ready.
        app.MapHealthChecks("/health/ready", new HealthCheckOptions
        {
            Predicate = reg => reg.Tags.Contains(ReadyTag),
            ResponseWriter = HealthResponseWriter.WriteMinimal
        }).AllowAnonymous();

        // Kafka connectivity for monitoring and alerting; no probe calls this. 503 while degraded.
        app.MapHealthChecks("/health/kafka", new HealthCheckOptions
        {
            Predicate = reg => reg.Tags.Contains(KafkaTag),
            ResponseWriter = UIResponseWriter.WriteHealthCheckUIResponse,
            ResultStatusCodes =
            {
                [HealthStatus.Healthy] = StatusCodes.Status200OK,
                [HealthStatus.Degraded] = StatusCodes.Status503ServiceUnavailable,
                [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable,
            },
        }).AllowAnonymous();

        app.MapHub<PriceStreamHub>("/hubs/pricestream");

        return app;
    }
}
