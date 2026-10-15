using Microsoft.AspNetCore.Diagnostics.HealthChecks;
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

    public static WebApplication Create(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);
        var services = builder.Services;
        var configuration = builder.Configuration;

        // Configuration
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));
        services.Configure<RedisOptions>(configuration.GetSection(RedisOptions.SectionName));
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

        // Kafka → TPL Dataflow → SignalR pipeline (singleton pipeline, stateless for horizontal scaling)
        services.AddSingleton<EpexDataflowPipeline>();
        var kafkaEnabled = configuration.GetValue("Kafka:Enabled", true);
        if (kafkaEnabled)
        {
            services.AddHostedService<KafkaEpexConsumerService>();
        }
        services.AddSingleton<SignalRPriceBroadcaster>();
        services.AddHostedService(sp => sp.GetRequiredService<SignalRPriceBroadcaster>());

        // SignalR with optional Redis backplane: when multiple pods consume different Kafka partitions (symbols),
        // the backplane broadcasts group messages (e.g. symbol:DE, symbol:FR) to all pods so a client on one pod
        // can receive updates for symbols consumed by another pod.
        var signalrBuilder = services.AddSignalR();
        var redisConnection = configuration.GetValue<string>(RedisOptions.SectionName + ":Configuration");
        if (!string.IsNullOrWhiteSpace(redisConnection))
        {
            signalrBuilder.AddStackExchangeRedis(redisConnection, options =>
            {
                var prefix = configuration.GetValue<string>(RedisOptions.SectionName + ":InstanceName");
                if (!string.IsNullOrWhiteSpace(prefix))
                    options.Configuration.ChannelPrefix = StackExchange.Redis.RedisChannel.Literal(prefix);
            });
        }

        // Liveness: process is up and the dataflow pipeline has not faulted (restart if it fails).
        // Readiness: can serve traffic (pipeline, Kafka, and Redis when the backplane is enabled).
        var healthChecks = services.AddHealthChecks()
            .AddCheck("self", () => HealthCheckResult.Healthy(), tags: [ LiveTag ])
            .AddCheck<PipelineHealthCheck>("pipeline", tags: [ LiveTag, ReadyTag ]);

        if (kafkaEnabled)
        {
            healthChecks.AddCheck<KafkaHealthCheck>("kafka", tags: [ ReadyTag ]);
        }
            
        if (!string.IsNullOrWhiteSpace(redisConnection))
        {
            healthChecks.AddRedis(redisConnection, "redis", tags: [ ReadyTag ]);
        }

        services.AddRazorPages();

        var app = builder.Build();

        if (!app.Environment.IsDevelopment())
        {
            app.UseExceptionHandler("/Error");
            app.UseHsts();
        }

        app.UseHttpsRedirection();
        app.UseRouting();
        app.UseAuthorization();

        // Liveness: process is up and the pipeline has not faulted (restart if it fails)
        app.MapHealthChecks("/health/live", new HealthCheckOptions
        {
            Predicate = reg => reg.Tags.Contains(LiveTag),
            ResponseWriter = HealthResponseWriter.WriteMinimal
        }).AllowAnonymous();

        // Readiness: pod can receive traffic (pipeline, Kafka, Redis). LB stops sending if not ready.
        app.MapHealthChecks("/health/ready", new HealthCheckOptions
        {
            Predicate = reg => reg.Tags.Contains(ReadyTag),
            ResponseWriter = HealthResponseWriter.WriteMinimal
        }).AllowAnonymous();

        app.MapHub<PriceStreamHub>("/hubs/pricestream");

        return app;
    }
}
