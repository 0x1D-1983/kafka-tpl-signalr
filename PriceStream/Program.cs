using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using PriceStream.Health;
using PriceStream.Hubs;
using PriceStream.Services;

var builder = WebApplication.CreateBuilder(args);

// Configuration
builder.Services.Configure<KafkaOptions>(
    builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.Configure<RedisOptions>(
    builder.Configuration.GetSection(RedisOptions.SectionName));

// Kafka → TPL Dataflow → SignalR pipeline (singleton pipeline, stateless for horizontal scaling)
builder.Services.AddSingleton<EpexDataflowPipeline>();
builder.Services.AddHostedService<KafkaEpexConsumerService>();
builder.Services.AddHostedService<SignalRPriceBroadcaster>();

// SignalR with optional Redis backplane: when multiple pods consume different Kafka partitions (symbols),
// the backplane broadcasts group messages (e.g. symbol:DE, symbol:FR) to all pods so a client on one pod
// can receive updates for symbols consumed by another pod.
var signalrBuilder = builder.Services.AddSignalR();
var redisConnection = builder.Configuration.GetValue<string>(RedisOptions.SectionName + ":Configuration");
if (!string.IsNullOrWhiteSpace(redisConnection))
{
    signalrBuilder.AddStackExchangeRedis(redisConnection, options =>
    {
        var prefix = builder.Configuration.GetValue<string>(RedisOptions.SectionName + ":InstanceName");
        if (!string.IsNullOrWhiteSpace(prefix))
            options.Configuration.ChannelPrefix = StackExchange.Redis.RedisChannel.Literal(prefix);
    });
}

// Health checks for load balancer / Kubernetes: liveness = process alive, readiness = can serve traffic (Kafka + Redis when backplane enabled)
var healthChecks = builder.Services.AddHealthChecks()
    .AddCheck("self", () => Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy(), tags: new[] { "live" })
    .AddCheck<KafkaHealthCheck>("kafka", tags: new[] { "ready" });
if (!string.IsNullOrWhiteSpace(redisConnection))
    healthChecks.AddRedis(redisConnection, "redis", tags: new[] { "ready" });

builder.Services.AddRazorPages();

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseRouting();
app.UseAuthorization();

// Liveness: pod is alive (restart if fails)
app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate = reg => reg.Tags.Contains("live"),
    ResponseWriter = HealthResponseWriter.WriteMinimal
}).AllowAnonymous();

// Readiness: pod can receive traffic (LB stops sending if not ready)
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = reg => reg.Tags.Contains("ready"),
    ResponseWriter = HealthResponseWriter.WriteMinimal
}).AllowAnonymous();

app.MapHub<PriceStreamHub>("/hubs/pricestream");

app.Run();
