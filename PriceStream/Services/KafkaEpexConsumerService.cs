using System.Threading.Tasks.Dataflow;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using PriceStream.Models;

namespace PriceStream.Services;

public sealed class KafkaEpexConsumerService : BackgroundService
{
    private readonly ILogger<KafkaEpexConsumerService> _logger;
    private readonly KafkaOptions _options;
    private readonly EpexDataflowPipeline _pipeline;

    public KafkaEpexConsumerService(
        ILogger<KafkaEpexConsumerService> logger,
        IOptions<KafkaOptions> options,
        EpexDataflowPipeline pipeline)
    {
        _logger = logger;
        _options = options.Value;
        _pipeline = pipeline;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
            EnableAutoOffsetStore = true
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(_options.Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromSeconds(5));
                    if (result is null) continue;

                    var accepted = _pipeline.Post(result.Message.Value ?? string.Empty);
                    if (!accepted)
                        _logger.LogWarning("Pipeline backpressure: message dropped for offset {Offset}", result.Offset.Value);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogWarning(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        finally
        {
            consumer.Close();
        }

        await Task.CompletedTask;
    }
}

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "epex-market-data";
    public string ConsumerGroupId { get; set; } = "pricestream-signalr";
}
