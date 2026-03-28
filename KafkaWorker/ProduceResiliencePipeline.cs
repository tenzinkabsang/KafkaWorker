using Polly;
using Polly.Retry;

namespace KafkaWorker;

/// <summary>
/// Shared resilience pipeline for Kafka produce operations (DLQ publish and DLQ reprocess).
/// Uses exponential backoff with jitter and Polly's default retry count.
/// </summary>
internal static class ProduceResiliencePipeline
{
    public static readonly ResiliencePipeline Instance = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = RetryConstants.DefaultRetryCount,
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true
        })
        .Build();
}
