using System.Diagnostics.Metrics;

namespace KafkaWorker;

internal sealed class KafkaWorkerMetrics : IDisposable
{
    public static readonly string MeterName = "KafkaWorker";

    private readonly Meter _meter = new(MeterName);

    public Counter<long> MessagesProcessed { get; }
    public Histogram<double> ProcessingDuration { get; }
    public Counter<long> DlqPublished { get; }
    public Counter<long> DlqReprocessed { get; }
    public Counter<long> DlqSkipped { get; }
    public Histogram<int> BatchSize { get; }
    public Histogram<double> BatchProcessingDuration { get; }
    public Counter<long> BatchFallbacks { get; }

    public KafkaWorkerMetrics()
    {
        MessagesProcessed = _meter.CreateCounter<long>(
            "kafkaworker.messages.processed",
            description: "Number of messages processed by the consumer");

        ProcessingDuration = _meter.CreateHistogram<double>(
            "kafkaworker.messages.processing_duration",
            unit: "ms",
            description: "Duration of message processing in milliseconds");

        DlqPublished = _meter.CreateCounter<long>(
            "kafkaworker.messages.dlq_published",
            description: "Number of messages published to the dead letter queue");

        DlqReprocessed = _meter.CreateCounter<long>(
            "kafkaworker.dlq.messages_reprocessed",
            description: "Number of messages reprocessed from the dead letter queue");

        DlqSkipped = _meter.CreateCounter<long>(
            "kafkaworker.dlq.messages_skipped",
            description: "Number of messages skipped during DLQ reprocessing");

        BatchSize = _meter.CreateHistogram<int>(
            "kafkaworker.batch.size",
            description: "Number of messages handed to the batch handler in a single call");

        BatchProcessingDuration = _meter.CreateHistogram<double>(
            "kafkaworker.batch.processing_duration",
            unit: "ms",
            description: "Duration of a batch handler call in milliseconds");

        BatchFallbacks = _meter.CreateCounter<long>(
            "kafkaworker.batch.fallbacks",
            description: "Number of batches that failed and were re-processed one message at a time");
    }

    public void Dispose() => _meter.Dispose();
}
