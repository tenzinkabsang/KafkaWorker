namespace KafkaWorker;

/// <summary>
/// Presents an <see cref="IBatchMessageHandler{TMessage}"/> as an <see cref="IMessageHandler{TMessage}"/>
/// by invoking it with a single-message batch.
/// </summary>
/// <remarks>
/// Registered by <c>AddKafkaWorkerBatch</c> so that every path which reprocesses one message at a
/// time keeps working unchanged when the application supplies only a batch handler: the consumer's
/// per-message fallback after a failed batch, and the DLQ consumer's in-place reprocessing. Those
/// paths resolve <see cref="IMessageHandler{TMessage}"/> and are unaware batching is in play.
/// </remarks>
internal sealed class BatchMessageHandlerAdapter<TMessage>(IBatchMessageHandler<TMessage> handler)
    : IMessageHandler<TMessage> where TMessage : class
{
    public Task HandleMessageAsync(TMessage message, CancellationToken stoppingToken)
        => handler.HandleBatchAsync(new[] { message }, stoppingToken);
}
