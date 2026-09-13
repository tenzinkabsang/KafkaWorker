namespace KafkaWorker;

/// <summary>
/// Records whether the consumer for <typeparamref name="TMessage"/> was registered in batch mode.
/// </summary>
/// <remarks>
/// The mode is fixed at registration rather than discovered at runtime because it changes the
/// underlying client configuration: batch consumers disable the client's background auto-commit and
/// commit synchronously at each batch boundary, which bounds redelivery after a hard crash to a
/// single batch instead of however much throughput fit into <c>AutoCommitIntervalMs</c>.
/// </remarks>
internal sealed class BatchConsumerOptions<TMessage>(bool isEnabled) where TMessage : class
{
    public bool IsEnabled { get; } = isEnabled;
}
