namespace KafkaWorker;

/// <summary>
/// Bounds a <c>PeekAsync</c> call on <see cref="IDlqInspector{TMessage}"/>.
/// </summary>
public sealed record DlqPeekOptions
{
    /// <summary>
    /// The most entries to return, counted across every partition rather than per partition.
    /// Reading round-robins across the assigned partitions so one busy partition cannot crowd the
    /// others out of the result. Defaults to 50.
    /// </summary>
    public int MaxMessages { get; init; } = 50;

    /// <summary>
    /// How long the whole read may take before returning what it has. Defaults to 10 seconds.
    /// A peek normally returns as soon as every partition reaches the end of its log, so this is a
    /// ceiling rather than a wait.
    /// </summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The partition to read. Null reads every partition of the dead letter topic. Required when
    /// <see cref="FromOffset"/> is set.
    /// </summary>
    public int? Partition { get; init; }

    /// <summary>
    /// The offset to start reading from, for paging deeper into a partition. Null starts where the
    /// next sweep would. Only valid alongside <see cref="Partition"/> — an offset means nothing
    /// without one, so setting it alone throws.
    /// </summary>
    public long? FromOffset { get; init; }
}
