namespace KafkaWorker;

/// <summary>
/// How much of one dead letter topic partition is still waiting to be reprocessed.
/// </summary>
public sealed record DlqPartitionDepth
{
    /// <summary>The partition this describes.</summary>
    public required int Partition { get; init; }

    /// <summary>The oldest offset still retained by the broker.</summary>
    public required long Low { get; init; }

    /// <summary>The end of the log: one past the newest record.</summary>
    public required long High { get; init; }

    /// <summary>
    /// The offset the next sweep resumes from — the DLQ consumer group's committed offset, or where
    /// <see cref="KafkaWorkerConfig.DeadLetterStartFrom"/> resolves to when the group has never
    /// committed for this partition.
    /// </summary>
    public required long ResumesAt { get; init; }

    /// <summary>
    /// Records the next sweep will read from this partition. Never negative: a resume point past the
    /// end of the log (after a retention drop, say) reports zero rather than a nonsense count.
    /// </summary>
    public long Pending => Math.Max(0, High - ResumesAt);
}
