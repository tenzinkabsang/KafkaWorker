namespace KafkaWorker;

/// <summary>
/// How much is waiting in a dead letter topic, across every partition.
/// </summary>
/// <remarks>
/// Computed from broker metadata rather than from this process's consumer assignment, so every
/// replica reports the same numbers for the whole topic — not just the partitions its own DLQ
/// consumer happens to own.
/// </remarks>
public sealed record DlqDepth
{
    /// <summary>The dead letter topic these numbers describe.</summary>
    public required string Topic { get; init; }

    /// <summary>Per-partition detail, ordered by partition.</summary>
    public required IReadOnlyList<DlqPartitionDepth> Partitions { get; init; }

    /// <summary>Records the next sweep will read across the whole topic.</summary>
    public long Pending => Partitions.Sum(p => p.Pending);

    /// <summary>True when nothing is waiting to be reprocessed.</summary>
    public bool IsEmpty => Pending == 0;
}
