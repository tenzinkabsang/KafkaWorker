using Confluent.Kafka;
using NSubstitute;

namespace KafkaWorker.Tests;

public class DlqConsumerFactoryTests
{
    private const string TestDlqTopic = "test-dlq-topic";
    private static readonly DateTimeOffset StartFrom = new(2026, 6, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan BrokerTimeout = TimeSpan.FromSeconds(10);

    public class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }

    private static TopicPartition Partition0 => new(TestDlqTopic, new Partition(0));
    private static TopicPartition Partition1 => new(TestDlqTopic, new Partition(1));

    private static List<TopicPartitionOffset> ResolveStartOffsets(
        IConsumer<string, TestMessage> consumer, List<TopicPartition> partitions)
        => DlqConsumerFactory<string, TestMessage>
            .ResolveStartOffsets(consumer, partitions, StartFrom, BrokerTimeout)
            .ToList();

    [Fact]
    public void ResolveStartOffsets_NoCommittedOffsets_SeeksAllPartitionsByTimestamp()
    {
        var consumer = Substitute.For<IConsumer<string, TestMessage>>();
        var partitions = new List<TopicPartition> { Partition0, Partition1 };
        consumer.Committed(partitions, BrokerTimeout).Returns(
        [
            new TopicPartitionOffset(Partition0, Offset.Unset),
            new TopicPartitionOffset(Partition1, Offset.Unset),
        ]);
        consumer.OffsetsForTimes(Arg.Any<IEnumerable<TopicPartitionTimestamp>>(), Arg.Any<TimeSpan>()).Returns(
        [
            new TopicPartitionOffset(Partition0, new Offset(100)),
            new TopicPartitionOffset(Partition1, new Offset(200)),
        ]);

        var result = ResolveStartOffsets(consumer, partitions);

        Assert.Equal(new Offset(100), result.Single(r => r.Partition.Value == 0).Offset);
        Assert.Equal(new Offset(200), result.Single(r => r.Partition.Value == 1).Offset);
    }

    [Fact]
    public void ResolveStartOffsets_AllPartitionsCommitted_ResumesWithoutTimestampLookup()
    {
        var consumer = Substitute.For<IConsumer<string, TestMessage>>();
        var partitions = new List<TopicPartition> { Partition0, Partition1 };
        consumer.Committed(partitions, BrokerTimeout).Returns(
        [
            new TopicPartitionOffset(Partition0, new Offset(1500)),
            new TopicPartitionOffset(Partition1, new Offset(2500)),
        ]);

        var result = ResolveStartOffsets(consumer, partitions);

        Assert.Equal(new Offset(1500), result.Single(r => r.Partition.Value == 0).Offset);
        Assert.Equal(new Offset(2500), result.Single(r => r.Partition.Value == 1).Offset);
        consumer.DidNotReceive().OffsetsForTimes(
            Arg.Any<IEnumerable<TopicPartitionTimestamp>>(), Arg.Any<TimeSpan>());
    }

    [Fact]
    public void ResolveStartOffsets_MixedPartitions_CommittedResumeAndUncommittedSeekByTimestamp()
    {
        var consumer = Substitute.For<IConsumer<string, TestMessage>>();
        var partitions = new List<TopicPartition> { Partition0, Partition1 };
        consumer.Committed(partitions, BrokerTimeout).Returns(
        [
            new TopicPartitionOffset(Partition0, new Offset(1500)),
            new TopicPartitionOffset(Partition1, Offset.Unset),
        ]);

        List<TopicPartitionTimestamp>? requested = null;
        consumer.OffsetsForTimes(
                Arg.Do<IEnumerable<TopicPartitionTimestamp>>(x => requested = x.ToList()),
                Arg.Any<TimeSpan>())
            .Returns([new TopicPartitionOffset(Partition1, new Offset(9000))]);

        var result = ResolveStartOffsets(consumer, partitions);

        // The committed partition resumes where it left off; only the uncommitted one seeks by timestamp
        Assert.Equal(new Offset(1500), result.Single(r => r.Partition.Value == 0).Offset);
        Assert.Equal(new Offset(9000), result.Single(r => r.Partition.Value == 1).Offset);

        Assert.NotNull(requested);
        var lookup = Assert.Single(requested);
        Assert.Equal(Partition1, lookup.TopicPartition);
        Assert.Equal(new Timestamp(StartFrom).UnixTimestampMs, lookup.Timestamp.UnixTimestampMs);
    }

    [Fact]
    public void ResolveStartOffsets_TimestampBeyondNewestMessage_UsesOffsetEnd()
    {
        // OffsetsForTimes returns Offset.End when the timestamp exceeds the partition's last
        // message — the resolved assignment must pass that through so only new messages are read.
        var consumer = Substitute.For<IConsumer<string, TestMessage>>();
        var partitions = new List<TopicPartition> { Partition0 };
        consumer.Committed(partitions, BrokerTimeout).Returns(
        [
            new TopicPartitionOffset(Partition0, Offset.Unset),
        ]);
        consumer.OffsetsForTimes(Arg.Any<IEnumerable<TopicPartitionTimestamp>>(), Arg.Any<TimeSpan>())
            .Returns([new TopicPartitionOffset(Partition0, Offset.End)]);

        var result = ResolveStartOffsets(consumer, partitions);

        Assert.Equal(Offset.End, Assert.Single(result).Offset);
    }
}
