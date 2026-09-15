using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace KafkaWorker.Tests;

public class DlqInspectorTests : IDisposable
{
    private const string TestDlqTopic = "test-dlq-topic";
    private const string TestOriginalTopic = "test-original-topic";
    private const int TestMaxReprocessAttempts = 3;

    private readonly IConsumer<string, TestMessage> _consumer;
    private readonly TestInspectorClientFactory _clientFactory;
    private readonly ILogger<DlqInspector<string, TestMessage>> _logger;
    private readonly CancellationTokenSource _cts = new();

    public DlqInspectorTests()
    {
        _consumer = Substitute.For<IConsumer<string, TestMessage>>();
        _logger = Substitute.For<ILogger<DlqInspector<string, TestMessage>>>();
        _logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);

        _clientFactory = new TestInspectorClientFactory(_consumer);

        SetPartitions(0);
    }

    public void Dispose()
    {
        _consumer.Dispose();
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

    public class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }

    /// <summary>
    /// Hand-written rather than substituted: NSubstitute cannot proxy the library's internal
    /// interfaces, which is why the DLQ consumer suite writes its factory double the same way.
    /// </summary>
    private sealed class TestInspectorClientFactory(IConsumer<string, TestMessage> consumer)
        : IDlqInspectorClientFactory<string, TestMessage>
    {
        public IReadOnlyList<int> Partitions { get; set; } = [];

        public IConsumer<string, TestMessage> CreateConsumer() => consumer;

        public IReadOnlyList<int> GetTopicPartitions(
            IConsumer<string, TestMessage> consumer, string topic, TimeSpan timeout) => Partitions;
    }

    #region Helpers

    private DlqInspector<string, TestMessage> CreateInspector(
        string? deadLetterTopic = TestDlqTopic,
        int maxReprocessAttempts = TestMaxReprocessAttempts,
        DateTimeOffset? startFrom = null)
    {
        var config = new KafkaWorkerConfig
        {
            GroupId = "test-group",
            Topic = TestOriginalTopic,
            DeadLetterTopic = deadLetterTopic,
            DeadLetterMaxReprocessAttempts = maxReprocessAttempts,
            DeadLetterStartFrom = startFrom
        };

        var optionsMonitor = Substitute.For<IOptionsMonitor<KafkaWorkerConfig>>();
        optionsMonitor.Get(typeof(TestMessage).FullName).Returns(config);

        return new DlqInspector<string, TestMessage>(_clientFactory, optionsMonitor, _logger);
    }

    /// <summary>Points the factory's metadata lookup at the given partition ids.</summary>
    private void SetPartitions(params int[] partitions)
        => _clientFactory.Partitions = partitions;

    /// <summary>Sets the low/high watermarks reported for every partition.</summary>
    private void SetWatermarks(long low, long high)
        => _consumer.QueryWatermarkOffsets(Arg.Any<TopicPartition>(), Arg.Any<TimeSpan>())
            .Returns(new WatermarkOffsets(new Offset(low), new Offset(high)));

    /// <summary>
    /// Sets what the DLQ consumer group has committed. <see cref="Offset.Unset"/> means the group
    /// has never committed for that partition.
    /// </summary>
    private void SetCommitted(params (int Partition, long Offset)[] committed)
        => _consumer.Committed(Arg.Any<IEnumerable<TopicPartition>>(), Arg.Any<TimeSpan>())
            .Returns(committed
                .Select(c => new TopicPartitionOffset(TestDlqTopic, new Partition(c.Partition), new Offset(c.Offset)))
                .ToList());

    private static ConsumeResult<string, TestMessage> Entry(
        string key = "test-key",
        TestMessage? value = null,
        int partition = 0,
        long offset = 1,
        bool isInvalid = false,
        bool deserializationFailed = false,
        int reprocessAttempt = 0,
        string? error = null,
        string? originalTopic = TestOriginalTopic)
    {
        var headers = new Headers();

        if (originalTopic is not null)
        {
            headers.Add(KafkaHeaders.OriginalTopic, Encoding.UTF8.GetBytes(originalTopic));
        }

        if (isInvalid)
        {
            headers.Add(KafkaHeaders.InvalidMessage, Encoding.UTF8.GetBytes("true"));
        }

        if (deserializationFailed)
        {
            headers.Add(KafkaHeaders.DeserializationFailed, Encoding.UTF8.GetBytes("true"));
        }

        if (reprocessAttempt > 0)
        {
            headers.Add(KafkaHeaders.ReprocessedAttempt, Encoding.UTF8.GetBytes(reprocessAttempt.ToString()));
        }

        if (error is not null)
        {
            headers.Add(KafkaHeaders.ErrorMessage, Encoding.UTF8.GetBytes(error));
        }

        return new ConsumeResult<string, TestMessage>
        {
            Topic = TestDlqTopic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, TestMessage>
            {
                Key = key,
                Value = value ?? new TestMessage { Data = "dlq-data" },
                Headers = headers,
                Timestamp = new Timestamp(DateTime.UtcNow)
            },
            IsPartitionEOF = false
        };
    }

    /// <summary>A tombstone: a real record whose value is null.</summary>
    private static ConsumeResult<string, TestMessage> Tombstone(int partition = 0, long offset = 1)
    {
        var result = Entry(partition: partition, offset: offset);
        result.Message.Value = null!;
        return result;
    }

    /// <summary>The end-of-partition marker the inspector's consumer enables.</summary>
    private static ConsumeResult<string, TestMessage> Eof(int partition = 0) => new()
    {
        Topic = TestDlqTopic,
        Partition = new Partition(partition),
        Offset = new Offset(0),
        IsPartitionEOF = true
    };

    private static ConsumeException Poison(int partition = 0, long offset = 5, string? originalTopic = TestOriginalTopic)
    {
        var headers = new Headers();
        if (originalTopic is not null)
        {
            headers.Add(KafkaHeaders.OriginalTopic, Encoding.UTF8.GetBytes(originalTopic));
        }

        return new ConsumeException(
            new ConsumeResult<byte[], byte[]>
            {
                Topic = TestDlqTopic,
                Partition = new Partition(partition),
                Offset = new Offset(offset),
                Message = new Message<byte[], byte[]> { Headers = headers }
            },
            new Error(ErrorCode.Local_ValueDeserialization, "value deserialization failed"));
    }

    /// <summary>
    /// Returns the given items from successive Consume calls. A <see cref="ConsumeException"/> in
    /// the sequence is thrown rather than returned, standing in for a poison record.
    /// </summary>
    private void SetupConsumeSequence(params object[] items)
    {
        var callIndex = 0;
        _consumer.Consume(Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                if (callIndex >= items.Length)
                {
                    return null!;
                }

                var item = items[callIndex++];
                if (item is ConsumeException consumeException)
                {
                    throw consumeException;
                }

                return (ConsumeResult<string, TestMessage>)item;
            });
    }

    #endregion

    #region Depth

    [Fact]
    public async Task GetDepth_SumsPendingAcrossEveryPartition()
    {
        SetPartitions(0, 1, 2);
        SetWatermarks(low: 0, high: 100);
        SetCommitted((0, 90), (1, 80), (2, 100));
        var sut = CreateInspector();

        var depth = await sut.GetDepthAsync(_cts.Token);

        Assert.Equal(TestDlqTopic, depth.Topic);
        Assert.Equal(3, depth.Partitions.Count);
        Assert.Equal(30, depth.Pending);        // 10 + 20 + 0
        Assert.False(depth.IsEmpty);
    }

    [Fact]
    public async Task GetDepth_CoversEveryPartitionRegardlessOfAssignment()
    {
        // The whole point: partitions come from broker metadata, so a replica whose own DLQ consumer
        // owns nothing still reports the entire topic.
        SetPartitions(0, 1, 2);
        SetWatermarks(low: 0, high: 10);
        SetCommitted((0, 0), (1, 0), (2, 0));
        _consumer.Assignment.Returns([]);
        var sut = CreateInspector();

        var depth = await sut.GetDepthAsync(_cts.Token);

        Assert.Equal(30, depth.Pending);
    }

    [Fact]
    public async Task GetDepth_UncommittedPartitionResumesAtTheLowWatermark()
    {
        SetPartitions(0);
        SetWatermarks(low: 25, high: 100);
        SetCommitted((0, Offset.Unset));
        var sut = CreateInspector();

        var depth = await sut.GetDepthAsync(_cts.Token);

        // Earliest reset means an uncommitted sweep starts at the oldest retained record, not at 0.
        Assert.Equal(25, depth.Partitions[0].ResumesAt);
        Assert.Equal(75, depth.Pending);
    }

    [Fact]
    public async Task GetDepth_IsEmptyWhenEverythingIsCommitted()
    {
        SetPartitions(0, 1);
        SetWatermarks(low: 0, high: 42);
        SetCommitted((0, 42), (1, 42));
        var sut = CreateInspector();

        var depth = await sut.GetDepthAsync(_cts.Token);

        Assert.True(depth.IsEmpty);
        Assert.Equal(0, depth.Pending);
    }

    [Fact]
    public async Task GetDepth_NeverReportsNegativePending()
    {
        // A committed offset past the end of the log, e.g. after retention dropped records.
        SetPartitions(0);
        SetWatermarks(low: 500, high: 500);
        SetCommitted((0, 900));
        var sut = CreateInspector();

        var depth = await sut.GetDepthAsync(_cts.Token);

        Assert.Equal(0, depth.Pending);
    }

    [Fact]
    public async Task GetDepth_ReturnsEmptyWhenTheTopicDoesNotExist()
    {
        SetPartitions();
        var sut = CreateInspector();

        var depth = await sut.GetDepthAsync(_cts.Token);

        Assert.Empty(depth.Partitions);
        Assert.True(depth.IsEmpty);
    }

    [Fact]
    public async Task GetDepth_NeverCommitsOrStoresOffsets()
    {
        SetPartitions(0);
        SetWatermarks(low: 0, high: 10);
        SetCommitted((0, 5));
        var sut = CreateInspector();

        await sut.GetDepthAsync(_cts.Token);

        AssertReadOnly();
    }

    #endregion

    #region Peek - classification

    [Fact]
    public async Task Peek_ClassifiesARetryableMessageAndCountsRemainingAttempts()
    {
        SetCommitted((0, 0));
        SetupConsumeSequence(Entry(offset: 7, reprocessAttempt: 1, error: "boom"), Eof());
        var sut = CreateInspector();

        var entries = await sut.PeekAsync(cancellationToken: _cts.Token);

        var entry = Assert.Single(entries);
        Assert.Equal(DlqEntryState.Retryable, entry.State);
        Assert.Equal(1, entry.ReprocessAttempts);
        Assert.Equal(2, entry.RemainingAttempts);
        Assert.Equal("boom", entry.Error);
        Assert.Equal(TestOriginalTopic, entry.SourceTopic);
        Assert.Equal(7, entry.Offset);
        Assert.NotNull(entry.Message);
    }

    [Fact]
    public async Task Peek_ClassifiesAnInvalidMessage()
    {
        SetCommitted((0, 0));
        SetupConsumeSequence(Entry(isInvalid: true, reprocessAttempt: 0), Eof());
        var sut = CreateInspector();

        var entries = await sut.PeekAsync(cancellationToken: _cts.Token);

        // Invalid is permanent, so no attempts remain however few have been used.
        Assert.Equal(DlqEntryState.Invalid, Assert.Single(entries).State);
        Assert.Equal(0, entries[0].RemainingAttempts);
    }

    [Fact]
    public async Task Peek_ClassifiesAMessageWhoseAttemptsAreExhausted()
    {
        SetCommitted((0, 0));
        SetupConsumeSequence(Entry(reprocessAttempt: TestMaxReprocessAttempts), Eof());
        var sut = CreateInspector();

        var entries = await sut.PeekAsync(cancellationToken: _cts.Token);

        Assert.Equal(DlqEntryState.AttemptsExhausted, Assert.Single(entries).State);
        Assert.Equal(0, entries[0].RemainingAttempts);
    }

    [Fact]
    public async Task Peek_ClassifiesATombstone()
    {
        SetCommitted((0, 0));
        SetupConsumeSequence(Tombstone(offset: 3), Eof());
        var sut = CreateInspector();

        var entries = await sut.PeekAsync(cancellationToken: _cts.Token);

        var entry = Assert.Single(entries);
        Assert.Equal(DlqEntryState.Tombstone, entry.State);
        Assert.Null(entry.Message);
    }

    [Fact]
    public async Task Peek_ClassifiesACapturedPoisonRecordFromItsHeader()
    {
        SetCommitted((0, 0));
        SetupConsumeSequence(Entry(deserializationFailed: true), Eof());
        var sut = CreateInspector();

        var entries = await sut.PeekAsync(cancellationToken: _cts.Token);

        Assert.Equal(DlqEntryState.Undeserializable, Assert.Single(entries).State);
    }

    [Fact]
    public async Task Peek_ReportsAnUndeserializableRecordInsteadOfThrowing()
    {
        SetCommitted((0, 0));
        SetupConsumeSequence(Poison(offset: 5), Entry(offset: 6), Eof());
        var sut = CreateInspector();

        var entries = await sut.PeekAsync(cancellationToken: _cts.Token);

        // Surfacing what cannot be read is the point; the read also carries on past it.
        Assert.Equal(2, entries.Count);
        Assert.Equal(DlqEntryState.Undeserializable, entries[0].State);
        Assert.Equal(5, entries[0].Offset);
        Assert.Null(entries[0].Message);
        Assert.Equal("value deserialization failed", entries[0].Error);
        Assert.Equal(TestOriginalTopic, entries[0].SourceTopic);
        Assert.Equal(DlqEntryState.Retryable, entries[1].State);
    }

    #endregion

    #region Peek - bounds and ordering

    [Fact]
    public async Task Peek_StopsAtMaxMessages()
    {
        SetCommitted((0, 0));
        SetupConsumeSequence(Entry(offset: 1), Entry(offset: 2), Entry(offset: 3), Entry(offset: 4), Eof());
        var sut = CreateInspector();

        var entries = await sut.PeekAsync(new DlqPeekOptions { MaxMessages = 2 }, _cts.Token);

        Assert.Equal(2, entries.Count);
    }

    [Fact]
    public async Task Peek_EndsAsSoonAsEveryPartitionReachesTheEndOfItsLog()
    {
        SetPartitions(0, 1);
        SetCommitted((0, 0), (1, 0));
        SetupConsumeSequence(Entry(partition: 0, offset: 1), Eof(0), Entry(partition: 1, offset: 1), Eof(1));
        var sut = CreateInspector();

        // A generous budget that the read must not spend: EOF on both partitions ends it immediately.
        var entries = await sut.PeekAsync(
            new DlqPeekOptions { MaxMessages = 500, Timeout = TimeSpan.FromMinutes(5) }, _cts.Token);

        Assert.Equal(2, entries.Count);
    }

    [Fact]
    public async Task Peek_ReturnsEntriesInPartitionThenOffsetOrder()
    {
        SetPartitions(0, 1);
        SetCommitted((0, 0), (1, 0));
        SetupConsumeSequence(
            Entry(partition: 1, offset: 5),
            Entry(partition: 0, offset: 9),
            Entry(partition: 0, offset: 2),
            Eof(0), Eof(1));
        var sut = CreateInspector();

        var entries = await sut.PeekAsync(cancellationToken: _cts.Token);

        Assert.Equal([(0, 2L), (0, 9L), (1, 5L)], entries.Select(e => (e.Partition, e.Offset)));
    }

    [Fact]
    public async Task Peek_ReturnsEmptyWhenTheTopicDoesNotExist()
    {
        SetPartitions();
        var sut = CreateInspector();

        var entries = await sut.PeekAsync(cancellationToken: _cts.Token);

        Assert.Empty(entries);
    }

    #endregion

    #region Peek - assignment

    [Fact]
    public async Task Peek_AssignsEveryPartitionWhereTheSweepWouldResume()
    {
        SetPartitions(0, 1);
        SetCommitted((0, 40), (1, 70));
        SetupConsumeSequence(Eof(0), Eof(1));
        var sut = CreateInspector();

        await sut.PeekAsync(cancellationToken: _cts.Token);

        _consumer.Received(1).Assign(Arg.Is<IEnumerable<TopicPartitionOffset>>(assignment =>
            assignment.Count() == 2
            && assignment.Any(tpo => tpo.Partition.Value == 0 && tpo.Offset.Value == 40)
            && assignment.Any(tpo => tpo.Partition.Value == 1 && tpo.Offset.Value == 70)));
    }

    [Fact]
    public async Task Peek_AssignsAtTheRequestedOffsetWhenPaging()
    {
        SetPartitions(0, 1, 2);
        SetupConsumeSequence(Eof(1));
        var sut = CreateInspector();

        await sut.PeekAsync(new DlqPeekOptions { Partition = 1, FromOffset = 1500 }, _cts.Token);

        _consumer.Received(1).Assign(Arg.Is<IEnumerable<TopicPartitionOffset>>(assignment =>
            assignment.Count() == 1
            && assignment.Single().Partition.Value == 1
            && assignment.Single().Offset.Value == 1500));
    }

    [Fact]
    public async Task Peek_NarrowsToASinglePartitionWhenAsked()
    {
        SetPartitions(0, 1, 2);
        SetCommitted((2, 0));
        SetupConsumeSequence(Eof(2));
        var sut = CreateInspector();

        await sut.PeekAsync(new DlqPeekOptions { Partition = 2 }, _cts.Token);

        _consumer.Received(1).Assign(Arg.Is<IEnumerable<TopicPartitionOffset>>(
            assignment => assignment.Single().Partition.Value == 2));
    }

    [Fact]
    public async Task Peek_RejectsAnOffsetWithoutAPartition()
    {
        var sut = CreateInspector();

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            () => sut.PeekAsync(new DlqPeekOptions { FromOffset = 100 }, _cts.Token));

        Assert.Contains(nameof(DlqPeekOptions.Partition), ex.Message);
    }

    [Fact]
    public async Task Peek_RejectsAPartitionTheTopicDoesNotHave()
    {
        SetPartitions(0, 1);
        var sut = CreateInspector();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => sut.PeekAsync(new DlqPeekOptions { Partition = 7 }, _cts.Token));
    }

    #endregion

    #region Isolation from the sweep

    [Fact]
    public async Task Peek_NeverCommitsOrStoresOffsets()
    {
        SetCommitted((0, 0));
        SetupConsumeSequence(Entry(offset: 1), Entry(offset: 2), Eof());
        var sut = CreateInspector();

        await sut.PeekAsync(cancellationToken: _cts.Token);

        AssertReadOnly();
    }

    [Fact]
    public async Task Peek_NeverSubscribes()
    {
        // Subscribing under the DLQ consumer group id would trigger a rebalance and pull partitions
        // out from under a sweep in flight. Manual assignment is what keeps the inspector invisible.
        SetCommitted((0, 0));
        SetupConsumeSequence(Entry(), Eof());
        var sut = CreateInspector();

        await sut.PeekAsync(cancellationToken: _cts.Token);

        _consumer.DidNotReceive().Subscribe(Arg.Any<string>());
        _consumer.DidNotReceive().Subscribe(Arg.Any<IEnumerable<string>>());
    }

    [Fact]
    public async Task Peek_ClosesTheConsumerItOpened()
    {
        SetCommitted((0, 0));
        SetupConsumeSequence(Eof());
        var sut = CreateInspector();

        await sut.PeekAsync(cancellationToken: _cts.Token);

        _consumer.Received(1).Close();
    }

    [Fact]
    public async Task Peek_ClosesTheConsumerEvenWhenTheReadThrows()
    {
        SetPartitions(0);
        _consumer.Committed(Arg.Any<IEnumerable<TopicPartition>>(), Arg.Any<TimeSpan>())
            .Returns(_ => throw new KafkaException(ErrorCode.Local_TimedOut));
        var sut = CreateInspector();

        await Assert.ThrowsAsync<KafkaException>(() => sut.PeekAsync(cancellationToken: _cts.Token));

        _consumer.Received(1).Close();
    }

    #endregion

    #region Configuration

    [Fact]
    public async Task GetDepth_ThrowsClearlyWhenNoDeadLetterTopicIsConfigured()
    {
        var sut = CreateInspector(deadLetterTopic: null);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.GetDepthAsync(_cts.Token));

        Assert.Contains("AddKafkaWorkerDeadLetter", ex.Message);
    }

    [Fact]
    public async Task GetDepth_ResolvesTheStartOffsetFromDeadLetterStartFromWhenNothingIsCommitted()
    {
        var startFrom = DateTimeOffset.UtcNow.AddHours(-1);
        SetPartitions(0);
        SetWatermarks(low: 0, high: 100);
        SetCommitted((0, Offset.Unset));
        _consumer.OffsetsForTimes(Arg.Any<IEnumerable<TopicPartitionTimestamp>>(), Arg.Any<TimeSpan>())
            .Returns([new TopicPartitionOffset(TestDlqTopic, new Partition(0), new Offset(60))]);
        var sut = CreateInspector(startFrom: startFrom);

        var depth = await sut.GetDepthAsync(_cts.Token);

        // Pending counts from where the sweep would actually start, not from the low watermark.
        Assert.Equal(60, depth.Partitions[0].ResumesAt);
        Assert.Equal(40, depth.Pending);
    }

    #endregion

    /// <summary>
    /// The inspector's core promise: reading the dead letter topic never moves the sweep's position.
    /// </summary>
    private void AssertReadOnly()
    {
        _consumer.DidNotReceive().Commit();
        _consumer.DidNotReceive().Commit(Arg.Any<IEnumerable<TopicPartitionOffset>>());
        _consumer.DidNotReceive().Commit(Arg.Any<ConsumeResult<string, TestMessage>>());
        _consumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
        _consumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
    }
}
