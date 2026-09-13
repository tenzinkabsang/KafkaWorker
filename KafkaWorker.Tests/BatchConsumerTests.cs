using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;

namespace KafkaWorker.Tests;

/// <summary>
/// Covers the batch consumption path: draining, dispatch, the per-message fallback after a failed
/// batch, per-partition offset storage, and the synchronous commit at each batch boundary.
/// </summary>
public class BatchConsumerTests : IDisposable
{
    private const string TestTopic = "test-topic";
    private const string TestDlqTopic = "test-dlq-topic";

    private readonly IConsumer<string, TestMessage> _kafkaConsumer;
    private readonly IProducer<string, TestMessage> _deadLetterProducer;
    private readonly IProducer<byte[], byte[]> _rawDeadLetterProducer;
    private readonly IMessageHandler<TestMessage> _messageHandler;
    private readonly IBatchMessageHandler<TestMessage> _batchHandler;
    private readonly ILogger<Consumer<string, TestMessage>> _logger;
    private readonly KafkaWorkerMetrics _metrics;
    private readonly CancellationTokenSource _cts;

    public BatchConsumerTests()
    {
        _kafkaConsumer = Substitute.For<IConsumer<string, TestMessage>>();
        _deadLetterProducer = Substitute.For<IProducer<string, TestMessage>>();
        _rawDeadLetterProducer = Substitute.For<IProducer<byte[], byte[]>>();
        _messageHandler = Substitute.For<IMessageHandler<TestMessage>>();
        _batchHandler = Substitute.For<IBatchMessageHandler<TestMessage>>();
        _logger = Substitute.For<ILogger<Consumer<string, TestMessage>>>();
        _logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
        _metrics = new KafkaWorkerMetrics();
        _cts = new CancellationTokenSource();
    }

    public void Dispose()
    {
        _kafkaConsumer.Dispose();
        _deadLetterProducer.Dispose();
        _rawDeadLetterProducer.Dispose();
        _cts.Dispose();
        _metrics.Dispose();
        GC.SuppressFinalize(this);
    }

    #region Helpers

    public class TestMessage
    {
        public string Data { get; set; } = string.Empty;
    }

    private Consumer<string, TestMessage> CreateConsumer(
        string? deadLetterTopic = TestDlqTopic,
        int maxRetries = 0,
        int maxBatchSize = 100,
        int batchLingerMs = 0)
    {
        var config = new KafkaWorkerConfig
        {
            GroupId = "test-group",
            Topic = TestTopic,
            DeadLetterTopic = deadLetterTopic,
            MaxRetries = maxRetries,
            MaxBatchSize = maxBatchSize,
            BatchLingerMs = batchLingerMs
        };

        var optionsMonitor = Substitute.For<IOptionsMonitor<KafkaWorkerConfig>>();
        optionsMonitor.Get(typeof(TestMessage).FullName).Returns(config);

        var serviceProvider = Substitute.For<IServiceProvider>();
        serviceProvider.GetService(typeof(IMessageHandler<TestMessage>)).Returns(_messageHandler);
        serviceProvider.GetService(typeof(IBatchMessageHandler<TestMessage>)).Returns(_batchHandler);
        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(serviceProvider);
        var scopeFactory = Substitute.For<IServiceScopeFactory>();
        scopeFactory.CreateScope().Returns(scope);

        return new Consumer<string, TestMessage>(
            _kafkaConsumer,
            new Lazy<IProducer<string, TestMessage>>(() => _deadLetterProducer),
            new RawDeadLetterProducer<TestMessage>(() => _rawDeadLetterProducer),
            scopeFactory,
            optionsMonitor,
            _metrics,
            new BatchConsumerOptions<TestMessage>(isEnabled: true),
            _logger);
    }

    private static ConsumeResult<string, TestMessage> Message(string key, long offset, int partition = 0)
        => new()
        {
            Topic = TestTopic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, TestMessage>
            {
                Key = key,
                Value = new TestMessage { Data = key }
            },
            IsPartitionEOF = false
        };

    private static ConsumeResult<string, TestMessage> Tombstone(long offset, int partition = 0)
        => new()
        {
            Topic = TestTopic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, TestMessage> { Key = "tombstone", Value = null! },
            IsPartitionEOF = false
        };

    /// <summary>
    /// Returns each result in turn from the timeout-based Consume overload the batch loop uses, then
    /// cancels and returns null so the drain ends and the loop exits.
    /// </summary>
    private void SetupBatchConsumeSequence(params ConsumeResult<string, TestMessage>[] results)
    {
        var callIndex = 0;
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                if (callIndex < results.Length)
                {
                    return results[callIndex++];
                }

                // The first exhaustion ends the drain so the batch is processed; only the next one
                // cancels. Cancelling here would hand processing an already-cancelled token, which
                // legitimately aborts retries and DLQ publishing.
                callIndex++;
                if (callIndex > results.Length + 1)
                {
                    _cts.Cancel();
                }

                return null!;
            });
    }

    private async Task RunAsync(Consumer<string, TestMessage> sut)
    {
        await sut.StartAsync(_cts.Token);
        while (!_cts.Token.IsCancellationRequested)
        {
            await Task.Delay(20);
        }
        await sut.StopAsync(CancellationToken.None);
    }

    #endregion

    #region Dispatch

    [Fact]
    public async Task BatchMode_HandsEveryDrainedMessageToTheHandlerInOneCall()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("a", 10), Message("b", 11), Message("c", 12));

        await RunAsync(sut);

        await _batchHandler.Received(1).HandleBatchAsync(
            Arg.Is<IReadOnlyList<TestMessage>>(b => b.Count == 3
                && b[0].Data == "a" && b[1].Data == "b" && b[2].Data == "c"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task BatchMode_NeverExceedsMaxBatchSize()
    {
        using var sut = CreateConsumer(maxBatchSize: 2);
        SetupBatchConsumeSequence(Message("a", 1), Message("b", 2), Message("c", 3), Message("d", 4));

        await RunAsync(sut);

        await _batchHandler.DidNotReceive().HandleBatchAsync(
            Arg.Is<IReadOnlyList<TestMessage>>(b => b.Count > 2), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task BatchMode_DoesNotInvokeSingleMessageHandlerWhenBatchSucceeds()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("a", 1), Message("b", 2));

        await RunAsync(sut);

        await _messageHandler.DidNotReceive().HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region Offsets and commits

    [Fact]
    public async Task BatchMode_StoresHighestOffsetPlusOneForThePartition()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("a", 10), Message("b", 11), Message("c", 12));

        await RunAsync(sut);

        // The offset store holds the NEXT offset to commit, so the highest consumed offset + 1.
        _kafkaConsumer.Received(1).StoreOffset(
            Arg.Is<TopicPartitionOffset>(tpo => tpo.Partition.Value == 0 && tpo.Offset.Value == 13));
    }

    [Fact]
    public async Task BatchMode_StoresOffsetsPerPartitionForABatchSpanningPartitions()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(
            Message("a", 5, partition: 0),
            Message("b", 40, partition: 1),
            Message("c", 6, partition: 0),
            Message("d", 41, partition: 1));

        await RunAsync(sut);

        _kafkaConsumer.Received(1).StoreOffset(
            Arg.Is<TopicPartitionOffset>(tpo => tpo.Partition.Value == 0 && tpo.Offset.Value == 7));
        _kafkaConsumer.Received(1).StoreOffset(
            Arg.Is<TopicPartitionOffset>(tpo => tpo.Partition.Value == 1 && tpo.Offset.Value == 42));
    }

    [Fact]
    public async Task BatchMode_CommitsSynchronouslyAtTheBatchBoundary()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("a", 1), Message("b", 2));

        await RunAsync(sut);

        _kafkaConsumer.Received(1).Commit();
    }

    [Fact]
    public async Task BatchMode_SurvivesACommitFailure()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("a", 1));
        _kafkaConsumer.When(c => c.Commit()).Throw(new KafkaException(ErrorCode.RebalanceInProgress));

        await RunAsync(sut);

        // The batch was handled and the host was not brought down by the failed commit.
        await _batchHandler.Received(1).HandleBatchAsync(Arg.Any<IReadOnlyList<TestMessage>>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task BatchMode_StoresNothingWhenCancelledMidBatch()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("a", 1), Message("b", 2));
        _batchHandler.HandleBatchAsync(Arg.Any<IReadOnlyList<TestMessage>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                _cts.Cancel();
                throw new OperationCanceledException(_cts.Token);
            });

        await RunAsync(sut);

        // A batch abandoned at shutdown must be redelivered, never committed past.
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<ConsumeResult<string, TestMessage>>());
    }

    #endregion

    #region Fallback to the per-message path

    [Fact]
    public async Task FailedBatch_ReprocessesEveryMessageIndividually()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("a", 1), Message("b", 2), Message("c", 3));
        _batchHandler.HandleBatchAsync(Arg.Any<IReadOnlyList<TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("batch write failed"));

        await RunAsync(sut);

        await _messageHandler.Received(1).HandleMessageAsync(Arg.Is<TestMessage>(m => m.Data == "a"), Arg.Any<CancellationToken>());
        await _messageHandler.Received(1).HandleMessageAsync(Arg.Is<TestMessage>(m => m.Data == "b"), Arg.Any<CancellationToken>());
        await _messageHandler.Received(1).HandleMessageAsync(Arg.Is<TestMessage>(m => m.Data == "c"), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FailedBatch_DeadLettersOnlyTheMessageThatActuallyFails()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("good-1", 1), Message("bad", 2), Message("good-2", 3));
        _batchHandler.HandleBatchAsync(Arg.Any<IReadOnlyList<TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("batch write failed"));
        _messageHandler.HandleMessageAsync(Arg.Is<TestMessage>(m => m.Data == "bad"), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("this one is genuinely broken"));

        await RunAsync(sut);

        // One bad message must not drag the rest of its batch into the DLQ.
        await _deadLetterProducer.Received(1).ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m => m.Key == "bad"),
            Arg.Any<CancellationToken>());
        await _deadLetterProducer.DidNotReceive().ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m => m.Key != "bad"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FailedBatch_StoresOffsetsOneAtATimeRatherThanForTheWholeBatch()
    {
        var first = Message("a", 1);
        var second = Message("b", 2);
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(first, second);
        _batchHandler.HandleBatchAsync(Arg.Any<IReadOnlyList<TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("batch write failed"));

        await RunAsync(sut);

        _kafkaConsumer.Received(1).StoreOffset(first);
        _kafkaConsumer.Received(1).StoreOffset(second);
        _kafkaConsumer.DidNotReceive().StoreOffset(Arg.Any<TopicPartitionOffset>());
    }

    [Fact]
    public async Task FailedBatch_RetriesEachMessageAccordingToMaxRetries()
    {
        using var sut = CreateConsumer(maxRetries: 2);
        SetupBatchConsumeSequence(Message("a", 1));
        _batchHandler.HandleBatchAsync(Arg.Any<IReadOnlyList<TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("batch write failed"));
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("still failing"));

        await RunAsync(sut);

        // The per-message path owns retry: 1 initial attempt + MaxRetries.
        await _messageHandler.Received(3).HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FailedBatch_RoutesInvalidMessagesStraightToTheDlq()
    {
        using var sut = CreateConsumer(maxRetries: 3);
        SetupBatchConsumeSequence(Message("a", 1));
        _batchHandler.HandleBatchAsync(Arg.Any<IReadOnlyList<TestMessage>>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad data"));
        _messageHandler.HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidMessageException("bad data"));

        await RunAsync(sut);

        // InvalidMessageException regains its meaning in the fallback: no retry, straight to the DLQ.
        await _messageHandler.Received(1).HandleMessageAsync(Arg.Any<TestMessage>(), Arg.Any<CancellationToken>());
        await _deadLetterProducer.Received(1).ProduceAsync(
            TestDlqTopic,
            Arg.Is<Message<string, TestMessage>>(m => m.Headers.Any(h => h.Key == KafkaHeaders.InvalidMessage)),
            Arg.Any<CancellationToken>());
    }

    #endregion

    #region Tombstones and poison messages

    [Fact]
    public async Task Tombstones_AreFilteredOutBeforeTheHandlerSeesThem()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("a", 1), Tombstone(2), Message("b", 3));

        await RunAsync(sut);

        await _batchHandler.Received(1).HandleBatchAsync(
            Arg.Is<IReadOnlyList<TestMessage>>(b => b.Count == 2 && b.All(m => m != null)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Tombstones_AdvanceTheOffsetWithTheirBatch()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Message("a", 1), Message("b", 2), Tombstone(3));

        await RunAsync(sut);

        // The tombstone's offset is covered by the batch's stored offset, not stored during the drain.
        _kafkaConsumer.Received(1).StoreOffset(
            Arg.Is<TopicPartitionOffset>(tpo => tpo.Offset.Value == 4));
    }

    [Fact]
    public async Task TombstoneOnlyBatch_SkipsTheHandlerButStillAdvances()
    {
        using var sut = CreateConsumer();
        SetupBatchConsumeSequence(Tombstone(7), Tombstone(8));

        await RunAsync(sut);

        await _batchHandler.DidNotReceive().HandleBatchAsync(Arg.Any<IReadOnlyList<TestMessage>>(), Arg.Any<CancellationToken>());
        _kafkaConsumer.Received(1).StoreOffset(
            Arg.Is<TopicPartitionOffset>(tpo => tpo.Offset.Value == 9));
    }

    [Fact]
    public async Task PoisonMessage_IsSkippedOnlyAfterTheBatchAheadOfItIsHandled()
    {
        var poison = new ConsumeException(
            new ConsumeResult<byte[], byte[]>
            {
                Topic = TestTopic,
                Partition = new Partition(0),
                Offset = new Offset(3),
                Message = new Message<byte[], byte[]> { Key = [1], Value = [2] }
            },
            new Error(ErrorCode.Local_ValueDeserialization));

        using var sut = CreateConsumer();
        var callIndex = 0;
        var results = new[] { Message("a", 1), Message("b", 2) };
        _kafkaConsumer.Consume(Arg.Any<TimeSpan>())
            .Returns(_ =>
            {
                if (callIndex < results.Length)
                {
                    return results[callIndex++];
                }

                if (callIndex++ == results.Length)
                {
                    throw poison;
                }

                _cts.Cancel();
                return null!;
            });

        await RunAsync(sut);

        Received.InOrder(() =>
        {
            // The accumulated batch must be committed before the poison record's offset is stored,
            // or the skip would commit past messages that were never processed.
            _kafkaConsumer.StoreOffset(Arg.Is<TopicPartitionOffset>(tpo => tpo.Offset.Value == 3));
            _kafkaConsumer.Commit();
            _kafkaConsumer.StoreOffset(Arg.Is<TopicPartitionOffset>(tpo => tpo.Offset.Value == 4));
            _kafkaConsumer.Commit();
        });
    }

    #endregion
}
