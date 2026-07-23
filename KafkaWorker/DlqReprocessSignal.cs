using System.Threading.Channels;

namespace KafkaWorker;

/// <summary>
/// Channel-backed implementation of <see cref="IDlqReprocessTrigger{TMessage}"/>.
/// A bounded channel of capacity 1 with <see cref="BoundedChannelFullMode.DropWrite"/>
/// coalesces concurrent or repeated triggers into a single pending batch.
/// </summary>
internal sealed class DlqReprocessSignal<TMessage> : IDlqReprocessTrigger<TMessage> where TMessage : class
{
    private readonly Channel<bool> _channel = Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.DropWrite
    });

    /// <summary>The reader the DLQ consumer waits on alongside its interval timer.</summary>
    public ChannelReader<bool> Reader => _channel.Reader;

    public void Trigger() => _channel.Writer.TryWrite(true);
}
