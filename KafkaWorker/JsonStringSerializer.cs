using System.Text.Json;
using Confluent.Kafka;

namespace KafkaWorker;

internal class JsonStringSerializer<T> : ISerializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
    {
        return JsonSerializer.SerializeToUtf8Bytes(data);
    }
}