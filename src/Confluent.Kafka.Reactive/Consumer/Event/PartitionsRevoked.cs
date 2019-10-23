using System.Collections.Generic;

namespace Confluent.Kafka.Reactive.Consumer.Event
{
    public class PartitionsRevoked : IEvent
    {
        public PartitionsRevoked(IEnumerable<TopicPartitionOffset> partitions)
        {
            Partitions = partitions;
        }

        public IEnumerable<TopicPartitionOffset> Partitions { get; }
    }
}
