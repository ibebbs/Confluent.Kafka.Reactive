using System.Collections.Generic;

namespace Confluent.Kafka.Reactive.Consumer.Event
{
    public class PartitionsAssigned : IEvent
    {
        public PartitionsAssigned(IEnumerable<TopicPartition> partitions)
        {
            Partitions = partitions;
        }

        public IEnumerable<TopicPartition> Partitions { get; }
    }
}
