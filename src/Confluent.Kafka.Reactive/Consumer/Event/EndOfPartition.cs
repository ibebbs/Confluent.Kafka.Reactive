namespace Confluent.Kafka.Reactive.Consumer.Event
{
    public class EndOfPartition : IEvent
    {
        public EndOfPartition(string topic, Partition partition, Offset offset)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
        }

        public string Topic { get; }
        public Partition Partition { get; }
        public Offset Offset { get; }
    }
}
