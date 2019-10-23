namespace Confluent.Kafka.Reactive.Consumer.Command
{
    public class Assign : ICommand
    {
        public TopicPartition Topic { get; set; }

        public Offset? Offset { get; set; }
    }
}
