namespace Confluent.Kafka.Reactive.Consumer.Command
{
    public class Seek : ICommand
    {
        public TopicPartitionOffset Topic { get; set; }
    }
}
