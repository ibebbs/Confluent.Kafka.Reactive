namespace Confluent.Kafka.Reactive.Consumer.Command
{
    public class Subscribe : ICommand
    {
        public Subscribe(string topic)
        {
            Topic = topic;
        }

        public string Topic { get; }
    }
}
