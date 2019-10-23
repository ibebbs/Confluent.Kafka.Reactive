namespace Confluent.Kafka.Reactive.Producer.Command
{
    public class Produce<TKey, TValue> : ICommand
    {
        public Produce(string topic, Message<TKey, TValue> message)
        {
            Topic = topic;
            Message = message;
        }

        public string Topic { get; }

        public Message<TKey, TValue> Message { get; }
    }
}
