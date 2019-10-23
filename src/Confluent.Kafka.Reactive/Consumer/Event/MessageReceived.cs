namespace Confluent.Kafka.Reactive.Consumer.Event
{
    public class MessageReceived<TKey, TValue> : IEvent
    {
        public MessageReceived(ConsumeResult<TKey, TValue> consumeResult)
        {
            ConsumeResult = consumeResult;
        }

        public ConsumeResult<TKey, TValue> ConsumeResult { get; }
    }
}
