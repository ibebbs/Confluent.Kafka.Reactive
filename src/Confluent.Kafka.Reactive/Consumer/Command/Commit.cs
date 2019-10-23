namespace Confluent.Kafka.Reactive.Consumer.Command
{
    public class Commit<TKey, TValue> : ICommand
    {
        public Commit(ConsumeResult<TKey, TValue> consumeResult)
        {
            ConsumeResult = consumeResult;
        }

        public ConsumeResult<TKey, TValue> ConsumeResult { get; }
    }
}
