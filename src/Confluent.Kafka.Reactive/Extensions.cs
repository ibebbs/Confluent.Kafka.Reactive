namespace Confluent.Kafka.Reactive
{
    public static class Extensions
    {
        public static IConsumer<TKey,TValue> ToReactiveConsumer<TKey, TValue>(this ConsumerConfig config)
        {
            return new Consumer.Instance<TKey, TValue>(config);
        }
    }
}
