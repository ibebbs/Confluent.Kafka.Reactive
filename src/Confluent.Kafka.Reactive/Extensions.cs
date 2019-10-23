using System;

namespace Confluent.Kafka.Reactive
{
    public static class Extensions
    {
        public static IConsumer<TKey,TValue> ToReactiveConsumer<TKey, TValue>(this ConsumerConfig config, Func<ConsumerBuilder<TKey,TValue>, ConsumerBuilder<TKey, TValue>> modifiers = null)
        {
            return new Consumer.Instance<TKey, TValue>(config, modifiers);
        }
    }
}
