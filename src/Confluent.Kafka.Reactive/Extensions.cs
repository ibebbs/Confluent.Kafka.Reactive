using System;

namespace Confluent.Kafka.Reactive
{
    public static class Extensions
    {
        public static IConsumer<TKey,TValue> ToReactiveConsumer<TKey, TValue>(this ConsumerConfig config, Func<ConsumerBuilder<TKey,TValue>, ConsumerBuilder<TKey, TValue>> modifiers = null)
        {
            return new Consumer.Instance<TKey, TValue>(config, modifiers);
        }

        public static IConsumer<TKey, TValue> ToReactiveConsumer<TKey, TValue>(this ConsumerConfig config, Func<ConsumerConfig, Consumer.IAdapter<TKey, TValue>> adapterFactory)
        {
            return new Consumer.Instance<TKey, TValue>(config, null, adapterFactory);
        }

        public static IProducer<TKey, TValue> ToReactiveProducer<TKey, TValue>(this ProducerConfig config, Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> modifiers = null)
        {
            return new Producer.Instance<TKey, TValue>(config, modifiers);
        }
    }
}
