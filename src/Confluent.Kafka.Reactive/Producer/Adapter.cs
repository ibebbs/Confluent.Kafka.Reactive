using System;
using System.Reactive.Concurrency;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive.Producer
{
    public interface IAdapter<TKey,TValue> : IDisposable
    {
        IDisposable Perform(Action<Kafka.IProducer<TKey, TValue>, IObserver<IEvent>> action, IScheduler scheduler);

        IObservable<IEvent> Events { get; }
    }

    internal class Adapter<TKey, TValue> : IAdapter<TKey, TValue>
    {
        private static readonly Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> NullModifier = pb => pb;

        private readonly Kafka.IProducer<TKey, TValue> _producer;
        private readonly Subject<IEvent> _events;

        public Adapter(ProducerConfig config, Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> modifier)
        {
            _producer = (modifier ?? NullModifier).Invoke(new ProducerBuilder<TKey, TValue>(config)).Build();
            _events = new Subject<IEvent>();
        }

        public void Dispose()
        {
            _producer.Dispose();
            _events.Dispose();
        }

        public IDisposable Perform(Action<Kafka.IProducer<TKey, TValue>, IObserver<IEvent>> action, IScheduler scheduler)
        {
            return scheduler.Schedule(() => action(_producer, _events));
        }

        public IObservable<IEvent> Events => _events;
    }
}
