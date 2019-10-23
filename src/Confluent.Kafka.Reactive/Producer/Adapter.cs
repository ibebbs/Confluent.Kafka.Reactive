using System;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive.Producer
{
    public interface IAdapter<TKey,TValue> : IDisposable
    {
        IObservable<IEvent> Produce(Command.Produce<TKey, TValue> produce, IScheduler scheduler);

        IObservable<IEvent> Flush(Command.Flush flush, IScheduler scheduler);

        IObservable<IEvent> Events { get; }
    }

    internal class Adapter<TKey, TValue> : IAdapter<TKey, TValue>
    {
        private static readonly Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> NullModifier = pb => pb;

        private readonly Kafka.IProducer<TKey, TValue> _producer;

        // Asynchronous or unsolicited events are raised through this subject
        private readonly Subject<IEvent> _events;

        public Adapter(ProducerConfig config, Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> modifier = null)
        {
            _producer = (modifier ?? NullModifier).Invoke(new ProducerBuilder<TKey, TValue>(config))
                .SetErrorHandler(ErrorHandler)
                .SetLogHandler(LogHandler)
                .SetStatisticsHandler(StatisticsHandler)
                .Build();
            _events = new Subject<IEvent>();
        }

        public void Dispose()
        {
            _producer.Dispose();
            _events.Dispose();
        }

        private void StatisticsHandler(Kafka.IProducer<TKey, TValue> producer, string statistics)
        {
            if (producer.Equals(_producer))
            {
                _events.OnNext(new Event.StatisticsReceived(statistics));
            }
        }

        private void LogHandler(Kafka.IProducer<TKey, TValue> producer, LogMessage message)
        {
            if (producer.Equals(_producer))
            {
                _events.OnNext(new Event.Log(message));
            }
        }

        private void ErrorHandler(Kafka.IProducer<TKey, TValue> producer, Error error)
        {
            if (producer.Equals(_producer))
            {
                _events.OnNext(new Event.Errored(error));
            }
        }

        public IObservable<IEvent> Produce(Command.Produce<TKey, TValue> produce, IScheduler scheduler)
        {
            return Observable
                .Start(() => _producer.Produce(produce.Topic, produce.Message, dr => _events.OnNext(new Event.Delivered<TKey, TValue>(dr))), scheduler)
                .SelectMany(_ => Observable.Empty<IEvent>());
        }

        public IObservable<IEvent> Flush(Command.Flush flush, IScheduler scheduler)
        {
            return Observable
                .Start(() => _producer.Flush(flush.Timeout), scheduler)
                .Select(count => new Event.Flushed(count));
        }

        public IObservable<IEvent> Events => _events;
    }
}
