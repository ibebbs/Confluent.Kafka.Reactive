using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive.Consumer
{
    internal interface IWrapper<TKey, TValue> : IDisposable
    {
        IObservable<ConsumeResult<TKey, TValue>> Consume(TimeSpan timeout, IScheduler scheduler);

        IDisposable Perform(Action<Kafka.IConsumer<TKey, TValue>> action, IScheduler scheduler);

        IObservable<IEvent> Events { get; }
    }

    internal class Wrapper<TKey, TValue> : IWrapper<TKey, TValue>
    {
        private static readonly Func<ConsumerBuilder<TKey, TValue>, ConsumerBuilder<TKey, TValue>> NullModifier = cb => cb;

        private readonly Kafka.IConsumer<TKey, TValue> _consumer;
        private readonly Subject<IEvent> _events;

        public Wrapper(ConsumerConfig config, Func<ConsumerBuilder<TKey, TValue>, ConsumerBuilder<TKey, TValue>> modifier)
        {
            _consumer = (modifier ?? NullModifier).Invoke(new ConsumerBuilder<TKey, TValue>(config))
                .SetPartitionsAssignedHandler(PartitionsAssignedHandler)
                .SetPartitionsRevokedHandler(PartitionsRevokedHandler)
                .SetOffsetsCommittedHandler(OffsetsCommittedHandler)
                .SetStatisticsHandler(StatisticsHandler)
                .SetLogHandler(LogMessageHandler)
                .Build();

            _events = new Subject<IEvent>();
        }

        public void Dispose()
        {
            _consumer.Dispose();
        }

        private void PartitionsAssignedHandler(Kafka.IConsumer<TKey, TValue> consumer, List<TopicPartition> partitions)
        {
            if (consumer.Equals(_consumer))
            {
                _events.OnNext(new Event.PartitionsAssigned(partitions));
            }
        }

        private void PartitionsRevokedHandler(Kafka.IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> partitions)
        {
            if (consumer.Equals(_consumer))
            {
                _events.OnNext(new Event.PartitionsRevoked(partitions));
            }
        }

        private void OffsetsCommittedHandler(Kafka.IConsumer<TKey, TValue> consumer, CommittedOffsets committedOffsets)
        {
            if (consumer.Equals(_consumer))
            {
                _events.OnNext(new Event.OffsetsCommitted(committedOffsets));
            }
        }

        private void StatisticsHandler(Kafka.IConsumer<TKey, TValue> consumer, string statistics)
        {
            if (consumer.Equals(_consumer))
            {
                _events.OnNext(new Event.StatisticsReceived(statistics));
            }
        }

        private void LogMessageHandler(Kafka.IConsumer<TKey, TValue> consumer, LogMessage logMessage)
        {
            if (consumer.Equals(_consumer))
            {
                _events.OnNext(new Event.LogMessageReceived(logMessage));
            }
        }

        public IObservable<ConsumeResult<TKey, TValue>> Consume(TimeSpan timeout, IScheduler scheduler)
        {
            return Observable.Start(() => _consumer.Consume(timeout), scheduler);
        }

        public IDisposable Perform(Action<Kafka.IConsumer<TKey, TValue>> action, IScheduler scheduler)
        {
            return scheduler.Schedule(() => action(_consumer));
        }

        public IObservable<IEvent> Events => _events;
    }
}
