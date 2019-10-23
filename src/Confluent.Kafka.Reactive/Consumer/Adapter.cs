using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive.Consumer
{
    public interface IAdapter<TKey, TValue> : IDisposable
    {
        IObservable<ConsumeResult<TKey, TValue>> Consume(TimeSpan timeout, IScheduler scheduler);

        IObservable<Unit> Seek(Command.Seek seek, IScheduler scheduler);
        IObservable<Unit> Commit(Command.Commit<TKey, TValue> commit, IScheduler scheduler);
        IObservable<Unit> Subscribe(Command.Subscribe subscription, IScheduler scheduler);
        IObservable<Unit> Assign(Command.Assign assignment, IScheduler scheduler);

        IObservable<IEvent> Events { get; }

    }

    internal class Adapter<TKey, TValue> : IAdapter<TKey, TValue>
    {
        private static readonly Func<ConsumerBuilder<TKey, TValue>, ConsumerBuilder<TKey, TValue>> NullModifier = cb => cb;

        private readonly Kafka.IConsumer<TKey, TValue> _consumer;
        private readonly Subject<IEvent> _events;

        public Adapter(ConsumerConfig config, Func<ConsumerBuilder<TKey, TValue>, ConsumerBuilder<TKey, TValue>> modifier = null)
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
            _events.Dispose();
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

        public IObservable<Unit> Seek(Command.Seek seek, IScheduler scheduler)
        {
            return Observable.Start(() => _consumer.Seek(seek.Topic), scheduler);
        }

        public IObservable<Unit> Commit(Command.Commit<TKey, TValue> commit, IScheduler scheduler)
        {
            return Observable.Start(() => _consumer.Commit(commit.ConsumeResult), scheduler);
        }

        public IObservable<Unit> Subscribe(Command.Subscribe subscription, IScheduler scheduler)
        {
            return Observable.Start(() => _consumer.Subscribe(subscription.Topic), scheduler);
        }

        public IObservable<Unit> Assign(Command.Assign assignment, IScheduler scheduler)
        {
            if (assignment.Offset.HasValue)
            {
                return Observable.Start(() => _consumer.Assign(new TopicPartitionOffset(assignment.Topic, assignment.Offset.Value)), scheduler);
            }
            else
            {
                return Observable.Start(() => _consumer.Assign(assignment.Topic), scheduler);
            }
        }

        public IObservable<IEvent> Events => _events;
    }
}
