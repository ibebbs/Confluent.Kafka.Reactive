using System;
using System.Collections.Generic;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive.Consumer
{
    internal class Instance<TKey, TValue> : IConsumer<TKey,TValue>
    {
        private readonly ConsumerBuilder<TKey, TValue> _consumerBuilder;
        private readonly Func<ConsumerBuilder<TKey, TValue>, Kafka.IConsumer<TKey, TValue>> _consumerFactory;
        private readonly IScheduler _scheduler;
        private readonly Subject<IEvent> _events;
        private readonly Subject<ICommand> _commands;
        private readonly Lazy<RefCountDisposable> _connection;

        public Instance(ConsumerConfig config, IScheduler scheduler = null, Func<ConsumerBuilder<TKey, TValue>, Kafka.IConsumer<TKey, TValue>> consumerFactory = null)
        {
            _scheduler = scheduler ?? new EventLoopScheduler();

            _events = new Subject<IEvent>();
            _commands = new Subject<ICommand>();

            _consumerBuilder = new ConsumerBuilder<TKey, TValue>(config)
                .SetPartitionsAssignedHandler(PartitionsAssignedHandler)
                .SetPartitionsRevokedHandler(PartitionsRevokedHandler)
                .SetOffsetsCommittedHandler(OffsetsCommittedHandler)
                .SetStatisticsHandler(StatisticsHandler)
                .SetLogHandler(LogMessageHandler);

            _consumerFactory = consumerFactory ?? new Func<ConsumerBuilder<TKey, TValue>, Kafka.IConsumer<TKey, TValue>>(builder => builder.Build());

            _connection = new Lazy<RefCountDisposable>(
                () => new RefCountDisposable(CreateObservable().Subscribe(_events))
            );
        }

        private static Action<Kafka.IConsumer<TKey, TValue>> Apply(Command.Commit<TKey, TValue> commit)
        {
            return consumer => consumer.Commit(commit.ConsumeResult);
        }

        private static Action<Kafka.IConsumer<TKey, TValue>> Apply(Command.Subscribe subscription)
        {
            return consumer => consumer.Subscribe(subscription.Topic);
        }

        private static Action<Kafka.IConsumer<TKey, TValue>> Apply(Command.Assign assignment)
        {
            return consumer =>
            {
                if (assignment.Offset.HasValue)
                {
                    consumer.Assign(new TopicPartitionOffset(assignment.Topic, assignment.Offset.Value));
                }
                else
                {
                    consumer.Assign(assignment.Topic);
                }
            };
        }

        private static Action<Kafka.IConsumer<TKey, TValue>> Apply(Command.Seek seek)
        {
            return consumer => consumer.Seek(seek.Topic);
        }

        private static Action<Kafka.IConsumer<TKey, TValue>> Apply(ICommand command)
        {
            switch (command)
            {
                case Command.Commit<TKey, TValue> commit: return Apply(commit);
                case Command.Subscribe subscription: return Apply(subscription);
                case Command.Assign assign: return Apply(assign);
                case Command.Seek seek: return Apply(seek);
                default: return consumer => { };
            }
        }

        private IObservable<IEvent> CreateObservable()
        {
            return Observable.Create<IEvent>(
                observer =>
                {
                    var consumer = _consumerFactory(_consumerBuilder);

                    var commitSubscription = _commands
                        .Select(command => Apply(command))
                        .ObserveOn(_scheduler)
                        .Subscribe(action => action(consumer));

                    var consumeLoop = Observable
                        .Defer(() => Observable
                            .Start(() => consumer.Consume(TimeSpan.FromMilliseconds(100)), _scheduler))
                        .Repeat()
                        .Where(consumeResult => consumeResult != null)
                        .Publish();

                    var messageReceived = consumeLoop
                        .Where(consumeResult => !consumeResult.IsPartitionEOF)
                        .Select(consumeResult => new Event.MessageReceived<TKey, TValue>(consumeResult));

                    var endOfPartition = consumeLoop
                        .Where(consumeResult => consumeResult.IsPartitionEOF)
                        .Select(consumeResult => new Event.EndOfPartition(consumeResult.Topic, consumeResult.Partition, consumeResult.Offset));

                    var consumeSubscription = Observable.Merge<IEvent>(messageReceived, endOfPartition).Subscribe(observer);

                    return new CompositeDisposable(
                        consumeLoop.Connect(),
                        consumeSubscription,
                        commitSubscription,
                        consumer
                    );
                }
            );
        }

        private void PartitionsAssignedHandler(Kafka.IConsumer<TKey, TValue> consumer, List<TopicPartition> partitions)
        {
            _events.OnNext(new Event.PartitionsAssigned(partitions));
        }

        private void PartitionsRevokedHandler(Kafka.IConsumer<TKey, TValue> consumer, List<TopicPartitionOffset> partitions)
        {
            _events.OnNext(new Event.PartitionsRevoked(partitions));
        }

        private void OffsetsCommittedHandler(Kafka.IConsumer<TKey, TValue> consumer, CommittedOffsets committedOffsets)
        {
            _events.OnNext(new Event.OffsetsCommitted(committedOffsets));
        }

        private void StatisticsHandler(Kafka.IConsumer<TKey, TValue> consumer, string statistics)
        {
            _events.OnNext(new Event.StatisticsReceived(statistics));
        }

        private void LogMessageHandler(Kafka.IConsumer<TKey, TValue> consumer, LogMessage logMessage)
        {
            _events.OnNext(new Event.LogMessageReceived(logMessage));
        }

        void IObserver<ICommand>.OnCompleted()
        {
            _commands.OnCompleted();
        }

        void IObserver<ICommand>.OnError(Exception error)
        {
            _commands.OnError(error);
        }

        void IObserver<ICommand>.OnNext(ICommand command)
        {
            _commands.OnNext(command);
        }

        public IDisposable Connect()
        {
            return _connection.Value.GetDisposable();
        }

        public IDisposable Subscribe(IObserver<IEvent> observer)
        {
            return _events.Subscribe(observer);
        }

        public void Subscribe(string topic)
        {
            _commands.OnNext(new Command.Subscribe(topic));
        }
    }
}
