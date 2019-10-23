using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive.Consumer
{
    internal class Instance<TKey, TValue> : IConsumer<TKey,TValue>
    {
        private readonly ConsumerConfig _config;
        private readonly IScheduler _scheduler;
        private readonly Func<ConsumerConfig, IAdapter<TKey, TValue>> _adapterFactory;

        private readonly Subject<IEvent> _events;
        private readonly Subject<ICommand> _commands;
        private readonly Lazy<ImmediateRefCountDisposable> _connection;

        private static IAdapter<TKey, TValue> AdapterFactory(ConsumerConfig config, Func<ConsumerBuilder<TKey, TValue>, ConsumerBuilder<TKey, TValue>> modifier)
        {
            return new Adapter<TKey, TValue>(config, modifier);
        }

        public Instance(ConsumerConfig config, IScheduler scheduler, Func<ConsumerConfig, IAdapter<TKey, TValue>> adapterFactory)
        {
            _config = config;
            _scheduler = scheduler;
            _adapterFactory = adapterFactory;

            _events = new Subject<IEvent>();
            _commands = new Subject<ICommand>();

            _connection = new Lazy<ImmediateRefCountDisposable>(
                () => new ImmediateRefCountDisposable(Start())
            );
        }

        public Instance(ConsumerConfig config, Func<ConsumerBuilder<TKey,TValue>, ConsumerBuilder<TKey, TValue>> modifier) : this(config, new EventLoopScheduler(), c => AdapterFactory(c, modifier)) { }

        public void Dispose()
        {
            (_connection.IsValueCreated ? _connection.Value : null)?.Dispose();
        }

        private static Action<Kafka.IConsumer<TKey, TValue>, IObserver<IEvent>> Apply(Command.Commit<TKey, TValue> commit)
        {
            return (consumer, events) => consumer.Commit(commit.ConsumeResult);
        }

        private static Action<Kafka.IConsumer<TKey, TValue>, IObserver<IEvent>> Apply(Command.Subscribe subscription)
        {
            return (consumer, events) => consumer.Subscribe(subscription.Topic);
        }

        private static Action<Kafka.IConsumer<TKey, TValue>, IObserver<IEvent>> Apply(Command.Assign assignment)
        {
            return (consumer, events) =>
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

        private static Action<Kafka.IConsumer<TKey, TValue>, IObserver<IEvent>> Apply(Command.Seek seek)
        {
            return (consumer, events) => consumer.Seek(seek.Topic);
        }

        private static Action<Kafka.IConsumer<TKey, TValue>, IObserver<IEvent>> Apply(ICommand command)
        {
            switch (command)
            {
                case Command.Commit<TKey, TValue> commit: return Apply(commit);
                case Command.Subscribe subscription: return Apply(subscription);
                case Command.Assign assign: return Apply(assign);
                case Command.Seek seek: return Apply(seek);
                default: return (consumer, events) => { };
            }
        }

        private IDisposable Start()
        {
            return Observable
                .Create<IEvent>(
                    observer =>
                    {
                        var adapter = _adapterFactory(_config);

                        var commandSubscription = _commands
                            .Select(command => Apply(command))
                            .Subscribe(action => adapter.Perform(action, _scheduler));

                        var consumeLoop = Observable
                            .Defer(() => adapter.Consume(TimeSpan.FromMilliseconds(100), _scheduler))
                            .Repeat()
                            .Where(consumeResult => consumeResult != null)
                            .Publish();

                        var messageReceived = consumeLoop
                            .Where(consumeResult => !consumeResult.IsPartitionEOF)
                            .Select(consumeResult => new Event.MessageReceived<TKey, TValue>(consumeResult));

                        var endOfPartition = consumeLoop
                            .Where(consumeResult => consumeResult.IsPartitionEOF)
                            .Select(consumeResult => new Event.EndOfPartition(consumeResult.Topic, consumeResult.Partition, consumeResult.Offset));

                        var consumeSubscription = Observable
                            .Merge(adapter.Events, messageReceived, endOfPartition)
                            .Subscribe(Observer.Synchronize(observer));

                        return new CompositeDisposable(
                            consumeLoop.Connect(),
                            consumeSubscription,
                            commandSubscription,
                            adapter
                        );
                    })
                .Subscribe(_events);
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
