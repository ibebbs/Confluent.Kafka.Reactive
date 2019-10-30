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

        private static IAdapter<TKey, TValue> AdapterFactory(ConsumerConfig config)
        {
            return new Adapter<TKey, TValue>(config);
        }

        public Instance(ConsumerConfig config, IScheduler scheduler, Func<ConsumerConfig, IAdapter<TKey, TValue>> adapterFactory)
        {
            _config = config;
            _scheduler = scheduler ?? new EventLoopScheduler();
            _adapterFactory = adapterFactory ?? AdapterFactory;

            _events = new Subject<IEvent>();
            _commands = new Subject<ICommand>();

            _connection = new Lazy<ImmediateRefCountDisposable>(
                () => new ImmediateRefCountDisposable(Start())
            );
        }

        public Instance(ConsumerConfig config, Func<ConsumerBuilder<TKey,TValue>, ConsumerBuilder<TKey, TValue>> modifier) : this(config, null, c => AdapterFactory(c, modifier)) { }

        public void Dispose()
        {
            (_connection.IsValueCreated ? _connection.Value : null)?.Dispose();
        }

        private Func<IAdapter<TKey, TValue>, IObservable<IEvent>> Apply(Command.Commit<TKey, TValue> commit)
        {
            return adapter => adapter.Commit(commit, _scheduler);
        }

        private Func<IAdapter<TKey, TValue>, IObservable<IEvent>> Apply(Command.Subscribe subscription)
        {
            return adapter => adapter.Subscribe(subscription, _scheduler);
        }

        private Func<IAdapter<TKey, TValue>, IObservable<IEvent>> Apply(Command.Assign assignment)
        {
            return adapter => adapter.Assign(assignment, _scheduler);
        }

        private Func<IAdapter<TKey, TValue>, IObservable<IEvent>> Apply(Command.Seek seek)
        {
            return adapter => adapter.Seek(seek, _scheduler);
        }

        private Func<IAdapter<TKey,TValue>, IObservable<IEvent>> Apply(ICommand command)
        {
            switch (command)
            {
                case Command.Commit<TKey, TValue> commit: return Apply(commit);
                case Command.Subscribe subscription: return Apply(subscription);
                case Command.Assign assign: return Apply(assign);
                case Command.Seek seek: return Apply(seek);
                default: return adapter => Observable.Empty<IEvent>();
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
                            .SelectMany(action => action(adapter))
                            .Subscribe(_events);

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
                            .Do(@event => @event.Log<TKey,TValue>())
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
