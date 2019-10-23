using System;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive.Producer
{
    internal class Instance<TKey, TValue> : IProducer<TKey, TValue>
    {
        private static IAdapter<TKey, TValue> AdapterFactory(ProducerConfig config, Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> modifier)
        {
            return new Adapter<TKey, TValue>(config, modifier);
        }

        private readonly ProducerConfig _config;
        private readonly IScheduler _scheduler;
        private readonly Func<ProducerConfig, IAdapter<TKey, TValue>> _adapterFactory;

        private readonly Subject<ICommand> _commands;
        private readonly Subject<IEvent> _events;

        private readonly Lazy<ImmediateRefCountDisposable> _connection;

        public Instance(ProducerConfig config, IScheduler scheduler, Func<ProducerConfig, IAdapter<TKey, TValue>> adapterFactory)
        {
            _config = config;
            _scheduler = scheduler ?? new EventLoopScheduler();
            _adapterFactory = adapterFactory;

            _commands = new Subject<ICommand>();
            _events = new Subject<IEvent>();

            _connection = new Lazy<ImmediateRefCountDisposable>(() => new ImmediateRefCountDisposable(Start()));
        }

        public Instance(ProducerConfig config, Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> modifier) : this(config, null, c => AdapterFactory(c, modifier)) { }

        public void Dispose()
        {
            (_connection.IsValueCreated ? _connection.Value : null)?.Dispose();
            _commands.Dispose();
            _events.Dispose();
        }

        private Func<IAdapter<TKey, TValue>, IObservable<IEvent>> Apply(Command.Produce<TKey, TValue> produce)
        {
            return adapter => adapter.Produce(produce, _scheduler);
        }

        private Func<IAdapter<TKey, TValue>, IObservable<IEvent>> Apply(Command.Flush flush)
        {
            return adapter => adapter.Flush(flush, _scheduler);
        }

        private Func<IAdapter<TKey, TValue>, IObservable<IEvent>> Apply(ICommand command)
        {
            switch (command)
            {
                case Command.Produce<TKey, TValue> produce: return Apply(produce);
                case Command.Flush flush: return Apply(flush);
                default: return adapter => Observable.Empty<IEvent>();
            }
        }

        private IDisposable Start()
        {
            var adapter = _adapterFactory(_config);

            var eventSubscription = adapter.Events
                .Subscribe(_events);

            var commandSubscription = _commands
                .Select(command => Apply(command))
                .SelectMany(action => action(adapter))
                .Subscribe(_events);

            return new CompositeDisposable(
                commandSubscription,
                eventSubscription,
                adapter
            );
        }

        public IDisposable Connect()
        {
            return _connection.Value.GetDisposable();
        }

        public IObservable<IEvent> Events => _events;

        public IObserver<ICommand> Commands => _commands;
    }
}
