using System;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive.Producer
{
    internal class Instance<TKey, TValue> : IProducer<TKey, TValue>
    {
        private static readonly Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> NullModifier = cb => cb;

        private readonly ProducerConfig _config;
        private readonly Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> _modifier;
        private readonly Subject<ICommand> _commands;
        private readonly Lazy<ImmediateRefCountDisposable> _connection;

        public Instance(ProducerConfig config, Func<ProducerBuilder<TKey, TValue>, ProducerBuilder<TKey, TValue>> modifier)
        {
            _config = config;
            _modifier = modifier ?? NullModifier;
            _commands = new Subject<ICommand>();
            _connection = new Lazy<ImmediateRefCountDisposable>(() => new ImmediateRefCountDisposable(Subscribe()));
        }

        private IDisposable Subscribe()
        {
            var producer = _modifier(new ProducerBuilder<TKey, TValue>(_config)).Build();

            var produce = _commands
                .OfType<Command.Produce<TKey, TValue>>()
                .Subscribe(production => producer.Produce(production.Topic, production.Message));

            return new CompositeDisposable(
                produce,
                producer
            );
        }

        public IDisposable Connect()
        {
            return _connection.Value.GetDisposable();
        }

        public IObserver<ICommand> Commands => _commands;
    }
}
