using System;

namespace Confluent.Kafka.Reactive
{
    public interface IProducer<TKey, TValue> : IDisposable
    {
        IDisposable Connect();

        IObserver<Producer.ICommand> Commands { get; }
    }
}
