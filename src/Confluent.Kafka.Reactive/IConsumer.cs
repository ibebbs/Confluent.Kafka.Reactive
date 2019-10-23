using System;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive
{
    public interface IConsumer<TKey, TValue> : IConnectableObservable<Consumer.IEvent>, IObserver<Consumer.ICommand>, IDisposable
    {
        void Subscribe(string topic);
    }
}
