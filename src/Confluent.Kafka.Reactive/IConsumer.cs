using System;
using System.Reactive.Subjects;

namespace Confluent.Kafka.Reactive
{
    public interface IConsumer<TKey, TValue> : IConnectableObservable<Consumer.IEvent>, IObserver<Consumer.ICommand>
    {
        void Subscribe(string topic);
    }
}
