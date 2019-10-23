namespace Confluent.Kafka.Reactive.Producer.Event
{
    public class Delivered<TKey,TValue> : IEvent
    {
        public Delivered(DeliveryReport<TKey,TValue> report)
        {
            Report = report;
        }

        public DeliveryReport<TKey, TValue> Report { get; }
    }
}
