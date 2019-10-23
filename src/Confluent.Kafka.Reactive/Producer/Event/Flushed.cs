namespace Confluent.Kafka.Reactive.Producer.Event
{
    public class Flushed : IEvent
    {
        public Flushed(int count)
        {
            Count = count;
        }

        public int Count { get; }
    }
}
