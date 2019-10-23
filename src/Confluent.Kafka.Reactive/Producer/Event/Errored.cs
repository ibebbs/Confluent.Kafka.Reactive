namespace Confluent.Kafka.Reactive.Producer.Event
{
    public class Errored : IEvent
    {
        public Errored(Error error)
        {
            Error = error;
        }

        public Error Error { get; }
    }
}
