namespace Confluent.Kafka.Reactive.Producer.Event
{
    public class Log : IEvent
    {
        public Log(LogMessage message)
        {
            Message = message;
        }

        public LogMessage Message { get; }
    }
}
