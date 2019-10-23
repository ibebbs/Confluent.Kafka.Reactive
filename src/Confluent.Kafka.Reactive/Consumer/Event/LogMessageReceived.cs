namespace Confluent.Kafka.Reactive.Consumer.Event
{
    public class LogMessageReceived : IEvent
    {
        public LogMessageReceived(LogMessage logMessage)
        {
            LogMessage = logMessage;
        }

        public LogMessage LogMessage { get; }
    }
}
