namespace Confluent.Kafka.Reactive.Consumer.Event
{
    public class StatisticsReceived : IEvent
    {
        public StatisticsReceived(string statistics)
        {
            Statistics = statistics;
        }

        public string Statistics { get; }
    }
}
