namespace Confluent.Kafka.Reactive.Producer.Event
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
