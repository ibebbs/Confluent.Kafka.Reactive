using System;

namespace Confluent.Kafka.Reactive.Producer.Command
{
    public class Flush : ICommand
    {
        public Flush(TimeSpan timeout)
        {
            Timeout = timeout;
        }

        public TimeSpan Timeout { get; }
    }
}
