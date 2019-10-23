namespace Confluent.Kafka.Reactive.Consumer.Event
{
    public class OffsetsCommitted : IEvent
    {
        public OffsetsCommitted(CommittedOffsets committedOffsets)
        {
            CommittedOffsets = committedOffsets;
        }

        public CommittedOffsets CommittedOffsets { get; }
    }
}
