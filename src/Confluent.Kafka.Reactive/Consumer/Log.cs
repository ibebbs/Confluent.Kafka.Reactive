using System.Diagnostics.Tracing;
using System.Threading;

namespace Confluent.Kafka.Reactive.Consumer
{
    [EventSource(Name = "Confluent-Kafka-Reactive-Consumer")]
    public sealed class Log : EventSource
    {
        public static readonly Log Event = new Log();

        private readonly IncrementingEventCounter _messagesReceived;
        private readonly EventCounter _partitionsAssigned;

        private int _partionCount;

        public Log()
        {
            _messagesReceived = new IncrementingEventCounter("MessagesReceived", this);
            _partitionsAssigned = new EventCounter("PartitionsAssigned", this);
        }

        [Event(1, Message = "Assigned partition '{1}' of topic '{0}'")]
        public void AssignedPartition(string topic, int partition)
        {
            Interlocked.Increment(ref _partionCount);
            _partitionsAssigned.WriteMetric(_partionCount);

            if (IsEnabled(EventLevel.Informational, EventKeywords.None))
            {
                WriteEvent(1, topic, partition);
            }
        }

        [Event(2, Message = "Assigned partition '{1}' of topic '{0}'")]
        public void PartitionRevoked(string topic, int partition)
        {
            Interlocked.Decrement(ref _partionCount);
            _partitionsAssigned.WriteMetric(_partionCount);

            if (IsEnabled(EventLevel.Informational, EventKeywords.None))
            {
                WriteEvent(2, topic, partition);
            }
        }

        [Event(3, Message = "End of partition '{1}' of topic '{0}' at '{2}'")]
        public void EndOfPartition(string topic, int partition, long at)
        {
            if (IsEnabled(EventLevel.Informational, EventKeywords.None))
            {
                WriteEvent(3, topic, partition, at);
            }
        }

        [Event(4, Message = "Offsets committed for partition '{1}' of topic '{0}' at '{2}'")]
        public void OffsetsCommitted(string topic, int partition, long at)
        {
            if (IsEnabled(EventLevel.Informational, EventKeywords.None))
            {
                WriteEvent(4, topic, partition, at);
            }
        }

        [Event(5, Message = "Message received for partition '{1}' of topic '{0}' at '{2}'")]
        public void MessageReceived(string topic, int partition, long at)
        {
            _messagesReceived.Increment();

            if (IsEnabled(EventLevel.Verbose, EventKeywords.None))
            {
                WriteEvent(5, topic, partition, at);
            }
        }
    }

    public static class LogHelper
    {
        public static void Log(this Event.PartitionsAssigned @event)
        {
            foreach (var partition in @event.Partitions)
            {
                Consumer.Log.Event.AssignedPartition(partition.Topic, partition.Partition.Value);
            }
        }

        public static void Log(this Event.PartitionsRevoked @event)
        {
            foreach (var partition in @event.Partitions)
            {
                Consumer.Log.Event.PartitionRevoked(partition.Topic, partition.Partition.Value);
            }
        }

        public static void Log(this Event.EndOfPartition @event)
        {
            Consumer.Log.Event.EndOfPartition(@event.Topic, @event.Partition.Value, @event.Offset.Value);
        }

        public static void Log(this Event.OffsetsCommitted @event)
        {
            foreach (var committed in @event.CommittedOffsets.Offsets)
            {
                Consumer.Log.Event.OffsetsCommitted(committed.Topic, committed.Partition.Value, committed.Offset.Value);
            }
        }

        public static void Log<TKey, TValue>(this Event.MessageReceived<TKey, TValue> @event)
        {
            Consumer.Log.Event.MessageReceived(@event.ConsumeResult.Topic, @event.ConsumeResult.Partition.Value, @event.ConsumeResult.Offset.Value);
        }

        public static void Log<TKey,TValue>(this IEvent @event)
        {
            if (Consumer.Log.Event.IsEnabled())
            {
                switch (@event)
                {
                    case Event.PartitionsAssigned partitionAssigned:
                        partitionAssigned.Log();
                        break;
                    case Event.PartitionsRevoked partitionRevoked:
                        partitionRevoked.Log();
                        break;
                    case Event.EndOfPartition endOfPartition:
                        endOfPartition.Log();
                        break;
                    case Event.OffsetsCommitted offsetsCommitted:
                        offsetsCommitted.Log();
                        break;
                    case Event.MessageReceived<TKey, TValue> messageReceived:
                        messageReceived.Log();
                        break;
                }
            }
        }
    }
}
