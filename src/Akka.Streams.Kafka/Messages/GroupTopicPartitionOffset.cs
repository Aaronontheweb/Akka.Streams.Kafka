using System;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Messages
{
    /// <summary>
    /// Offset position for a groupId, topic, partition.
    /// </summary>
    public sealed record GroupTopicPartitionOffset(string GroupId, string Topic, int Partition, Offset Offset)
    {
        public GroupTopicPartitionOffset(GroupTopicPartition groupTopicPartition, Offset offset)
            : this(groupTopicPartition.GroupId, groupTopicPartition.Topic, groupTopicPartition.Partition, offset)
        {
        }

        /// <summary>
        /// Group topic partition info
        /// </summary>
        public GroupTopicPartition GroupTopicPartition => new(GroupId, Topic, Partition);
    }
    
    /// <summary>
    /// Group, topic and partition info
    /// </summary>
    public sealed record GroupTopicPartition(string GroupId, string Topic, int Partition);

    public sealed class OffsetAndMetadata : IEquatable<OffsetAndMetadata>
    {
        public OffsetAndMetadata(Offset offset, string metadata)
        {
            Offset = offset;
            Metadata = metadata;
        }

        /// <summary>
        /// Kafka partition offset value
        /// </summary>
        public Offset Offset { get; }
        /// <summary>
        /// Metadata
        /// </summary>
        public string Metadata { get; }

        public bool Equals(OffsetAndMetadata? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Offset.Equals(other.Offset) && Metadata == other.Metadata;
        }

        public override bool Equals(object? obj) => ReferenceEquals(this, obj) || obj is OffsetAndMetadata other && Equals(other);

        public override int GetHashCode()
        {
            unchecked
            {
                return (Offset.GetHashCode() * 397) ^ (Metadata != null ? Metadata.GetHashCode() : 0);
            }
        }
    }
}