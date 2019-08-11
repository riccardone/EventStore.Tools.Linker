using System;

namespace Est.CrossClusterReplication
{
    public class PerfTuneSettings : ICloneable
    {
        public int MaxBufferSize { get; }
        public int MaxLiveQueue { get; }
        public int ReadBatchSize { get; }

        public PerfTuneSettings(int maxBufferSize, int maxLiveQueue, int readBatchSize)
        {
            MaxBufferSize = maxBufferSize;
            MaxLiveQueue = maxLiveQueue;
            ReadBatchSize = readBatchSize;
        }

        public static PerfTuneSettings Default => new PerfTuneSettings(10, 500, 5);

        public override string ToString()
        {
            return $"MaxBufferSize: {MaxBufferSize}, MaxLiveQueue: {MaxLiveQueue}, ReadBatchSize: {ReadBatchSize}";
        }

        public object Clone()
        {
            return new PerfTuneSettings(MaxBufferSize, MaxLiveQueue, ReadBatchSize);
        }

        public override bool Equals(object obj)
        {
            if (!(obj is PerfTuneSettings item))
                return false;
            return MaxBufferSize == item.MaxBufferSize &&
                   MaxLiveQueue == item.MaxLiveQueue &&
                   ReadBatchSize == item.ReadBatchSize;
        }

        protected bool Equals(PerfTuneSettings other)
        {
            return MaxBufferSize == other.MaxBufferSize && MaxLiveQueue == other.MaxLiveQueue &&
                   ReadBatchSize == other.ReadBatchSize;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = MaxBufferSize;
                hashCode = (hashCode * 397) ^ MaxLiveQueue;
                hashCode = (hashCode * 397) ^ ReadBatchSize;
                return hashCode;
            }
        }
    }
}
