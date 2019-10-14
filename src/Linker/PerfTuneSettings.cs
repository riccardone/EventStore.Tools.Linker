using System;

namespace Linker
{
    public class PerfTuneSettings : ICloneable
    {
        public int MaxBufferSize { get; }
        public int MaxLiveQueue { get; }
        public int ReadBatchSize { get; }
        public bool ResolveLinkTos { get; }

        public PerfTuneSettings(int maxBufferSize, int maxLiveQueue, int readBatchSize, bool resolveLinkTos)
        {
            MaxBufferSize = maxBufferSize;
            MaxLiveQueue = maxLiveQueue;
            ReadBatchSize = readBatchSize;
            ResolveLinkTos = resolveLinkTos;
        }

        public static PerfTuneSettings Default => new PerfTuneSettings(Settings.Default().MaxBufferSize,
            Settings.Default().MaxLiveQueue, Settings.Default().ReadBatchSize, Settings.Default().ResolveLinkTos);

        public override string ToString()
        {
            return $"MaxBufferSize: {MaxBufferSize}, MaxLiveQueue: {MaxLiveQueue}, ReadBatchSize: {ReadBatchSize}";
        }

        public object Clone()
        {
            return new PerfTuneSettings(MaxBufferSize, MaxLiveQueue, ReadBatchSize, ResolveLinkTos);
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
