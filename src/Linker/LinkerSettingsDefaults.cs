﻿namespace Linker;

public static class LinkerSettingsDefaults
{
    public const int SynchronisationInterval = 1000;
    public const bool HandleConflicts = false;
    public const int StatsInterval = 1000;
    public const int MaxBufferSize = 10;
    public const int MaxLiveQueue = 500;
    public const int ReadBatchSize = 5;
    public const bool ResolveLinkTos = true;
}