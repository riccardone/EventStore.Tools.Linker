﻿using System;
using Linker;
using NLog;

namespace LinkerConsoleApp
{
    public class NLogger : ILinkerLogger
    {
        private static Logger Log = LogManager.GetCurrentClassLogger();
        public void Info(string message)
        {
            Log.Info(message);
        }

        public void Warn(string message)
        {
            Log.Warn(message);
        }

        public void Warn(string message, Exception ex)
        {
            Log.Warn($"{message}: {ex.GetBaseException().Message}");
        }

        public void Error(string message)
        {
            Log.Error(message);
        }

        public void Error(string message, Exception ex)
        {
            Log.Error($"{message}: {ex.GetBaseException().Message}");
        }

        public void Debug(string message)
        {
            Log.Debug(message);
        }
    }
}
