using System;

namespace Linker
{
    public interface ILogger
    {
        void Info(string message);
        void Warn(string message);
        void Warn(string message, Exception ex);
        void Error(string message);
        void Error(string message, Exception ex);
        void Debug(string message);
    }
}
