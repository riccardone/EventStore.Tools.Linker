using System;

namespace Linker
{
    public class SimpleConsoleLogger : ILinkerLogger
    {
        private readonly string _moduleName;

        public SimpleConsoleLogger(string moduleName)
        {
            _moduleName = moduleName;
        }

        public void Info(string message)
        {
            Console.WriteLine($"{DateTime.Now:F}|{_moduleName}|{message}");
        }

        public void Warn(string message)
        {
            Console.WriteLine($"{DateTime.Now:F}|{_moduleName}|{message}");
        }

        public void Warn(string message, Exception ex)
        {
            Console.WriteLine($"{DateTime.Now:F}|{_moduleName}|{message}");
            Console.WriteLine($"{DateTime.Now:F}|{_moduleName}|{ex.GetBaseException().Message}");
        }

        public void Error(string message)
        {
            Console.WriteLine($"{DateTime.Now:F}|{_moduleName}|{message}");
        }

        public void Error(string message, Exception ex)
        {
            Console.WriteLine($"{DateTime.Now:F}|{_moduleName}|{message}");
            Console.WriteLine($"{DateTime.Now:F}|{_moduleName}|{ex.GetBaseException().Message}");
        }

        public void Debug(string message)
        {
            Console.WriteLine($"{DateTime.Now:F}|{_moduleName}|{message}");
        }
    }
}
