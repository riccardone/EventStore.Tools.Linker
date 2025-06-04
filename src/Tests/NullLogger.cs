using Linker;
using System;

namespace Tests;

public class NullLogger : ILinkerLogger
{
    public void Info(string message)
    {
        
    }

    public void Warn(string message)
    {
        
    }

    public void Warn(string message, Exception ex)
    {
        
    }

    public void Error(string message)
    {
        
    }

    public void Error(string message, Exception ex)
    {
        
    }

    public void Debug(string message)
    {
        
    }
}