using System;
using Linker;
using Microsoft.Extensions.Logging;

namespace SimpleClient;

public class SimpleLogger : ILinkerLogger
{
    private readonly ILogger _logger;

    public SimpleLogger(ILogger logger)
    {
        _logger = logger;
    }

    public void Info(string message)
    {
        _logger.LogInformation(message);
    }

    public void Warn(string message)
    {
        _logger.LogWarning(message);
    }

    public void Warn(string message, Exception ex)
    {
        _logger.LogWarning(ex, message);
    }

    public void Error(string message)
    {
        _logger.LogError(message);
    }

    public void Error(string message, Exception ex)
    {
        _logger.LogError(ex, message);
    }

    public void Debug(string message)
    {
        _logger.LogDebug(message);
    }
}