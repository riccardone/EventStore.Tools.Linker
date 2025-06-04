using Microsoft.Extensions.Logging;

namespace Linker;

public class Logger : ILinkerLogger
{
    private readonly string _moduleName;
    private readonly ILogger _logger;

    private Logger(string moduleName)
    {
        _moduleName = moduleName;
    }

    public Logger(string linkerServiceName, ILoggerFactory loggerFactory) : this(linkerServiceName)
    {
        _logger = loggerFactory.CreateLogger(linkerServiceName);
    }

    public void Info(string message)
    {
        _logger.LogInformation($"{DateTime.Now:F}|{_moduleName}|{message}");
    }

    public void Warn(string message)
    {
        _logger.LogInformation($"{DateTime.Now:F}|{_moduleName}|{message}");
    }

    public void Warn(string message, Exception ex)
    {
        _logger.LogInformation($"{DateTime.Now:F}|{_moduleName}|{message}");
        _logger.LogInformation($"{DateTime.Now:F}|{_moduleName}|{ex.GetBaseException().Message}");
    }

    public void Error(string message)
    {
        _logger.LogInformation($"{DateTime.Now:F}|{_moduleName}|{message}");
    }

    public void Error(string message, Exception ex)
    {
        _logger.LogInformation($"{DateTime.Now:F}|{_moduleName}|{message}");
        _logger.LogInformation($"{DateTime.Now:F}|{_moduleName}|{ex.GetBaseException().Message}");
    }

    public void Debug(string message)
    {
        _logger.LogInformation($"{DateTime.Now:F}|{_moduleName}|{message}");
    }
}