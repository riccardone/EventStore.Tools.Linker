using System;
using System.IO;
using Est.CrossClusterReplication.Contracts;
using Est.CrossClusterReplication.Model;
using Newtonsoft.Json;
using NLog;

namespace Est.CrossClusterReplication
{
    public class ConfigFromFile : IConfigProvider
    {
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();
        private readonly string _configPath;

        public ConfigFromFile(string configPath)
        {
            _configPath = configPath;
        }

        public Root GetSettings()
        {
            try
            {
                return JsonConvert.DeserializeObject<Root>(File.ReadAllText(_configPath));
            }
            catch (FileNotFoundException)
            {
                Log.Info("GeoReplica Configuration file not found");
            }
            catch (Exception ex)
            {
                Log.Error($"GeoReplica Unhandled exception while reading configuration file: {ex.GetBaseException().Message}");
            }
            return null;
        }
    }
}
