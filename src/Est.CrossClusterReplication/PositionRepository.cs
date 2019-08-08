using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using Est.CrossClusterReplication.Contracts;
using EventStore.ClientAPI;
using Newtonsoft.Json;
using NLog;

namespace Est.CrossClusterReplication
{
    public class PositionRepository : IPositionRepository
    {
        private static readonly Logger Log = LogManager.GetCurrentClassLogger();
        private readonly string _positionStreamName;
        private readonly IConnectionBuilder _anotherConnectionBuilder;
        public string PositionEventType { get; }
        private IEventStoreConnection _connection;
        private static Timer _timer;
        private Position _position = Position.Start;
        private Position _lastSavedPosition = Position.Start;

        public PositionRepository(string positionStreamName, string positionEventType, IConnectionBuilder anotherConnectionBuilder, int interval = 1000)
        {
            _positionStreamName = positionStreamName;
            _anotherConnectionBuilder = anotherConnectionBuilder;
            PositionEventType = positionEventType;
            _timer = new Timer(interval);
            _timer.Elapsed += _timer_Elapsed;
            _timer.Enabled = true;
        }

        private void _timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            if (_lastSavedPosition.Equals(_position))
                return;
            _connection.AppendToStreamAsync(_positionStreamName, ExpectedVersion.Any,
                new[] { new EventData(Guid.NewGuid(), PositionEventType, true, SerializeObject(_position), null) },
                _anotherConnectionBuilder.Credentials);
            _lastSavedPosition = _position;
        }

        public async Task Start()
        {
            _connection?.Close();
            _connection = EventStoreConnection.Create(_anotherConnectionBuilder.ConnectionSettings,
                _anotherConnectionBuilder.ConnectionString, _anotherConnectionBuilder.ConnectionName);
            _connection.Connected += _connection_Connected;
            _connection.ErrorOccurred += _connection_ErrorOccurred;
            _connection.Disconnected += _connection_Disconnected;
            await _connection.ConnectAsync();
        }

        private void _connection_Disconnected(object sender, ClientConnectionEventArgs e)
        {
            Log.Warn($"Disconnected '{e.Connection.ConnectionName}'");
            Stop();
            Start();
        }

        public void Stop()
        {
            _connection.Connected -= _connection_Connected;
            _connection.ErrorOccurred -= _connection_ErrorOccurred;
            _connection.Disconnected -= _connection_Disconnected;
            _connection?.Close();
            _timer.Stop();
            Log.Info("PositionRepository stopped");
        }

        private void InitStream()
        {
            try
            {
                _connection?.SetStreamMetadataAsync(_positionStreamName, ExpectedVersion.Any,
                    SerializeObject(new Dictionary<string, int> { { "$maxCount", 1 } }),
                    _anotherConnectionBuilder.Credentials);
            }
            catch (Exception ex)
            {
                Log.Warn("Error while initializing stream {error}", ex.GetBaseException().Message);
            }
        }

        public Position Get()
        {
            try
            {
                var evts = _connection.ReadStreamEventsBackwardAsync(_positionStreamName, StreamPosition.End, 10, true, _anotherConnectionBuilder.Credentials).Result;
                _position = evts.Events.Any()
                    ? DeserializeObject<Position>(evts.Events[0].OriginalEvent.Data)
                    : Position.Start;
            }
            catch (Exception e)
            {
                Log.Warn($"Error while reading the position: {e.GetBaseException().Message}");
            }
            return _position;
        }

        public void Set(Position position)
        {
            _position = position;
        }

        private void _connection_ErrorOccurred(object sender, ClientErrorEventArgs e)
        {
            Log.Warn($"Error while using position repo connection: {e.Exception.GetBaseException().Message}");
        }

        private void _connection_Connected(object sender, ClientConnectionEventArgs e)
        {
            Log.Info("PositionRepository connected");
            _timer.Start();
            InitStream();
        }

        private static byte[] SerializeObject(object obj)
        {
            var jsonObj = JsonConvert.SerializeObject(obj);
            var data = Encoding.UTF8.GetBytes(jsonObj);
            return data;
        }

        private static T DeserializeObject<T>(byte[] data)
        {
            return JsonConvert.DeserializeObject<T>(Encoding.ASCII.GetString(data));
        }
    }
}
