using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Est.GeoReplica;
using Est.GeoReplica.Plugin;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using Moq;
using NUnit.Framework;

namespace Estr.Tests
{
    public class ReplicaServiceTests
    {
        [Test]
        public void Replica_start_without_errors()
        {
            // Set up
            var subscriberBuilder = new Mock<IConnectionBuilder>();
            var connection = new Mock<IEventStoreConnection>();
            subscriberBuilder.Setup(a => a.Build()).Returns(connection.Object);
            var appenderBuilder = new Mock<IConnectionBuilder>();
            appenderBuilder.Setup(a => a.Build()).Returns(connection.Object);
            var positionRepo = new Mock<IPositionRepository>();
            var sut = new ReplicaService(subscriberBuilder.Object, appenderBuilder.Object, positionRepo.Object, null,
                1000, 1000);

            // Act
            var result = sut.Start().Result;

            // Verify
            Assert.IsTrue(result);
        }

        [Test]
        public void Replica_replicate_data()
        {
            // Set up
            var subscriberBuilder = new ConnectionBuilder(new Uri("tcp://localhost:1113"), ConnectionSettings.Default, "from",
                new UserCredentials("admin", "changeit"));
            var appenderBuilder = new ConnectionBuilder(new Uri("tcp://localhost:1114"), ConnectionSettings.Default, "to",
                new UserCredentials("admin", "changeit"));
            var positionBuilder = new ConnectionBuilder(new Uri("tcp://localhost:1114"), ConnectionSettings.Default, "to-for-position",
                new UserCredentials("admin", "changeit"));
            var positionRepo = new PositionRepository("PositionStream", "PositionUpdated", positionBuilder);
            var sut = new ReplicaService(subscriberBuilder, appenderBuilder, positionRepo, null,
                1000, 1000);

            var guid01 = AppendEvent("{name:'ric01'}", "test");
            var guid02 = AppendEvent("{name:'ric02'}", "test");

            // Act
            sut.Start().Wait();
            
            // Verify
            Assert.Pass();
        }

        private Guid AppendEvent(string body, string eventType)
        {
            var senderBuilder = new ConnectionBuilder(new Uri("tcp://localhost:1112"), ConnectionSettings.Default, "from",
                new UserCredentials("admin", "changeit"));
            var senderConn = senderBuilder.Build();
            senderConn.ConnectAsync().Wait();
            var guid = Guid.NewGuid();
            senderConn.AppendToStreamAsync("test", ExpectedVersion.Any,
                new List<EventData> { new EventData(guid, eventType, true, Encoding.ASCII.GetBytes(body), null) }).Wait();
            return guid;
        }
    }
}