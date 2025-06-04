using EventStore.PositionRepository.Gprc;
using KurrentDB.Client;
using Linker;
using Moq;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Tests;

public class LinkerServiceTests
{
    [Test]
    public void Replica_start_without_errors()
    {
        // Set up
        var originBuilder = new Mock<Linker.ILinkerConnectionBuilder>();
        var connection = new Mock<KurrentDBClient>();
        originBuilder.Setup(a => a.Build()).Returns(connection.Object);
        var destinationBuilder = new Mock<Linker.ILinkerConnectionBuilder>();
        destinationBuilder.Setup(a => a.Build()).Returns(connection.Object);
        var positionRepo = new Mock<IPositionRepository>();
        var sut = new LinkerService(originBuilder.Object, destinationBuilder.Object, positionRepo.Object, null,
            Settings.Default(), new NullLogger());

        // Act
        var result = sut.Start().Result;

        // Verify
        ClassicAssert.IsTrue(result);
    }
}