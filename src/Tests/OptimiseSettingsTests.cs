namespace Tests
{
    [TestFixture]
    public class OptimiseSettingsTests
    {
        [Test]
        public void given_fast_execution_I_expect_to_increase_settings()
        {
            // Set up
            var sut = new LinkerHelper();
            
            // Act
            var result = sut.OptimizeSettings(100, PerfTuneSettings.Default);

            // Verify
            Assert.IsTrue(PerfTuneSettings.Default.MaxBufferSize < result.MaxBufferSize);
        }
    }
}
