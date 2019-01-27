using NUnit.Framework;

namespace ClickHouseClient.Test
{
    public class ConnectionSettingsTest
    {
        [TestCase("Host=localhost;Port=9000")]
        [TestCase("Host=localhost;Port=9000;")]
        [TestCase("Host =localhost;Port=9000")]
        [TestCase("Host= localhost;Port=9000")]
        [TestCase("Host=localhost;Port =9000;")]
        [TestCase("Host=localhost;Port= 9000;")]
        [TestCase("Host=localhost ;Port=9000;")]
        [TestCase("Host=localhost; Port=9000;")]
        [TestCase("Host   = localhost;Port   = 9000;")]
        [TestCase("Host=localhost     ;   Port=9000;")]
        [TestCase("      Host    =    localhost    ; Port =    9000  ;  ")]
        [TestCase("      Host    =    localhost    ; Port =    9000    ")]
        public void Parse_SimpleString(string connectionString)
        {
            var settings = new ConnectionSettings(connectionString);
            Assert.AreEqual("localhost", settings.Host);
            Assert.AreEqual(9000, settings.Port);
        }

        [Test]
        public void Parse_UnknownKey()
        {
            Assert.Throws<ConnectionStringException>(() => new ConnectionSettings("Qwerty123=qwerty"));
        }

        [TestCase("Port")]
        [TestCase("Host=localhost;Port")]
        public void Parse_KeyWithoutValue(string connectionString)
        {
            Assert.Throws<ConnectionStringException>(() => new ConnectionSettings(connectionString));
        }

        [TestCase("Host=localhost;Port=")]
        [TestCase("Host=localhost;Port=port")]
        public void Parse_InvalidIntegerValue(string connectionString)
        {
            Assert.Throws<ConnectionStringException>(() => new ConnectionSettings(connectionString));
        }

        [Test]
        public void ToString_SimpleString()
        {
            var settings = new ConnectionSettings("Host=localhost;Port=9000");
            Assert.AreEqual("Host=localhost;Port=9000;User=default;Database=default;", settings.ToString());
        }
    }
}
