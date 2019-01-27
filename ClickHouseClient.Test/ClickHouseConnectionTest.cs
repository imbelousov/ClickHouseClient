using System.Data;
using System.Threading.Tasks;
using NUnit.Framework;

namespace ClickHouseClient.Test
{
    [TestFixture]
    public class ClickHouseConnectionTest
    {
        [Test]
        public void OpenAndClose()
        {
            var connection = new ClickHouseConnection();
            connection.Open();
            Assert.AreEqual(ConnectionState.Open, connection.State);
            connection.Close();
            Assert.AreEqual(ConnectionState.Closed, connection.State);
        }

        [Test]
        public async Task OpenAndCloseAsync()
        {
            var connection = new ClickHouseConnection();
            await connection.OpenAsync();
            Assert.AreEqual(ConnectionState.Open, connection.State);
            connection.Close();
            Assert.AreEqual(ConnectionState.Closed, connection.State);
        }
    }
}
