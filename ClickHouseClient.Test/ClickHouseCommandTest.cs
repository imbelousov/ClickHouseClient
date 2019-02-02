using NUnit.Framework;

namespace ClickHouseClient.Test
{
    [TestFixture]
    public class ClickHouseCommandTest
    {
        private ClickHouseConnection _connection;
        private ClickHouseCommand _command;

        [SetUp]
        public void SetUp()
        {
            _connection = new ClickHouseConnection();
            _connection.Open();
            _command = (ClickHouseCommand) _connection.CreateCommand();
        }

        [TearDown]
        public void TearDown()
        {
            _connection.Dispose();
        }

        [TestCase("select cast(1 as Int8)")]
        [TestCase("select cast(-1 as Int8)")]
        [TestCase("select cast(1 as UInt8)")]
        [TestCase("select cast(1 as Int16)")]
        [TestCase("select cast(-1 as Int16)")]
        [TestCase("select cast(1 as UInt16)")]
        [TestCase("select cast(1 as Int32)")]
        [TestCase("select cast(-1 as Int32)")]
        [TestCase("select cast(1 as UInt32)")]
        [TestCase("select cast(1 as Int64)")]
        [TestCase("select cast(-1 as Int64)")]
        [TestCase("select cast(1 as UInt64)")]
        [TestCase("select cast(1 as Float32)")]
        [TestCase("select cast(-1.33 as Float32)")]
        [TestCase("select cast(1 as Float64)")]
        [TestCase("select cast(-1.33 as Float64)")]
        public void ExecuteNonQuery_ColumnTypes(string sql)
        {
            _command.CommandText = sql;
            _command.ExecuteNonQuery();
        }

        [Test]
        public void ExecuteNonQuery_CreateTable()
        {
            _command.CommandText = "create table if not exists my_test_table (id Int32) engine MergeTree order by id";
            _command.ExecuteNonQuery();
        }

        [Test]
        public void ExecuteNonQuery_Exception()
        {
            _command.CommandText = "qwerty";
            Assert.Throws<ClickHouseException>(() => _command.ExecuteNonQuery());
        }
    }
}
