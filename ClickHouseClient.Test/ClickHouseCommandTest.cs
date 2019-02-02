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
        [TestCase("select cast(1 as Nullable(UInt8))")]
        [TestCase("select cast(null as Nullable(UInt8))")]
        [TestCase("select cast(1 as Nullable(Int8))")]
        [TestCase("select cast(null as Nullable(Int8))")]
        [TestCase("select cast(1 as Int16)")]
        [TestCase("select cast(-1 as Int16)")]
        [TestCase("select cast(1 as UInt16)")]
        [TestCase("select cast(1 as Nullable(UInt16))")]
        [TestCase("select cast(null as Nullable(UInt16))")]
        [TestCase("select cast(1 as Nullable(Int16))")]
        [TestCase("select cast(null as Nullable(Int16))")]
        [TestCase("select cast(1 as Int32)")]
        [TestCase("select cast(-1 as Int32)")]
        [TestCase("select cast(1 as UInt32)")]
        [TestCase("select cast(1 as Nullable(UInt32))")]
        [TestCase("select cast(null as Nullable(UInt32))")]
        [TestCase("select cast(1 as Nullable(Int32))")]
        [TestCase("select cast(null as Nullable(Int32))")]
        [TestCase("select cast(1 as Int64)")]
        [TestCase("select cast(-1 as Int64)")]
        [TestCase("select cast(1 as UInt64)")]
        [TestCase("select cast(1 as Nullable(UInt64))")]
        [TestCase("select cast(null as Nullable(UInt64))")]
        [TestCase("select cast(1 as Nullable(Int64))")]
        [TestCase("select cast(null as Nullable(Int64))")]
        [TestCase("select cast(1 as Float32)")]
        [TestCase("select cast(-1.33 as Float32)")]
        [TestCase("select cast(1 as Nullable(Float32))")]
        [TestCase("select cast(null as Nullable(Float32))")]
        [TestCase("select cast(1 as Float64)")]
        [TestCase("select cast(-1.33 as Float64)")]
        [TestCase("select cast(1 as Nullable(Float64))")]
        [TestCase("select cast(null as Nullable(Float64))")]
        [TestCase("select cast('1' as String)")]
        [TestCase("select cast('1' as Nullable(String))")]
        [TestCase("select cast(null as Nullable(String))")]
        [TestCase("select null")]
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

        [Test]
        public void ExecuteNonQuery_Nullable()
        {
            _command.CommandText = @"
                select * from (
                    select cast(1 as Nullable(Int32)) as x
                    union all
                    select cast(2 as Nullable(Int32))
                    union all
                    select cast(null as Nullable(Int32))
                    union all
                    select cast(4 as Nullable(Int32))
                    union all
                    select cast(null as Nullable(Int32))
                    union all
                    select cast(6 as Nullable(Int32))
                )
            ";
            _command.ExecuteNonQuery();
        }
    }
}
