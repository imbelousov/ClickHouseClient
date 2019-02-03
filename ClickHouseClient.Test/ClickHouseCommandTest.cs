using System;
using System.Collections.Generic;
using System.Linq;
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

        public static IEnumerable<string> SimpleSelectQueries
        {
            get
            {
                yield return "select cast(1 as Int8)";
                yield return "select cast(-1 as Int8)";
                yield return "select cast(1 as UInt8)";
                yield return "select cast(1 as Nullable(UInt8))";
                yield return "select cast(null as Nullable(UInt8))";
                yield return "select cast(1 as Nullable(Int8))";
                yield return "select cast(null as Nullable(Int8))";
                yield return "select cast(1 as Int16)";
                yield return "select cast(-1 as Int16)";
                yield return "select cast(1 as UInt16)";
                yield return "select cast(1 as Nullable(UInt16))";
                yield return "select cast(null as Nullable(UInt16))";
                yield return "select cast(1 as Nullable(Int16))";
                yield return "select cast(null as Nullable(Int16))";
                yield return "select cast(1 as Int32)";
                yield return "select cast(-1 as Int32)";
                yield return "select cast(1 as UInt32)";
                yield return "select cast(1 as Nullable(UInt32))";
                yield return "select cast(null as Nullable(UInt32))";
                yield return "select cast(1 as Nullable(Int32))";
                yield return "select cast(null as Nullable(Int32))";
                yield return "select cast(1 as Int64)";
                yield return "select cast(-1 as Int64)";
                yield return "select cast(1 as UInt64)";
                yield return "select cast(1 as Nullable(UInt64))";
                yield return "select cast(null as Nullable(UInt64))";
                yield return "select cast(1 as Nullable(Int64))";
                yield return "select cast(null as Nullable(Int64))";
                yield return "select cast(1 as Float32)";
                yield return "select cast(-1.33 as Float32)";
                yield return "select cast(1 as Nullable(Float32))";
                yield return "select cast(null as Nullable(Float32))";
                yield return "select cast(1 as Float64)";
                yield return "select cast(-1.33 as Float64)";
                yield return "select cast(1 as Nullable(Float64))";
                yield return "select cast(null as Nullable(Float64))";
                yield return "select cast('1' as String)";
                yield return "select cast('1' as Nullable(String))";
                yield return "select cast(null as Nullable(String))";
                yield return "select null";
                yield return "select generateUUIDv4()";
            }
        }

        [TestCaseSource(nameof(SimpleSelectQueries))]
        public void ExecuteNonQuery_SimpleSelect(string sql)
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
        public void ExecuteNonQuery_SelectMultipleNullable()
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

        [TestCaseSource(nameof(SimpleSelectQueries))]
        public void ExecuteDbDataReader_SimpleSelect(string sql)
        {
            _command.CommandText = sql;
            var reader = _command.ExecuteReader();
            var first = reader.Read();
            var value = reader.GetValue(0);
            var second = reader.Read();
            Assert.IsTrue(first);
            Assert.IsFalse(second);
        }

        [Test]
        public void ExecuteDbDataReader_SelectMultipleNullable()
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
            var actual = ReadSingleColumn<int?>();
            CollectionAssert.AreEqual(new int?[] {1, 2, null, 4, null, 6}, actual);
        }

        [Test]
        public void ExecuteDbDataReader_Uuid()
        {
            var expected = Guid.NewGuid();
            _command.CommandText = $"select cast('{expected}' as UUID)";
            var actual = ReadSingleColumn<Guid>().Single();
            Assert.AreEqual(expected, actual);
        }

        private List<T> ReadSingleColumn<T>()
        {
            var reader = _command.ExecuteReader();
            var result = new List<T>();
            while (reader.Read())
            {
                result.Add((T) reader.GetValue(0));
            }
            return result;
        }
    }
}
