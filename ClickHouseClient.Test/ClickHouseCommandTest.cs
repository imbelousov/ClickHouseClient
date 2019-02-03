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

        public static IEnumerable<TestCaseData> SimpleSelectQueries
        {
            get
            {
                yield return new TestCaseData("select cast(1 as Int8)", (sbyte) 1);
                yield return new TestCaseData("select cast(-1 as Int8)", (sbyte) -1);
                yield return new TestCaseData("select cast(1 as UInt8)", (byte) 1);
                yield return new TestCaseData("select cast(1 as Nullable(UInt8))", (byte?) 1);
                yield return new TestCaseData("select cast(null as Nullable(UInt8))", null);
                yield return new TestCaseData("select cast(1 as Nullable(Int8))", (sbyte?) 1);
                yield return new TestCaseData("select cast(null as Nullable(Int8))", null);
                yield return new TestCaseData("select cast(1 as Int16)", (short) 1);
                yield return new TestCaseData("select cast(-1 as Int16)", (short) -1);
                yield return new TestCaseData("select cast(1 as UInt16)", (ushort) 1);
                yield return new TestCaseData("select cast(1 as Nullable(UInt16))", (ushort?) 1);
                yield return new TestCaseData("select cast(null as Nullable(UInt16))", null);
                yield return new TestCaseData("select cast(1 as Nullable(Int16))", (short?) 1);
                yield return new TestCaseData("select cast(null as Nullable(Int16))", null);
                yield return new TestCaseData("select cast(1 as Int32)", 1);
                yield return new TestCaseData("select cast(-1 as Int32)", -1);
                yield return new TestCaseData("select cast(1 as UInt32)", 1U);
                yield return new TestCaseData("select cast(1 as Nullable(UInt32))", (uint?) 1);
                yield return new TestCaseData("select cast(null as Nullable(UInt32))", null);
                yield return new TestCaseData("select cast(1 as Nullable(Int32))", (int?) 1);
                yield return new TestCaseData("select cast(null as Nullable(Int32))", null);
                yield return new TestCaseData("select cast(1 as Int64)", 1L);
                yield return new TestCaseData("select cast(-1 as Int64)", -1L);
                yield return new TestCaseData("select cast(1 as UInt64)", 1UL);
                yield return new TestCaseData("select cast(1 as Nullable(UInt64))", (ulong?) 1UL);
                yield return new TestCaseData("select cast(null as Nullable(UInt64))", null);
                yield return new TestCaseData("select cast(1 as Nullable(Int64))", (long?) 1L);
                yield return new TestCaseData("select cast(null as Nullable(Int64))", null);
                yield return new TestCaseData("select cast(1 as Float32)", 1.0f);
                yield return new TestCaseData("select cast(-1.33 as Float32)", -1.33f);
                yield return new TestCaseData("select cast(1 as Nullable(Float32))", (float?) 1.0f);
                yield return new TestCaseData("select cast(null as Nullable(Float32))", null);
                yield return new TestCaseData("select cast(1 as Float64)", 1.0);
                yield return new TestCaseData("select cast(-1.33 as Float64)", -1.33);
                yield return new TestCaseData("select cast(1 as Nullable(Float64))", (double?) 1);
                yield return new TestCaseData("select cast(null as Nullable(Float64))", null);
                yield return new TestCaseData("select cast('1' as String)", "1");
                yield return new TestCaseData("select cast('1' as Nullable(String))", "1");
                yield return new TestCaseData("select cast(null as Nullable(String))", null);
                yield return new TestCaseData("select null", null);
                yield return new TestCaseData("select cast('d7ed2d6d-b404-449b-9469-0845ac4767b7' as UUID)", Guid.Parse("d7ed2d6d-b404-449b-9469-0845ac4767b7"));
                yield return new TestCaseData("select cast('2015-06-01 12:30:00' as DateTime)", new DateTime(2015, 6, 1, 12, 30, 0));
                yield return new TestCaseData("select cast(null as Nullable(DateTime))", null);
                yield return new TestCaseData("select cast('2015-06-01' as Date)", new DateTime(2015, 6, 1, 0, 0, 0));
                yield return new TestCaseData("select cast(null as Nullable(Date))", null);
            }
        }

        [TestCaseSource(nameof(SimpleSelectQueries))]
        public void ExecuteNonQuery_SimpleSelect(string sql, object expected)
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
        public void ExecuteDbDataReader_SimpleSelect(string sql, object expected)
        {
            _command.CommandText = sql;
            var reader = _command.ExecuteReader();
            var first = reader.Read();
            var actual = reader.GetValue(0);
            var second = reader.Read();
            Assert.IsTrue(first);
            Assert.IsFalse(second);
            Assert.AreEqual(expected, actual);
        }

        [Test]
        public void ExecuteDbDataReader_SelectMultipleNullable()
        {
            _command.CommandText = @"
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
            ";
            var actual = ReadSingleColumn<int?>();
            CollectionAssert.AreEqual(new int?[] {1, 2, null, 4, null, 6}, actual);
        }

        [Test]
        public void ExecuteDbDataReader_Array()
        {
            var expected = new object[] {1, 2, 3, 10};
            _command.CommandText = $"select [{string.Join(", ", expected)}]";
            var actual = ReadSingleColumn<object[]>().Single();
            CollectionAssert.AreEqual(expected, actual);
        }

        [Test]
        public void ExecuteDbDataReader_Arrays()
        {
            var expected1 = new object[] {1, 2, 3, 10};
            var expected2 = new object[] {3, 5};
            var expected3 = new object[] { };
            var expected4 = new object[] {-1, 5, 3};
            _command.CommandText = $@"
                select [{string.Join(", ", expected1)}] as x
                union all
                select [{string.Join(", ", expected2)}]
                union all
                select [{string.Join(", ", expected3)}]
                union all
                select [{string.Join(", ", expected4)}]
            ";
            var actual = ReadSingleColumn<object[]>();
            CollectionAssert.AreEqual(expected1, actual[0]);
            CollectionAssert.AreEqual(expected2, actual[1]);
            CollectionAssert.AreEqual(expected3, actual[2]);
            CollectionAssert.AreEqual(expected4, actual[3]);
        }

        [Test]
        public void ExecuteDbDataReader_InnerArrays()
        {
            var expected1 = new object[] {1, 2, 3, 10};
            var expected2 = new object[] {3, 5};
            _command.CommandText = $@"
                select [[{string.Join(", ", expected1)}], [{string.Join(", ", expected2)}]]
            ";
            var actual = ReadSingleColumn<object[]>().Single();
            CollectionAssert.AreEqual(expected1, (object[]) actual[0]);
            CollectionAssert.AreEqual(expected2, (object[]) actual[1]);
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
