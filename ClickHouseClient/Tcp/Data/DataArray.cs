﻿namespace ClickHouseClient.Tcp.Data
{
    /// <summary>
    /// Represents an array of items which are stored in column
    /// </summary>
    internal abstract class DataArray
    {
        public int Count { get; private set; }

        public abstract object this[int index] { get; }

        /// <summary>
        /// Reads array from the stream
        /// </summary>
        /// <param name="reader">Stream reader</param>
        /// <param name="type">Full name of ClickHouse data type</param>
        /// <param name="rowCount">Expected number of items in array</param>
        /// <returns>Array</returns>
        public static DataArray Read(StreamReader reader, string type, int rowCount)
        {
            DataArray array;
            if (type.StartsWith("Nullable("))
                array = new NullableArray();
            else if (IsInteger(type))
                array = new IntegerArray();
            else
                throw new TcpProtocolException($"Unexpected column type: {type}");
            array.Count = rowCount;
            array.ReadImpl(reader, type, rowCount);
            return array;
        }

        protected abstract void ReadImpl(StreamReader reader, string type, int rowCount);

        private static bool IsInteger(string type)
        {
            return type == "Int8" || type == "Int16" || type == "Int32" || type == "Int64" || type == "UInt8" || type == "UInt16" || type == "UInt32" || type == "UInt64";
        }
    }
}