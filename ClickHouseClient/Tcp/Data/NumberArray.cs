using System;
using System.Runtime.InteropServices;

namespace ClickHouseClient.Tcp.Data
{
    internal class NumberArray : DataArray
    {
        private Array _data;

        public override object this[int index] => _data.GetValue(index);

        protected override void ReadImpl(StreamReader reader, string type, int rowCount)
        {
            switch (type)
            {
                case "Int8":
                    Read<sbyte>(reader, rowCount, 1);
                    break;
                case "Int16":
                    Read<short>(reader, rowCount, 2);
                    break;
                case "Int32":
                    Read<int>(reader, rowCount, 4);
                    break;
                case "Int64":
                    Read<long>(reader, rowCount, 8);
                    break;
                case "UInt8":
                    Read<byte>(reader, rowCount, 1);
                    break;
                case "UInt16":
                    Read<ushort>(reader, rowCount, 2);
                    break;
                case "UInt32":
                    Read<uint>(reader, rowCount, 4);
                    break;
                case "UInt64":
                    Read<ulong>(reader, rowCount, 8);
                    break;
                case "Float32":
                    Read<float>(reader, rowCount, 4);
                    break;
                case "Float64":
                    Read<double>(reader, rowCount, 8);
                    break;
            }
        }

        private void Read<T>(StreamReader reader, int rowCount, int size)
        {
            var buffer = new byte[rowCount * size];
            reader.Read(buffer, 0, buffer.Length);
            _data = new T[rowCount];
            var ptr = Marshal.UnsafeAddrOfPinnedArrayElement(_data, 0);
            Marshal.Copy(buffer, 0, ptr, buffer.Length);
        }
    }
}
