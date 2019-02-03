using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ClickHouseClient.Tcp.Data
{
    internal class UuidArray : DataArray
    {
        private Guid[] _data;

        public override object this[int index] => _data[index];

        protected override void ReadImpl(StreamReader reader, string type, int rowCount)
        {
            var buffer = new byte[rowCount * 16];
            reader.Read(buffer, 0, buffer.Length);
            for (var i = 0; i < rowCount; i++)
            {
                FixByteOrder(buffer, i * 16);
            }
            _data = new Guid[rowCount];
            var ptr = Marshal.UnsafeAddrOfPinnedArrayElement(_data, 0);
            Marshal.Copy(buffer, 0, ptr, buffer.Length);
        }

        private void FixByteOrder(byte[] buffer, int offset)
        {
            var a = buffer[offset];
            var b = buffer[offset + 1];
            var c = buffer[offset + 2];
            var d = buffer[offset + 3];
            buffer[offset] = buffer[offset + 4];
            buffer[offset + 1] = buffer[offset + 5];
            buffer[offset + 2] = buffer[offset + 6];
            buffer[offset + 3] = buffer[offset + 7];
            buffer[offset + 4] = c;
            buffer[offset + 5] = d;
            buffer[offset + 6] = a;
            buffer[offset + 7] = b;
            Array.Reverse(buffer, offset + 8, 8);
        }
    }
}
