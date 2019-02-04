using System;
using System.Runtime.InteropServices;

namespace ClickHouseClient.Tcp.Data
{
    internal class DateTimeArray : DataArray
    {
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private uint[] _data;

        public override object this[int index] => UnixEpoch.AddSeconds(_data[index]).ToLocalTime();

        protected override void ReadImpl(StreamReader reader, string type, int rowCount)
        {
            var buffer = new byte[rowCount * 4];
            reader.Read(buffer, 0, buffer.Length);
            _data = new uint[rowCount];
            var ptr = Marshal.UnsafeAddrOfPinnedArrayElement(_data, 0);
            Marshal.Copy(buffer, 0, ptr, buffer.Length);
        }
    }
}
