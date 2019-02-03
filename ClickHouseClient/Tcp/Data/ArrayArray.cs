using System.Linq;
using System.Runtime.InteropServices;

namespace ClickHouseClient.Tcp.Data
{
    internal class ArrayArray : DataArray
    {
        private DataArray _innerArray;
        private ulong[] _offsets;

        public override object this[int index] => GetItem(index);

        protected override void ReadImpl(StreamReader reader, string type, int rowCount)
        {
            ReadOffsets(reader, rowCount);
            var innerType = GetInnerType(type);
            var innerRowCount = (int) _offsets.LastOrDefault();
            _innerArray = Read(reader, innerType, innerRowCount);
        }

        private string GetInnerType(string type)
        {
            return type.Substring(6, type.Length - 7);
        }

        private void ReadOffsets(StreamReader reader, int rowCount)
        {
            var buffer = new byte[rowCount * 8];
            reader.Read(buffer, 0, buffer.Length);
            _offsets = new ulong[rowCount];
            var ptr = Marshal.UnsafeAddrOfPinnedArrayElement(_offsets, 0);
            Marshal.Copy(buffer, 0, ptr, buffer.Length);
        }

        private object[] GetItem(int index)
        {
            int startIndex, endIndex;
            if (index == 0)
            {
                startIndex = 0;
                endIndex = (int) _offsets[0];
            }
            else
            {
                startIndex = (int) _offsets[index - 1];
                endIndex = (int) _offsets[index];
            }
            var result = new object[endIndex - startIndex];
            for (var i = startIndex; i < endIndex; i++)
            {
                result[i - startIndex] = _innerArray[i];
            }
            return result;
        }
    }
}
