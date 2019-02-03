using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace ClickHouseClient.Tcp.Data
{
    internal class EnumArray : DataArray
    {
        private Dictionary<int, string> _dictionary;
        private int _digits;
        private Array _data;

        public override object this[int index] => _dictionary[(int) _data.GetValue(index)];

        protected override void ReadImpl(StreamReader reader, string type, int rowCount)
        {
            ParseType(type);
            switch (_digits)
            {
                case 1:
                    ReadAsEnum8(reader, rowCount);
                    break;
                case 2:
                    ReadAsEnum16(reader, rowCount);
                    break;
                default:
                    throw new NotSupportedException($"{_digits * 8}-based enum type is not supported");
            }
        }

        private void ReadAsEnum8(StreamReader reader, int rowCount)
        {
            var buffer = new byte[rowCount];
            reader.Read(buffer, 0, buffer.Length);
            _data = new sbyte[rowCount];
            var ptr = Marshal.UnsafeAddrOfPinnedArrayElement(_data, 0);
            Marshal.Copy(buffer, 0, ptr, buffer.Length);
        }

        private void ReadAsEnum16(StreamReader reader, int rowCount)
        {
            var buffer = new byte[rowCount * 2];
            reader.Read(buffer, 0, buffer.Length);
            _data = new short[rowCount];
            var ptr = Marshal.UnsafeAddrOfPinnedArrayElement(_data, 0);
            Marshal.Copy(buffer, 0, ptr, buffer.Length);
        }

        private void ParseType(string type)
        {
            _dictionary = new Dictionary<int, string>();
            _digits = GetDigits(type);
            var isKey = false;
            var isValue = false;
            var key = 0;
            var isNegativeKey = false;
            var value = new List<char>();
            foreach (var c in type.Skip(5 + _digits))
            {
                if (isKey)
                {
                    if (char.IsWhiteSpace(c))
                        continue;
                    if (c == ',' || c == ')')
                    {
                        isKey = false;
                        if (isNegativeKey)
                            key *= -1;
                        _dictionary[key] = new string(value.ToArray());
                        key = 0;
                        value.Clear();
                    }
                    else if (c == '-')
                        isNegativeKey = true;
                    else
                        key = 10 * key + (c - '0');
                }
                else if (isValue)
                {
                    if (c == '\'')
                        isValue = false;
                    else
                        value.Add(c);
                }
                else
                {
                    if (char.IsWhiteSpace(c))
                        continue;
                    if (c == '=')
                        isKey = true;
                    else if (c == '\'')
                        isValue = true;
                }
            }
        }

        private int GetDigits(string type)
        {
            if (type[4] == '8' && type[5] == '(')
                return 1;
            if (type[4] == '1' && type[5] == '6' && type[6] == '(')
                return 2;
            throw new NotSupportedException($"Enum type {type} is not supported");
        }
    }
}
