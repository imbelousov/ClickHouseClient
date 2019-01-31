using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient.Tcp
{
    internal class StreamWriter
    {
        private readonly Stream _stream;
        private readonly ArrayPool<byte> _bufferPool;

        public StreamWriter(Stream stream)
        {
            _stream = stream;
            _bufferPool = ArrayPool<byte>.Shared;
        }

        public void WriteInteger(long integer)
        {
            WriteInteger((ulong) integer);
        }

        public void WriteInteger(ulong integer)
        {
            var buffer = _bufferPool.Rent(9);
            var i = 0;
            for (; i < 9; i++)
            {
                var b = integer & 0x7F;
                integer >>= 7;
                if (integer > 0)
                    b = b | 0x80;
                buffer[i] = (byte) b;
                if (integer == 0)
                    break;
            }
            Write(buffer, 0, i + 1);
            _bufferPool.Return(buffer);
        }

        public Task WriteIntegerAsync(long integer, CancellationToken cancellationToken)
        {
            return WriteIntegerAsync((ulong) integer, cancellationToken);
        }

        public async Task WriteIntegerAsync(ulong integer, CancellationToken cancellationToken)
        {
            var buffer = _bufferPool.Rent(9);
            var i = 0;
            for (; i < 9; i++)
            {
                var b = integer & 0x7F;
                integer >>= 7;
                if (integer > 0)
                    b = b | 0x80;
                buffer[i] = (byte) b;
                if (integer == 0)
                    break;
            }
            await WriteAsync(buffer, 0, i + 1, cancellationToken);
            _bufferPool.Return(buffer);
        }

        public void WriteString(string str)
        {
            str = str ?? string.Empty;
            var buffer = Encoding.UTF8.GetBytes(str);
            WriteInteger((ulong) buffer.Length);
            Write(buffer, 0, buffer.Length);
        }

        public async Task WriteStringAsync(string str, CancellationToken cancellationToken)
        {
            str = str ?? string.Empty;
            var buffer = Encoding.UTF8.GetBytes(str);
            await WriteIntegerAsync((ulong) buffer.Length, cancellationToken);
            await WriteAsync(buffer, 0, buffer.Length, cancellationToken);
        }

        public void Write(byte[] buffer, int offset, int count)
        {
            _stream.Write(buffer, offset, count);
        }

        public Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return _stream.WriteAsync(buffer, offset, count, cancellationToken);
        }
    }
}
