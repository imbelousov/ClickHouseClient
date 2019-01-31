using System;
using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ClickHouseClient.Tcp
{
    internal class StreamReader
    {
        private readonly Stream _stream;
        private readonly ArrayPool<byte> _bufferPool;

        public StreamReader(Stream stream)
        {
            _stream = stream;
            _bufferPool = ArrayPool<byte>.Shared;
        }

        public ulong ReadInteger()
        {
            var integer = 0UL;
            for (var i = 0; i < 9; i++)
            {
                var b = ReadByte();
                integer = integer | ((b & 0x7FUL) << (7 * i));
                if ((b & 0x80) == 0)
                    break;
            }
            return integer;
        }

        public async Task<ulong> ReadIntegerAsync(CancellationToken cancellationToken)
        {
            var integer = 0UL;
            for (var i = 0; i < 9; i++)
            {
                var b = await ReadByteAsync(cancellationToken);
                integer = integer | ((b & 0x7FUL) << (7 * i));
                if ((b & 0x80) == 0)
                    break;
            }
            return integer;
        }

        public int ReadInt32()
        {
            var buffer = _bufferPool.Rent(4);
            Read(buffer, 0, 4);
            var result = BitConverter.ToInt32(buffer, 0);
            _bufferPool.Return(buffer);
            return result;
        }

        public async Task<int> ReadInt32Async(CancellationToken cancellationToken)
        {
            var buffer = _bufferPool.Rent(4);
            await ReadAsync(buffer, 0, 4, cancellationToken);
            var result = BitConverter.ToInt32(buffer, 0);
            _bufferPool.Return(buffer);
            return result;
        }

        public string ReadString()
        {
            var t = ReadInteger();
            if (t > int.MaxValue)
                throw new IOException("Invalid string length");
            var l = (int) t;
            var buffer = _bufferPool.Rent(l);
            Read(buffer, 0, l);
            var s = Encoding.UTF8.GetString(buffer, 0, l);
            _bufferPool.Return(buffer);
            return s;
        }

        public async Task<string> ReadStringAsync(CancellationToken cancellationToken)
        {
            var t = await ReadIntegerAsync(cancellationToken);
            if (t > int.MaxValue)
                throw new IOException("Invalid string length");
            var l = (int) t;
            var buffer = _bufferPool.Rent(l);
            await ReadAsync(buffer, 0, l, cancellationToken);
            var s = Encoding.UTF8.GetString(buffer, 0, l);
            _bufferPool.Return(buffer);
            return s;
        }

        public byte ReadByte()
        {
            var buffer = _bufferPool.Rent(1);
            var read = _stream.Read(buffer, 0, 1);
            if (read == 0)
                throw new EndOfStreamException();
            var b = buffer[0];
            _bufferPool.Return(buffer);
            return b;
        }

        public Exception ReadException()
        {
            var code = ReadInt32();
            var name = ReadString();
            var message = ReadString();
            var stackTrace = ReadString();
            var hasInnerException = ReadByte() != 0;
            var innerException = null as Exception;
            if (hasInnerException)
                innerException = ReadException();
            return new ClickHouseException(code, name, message, stackTrace, innerException);
        }

        public async Task<Exception> ReadExceptionAsync(CancellationToken cancellationToken)
        {
            var code = await ReadInt32Async(cancellationToken);
            var name = await ReadStringAsync(cancellationToken);
            var message = await ReadStringAsync(cancellationToken);
            var stackTrace = await ReadStringAsync(cancellationToken);
            var hasInnerException = await ReadByteAsync(cancellationToken) != 0;
            var innerException = null as Exception;
            if (hasInnerException)
                innerException = await ReadExceptionAsync(cancellationToken);
            return new ClickHouseException(code, name, message, stackTrace, innerException);
        }

        public async Task<byte> ReadByteAsync(CancellationToken cancellationToken)
        {
            var buffer = _bufferPool.Rent(1);
            var read = await _stream.ReadAsync(buffer, 0, 1, cancellationToken);
            if (read == 0)
                throw new EndOfStreamException();
            var b = buffer[0];
            _bufferPool.Return(buffer);
            return b;
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                var read = _stream.Read(buffer, offset, count);
                if (read == 0)
                    throw new EndOfStreamException();
                offset += read;
                count -= read;
            }
        }

        public async Task ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            while (count > 0)
            {
                var read = await _stream.ReadAsync(buffer, offset, count, cancellationToken);
                if (read == 0)
                    throw new EndOfStreamException();
                offset += read;
                count -= read;
            }
        }
    }
}
