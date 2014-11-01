using System;
using System.IO;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzurePageBlobStream
{
    public class BufferedPageBlobStream:Stream
    {
        private WriteBuffer writeBuffer;
        private const long _cacheSize = 20 * 1024 * 1024;//5MB
        private MemoryStream cache = new MemoryStream();
        private long cacheStartingPosition;
        private Stream stream;

        protected BufferedPageBlobStream(CloudPageBlob pageBlob)
        {
            stream = PageBlobStream.Open(pageBlob);
            this.Position = stream.Position;
            writeBuffer = new WriteBuffer(stream.Length, stream.Position);
        }

        public long PendingWritesSize
        {
            get
            {
                if (!writeBuffer.PendingOperations.Any())
                    return 0;
                return writeBuffer.PendingOperations.Sum(x => x.Count);
            }
        }

        private void PreLoad(long startPosition)
        {
            var savedPosition = stream.Position;
            stream.Position = startPosition;
            var toLoad = _cacheSize;
            if (startPosition + _cacheSize > stream.Length)
            {
                toLoad = stream.Length - startPosition;
            }
            var buffer = new byte[toLoad];
            stream.Read(buffer, 0, buffer.Length);
            this.cache = new MemoryStream(buffer);
            cacheStartingPosition = startPosition;
            stream.Position = savedPosition;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            //stream.Position = this.Position;
            //var read = stream.Read(buffer, offset, count);
            //this.Position = stream.Position;
            //return read;
            if (this.writeBuffer.PendingOperations.Any())
            {
                this.Flush();
                PreLoad(this.Position);
            }

            var cacheOffset = this.Position - cacheStartingPosition;
            if (cacheOffset + count >= this.cache.Length)
            {
                PreLoad(this.Position);
                cacheOffset = this.Position - cacheStartingPosition;

            }

            cache.Position = cacheOffset;
            var bytesRead = cache.Read(buffer, offset, count);
            this.Position += bytesRead;
            return bytesRead;
        }

        public override void Flush()
        {
            if (writeBuffer.Length > stream.Length)
                stream.SetLength(writeBuffer.Length);
            foreach (var pendingOperation in writeBuffer.PendingOperations)
            {
                stream.Position = pendingOperation.Position;
                stream.Write(pendingOperation.Buffer, pendingOperation.Offset, pendingOperation.Count);
            }
            writeBuffer = new WriteBuffer(this.Length,this.Position);

        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            writeBuffer.AddWriteOperation(this.Position,buffer,offset,count);
            this.Position = writeBuffer.Position;
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length
        {
            get
            {
                if (writeBuffer.PendingOperations.Any())
                    return writeBuffer.Length;
                return stream.Length;
            }
        }

        public override long Position { get; set; }

        public static BufferedPageBlobStream Open(CloudPageBlob pageBlob)
        {
            if (!pageBlob.Exists())
            {
                pageBlob.Create(0);
            }
            return new BufferedPageBlobStream(pageBlob);
        }

        protected override void Dispose(bool disposing)
        {
            this.Flush();
            base.Dispose(disposing);
        }
    }
}