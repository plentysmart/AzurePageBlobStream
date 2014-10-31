using System;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzurePageBlobStream
{
    public class PageBlobStream:Stream
    {
        private readonly CloudPageBlob _pageBlob;
        private const string MetadataLengthKey = "STREAM_LENGTH";
        private const int PageSizeInBytes = 512;

        private PageBlobStream(CloudPageBlob pageBlob)
        {
            _pageBlob = pageBlob;
        }

        private long BlobLength
        {
            get
            {
                _pageBlob.FetchAttributes();
                return _pageBlob.Properties.Length;
            }
        }

        public override void Flush()
        {
            
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var pageStartAddress = PreviousPageAddress(Position);
            var pageBytes = NextPageAddress(count);
            var offsetInPage = (int)Position % PageSizeInBytes;

            var pagesBefore = (int)Math.Ceiling((decimal)offsetInPage / PageSizeInBytes);
            var bufferToMerge = new byte[pageBytes + (pagesBefore * PageSizeInBytes)];
            _pageBlob.DownloadRangeToByteArray(bufferToMerge, 0, pageStartAddress, bufferToMerge.Length);
            Buffer.BlockCopy(bufferToMerge,offsetInPage, buffer,offset,count);
            return count;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (BlobLength < Position + count)
            {
                var newSize = NextPageAddress(Position + count);
                _pageBlob.Resize(newSize);
            }
            var pageStartAddress = PreviousPageAddress(Position);
            var pageBytes = NextPageAddress(Position + count) - pageStartAddress;
            var offsetInPage = (int)Position % PageSizeInBytes;

            var bufferToMerge = new byte[pageBytes];
            _pageBlob.DownloadRangeToByteArray(bufferToMerge, 0, pageStartAddress, bufferToMerge.Length);
            Buffer.BlockCopy(buffer, offset, bufferToMerge, offsetInPage, count);
            using (var memoryStream = new MemoryStream(bufferToMerge, false))
            {
                memoryStream.Position = 0;
                _pageBlob.WritePages(memoryStream, pageStartAddress);
            }
            SetLengthInternal(Position + count);
            Position += count;
        }

        private long NextPageAddress(long position)
        {
            var previousPageAddress = PreviousPageAddress(position);
            return previousPageAddress + PageSizeInBytes;
        }
        private long PreviousPageAddress(long position)
        {
            var previousPageAddress = position - (position % PageSizeInBytes);
            return previousPageAddress;
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
                var length = _pageBlob.Metadata[MetadataLengthKey];
                var realLength = 0L;
                if (!long.TryParse(length, out realLength))
                {
                    SetLengthInternal(0);
                    return 0;
                }
                return realLength;
            }
        }

        public override long Position { get; set; }

        public static Stream Open(CloudPageBlob pageBlob)
        {
            if (!pageBlob.Exists())
            {
                pageBlob.Create(0);
            }
            return new PageBlobStream(pageBlob);
        }

        private void SetLengthInternal(long newLength)
        {
            _pageBlob.Metadata[MetadataLengthKey] = newLength.ToString();
            _pageBlob.SetMetadata();
        }
    }
}
