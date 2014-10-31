using System;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzurePageBlobStream
{
    public class PageBlobStream : Stream
    {
        private readonly CloudPageBlob _pageBlob;
        private const string MetadataLengthKey = "STREAM_LENGTH";
        private const int PageSizeInBytes = 512;

        protected PageBlobStream(CloudPageBlob pageBlob)
        {
            _pageBlob = pageBlob;
            Position = Length;
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
            if (value > this.Length)
            {
                var newSize = NextPageAddress(value);
                _pageBlob.Resize(newSize);
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var pageStartAddress = PreviousPageAddress(Position);
            var pageBytes = NextPageAddress(count);
            var offsetInPage = (int)Position % PageSizeInBytes;

            var pagesBefore = (int)Math.Ceiling((decimal)offsetInPage / PageSizeInBytes);
            var bufferToMerge = new byte[pageBytes + (pagesBefore * PageSizeInBytes)];
            _pageBlob.DownloadRangeToByteArray(bufferToMerge, 0, pageStartAddress, bufferToMerge.Length);
            Buffer.BlockCopy(bufferToMerge, offsetInPage, buffer, offset, count);
            int bytesRead = count;
            if (Position + count > Length)
                bytesRead = (int)(Length - Position);
            Position += bytesRead;
            return bytesRead;
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
            var offsetInFirstPage = (int) Position%PageSizeInBytes;

            var bufferToMerge = new byte[pageBytes];
            if (offsetInFirstPage > 0)
                _pageBlob.DownloadRangeToByteArray(bufferToMerge, 0, pageStartAddress, PageSizeInBytes);
            var offsetInLastPage = (offsetInFirstPage + count)%512;
            if (pageBytes > PageSizeInBytes && offsetInLastPage > 0)
                _pageBlob.DownloadRangeToByteArray(bufferToMerge, (int)pageBytes-512, PreviousPageAddress(Position + count),
                    PageSizeInBytes);

            Buffer.BlockCopy(buffer, offset, bufferToMerge, offsetInFirstPage, count);
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
                _pageBlob.FetchAttributes();
                var realLength = 0L;
                if (!_pageBlob.Metadata.ContainsKey(MetadataLengthKey) ||
                    !long.TryParse(_pageBlob.Metadata[MetadataLengthKey], out realLength))
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
