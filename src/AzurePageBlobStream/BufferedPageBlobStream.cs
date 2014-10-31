using System;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzurePageBlobStream
{
    public class BufferedPageBlobStream:PageBlobStream
    {
        private WriteBuffer writeBuffer;
        protected BufferedPageBlobStream(CloudPageBlob pageBlob) : base(pageBlob)
        {
            writeBuffer = new WriteBuffer(this.Length, this.Position);
        }


        public override void Flush()
        {
            base.SetLength(writeBuffer.Length);
            foreach (var pendingOperation in writeBuffer.PendingOperations)
            {
                base.Position = pendingOperation.Position;
                base.Write(pendingOperation.Buffer,pendingOperation.Offset,pendingOperation.Count);
            }
            writeBuffer = new WriteBuffer(this.Length,this.Position);

        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            writeBuffer.AddWriteOperation(this.Position,buffer,offset,count);
            this.Position = writeBuffer.Position;
        }
        public static Stream Open(CloudPageBlob pageBlob)
        {
            if (!pageBlob.Exists())
            {
                pageBlob.Create(0);
            }
            return new BufferedPageBlobStream(pageBlob);
        }
    }
}