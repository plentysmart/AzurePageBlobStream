using System;
using System.Collections.Generic;
using System.Linq;

namespace AzurePageBlobStream
{
    public class WriteBuffer
    {


        private List<WriteOperation> operations = new List<WriteOperation>();

        public WriteBuffer(long currentLength, long currentPosition)
        {
            this.Length = currentLength;
            this.Position= currentPosition;
        }

        public long Length { get; private set; }
        public long Position { get; private set; }

        public void AddWriteOperation(long position, byte[] buffer, int offset, int count)
        {
            var newOperation = new WriteOperation(position, buffer, offset, count);
            var overlapping = operations.FirstOrDefault(x => x.CanBeMerged(newOperation));
            if (overlapping != null)
                overlapping.Merge(newOperation);
            else
            {
                operations.Add(newOperation);
            }
            this.Position = position + count;
            this.Length = Math.Max(this.Length, this.Position);
        }

        public IEnumerable<WriteOperation> PendingOperations
        {
            get { return operations; }
        }
    }

    public class WriteOperation
    {
        public long Position { get; private set; }
        public byte[] Buffer { get; private set; }
        public int Offset { get; private set; }
        public int Count { get; private set; }
        public long EndPosition
        {
            get { return Position + Count; }
        }

        public WriteOperation(long position, byte[] buffer, int offset, int count)
        {
            Position = position;
            Buffer = buffer;
            Offset = offset;
            Count = count;
        }

        public bool CanBeMerged(WriteOperation operation)
        {
            if (operation.Position <= this.Position && this.Position <= operation.EndPosition)
                return true;

            if (operation.Position <= this.EndPosition && this.EndPosition <= operation.EndPosition)
                return true;
            
            if (operation.Position >= this.Position && this.EndPosition >= operation.EndPosition)
                return true;
            return false;
        }

        public void Merge(WriteOperation toMerge)
        {
            
            var position = Math.Min(this.Position, toMerge.Position);
            var endPosition = Math.Max(this.EndPosition, toMerge.EndPosition);
            var count = (int)(endPosition - position);


            var newBuffer = new byte[count];

            System.Buffer.BlockCopy(this.Buffer, this.Offset, newBuffer, (int) (this.Position - position), this.Count);
            System.Buffer.BlockCopy(toMerge.Buffer, toMerge.Offset, newBuffer, (int)(toMerge.Position - position), toMerge.Count);
            this.Position = position;
            this.Count = count;
            this.Buffer = newBuffer;
            this.Offset = 0;
        }
    }
}