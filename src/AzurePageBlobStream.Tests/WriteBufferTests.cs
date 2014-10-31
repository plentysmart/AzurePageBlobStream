using System;
using System.IO;
using Xunit;

namespace AzurePageBlobStream.Tests
{
    public class WriteBufferTests
    {
        [Fact]
        public void AddWriteOperation_ThreeOverlappingWrites_ShouldProduceOneWriteOperation()
        {
            var writeBuffer = new WriteBuffer(0, 0);
            using (var ms = new MemoryStream())
            {
                for (int i = 0; i < 3; i++)
                {
                    var data = GenerateRandomData(100);
                    writeBuffer.AddWriteOperation(ms.Position, data, 0, data.Length);
                    ms.Write(data, 0, 100);

                    Assert.Equal(ms.Position, writeBuffer.Position);
                    Assert.Equal(ms.Length, writeBuffer.Length);
                }

            }
        }

        private static byte[] GenerateRandomData(int bytes)
        {
            var data = new byte[bytes];
            new Random().NextBytes(data);
            return data;
        }
    }
}