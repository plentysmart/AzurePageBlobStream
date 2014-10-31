using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.Remoting.Channels;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Xunit;

namespace AzurePageBlobStream.Tests
{
    public class PageBlobStreamTests
    {
        private const string ContainerName = "page-blob-stream-tests";
        private const string PageBlobName = "PageBlob";

        private CloudStorageAccount account = CloudStorageAccount.DevelopmentStorageAccount;
        private CloudBlobClient client;
        private CloudBlobContainer container;

        public PageBlobStreamTests()
        {
            CloudStorageEmulatorShepherd shepherd = new CloudStorageEmulatorShepherd();
            shepherd.Start();
            this.client = account.CreateCloudBlobClient();
            this.container = client.GetContainerReference(ContainerName);
            if(container.Exists())
                container.Delete();
            container.Create();
        }

        [Fact]
        public void Write_EmptyStream_ShouldSetPosition()
        {
            var data = GenerateRandomData(1900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Flush();

            };
            VerifyAgainstMemoryStream(operation, x=> x.Position);
        }
        
        [Fact]
        public void Write_EmptyStream_ShouldSetLength()
        {
            var data = GenerateRandomData(1900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Flush();
            };
            VerifyAgainstMemoryStream(operation, x=> x.Length);
        }

        [Fact]
        public void Write_MultipleWrites_ShouldSetLength()
        {
            var data = GenerateRandomData(1900);
            var data2 = GenerateRandomData(3900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Write(data2, 0, data2.Length);
                stream.Flush();
            };
            VerifyAgainstMemoryStream(operation, x => x.Length);
        }

        [Fact]
        public void Write_MultipleWrites_ShouldSetPosition()
        {
            var data = GenerateRandomData(1900);
            var data2 = GenerateRandomData(3900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Write(data2, 0, data2.Length);
                stream.Flush();

            };
            VerifyAgainstMemoryStream(operation, x => x.Position);
        } 
        
        [Fact]
        public void Write_OverlapingWrites_ShouldSetLength()
        {
            var data = GenerateRandomData(1900);
            var data2 = GenerateRandomData(3900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Position = 10;
                stream.Write(data2, 0, data2.Length);
                stream.Flush();

            };
            VerifyAgainstMemoryStream(operation, x => x.Length);
        }

        [Fact]
        public void Write_OverlapingWrites_ShouldSetPosition()
        {
            var data = GenerateRandomData(1900);
            var data2 = GenerateRandomData(3900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Flush();
                stream.Position = 10;

                stream.Write(data2, 0, data2.Length);
                stream.Flush();

            };
            VerifyAgainstMemoryStream(operation, x => x.Position);
        }

        [Fact]
        public void WriteRead_MultipleWrites_StreamShouldReturnAllWritenData()
        {
            var data = GenerateRandomData(1900);
            var data2 = GenerateRandomData(3900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Write(data2, 0, data2.Length);
                stream.Flush();
            };
            VerifyAgainstMemoryStream(operation, x =>
            {
                var buffer = new byte[data.Length + data2.Length];
                x.Position = 0;
                x.Read(buffer, 0, buffer.Length);
                return buffer;
            });
        }

        [Fact]
        public void WriteRead_OverLappingWrites_StreamShouldReturnAllWritenData()
        {
            var data = GenerateRandomData(1900);
            var data2 = GenerateRandomData(3900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Position = 10;
                stream.Write(data2, 0, data2.Length);
                stream.Flush();

            };
            VerifyAgainstMemoryStream(operation, x =>
            {
                var buffer = new byte[data.Length + data2.Length];
                x.Position = 0;
                x.Read(buffer, 0, buffer.Length);
                return buffer;
            });
        }    
        [Fact]
        public void WriteRead_OverLappingWritesSecondWriteWithinFirst_StreamShouldReturnAllWritenData()
        {
            var data = GenerateRandomData(3900);
            var data2 = GenerateRandomData(1000);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Position = 10;
                stream.Write(data2, 0, data2.Length);
                stream.Flush();
            };
            VerifyAgainstMemoryStream(operation, x =>
            {
                var buffer = new byte[data.Length];
                x.Position = 0;
                x.Read(buffer, 0, buffer.Length);
                return buffer;
            });
        }

        [Fact]
        public void Open_OpeningExistingBlob_ShouldSetPositionToTheEndOfTheBlob()
        {
            var pageBlob = container.GetPageBlobReference(PageBlobName);
            var data = GenerateRandomData(1900);
            long length, position;
            using (var pageBlobStream = InitializeReadWriteStream(pageBlob))
            {
                pageBlobStream.Write(data,0,data.Length);
                pageBlobStream.Flush();
                length = pageBlobStream.Length;
                position = pageBlobStream.Position;
            }
            using (var pageBlobStream = InitializeReadWriteStream(pageBlob))
            {
                Assert.Equal(length, pageBlobStream.Length);
                Assert.Equal(position, pageBlobStream.Position);
            }
        }
        [Fact]
        public void Open_SubsequentWritesWithNewStream_ShouldHaveTheSameEffectAsWritingToSingleStream()
        {
            var multipleStreamsBlob = container.GetPageBlobReference(PageBlobName);
            var singleStreamBlob = container.GetPageBlobReference(PageBlobName +"Single");
            using (var singleStream = InitializeReadWriteStream(singleStreamBlob))
            {
                 var data = GenerateRandomData(1900);

                using (var pageBlobStream = InitializeReadWriteStream(multipleStreamsBlob))
                {
                    pageBlobStream.Write(data, 0, data.Length);
                    pageBlobStream.Flush();

                    singleStream.Write(data, 0, data.Length);
                    singleStream.Flush();

                }
                var data2 = GenerateRandomData(3212);
                using (var pageBlobStream = InitializeReadWriteStream(multipleStreamsBlob))
                {
                    pageBlobStream.Write(data2, 0, data.Length);
                    pageBlobStream.Flush();
                    singleStream.Write(data2, 0, data.Length);
                    singleStream.Flush();
                }
                
                using (var pageBlobStream = InitializeReadWriteStream(multipleStreamsBlob))
                {
                    Assert.Equal(singleStream.Position, pageBlobStream.Position);
                    Assert.Equal(singleStream.Length, pageBlobStream.Length);
                }
            }
        }

        [Fact]
        public void Read_EndOfTheStream_ShouldBeConsistentWithMemoryStream()
        {
            var data = GenerateRandomData(1900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Flush();
            };
            VerifyAgainstMemoryStream(operation, x =>
            {
                var buffer = new byte[400];
                x.Position = 1800;
                return x.Read(buffer, 0, buffer.Length);
            });
        } 

        [Fact]
        public void Read_EndOfTheStream_PositionShouldBeConsistentWithMemoryStream()
        {
            var data = GenerateRandomData(1900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Flush();

            };
            VerifyAgainstMemoryStream(operation, x =>
            {
                var buffer = new byte[400];
                x.Position = 1800;
                x.Read(buffer, 0, buffer.Length);
                return x.Position;
            });
        }    
        
        [Fact]
        public void Read_WithinStream_ShouldBeConsistentWithMemoryStream()
        {
            var data = GenerateRandomData(1900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Flush();
            };
            VerifyAgainstMemoryStream(operation, x =>
            {
                var buffer = new byte[400];
                x.Position = 0;
                x.Read(buffer, 0, buffer.Length);
                return x.Position;
            });
        }

        [Fact]
        public void ReadByte_EndOfTheStream_ShouldBeConsistentWithMemoryStream()
        {
            var data = GenerateRandomData(1900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Flush();
            };
            VerifyAgainstMemoryStream(operation, x =>
            {
                var readByte = x.ReadByte();
                return readByte;
            });
        }

        public void VerifyAgainstMemoryStream<T>(Action<Stream> operation, Func<Stream, T> propertyToVerify)
        {
            var pageBlob = container.GetPageBlobReference(PageBlobName);
            var pageBlobStream = InitializeReadWriteStream(pageBlob);
            var memoryStream = new MemoryStream();
            operation(memoryStream);
            operation(pageBlobStream);
            var expected = propertyToVerify(memoryStream);
            var actual = propertyToVerify(pageBlobStream);
            Assert.Equal(expected, actual);
        }


        private static byte[] GenerateRandomData(int bytes)
        {
            var data = new byte[bytes];
            new Random().NextBytes(data);
            return data;
        }

        private Stream InitializeReadWriteStream(CloudPageBlob pageBlob)
        {
            return BufferedPageBlobStream.Open(pageBlob);
        }
    }
}
