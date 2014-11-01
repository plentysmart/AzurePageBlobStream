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
        public void Write_MultipleWrites_ShouldWriteDataCorrecly()
        {
            var data = GenerateRandomData(1900);
            var data2 = GenerateRandomData(3900);
            var data3 = GenerateRandomData(3900);
            Action<Stream> operation = stream =>
            {
                stream.Write(data, 0, data.Length);
                stream.Write(data2, 0, data2.Length);
                stream.Write(data3, 0, data3.Length);
                stream.Flush();

            };
            VerifyAgainstMemoryStream(operation, x =>
            {
                var buffer = new byte[data.Length + 2* data2.Length];
                x.Position = 0;
                x.Read(buffer, 0, buffer.Length);
                return buffer;
            });
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

        [Fact]
        public void Write_CheckAgainstFileStream_RandomWrite_FileShouldBeTheSameAsPageBlob()
        {
            string fileName = string.Format("{0:dd-MM-yyyy_hh_mm_ss}.bin", DateTime.Now);
            var random = new Random();
            using (var stream = File.OpenWrite(fileName))
            {
                var pageblob = container.GetPageBlobReference(fileName);
                using (var blobStream = BufferedPageBlobStream.Open(pageblob))
                {
                    while (stream.Length < 100*1024*1024)
                    {
                        var data = GenerateRandomData(random.Next(5, 15*1024*1024), random);
                        stream.Write(data, 0, data.Length);
                        blobStream.Write(data, 0 ,data.Length);
                        stream.Flush();
                        blobStream.Flush();
                    }
                    for (int i = 0; i < 100; i++)
                    {
                        var startAddress = random.Next(0, (int)(stream.Length - 1));
                        var howManyBytes = random.Next(3, 100*1024);
                        var data = GenerateRandomData(howManyBytes, random);
                        stream.Position = startAddress;
                        blobStream.Position = startAddress;
                        stream.Write(data, 0, data.Length);
                        blobStream.Write(data, 0, data.Length);
                        stream.Flush();
                        blobStream.Flush();
                    }
                }
                stream.Close();
            }

            using (var stream = File.OpenRead(fileName))
            {
                var pageblob = container.GetPageBlobReference(fileName);
                using (var blobStream = BufferedPageBlobStream.Open(pageblob))
                {
                    blobStream.Position = 0;
                    stream.Position = 0;
                    AssertStreamsAreEqual(stream, blobStream);
                }
            }

        }

        private void AssertStreamsAreEqual(FileStream stream, Stream blobStream)
        {
            // Check the file sizes. If they are not the same, the files 
            // are not the same.
            Assert.Equal(stream.Length, blobStream.Length);

            // Read and compare a byte from each file until either a
            // non-matching set of bytes is found or until the end of
            // file1 is reached.
            int bytesReadFile;
            int bytesReadBlob;
            int bufferSize = 100000;
            do
            {
                var bufferFile = new byte[bufferSize];
                var bufferBlob = new byte[bufferSize];
                // Read one byte from each file.
                bytesReadFile = stream.Read(bufferFile, 0, bufferSize);
                bytesReadBlob = blobStream.Read(bufferBlob, 0, bufferSize);

                Assert.Equal(bytesReadBlob,bytesReadFile);
                Assert.Equal(bufferFile, bufferBlob);
            } while (bytesReadBlob == bytesReadFile && bytesReadBlob > 0);

            // Close the files.
            stream.Close();
            blobStream.Close();

            // Return the success of the comparison. "file1byte" is 
            // equal to "file2byte" at this point only if the files are 
            // the same.
            Assert.Equal(bytesReadBlob, bytesReadFile);
        }

        [Fact]
        public void Write_CheckAgainstFileStream_SequentialWrite_FileShouldBeTheSameAsPageBlob()
        {
            
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


        private static byte[] GenerateRandomData(int bytes, Random random = null)
        {
            var data = new byte[bytes];
            (random ?? new Random()).NextBytes(data);
            return data;
        }

        private Stream InitializeReadWriteStream(CloudPageBlob pageBlob)
        {
            return BufferedPageBlobStream.Open(pageBlob);
        }
    }
}
