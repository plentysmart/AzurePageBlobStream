﻿using System;
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
                stream.Position = 10;
                stream.Write(data2, 0, data2.Length);
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
            };
            VerifyAgainstMemoryStream(operation, x =>
            {
                var buffer = new byte[data.Length + data2.Length];
                x.Position = 0;
                x.Read(buffer, 0, buffer.Length);
                return buffer;
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
            return PageBlobStream.Open(pageBlob);
        }
    }
}
