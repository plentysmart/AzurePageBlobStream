using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Policy;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using ProtoBuf;
using Xunit;

namespace AzurePageBlobStream.Tests
{
    public class ProtoBufTests
    {
        private Random random = new Random();
        private const string ContainerName = "page-blob-stream-tests";

        private CloudStorageAccount account = CloudStorageAccount.DevelopmentStorageAccount;
        private CloudBlobClient client;
        private CloudBlobContainer container;

        public ProtoBufTests()
        {
            CloudStorageEmulatorShepherd shepherd = new CloudStorageEmulatorShepherd();
            shepherd.Start();
            this.client = account.CreateCloudBlobClient();
            this.container = client.GetContainerReference(ContainerName);
            if(container.Exists())
                container.Delete();
            container.Create();
        }

        [Fact(Skip = "Long running")]
        public void Write_MassiveStreamOfProtoBufMessages_ShouldBeTheSameAsFile()
        {
            string fileName = string.Format("{0:dd-MM-yyyy_hh_mm_ss}.bin", DateTime.Now);
            var seed = Guid.NewGuid().GetHashCode();
            random = new Random(seed);
            Debug.WriteLine("Seed: {0}", seed);
            long messageId = 0;
            //using (var stream = File.OpenWrite(fileName))
            //{
            var pageblob = container.GetPageBlobReference(fileName);
            if (!pageblob.Exists())
            {
                pageblob.Create(0);
            }
            var ms = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            using (var blobStream = new TestStream(pageblob, ms))
            {
                do
                {
                    var message = GenerateMessage(messageId);

                    //Serializer.SerializeWithLengthPrefix(stream, message, PrefixStyle.Fixed32BigEndian);
                    Serializer.SerializeWithLengthPrefix(blobStream, message, PrefixStyle.Fixed32BigEndian);
                    //Assert.Equal(stream.Position, blobStream.Position);
                    if (blobStream.PendingWritesSize > 3524*1024)
                    {
                        blobStream.Flush();
                        //stream.Flush();
                    }
                    messageId++;
                } while (blobStream.Length < 1900*1024*1024); //100MB
                blobStream.Flush();
            }
            //}
            Debug.WriteLine("Messages: {0}", messageId);
            //using (var stream = File.OpenRead(fileName))
            //{
            //    var pageblob = container.GetPageBlobReference(fileName);
            ms.Flush();
            using (var blobStream = new TestStream(pageblob, ms))
            {
                //stream.Position = 0;
                blobStream.Position = 0;
                //var deserializedFromFile = Serializer.DeserializeItems<Message>(stream,
                //    PrefixStyle.Fixed32BigEndian, -1).ToList();
                var deserializedFromBlob = Serializer.DeserializeItems<Message>(blobStream,
                    PrefixStyle.Fixed32BigEndian, -1);
                //Assert.Equal(deserializedFromFile, deserializedFromBlob);
                var messages = 0L;
                foreach (var message in deserializedFromBlob)
                {
                    messages++;
                }
                Assert.Equal(messageId, messages);
            }

        }

        [Fact]
        public void EqualityOperator_ForTheSameMessage_ShouldReturnTrue()
        {
            for (int i = 0; i < 1000; i++)
            {
                var message = GenerateMessage(1);
                Assert.True(message.IsEqualTo(message));
                Assert.Equal(message.GetHashCode(), message.GetHashCode());
            }
        } 
        
        [Fact]
        public void EqualityOperator_ForDifferentMessagesWithTheSameId_ShouldReturnFalse()
        {
            for (int i = 0; i < 10000; i++)
            {
                var message = GenerateMessage(1);
                var message2 = GenerateMessage(1);
                Assert.False(message.IsEqualTo(message2));
                Assert.NotEqual( message.GetHashCode(), message2.GetHashCode());
            }
        }

        public Message GenerateMessage(long id)
        {
            var currentTimeTicks = DateTime.Now.Ticks;
            Func<bool> randomBool = () => random.Next(0, 100) >= 50;
            var message = new Message()
            {
                Id = id,
                Content = randomBool() ? RandomString(random.Next(3, 400)) : null,
                BoolSimple = randomBool(),
                BoolNullable = randomBool() ? (bool?) randomBool() : null,
                SimpleDateTime = new DateTime(currentTimeTicks + random.Next(-5000000, 5000000)),
                NullableDateTime =
                    randomBool() ? (DateTime?) new DateTime(currentTimeTicks + random.Next(-5000000, 5000000)) : null,
                DecimalNullable = randomBool() ? (decimal?) random.NextDouble()*random.Next() : null,
                DecimalSimple = (decimal) random.NextDouble()*random.Next(),
                NullableLong = randomBool() ? (long?) random.Next()*random.Next() : null,
                SimpleLong = random.Next()*random.Next(),
                StringHashset = randomBool() ? new HashSet<string>(RandomStringArray()) : null,
                StringList = randomBool() ? new List<string>(RandomStringArray()) : null,
                //StringList =  new List<string>(RandomStringArray()),
            };
            return message;
        }

        private string[] RandomStringArray()
        {
            var elements = random.Next(0, 100);
            return Enumerable.Range(0, elements).Select(x => RandomString(random.Next(3, 100))).ToArray();
        }

        private string RandomString(int size)
        {
            StringBuilder builder = new StringBuilder();
            char ch;
            for (int i = 0; i < size; i++)
            {
                ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26*random.NextDouble() + 65)));
                builder.Append(ch);
            }

            return builder.ToString();
        }
    }

    public class TestStream : BufferedPageBlobStream
    {
        private Stream fileStream;
        public TestStream(CloudPageBlob pageBlob, Stream streamToCheckAgainst)
            : base(pageBlob)
        {

            fileStream = streamToCheckAgainst;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            fileStream.Seek(this.Position, SeekOrigin.Begin);
            fileStream.Write(buffer, offset, count);
            base.Write(buffer, offset, count);
            Assert.Equal(fileStream.Position,this.Position);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            fileStream.Seek(this.Position, SeekOrigin.Begin);

            var fileBuffer = new byte[buffer.Length];
            buffer.CopyTo(fileBuffer, 0);
            var fileReadCount  = fileStream.Read(fileBuffer, offset, count);
            var bytesCount = base.Read(buffer, offset, count);
            Assert.Equal(fileBuffer,buffer);
            Assert.Equal(bytesCount,fileReadCount);
            Assert.Equal(Position,Position);
            return bytesCount;
        }

        public override void Flush()
        {
            fileStream.Flush();
            base.Flush();
        }
    }

    [ProtoContract]
    public class Message : IEquatable<Message>
    {
        [ProtoMember(1)]
        public long Id { get; set; }
        
        [ProtoMember(2)]
        public string Content { get; set; }

        [ProtoMember(3)]
        public long SimpleLong{ get; set; }
        
        [ProtoMember(4)]
        public long? NullableLong{ get; set; }

        [ProtoMember(5)]
        public DateTime SimpleDateTime{ get; set; }

        [ProtoMember(6)]
        public DateTime? NullableDateTime{ get; set; }

        [ProtoMember(7)]
        public List<string> StringList{ get; set; }

        [ProtoMember(8)]
        public HashSet<string> StringHashset { get; set; }
        
        [ProtoMember(9)]
        public bool BoolSimple { get; set; } 
    
        [ProtoMember(10)]
        public bool? BoolNullable { get; set; }  
        
        [ProtoMember(11)]
        public decimal DecimalSimple { get; set; } 
    
        [ProtoMember(12)]
        public decimal? DecimalNullable{ get; set; }

        public bool IsEqualTo(Message message)
        {
            if (this.Id != message.Id)
                return false;
            if (this.Content != message.Content)
                return false;
            if (this.SimpleLong != message.SimpleLong)
                return false;
            if (this.NullableLong != message.NullableLong)
                return false;
            if (this.SimpleDateTime != message.SimpleDateTime)
                return false;
            if (this.NullableDateTime != message.NullableDateTime)
                return false;
            if (this.BoolSimple != message.BoolSimple)
                return false;
            if (this.BoolNullable != message.BoolNullable)
                return false;
            if (this.DecimalSimple != message.DecimalSimple)
                return false;
            if (this.DecimalNullable != message.DecimalNullable)
                return false;
            if (!EnumerableEquals(this.StringList, message.StringList))
                return false;
            if (!EnumerableEquals(this.StringHashset, message.StringHashset))
                return false;
            return true;
        }

        public bool EnumerableEquals<T>(IEnumerable<T> a, IEnumerable<T> b)
        {
            if (a == null && b == null)
                return true;
            if ((a == null || b == null))
                return false;

            return a.SequenceEqual(b);
        }


        public override int GetHashCode()
        {
            int hash = 13;
            hash = (hash*7) + Id.GetHashCode();
            if(Content!=null)
            hash = (hash * 7) + Content.GetHashCode();
            hash = (hash * 7) + SimpleLong.GetHashCode();
            hash = (hash * 7) + NullableLong.GetHashCode();
            hash = (hash * 7) + SimpleDateTime.GetHashCode();
            hash = (hash * 7) + NullableDateTime.GetHashCode();
            hash = (hash * 7) + BoolSimple.GetHashCode();
            hash = (hash * 7) + BoolNullable.GetHashCode();
            hash = (hash * 7) + DecimalSimple.GetHashCode();
            hash = (hash * 7) + DecimalNullable.GetHashCode();
            hash = (hash * 7) + DecimalNullable.GetHashCode();
            if (StringList != null)
            hash = (hash * 7) + StringList.GetHashCode();
            if (StringHashset != null)
            hash = (hash * 7) + StringHashset.GetHashCode();
            return hash;
        }


        public bool Equals(Message other)
        {
            return this.IsEqualTo(other);
        }

        public override bool Equals(object obj)
        {
            Message message = obj as Message;
            if (message != null)
            {
                return Equals(message);
            }
            else
            {
                return false;
            }
        }
    }
}