using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AzurePageBlobStream.Tests
{
    public class BufferedPageBlobStreamTests : PageBlobStreamTests
    {
        protected override Stream InitializeReadWriteStream(CloudPageBlob pageBlob)
        {
            return BufferedPageBlobStream.Open(pageBlob);
        }
    }
}