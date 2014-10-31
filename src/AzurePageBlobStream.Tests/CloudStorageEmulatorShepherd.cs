using System;
using System.Diagnostics;
using System.IO;
using System.Linq.Expressions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace AzurePageBlobStream.Tests
{
    public class CloudStorageEmulatorShepherd
    {
        public void Start()
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;

                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                blobClient.DefaultRequestOptions.ServerTimeout = TimeSpan.FromSeconds(1);
                CloudBlobContainer container = blobClient.GetContainerReference("test");
                container.Exists(new BlobRequestOptions()
                {
                    ServerTimeout = TimeSpan.FromSeconds(1)
                });

            }
            catch (TimeoutException)
            {
                StartEmulator();
            }
            catch (StorageException)
            {
                StartEmulator();
            }
        }

        private static void StartEmulator()
        {
            ProcessStartInfo processStartInfo = new ProcessStartInfo()
            {
                FileName = Path.Combine(
                    @"C:\Program Files\Microsoft SDKs\Azure\Emulator",
                    "csrun.exe"),
                Arguments = @"/devstore",
            };

            using (Process process = Process.Start(processStartInfo))
            {
                process.WaitForExit();
            }
        }
    }
}