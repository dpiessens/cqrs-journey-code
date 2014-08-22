// ==============================================================================================================
// Microsoft patterns & practices
// CQRS Journey project
// ==============================================================================================================
// ©2012 Microsoft. All rights reserved. Certain content used with permission from contributors
// http://go.microsoft.com/fwlink/p/?LinkID=258575
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance 
// with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License is 
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
// See the License for the specific language governing permissions and limitations under the License.
// ==============================================================================================================

namespace Infrastructure.Azure.BlobStorage
{
    using System;
    using System.IO;
    using System.Net;

    using Infrastructure.BlobStorage;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;

    public class CloudBlobStorage : IBlobStorage
    {
        private readonly CloudStorageAccount account;
        private readonly string rootContainerName;
        private readonly CloudBlobClient blobClient;
        private readonly BlobRequestOptions blobRequestWriteOptions;

        public CloudBlobStorage(CloudStorageAccount account, string rootContainerName)
        {
            this.account = account;
            this.rootContainerName = rootContainerName;

            this.blobClient = account.CreateCloudBlobClient();
            this.blobClient.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(1), 1);

            //this.readRetryPolicy.Retrying += (s, e) => Trace.TraceWarning("An error occurred in attempt number {1} to read from blob storage: {0}", e.LastException.Message, e.CurrentRetryCount);

            this.blobRequestWriteOptions = new BlobRequestOptions { RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(10), 1) };
            //this.writeRetryPolicy.Retrying += (s, e) => Trace.TraceWarning("An error occurred in attempt number {1} to write to blob storage: {0}", e.LastException.Message, e.CurrentRetryCount);

            var containerReference = this.blobClient.GetContainerReference(this.rootContainerName);
            containerReference.CreateIfNotExists(this.blobRequestWriteOptions);
        }

        public byte[] Find(string id)
        {
            var containerReference = this.blobClient.GetContainerReference(this.rootContainerName);
            var blobReference = containerReference.GetBlobReferenceFromServer(id);

            try
            {
                using (var memoryStream = new MemoryStream())
                {
                    blobReference.DownloadToStream(memoryStream);
                    return memoryStream.ToArray();
                } 
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    return null;
                }

                throw;
            }
        }

        public void Save(string id, string contentType, byte[] blob)
        {
            var client = this.account.CreateCloudBlobClient();
            var containerReference = client.GetContainerReference(this.rootContainerName);
            var blobReference = containerReference.GetBlobReferenceFromServer(id);

            blobReference.UploadFromByteArray(blob, 0, blob.Length, null, this.blobRequestWriteOptions);
        }

        public void Delete(string id)
        {
            var client = this.account.CreateCloudBlobClient();
            var containerReference = client.GetContainerReference(this.rootContainerName);
            var blobReference = containerReference.GetBlobReferenceFromServer(id);

            try
            {
                blobReference.DeleteIfExists();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    throw;
                }
            }
        }
    }
}
