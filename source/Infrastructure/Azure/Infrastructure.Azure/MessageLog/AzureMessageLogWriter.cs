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

namespace Infrastructure.Azure.MessageLog
{
    using System;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;

    public class AzureMessageLogWriter : IAzureMessageLogWriter
    {
        private readonly string tableName;
        private readonly CloudTableClient tableClient;

        public AzureMessageLogWriter(CloudStorageAccount account, string tableName)
        {
            if (account == null) throw new ArgumentNullException("account");
            if (tableName == null) throw new ArgumentNullException("tableName");
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName");

            this.tableName = tableName;
            this.tableClient = account.CreateCloudTableClient();
            this.tableClient.DefaultRequestOptions.RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(1), 10);

            tableClient.GetTableReference(tableName).CreateIfNotExists();
        }

        public void Save(MessageLogEntity entity)
        {
            var table = this.tableClient.GetTableReference(this.tableName);
            var operation = TableOperation.InsertOrReplace(entity);
            table.Execute(operation);
        }
    }
}
