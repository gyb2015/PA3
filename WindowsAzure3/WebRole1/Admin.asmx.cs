using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;

namespace WebRole1
{
    /// <summary>
    /// Summary description for WebService1
    /// </summary>
    [WebService(Namespace = "WebRole1")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // Allow this Web Service to be called from script, using ASP.NET AJAX
    [System.Web.Script.Services.ScriptService]
    public class WebService1 : System.Web.Services.WebService
    {
        PerformanceCounter cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", true);
        PerformanceCounter memCounter = new PerformanceCounter("Memory", "Available MBytes", true);

        [WebMethod]
        public void StartCrawler()
        {
            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the queue client
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            // Retrieve a reference to CommandQueue
            CloudQueue commandQueue = queueClient.GetQueueReference(WebRole.AZURE_COMMAND_QUEUE);

            // Create the queue if it doesn't already exist
            commandQueue.CreateIfNotExists();

            // Create queue message
            CloudQueueMessage message = new CloudQueueMessage(Commands.CMD_START_CRAWLER.ToString());

            // Add message to command queue
            commandQueue.AddMessage(message);
        }

        [WebMethod]
        public void ClearCrawler()
        {
            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the queue client
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            // Retrieve a reference to CommandQueue
            CloudQueue commandQueue = queueClient.GetQueueReference(WebRole.AZURE_COMMAND_QUEUE);

            // Create the queue if it doesn't already exist
            commandQueue.CreateIfNotExists();

            // Create queue message
            CloudQueueMessage message = new CloudQueueMessage(Commands.CMD_CLEAR_CRAWLER.ToString());

            // Add message to command queue
            commandQueue.AddMessage(message);
        }

        [WebMethod]
        public string GetPageTitle(string URL)
        {
            return null;
        }

        [WebMethod]
        public string GetStats()
        {
            string retval = string.Empty;

            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to CrawlingTable
            CloudTable crawlingTable = tableClient.GetTableReference(WebRole.AZURE_CRAWLING_TABLE);

            // Create the table if it doesn't exist
            crawlingTable.CreateIfNotExists();

            // Create a retrieve operation that takes a stats entity
            TableOperation retrieveOperation = TableOperation.Retrieve<StatsEntity>(StatsEntity.PARTITION_KEY, StatsEntity.ROW_KEY);

            // Execute the retrieve operation
            TableResult retrievedResult = crawlingTable.Execute(retrieveOperation);

            if (retrievedResult.Result == null)
            {
                return string.Empty;
            }

            // Convert result to StatsEntity
            StatsEntity statsEntity = (StatsEntity)retrievedResult.Result;

            // Crawler state
            retval += "Crawler State: " + statsEntity.CrawlerState.ToString() + @"<br />";

            // Machine counters
            retval += "CPU Utilization: " + Convert.ToUInt32(cpuCounter.NextValue()).ToString() + @"%<br />";
            retval += "RAM available: " + Convert.ToUInt32(memCounter.NextValue()).ToString() + @" MByte<br />";

            // #URLs crawled
            retval += @"#URLs crawled: " + statsEntity.NumUrlsCrawled.ToString() + @"<br />";

            // Size of queue
            retval += "Size of queue: " + statsEntity.SizeOfQueue.ToString() + @"<br />";

            // Size of index
            retval += "Size of index: " + statsEntity.SizeOfIndexInMByte.ToString("N", System.Globalization.CultureInfo.InvariantCulture) + @" MByte<br />";

            // Last 10 URLs crawled
            retval += "Last 10 URLs crawled: <br />";
            foreach(string link in statsEntity.Last10UrlsCrawled)
            {
                retval += link + @"<br />";
            }

            return retval;
        }

        /*
        [WebMethod]
        public void WorkerRoleCalculateSum(int a, int b, int c)
        {
            // Create a message and add it to the queue.
            CloudQueueMessage message = new CloudQueueMessage(a.ToString() + " " + b.ToString() + " " + c.ToString());
            queue.AddMessage(message);
         }

        [WebMethod]
        public int ReadSumFromTableStorage()
        {
            // Create a retrieve operation that takes a sum entity
            TableOperation retrieveOperation = TableOperation.Retrieve<SumEntity>("partition", "row");

            // Execute the retrieve operation
            TableResult retrievedResult = table.Execute(retrieveOperation);

            if (retrievedResult.Result != null)
            {
                return ((SumEntity)retrievedResult.Result).Sum;
            }

            return 0;
        }
        */
    }
}
