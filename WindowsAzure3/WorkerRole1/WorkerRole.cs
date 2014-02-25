using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Xml;
using System.IO;
using System.Threading;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using WebRole1;

namespace WorkerRole1
{
    public class WorkerRole : RoleEntryPoint
    {
        private const string crawlURL = @"http://www.cnn.com/";
        private const string crawlDomain = @"cnn.com";
        private StatsEntity statsEntity = new StatsEntity();
        private HashSet<string> crawledUrls = new HashSet<string>();
        private List<string> disallowedDirs = new List<string>();

        public override void Run()
        {
            // Initialize the queues and table
            InitalizeCommandQueue();
            InitializeCrawlingQueue();
            InitializeCrawlingTable();
            
            while (true)
            {
                Thread.Sleep(500);

                // Process CommandQueue
                ProcessCommandQueue();

                // Do crawling
                if (statsEntity.CrawlerState == CrawlerStates.Loading)
                {
                    OnCrawlerStateLoading();
                }
                else if (statsEntity.CrawlerState == CrawlerStates.Crawling)
                {
                    OnCrawlerStateCrawling();
                }

                // Do nothing if idle
            }
        }
        
        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections 
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            return base.OnStart();
        }

        private void InitalizeCommandQueue()
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

            // Clear the queue
            commandQueue.Clear();
        }

        private void InitializeCrawlingQueue()
        {
            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the queue client
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            // Retrieve a reference to CrawlingQueue
            CloudQueue crawlingQueue = queueClient.GetQueueReference(WebRole.AZURE_CRAWLING_QUEUE);

            // Create the queue if it doesn't already exist
            crawlingQueue.CreateIfNotExists();

            // Clear the queue
            crawlingQueue.Clear();
        }

        private void InitializeCrawlingTable()
        {
            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the table client
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to CrawlingTable
            CloudTable crawlingTable = tableClient.GetTableReference(WebRole.AZURE_CRAWLING_TABLE);

            // Create the table if it doesn't exist
            crawlingTable.CreateIfNotExists();

            // Clear the table
            crawlingTable.Delete();
            crawlingTable.Create();
        }

        private void ProcessCommandQueue()
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

            CloudQueueMessage message = commandQueue.GetMessage();
            
            if (message == null)
            {
                // Exit this function if there is no message
                return;
            }

            if (message.AsString == Commands.CMD_START_CRAWLER.ToString())
            {
                if (statsEntity.CrawlerState == CrawlerStates.Idle)
                {
                    // Start crawling if the current state is 'idle'
                    statsEntity.CrawlerState = CrawlerStates.Loading;
                }
            }
            else if (message.AsString == Commands.CMD_CLEAR_CRAWLER.ToString())
            {
                ClearCrawler();
            }

            // After reading the message, the client should delete it
            commandQueue.DeleteMessage(message);
        }

        private void OnCrawlerStateLoading()
        {
            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the queue client
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            // Retrieve a reference to CrawlingQueue
            CloudQueue crawlingQueue = queueClient.GetQueueReference(WebRole.AZURE_CRAWLING_QUEUE);

            // Create the queue if it doesn't already exist
            crawlingQueue.CreateIfNotExists();

            // Load the site maps and disallow list from robots.txt
            try
            {
                WebClient webClient = new WebClient();
                string data = webClient.DownloadString(crawlURL + "robots.txt");
                string[] lines = data.Split('\n');
                foreach(string line in lines)
                {
                    if (line.Contains("Sitemap"))
                    {
                        CloudQueueMessage message = new CloudQueueMessage(line.Substring(line.IndexOf("http"), line.Length - 9));
                        crawlingQueue.AddMessage(message);
                    }
                    else if (line.Contains("Disallow"))
                    {
                        disallowedDirs.Add(line.Remove(0, 11));
                    }
                }
            }
            catch (Exception)
            {
                // Do nothing
            }

            // Change state to 'crawling'
            statsEntity.CrawlerState = CrawlerStates.Crawling;
        }

        private void OnCrawlerStateCrawling()
        {
            WebPageEntity webpageEntity = new WebPageEntity();
            List<string> qualifiedUrls = new List<string>();
            XmlDocument doc = new XmlDocument();

            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the queue and table client
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to CrawlingQueue and CrawlingTable
            CloudQueue crawlingQueue = queueClient.GetQueueReference(WebRole.AZURE_CRAWLING_QUEUE);
            CloudTable crawlingTable = tableClient.GetTableReference(WebRole.AZURE_CRAWLING_TABLE);

            // Create the queue and table if it doesn't already exist
            crawlingQueue.CreateIfNotExists();
            crawlingTable.CreateIfNotExists();

            CloudQueueMessage message = crawlingQueue.GetMessage();

            if (message == null)
            {
                // No more webpage to crawl
                statsEntity.CrawlerState = CrawlerStates.Idle;
                return;
            }

            // Store URL to webpage entity
            webpageEntity.URL = message.AsString;

            // Load URL as XML document
            try
            {
                doc.Load(message.AsString);
            }
            catch(Exception)
            {
                // Remove problematic webpage and skip
                crawlingQueue.DeleteMessage(message);
                return;
            }
            
            // Store Title to webpage entity
            webpageEntity.Title = GetPageTitle(doc);

            // Put webpage entity to the crawling table
            crawlingTable.Execute(TableOperation.InsertOrReplace(webpageEntity));

            // Add this URL to the crawled hashset
            crawledUrls.Add(webpageEntity.URL);
            
            // Add the qualified links in this URL to the CrawlingQueue
            qualifiedUrls = GetPageLinks(doc);
            foreach (string url in qualifiedUrls)
            {
                crawlingQueue.AddMessage(new CloudQueueMessage(url));
            }

            // Update stats
            statsEntity.NumUrlsCrawled = statsEntity.NumUrlsCrawled + 1;
            statsEntity.Last10UrlsCrawled.Enqueue(webpageEntity.URL);
            while (statsEntity.Last10UrlsCrawled.Count > 10)
            {
                statsEntity.Last10UrlsCrawled.Dequeue();
            }
            statsEntity.SizeOfQueue = statsEntity.SizeOfQueue + (uint)qualifiedUrls.Count - 1;
            statsEntity.SizeOfIndexInMByte = statsEntity.SizeOfIndexInMByte + webpageEntity.GetEntitySizeMByte();

            // Put stats to table
            TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(statsEntity);
            TableResult tr = crawlingTable.Execute(insertOrReplaceOperation);

            // After processing the URL, the client should delete it
            crawlingQueue.DeleteMessage(message);
        }

        private string GetPageTitle(XmlDocument doc)
        {
            XmlReader reader = new XmlNodeReader(doc);

            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.Element)
                {
                    if (reader.Name.ToLower() == "title")
                    {
                        return reader.Name;
                    }
                }
            }

            return string.Empty;
        }

        private List<string> GetPageLinks(XmlDocument doc)
        {
            List<string> links = new List<string>();
            XmlReader reader = new XmlNodeReader(doc);

            // Look for <loc> and <a> tags
            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.Element)
                {
                    if (reader.Name.ToLower() == "loc")
                    {
                        if (reader.Read() && IsLinkAllowed(reader.Value))
                        {
                            links.Add(reader.Value.ToLower().Trim());
                        }
                    }
                    else if (reader.Name.ToLower() == "a")
                    {
                        if (reader.HasAttributes)
                        {
                            while (reader.MoveToNextAttribute())
                            {
                                if (reader.Name.ToLower() == "href")
                                {
                                    string URL = reader.Value.ToLower();
                                    if (URL[0] == '/')
                                    {
                                        // Append relative path to domain
                                        URL = crawlURL + URL.Substring(1, URL.Length - 1);
                                    }

                                    if (IsLinkAllowed(URL))
                                    {
                                        links.Add(URL);
                                    }
                                }
                                break;
                            }
                            reader.MoveToElement();
                        }
                    }
                }
            }

            return links;
        }

        private bool IsLinkAllowed(string URL)
        {
            // Check domain
            if (!URL.Contains(crawlDomain))
            {
                return false;
            }

            // Check disallow list
            foreach (string s in disallowedDirs)
            {
                if (URL.Contains(s))
                {
                    return false;
                }
            }

            // Check whether the page is already visited
            if (crawledUrls.Contains(URL))
            {
                return false;
            }

            return true;
        }

        private void ClearCrawler()
        {
            // Retrieve storage account from connection string
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the queue and table client
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to CrawlingQueue and CrawlingTable
            CloudQueue crawlingQueue = queueClient.GetQueueReference(WebRole.AZURE_CRAWLING_QUEUE);
            CloudTable crawlingTable = tableClient.GetTableReference(WebRole.AZURE_CRAWLING_TABLE);

            // Create the queue and table if it doesn't already exist
            crawlingQueue.CreateIfNotExists();
            crawlingTable.CreateIfNotExists();

            crawledUrls.Clear();
            crawlingQueue.Clear();
            disallowedDirs.Clear();
            crawlingTable.Delete();
            crawlingTable.CreateIfNotExists();
            statsEntity = new StatsEntity();
        }
    }
}
