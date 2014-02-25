using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;


namespace WebRole1
{
    public class StatsEntity : TableEntity
    {
        public const string PARTITION_KEY = "Partition";
        public const string ROW_KEY = "Row";

        public StatsEntity(CrawlerStates crawlerState, uint numUrlsCrawled, Queue<string> last10UrlsCrawled, uint sizeOfQueue, float sizeOfIndexInMByte)
        {
            this.CrawlerState = crawlerState;
            this.NumUrlsCrawled = numUrlsCrawled;
            this.Last10UrlsCrawled = last10UrlsCrawled;
            this.SizeOfQueue = sizeOfQueue;
            this.SizeOfIndexInMByte = sizeOfIndexInMByte;

            this.PartitionKey = PARTITION_KEY;
            this.RowKey = ROW_KEY;
        }

        public StatsEntity() : this(CrawlerStates.Idle, 0, new Queue<string>(), 0, 0) { }

        //
        // Properties
        //
        public CrawlerStates CrawlerState { get; set; }
        public uint NumUrlsCrawled { get; set; }
        public Queue<string> Last10UrlsCrawled { get; set; }
        public uint SizeOfQueue { get; set; }
        public float SizeOfIndexInMByte { get; set; }
    }
}