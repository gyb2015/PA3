using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace WebRole1
{
    public class WebPageEntity : TableEntity
    {
        // Hard code partition key name and use URL as row key
        public const string PARTITION_KEY = "WebPagePartition";

        private string url;

        public WebPageEntity(string url, string title)
        {
            this.url = url;
            this.Title = title;

            this.PartitionKey = PARTITION_KEY;
            this.RowKey = url;
        }

        public WebPageEntity() : this(string.Empty, string.Empty) { }

        //
        // Properties
        //
        public string URL
        {
            get
            {
                return this.url;
            }
            set
            {
                this.url = value;

                // Store only ordinary characters as row key
                string rowKey = string.Empty;
                foreach (char c in value)
                {
                    if (((c >= 'a') && (c <= 'z'))
                        || ((c >= 'A') && (c <= 'Z'))
                        || ((c >= '0') && (c <= '9')))
                    {
                        rowKey += c;
                    }
                }
                this.RowKey = rowKey;
            }
        }

        public string Title { get; set; }

        //
        // Methods
        //
        public float GetEntitySizeMByte()
        {
            // Overhead
            float sizeInByte = 4;

            // Len(PartitionKey + RowKey) * 2
            sizeInByte += (PARTITION_KEY.Length + URL.Length) * 2;

            //
            // For-Each Property: 8 bytes + Len(Property Name) * 2 bytes + Sizeof(.Net Property Type)
            //

            // URL
            sizeInByte += 8 + (3) * 2 + URL.Length * 2 + 4;

            // Title
            sizeInByte += 8 + (5) * 2 + Title.Length * 2 + 4;

            // Return size in Megabyte
            return sizeInByte / 1024 / 1024;
        }
    }
}
