using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Services.Client;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using Microsoft.WindowsAzure.StorageClient.Protocol;
using Microsoft.WindowsAzure.StorageClient.Tasks;
using System.Net;
using Common.Logging;
using System.Threading;
using System.Globalization;

namespace TableQuery
{
    class EntityOne : TableServiceEntity
    {
        public EntityOne()
        {
        }

        public EntityOne(string partitionKey, string rowKey, string name, string note)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.Name = name;
            this.Note = note;
        }

        public string Name { get; set; }
        public string Note { get; set; }

    }

    class KeyOnly : TableServiceEntity
    {
        public KeyOnly()
        {
        }

        public KeyOnly(string partitionKey, string rowKey)
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        public DateTime Deleted
        {
            get;
            set;
        }

    }


    partial class Program
    {
        static readonly ILog logger = LogManager.GetCurrentClassLogger();

        EntityOne[] data = new[] {
            new EntityOne("001","000","赤い", "アップル"),
            new EntityOne("001","001","赤い", "オラクル"),
            new EntityOne("001","002","赤い", "シャツ"),
        };

        EntityOne[] data1 = new[] {
            new EntityOne("001","000","", "\u3042\u3042\uFF01\uDBB8\uDF3A\u3055\u3063\u304D\u8D85\u5927\u304D\u306A\u30B4\u30AD\u30D6\u30EA\u304C\u90E8\u5C4B\u306B\u306F\u3057\u3063\u3066\u305F\u3088\u3001\u3069\u3046\u3057\u3087\u3046\uFF1F\uDBB8\uDF39\u79C1\u3053\u308C\u304C\u4E00\u756A\u6016\u3044\u3088\uFF01\u5BDD\u308C\u306A\u3044\u306A\u2026\u306A\u3093\u304B\u65B9\u6CD5\u304C\u3042\u308B\u304B\u306A\uDBB8\uDF3C\uDBB8\uDF41"),
            new EntityOne("001","001","", "\uDBB8\uDF3A\uDBB8\uDF39\u2026\uDBB8\uDF3C\uDBB8\uDF41"),
            new EntityOne("001","002","\uD867\uDE3D", "ホッケ"),
        };

        void DumpContext(string title, TableServiceContext context)
        {
            logger.Info(title);

            foreach (var e in context.Entities)
            {
                logger.Info(m => m("{0}\t{1}\t{2}\t{3}", e.Entity, e.ETag, e.Identity, e.ReadStreamUri));
            }
        }

        HttpWebRequest lastRequest;

        TableServiceContext GetTableServiceContext(CloudTableClient tables)
        {
            var context = tables.GetDataServiceContext();
            context.MergeOption = MergeOption.NoTracking;

            context.SendingRequest += (sender, args) =>
            {
                var request = args.Request as HttpWebRequest;
                request.Headers["x-ms-version"] = "2011-08-18";
                logger.Info(request.RequestUri);
                lastRequest = request;
            };


            return context;
        }

        void InsertOrMerge(CloudTableClient tables, string tableName)
        {
            var context = GetTableServiceContext(tables);

            foreach (var e in data)
            {
                context.AttachTo(tableName, e);
                context.UpdateObject(e);
            }

            var option = SaveChangesOptions.None; //  | SaveChangesOptions.Batch;
            context.SaveChangesWithRetries(option);

        }

        void InsertMany(CloudTableClient tables, string tableName, int num)
        {
            var context = GetTableServiceContext(tables);

            for (int i = num; i > 0; i--)
            {
                {
                    var e = new EntityOne("001", "000", "赤い", "アップル");
                    e.PartitionKey = i.ToString("00000000");
                    context.AttachTo(tableName, e);
                    context.UpdateObject(e);
                }

                var option = SaveChangesOptions.None;// | SaveChangesOptions.Batch;
                if ((i % 100 == 0) || (i == 1))
                {
                    context.SaveChangesWithRetries(option);
                    Console.WriteLine("{0}", i);
                }

            }
        }

        void AddObjects(CloudTableClient tables, string tableName)
        {
            var context = GetTableServiceContext(tables);

            foreach (var e in data)
            {
                context.AddObject(tableName, e);
            }

            var option = SaveChangesOptions.None; //  | SaveChangesOptions.Batch;
            context.SaveChangesWithRetries(option);

        }

        private ContinuationToken GetTableContinuationFromResponse(WebResponse response)
        {
            var np = "";
            var nr = "";

            np = response.Headers.Get("x-ms-continuation-NextPartitionKey");
            nr = response.Headers.Get("x-ms-continuation-NextRowKey");

            ContinuationToken token = null;

            if (np != null || nr != null)
            {
                token = new ContinuationToken()
                {
                    NextPartitionKey = np,
                    NextRowKey = nr
                };
            }
            return token;
        }


        void List(CloudTableClient tables, string tableName)
        {
            var lastRowKey = "";
            var lastPartitionKey = "";
            var count = 0;

            var context = GetTableServiceContext(tables);

            var query1 = context.CreateQuery<KeyOnly>(tableName).AsSaraTableQuery(context.RetryPolicy).AsQueryable();

            // take should be last 
            query1 = query1.Take(10000000);

            foreach (var e in query1)
            {
                count++;
                if (lastPartitionKey != e.PartitionKey)
                {
                    logger.Info(m => m("Start New PartitionKey:\t{0}", e.PartitionKey));
                }
                logger.Info(m => m("DATA:\t{0}\t{1}\t{2}\t{3}", e.PartitionKey, e.RowKey, e.Timestamp, e.Deleted));
                lastPartitionKey = e.PartitionKey;
                lastRowKey = e.RowKey;
            }

        }


        void List1(CloudTableClient tables, string tableName)
        {
            var hasData = true;
            var lastRowKey = "";
            var lastPartitionKey = "";
            var count = 0;

            while (hasData)
            {
                var context = GetTableServiceContext(tables);

                var query1 = context.CreateQuery<KeyOnly>(tableName).AsSaraTableQuery(context.RetryPolicy).AsQueryable();

                if (!String.IsNullOrEmpty(lastRowKey))
                    query1 = query1.Where(e => e.PartitionKey.CompareTo(lastPartitionKey) >= 0)
                        .Where(e => e.RowKey.CompareTo(lastRowKey) > 0);

                // take should be last 
                query1 = query1.Take(100000);

                hasData = false;
                foreach (var e in query1)
                {
                    count++;
                    hasData = true;
                    if (lastPartitionKey != e.PartitionKey)
                    {
                        logger.Info(m => m("Start New PartitionKey:\t{0}", e.PartitionKey));
                    }
                    logger.Info(m => m("DATA:\t{0}\t{1}\t{2}\t{3}", e.PartitionKey, e.RowKey, e.Timestamp, e.Deleted));
                    lastPartitionKey = e.PartitionKey;
                    lastRowKey = e.RowKey;
                }

            }

        }

        void List2(CloudTableClient tables, string tableName)
        {
            var hasData = true;
            var nextRowKey = "";
            var count = 0;

            while (hasData)
            {
                var context = GetTableServiceContext(tables);

                var query1 = context.CreateQuery<KeyOnly>(tableName).AsSaraTableQuery(context.RetryPolicy)
                    .Where(e => e.PartitionKey == "2520707516983831262-95b8ceee1e0d4c22b9cb04a2e9a9d6da");

                if (!String.IsNullOrEmpty(nextRowKey))
                    query1 = query1.Where(e => e.RowKey.CompareTo(nextRowKey) > 0);

                // take should be last 
                query1 = query1.Take(100000);

                hasData = false;
                foreach (var e in query1)
                {
                    count++;
                    hasData = true;
                    logger.Info(m => m("DATA:\t{0}\t{1}\t{2}\t{3}", e.PartitionKey, e.RowKey, e.Timestamp, e.Deleted));
                    nextRowKey = e.RowKey;
                }

            }

        }


        void Update(CloudTableClient tables, string tableName)
        {
            var context = GetTableServiceContext(tables);
            var query = context.CreateQuery<EntityOne>(tableName);

            foreach (var e in query)
            {
                logger.Info(m => m("{0}\t{1}\t{2}\t{3}", e.PartitionKey, e.RowKey, e.Name, e.Note));
                e.Note = "更新";
                context.UpdateObject(e);
            }

            var option = SaveChangesOptions.None; //  | SaveChangesOptions.Batch;
            context.SaveChangesWithRetries(option);
        }

        void CloudTableQuery001(CloudTableClient tables, string tableName)
        {
            var context = GetTableServiceContext(tables);

            // query is instance of CloudTableQuery
            var query1 = context.CreateQuery<KeyOnly>(tableName).AsTableServiceQuery();

            logger.Info(m => m("{0}\t{1}", query1 is CloudTableQuery<KeyOnly>, query1.GetType().Name));

            // now DataServiceOrderedQuery
            var query2 = query1.Where(e => e.PartitionKey.CompareTo("R") >= 0).Where(e => e.PartitionKey.CompareTo("S") < 0);

            logger.Info(m => m("{0}\t{1}", query2 is CloudTableQuery<KeyOnly>, query2.GetType().Name));

            foreach (var e in query2)
            {
                //                logger.Info(m => m("{0}\t{1}", e.PartitionKey, e.RowKey));
            }


            var query3 = from e in context.CreateQuery<KeyOnly>(tableName).AsTableServiceQuery()
                         where e.PartitionKey.CompareTo("0") >= 0
                         select e;

            foreach (var e in query3)
            {
                logger.Info(m => m("{0}\t{1}", e.PartitionKey, e.RowKey));
            }



        }

        /// <summary>
        /// UseDevelopmentStorage=true
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            // http://www.ietf.org/rfc/rfc4646.txt
            Thread.CurrentThread.CurrentUICulture = new CultureInfo("en-US");

            try
            {
                foreach (var s in args)
                {
                    Console.WriteLine(s);
                }

                var tableName = args[1] == "-" ? "" : args[1]; // empty の代わりに"-"を指定

                var connection = args[2];
                var tables = CloudStorageAccount.Parse(connection).CreateCloudTableClient();

                tables.RetryPolicy = SaraRetryPolicies.RetryExponential(RetryPolicies.DefaultClientRetryCount,
                    RetryPolicies.DefaultMinBackoff,
                    RetryPolicies.DefaultMaxBackoff,
                    RetryPolicies.DefaultClientBackoff
                    );


                var program = new Program();
                switch (args[0])
                {
                    case "-lt":
                        program.ListTable(tables, tableName);
                        break;

                    case "-dt":
                        program.DropTable(tables, tableName);
                        break;
                    default:
                        // 
                        if (!String.IsNullOrEmpty(tableName) && tables.CreateTableIfNotExist(tableName))
                        {
                            logger.Debug("table created");
                        }

                        switch (args[0])
                        {
                            case "-l":

                                program.List(tables, tableName);
                                break;

                            case "-a":
                                program.InsertMany(tables, tableName, 10000);
                                break;

                            case "-im":
                                program.InsertOrMerge(tables, tableName);
                                break;

                            case "-u":
                                program.Update(tables, tableName);
                                break;

                            case "-1":
                                program.CloudTableQuery001(tables, tableName);
                                break;
                        }
                        break;
                }
            }
            catch (Exception e)
            {
                logger.Error("Error:", e);
            }

        }
    }

    public class ContinuationToken
    {
        public ContinuationToken()
        {
        }

        public string NextPartitionKey;
        public string NextRowKey;
    }
}

