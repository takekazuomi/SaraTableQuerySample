﻿using System;
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

namespace TableQuery
{
    partial class Program
    {


        void ListTable(CloudTableClient tables, string name)
        {
            logger.Info("List Table");

            foreach (var t in tables.ListTables(name))
            {
                logger.Info(t);
            }
        }


        void DropTable(CloudTableClient tables, string name)
        {
            logger.Info("Drop Table");

           foreach (var t in tables.ListTables(name))
            {
                if (tables.DeleteTableIfExist(t))
                {
                    logger.Info(m=>m("{0} deleted", t));
                }
                else
                {
                     logger.Info(m=>m("{0} not deleted", t));
               }
            }
        }
    }
}
