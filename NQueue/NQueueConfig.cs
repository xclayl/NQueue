using System;
using System.Collections.Generic;
using System.Net.Http;
using NQueue.Internal;
using NQueue.Internal.SqlServer;

namespace NQueue
{

    public class NQueueServiceConfig
    {
        // public bool RunWorkers { get; set; } = true;
        public int QueueRunners { get; set; } = 1;
        public TimeSpan PollInterval { get; set; } = TimeSpan.FromMinutes(5);
        public Action<HttpClient, Uri> ConfigureAuth { get; set; } = (h, u) => { };
        public string ConnectionString { get; set; } = "";
        public IReadOnlyList<NQueueCronJob> CronJobs = new List<NQueueCronJob>();

        internal string CheckedConnectionString
        {
            get
            {
                if (string.IsNullOrWhiteSpace(ConnectionString))
                    throw new Exception("Please configure ConnectionString in AddNQueueHostedService()");
                return ConnectionString;
            }
        }

        public TimeZoneInfo TimeZone { get; set; } = TimeZoneInfo.Local;

        internal IWorkItemDbConnection GetWorkItemDbConnection()
        {
            return new WorkItemDbConnection(this);
        }

    }
}