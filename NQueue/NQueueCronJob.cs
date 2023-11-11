namespace NQueue
{


    public class NQueueCronJob
    {
        /// <param name="name">Name of the cron job to insert into the CronJob table</param>
        /// <param name="cronSpec">Visit https://crontab.guru/ to specify a cron spec, like 5 4 * * *</param>
        /// <param name="url">URL to insert into the WorkItem table.  A date can be inserted, like http://url/{0:yyyy-MM-dd}/</param>
        /// <param name="queueName">The queue name to insert into the WorkItem table</param>
        public NQueueCronJob(string name, string cronSpec, string url, string queueName)
        {
            Name = name;
            CronSpec = cronSpec;
            Url = url;
            QueueName = queueName;
        }

        public string Name { get; }
        public string CronSpec { get; }
        public string Url { get; }
        public string QueueName { get; }
    }

}