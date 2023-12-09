using System;

namespace NQueue.Internal.Model
{
    internal class CronJobInfo
    {
        public CronJobInfo(string cronJobName, DateTimeOffset lastRanAt)
        {
            CronJobName = cronJobName;
            LastRanAt = lastRanAt;
        }

        public string CronJobName { get; }
        public DateTimeOffset LastRanAt { get; }

    }
}