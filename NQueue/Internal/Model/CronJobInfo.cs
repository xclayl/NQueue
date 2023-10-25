using System;

namespace NQueue.Internal.Model
{
    internal class CronJobInfo
    {
        public CronJobInfo(int cronJobId, string cronJobName, DateTimeOffset lastRanAt)
        {
            CronJobId = cronJobId;
            CronJobName = cronJobName;
            LastRanAt = lastRanAt;
        }

        public int CronJobId { get; }
        public string CronJobName { get; }
        public DateTimeOffset LastRanAt { get; }

    }
}