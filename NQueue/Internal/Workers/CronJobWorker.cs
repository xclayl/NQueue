using System;
using System.Linq;
using System.Threading.Tasks;
using Cronos;
using Microsoft.Extensions.Logging;
using NQueue.Internal.Model;

namespace NQueue.Internal.Workers
{

    internal class CronJobWorker : AbstractTimerWorker
    {
        private readonly WorkItemContextFactory _contextFactory;
        private readonly ConfigFactory _configFactory;

        public CronJobWorker(WorkItemContextFactory contextFactory, TimeZoneInfo tz, ConfigFactory configFactory,
            ILoggerFactory loggerFactory) : base(TimeSpan.FromMinutes(1),
            typeof(CronJobWorker).FullName, tz, loggerFactory)
        {
            _contextFactory = contextFactory;
            _configFactory = configFactory;
        }

        protected internal override async Task<bool> ExecuteOne()
        {
            var query = await _contextFactory.Get();
            var cronJobState = (await query.GetCronJobState())
                .ToDictionary(r => r.CronJobName);

            var cronJobs = (await _configFactory.GetConfig()).CronJobs;
            var logger = CreateLogger();

            foreach (var cronJob in cronJobs)
                try
                {
                    CronJobInfo? state;
                    cronJobState.TryGetValue(cronJob.Name, out state);

                    var cronExpression = CronExpression.Parse(cronJob.CronSpec);
                    var next = cronExpression.GetNextOccurrence(
                        state?.LastRanAt ?? new DateTimeOffset(0, TimeSpan.Zero),
                        _tz);
                    if (next < DateTimeOffset.Now)
                    {
                        logger.LogInformation($"Cron Job triggered: {cronJob.Name}");
                        await ProcessCron(state, cronJob, query);
                    }
                }
                catch (Exception e)
                {
                    logger.LogError(e.ToString());
                }

            return false;
        }

        private async Task ProcessCron(CronJobInfo? state, NQueueCronJob nQueueCronJob, WorkItemDbQuery query)
        {
            await using var tran = await query.BeginTran();

            if (state == null)
            {
                var cronJobId = await tran.CreateCronJob(nQueueCronJob.Name);
                state = new CronJobInfo(cronJobId, nQueueCronJob.Name, new DateTimeOffset(0, TimeSpan.Zero));
            }

            var cron = await tran.SelectAndLockCronJob(state.CronJobId);

            if (cron.active)
            {
                var exp = CronExpression.Parse(nQueueCronJob.CronSpec);

                var nextOccurrence = exp.GetNextOccurrence(cron.lastRan, _tz);
                if (nextOccurrence != null && nextOccurrence > DateTimeOffset.Now)
                    return;

                var url = string.Format(nQueueCronJob.Url, nextOccurrence ?? Now(_tz));

                await tran.EnqueueWorkItem(new Uri(url), nQueueCronJob.QueueName,
                    $"From CronJob {state.CronJobId}", true);

                await tran.UpdateCronJobLastRanAt(state.CronJobId);
            }

            await tran.CommitAsync();
        }


        private static DateTimeOffset Now(TimeZoneInfo tz)
        {
            var nowLocal = DateTimeOffset.Now;
            return nowLocal.ToOffset(tz.GetUtcOffset(nowLocal));
        }
    }
}