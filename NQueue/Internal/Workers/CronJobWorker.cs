﻿using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cronos;
using Microsoft.Extensions.Logging;
using NQueue.Internal.Db;
using NQueue.Internal.Model;

namespace NQueue.Internal.Workers
{

    internal class CronJobWorker : AbstractTimerWorker
    {
        private readonly IWorkItemDbConnection _workItemDbConnection;
        private readonly ConfigFactory _configFactory;
        private readonly IInternalWorkItemServiceState _state;

        public CronJobWorker(IWorkItemDbConnection workItemDbConnection, TimeZoneInfo tz, ConfigFactory configFactory,
            ILoggerFactory loggerFactory, IInternalWorkItemServiceState state) : base(TimeSpan.FromMinutes(1),
            typeof(CronJobWorker).FullName ?? nameof(CronJobWorker), tz, loggerFactory)
        {
            _workItemDbConnection = workItemDbConnection;
            _configFactory = configFactory;
            _state = state;
        }

        protected internal override async ValueTask<bool> ExecuteOne()
        {
            var cronJobState = (await _workItemDbConnection.GetCronJobState())
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
                        await ProcessCron(state, cronJob);
                    }
                }
                catch (Exception e)
                {
                    logger.LogError(e.ToString());
                }

            return false;
        }

        public override void Dispose()
        {
            // do nothing
        }

        private async ValueTask ProcessCron(CronJobInfo? state, NQueueCronJob nQueueCronJob)
        {
            var poll = await _workItemDbConnection.AsTran(async tran =>
            {
                
                if (state == null)
                {
                    await tran.CreateCronJob(nQueueCronJob.Name);
                    state = new CronJobInfo(nQueueCronJob.Name, new DateTimeOffset(0, TimeSpan.Zero));
                }

                var cron = await tran.SelectAndLockCronJob(state.CronJobName);

                if (cron.active)
                {
                    var exp = CronExpression.Parse(nQueueCronJob.CronSpec);

                    var nextOccurrence = exp.GetNextOccurrence(cron.lastRan, _tz);
                    if (nextOccurrence != null && nextOccurrence > DateTimeOffset.Now)
                        return false;

                    var url = string.Format(nQueueCronJob.Url, nextOccurrence ?? Now(_tz));

                    var currentActivity = Activity.Current;
                    Activity.Current = null;
                    try
                    {
                        await tran.EnqueueWorkItem(new Uri(url), nQueueCronJob.QueueName,
                            $"From CronJob {state.CronJobName}", true);
                    }
                    finally
                    {
                        Activity.Current = currentActivity;
                    }

                    await tran.UpdateCronJobLastRanAt(state.CronJobName);
                }
                return true;
            });
            
            if (poll)
                _state.PollNow();

            
        }


        private static DateTimeOffset Now(TimeZoneInfo tz)
        {
            var nowLocal = DateTimeOffset.Now;
            return nowLocal.ToOffset(tz.GetUtcOffset(nowLocal));
        }
    }
}