using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db
{
    internal interface IWorkItemDbConnection
    {
        ValueTask<IWorkItemDbQuery> Get();
    }

    internal interface IWorkItemDbQuery
    {
        ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck();
        ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection);
        ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState();
        ValueTask<IWorkItemDbTransaction> BeginTran();
        ValueTask<WorkItemInfo?> NextWorkItem();
        ValueTask CompleteWorkItem(int workItemId);
        ValueTask FailWorkItem(int workItemId);
        ValueTask PurgeWorkItems();


        ValueTask DeleteAllNQueueDataForUnitTests();
    }

    internal interface IWorkItemDbTransaction : IAsyncDisposable
    {
        ValueTask CommitAsync();
        ValueTask EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection);
        ValueTask<int> CreateCronJob(string name);
        ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(int cronJobId);
        ValueTask UpdateCronJobLastRanAt(int cronJobId);
        
    }
}