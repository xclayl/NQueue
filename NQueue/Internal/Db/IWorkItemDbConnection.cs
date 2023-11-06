using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db
{
    internal interface IWorkItemDbConnection
    {
        ValueTask<IWorkItemDbProcs> Get();
        ValueTask<ICronTransaction> BeginTran();
        ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck();
        ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState();
    }

    internal interface IWorkItemDbProcs
    {
        ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson);
        ValueTask<WorkItemInfo?> NextWorkItem();
        ValueTask CompleteWorkItem(int workItemId);
        ValueTask FailWorkItem(int workItemId);
        ValueTask PurgeWorkItems();


        ValueTask DeleteAllNQueueDataForUnitTests();
    }

    internal interface ICronTransaction : IAsyncDisposable
    {
        ValueTask CommitAsync();
        ValueTask EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection);
        ValueTask<int> CreateCronJob(string name);
        ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(int cronJobId);
        ValueTask UpdateCronJobLastRanAt(int cronJobId);
        
    }
}