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
        ValueTask AsTran(Func<ICronTransaction, ValueTask> action);
        ValueTask<T> AsTran<T>(Func<ICronTransaction, ValueTask<T>> action);
        ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck();
        ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState();
        int ShardCount { get; }

        IReadOnlyList<int> GetShardOrderForTesting();
    }

    internal interface IWorkItemDbProcs
    {
        ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson);
        ValueTask<WorkItemInfo?> NextWorkItem(int shard);
        ValueTask CompleteWorkItem(long workItemId, int shard);
        ValueTask DelayWorkItem(long workItemId, int shard);
        ValueTask FailWorkItem(long workItemId, int shard);
        ValueTask PurgeWorkItems(int shard);


        ValueTask DeleteAllNQueueDataForUnitTests();
    }

    internal interface ICronTransaction : IAsyncDisposable
    {
        ValueTask CommitAsync();
        ValueTask EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection);
        ValueTask CreateCronJob(string name);
        ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(string cronJobName);
        ValueTask UpdateCronJobLastRanAt(string cronJobName);
        
    }
}