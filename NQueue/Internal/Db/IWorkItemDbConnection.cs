using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
        IAsyncEnumerable<WorkItemForTests> GetWorkItemsForTests();
        ValueTask<IReadOnlyList<QueueInfo>> GetQueuesForTesting();
        IAsyncEnumerable<WorkItemForTests> GetCompletedWorkItemsForTests();
    }

    internal interface IWorkItemDbProcs
    {
        ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson, string? blockQueueName);
        ValueTask<WorkItemInfo?> NextWorkItem(int shard);
        ValueTask<WorkItemInfo?> NextWorkItem(string queueName, int shard);
        ValueTask CompleteWorkItem(long workItemId, int shard, ILogger logger);
        ValueTask DelayWorkItem(long workItemId, int shard, ILogger logger);
        ValueTask FailWorkItem(long workItemId, int shard, ILogger logger);
        ValueTask PurgeWorkItems(int shard);
        ValueTask AcquireExternalLock(string queueName, string externalLockId);
        ValueTask ReleaseExternalLock(string queueName, string externalLockId);


        ValueTask DeleteAllNQueueDataForUnitTests();
    }

    internal interface ICronTransaction : IAsyncDisposable
    {
        ValueTask CommitAsync();
        ValueTask EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection, string? blockQueueName);
        ValueTask CreateCronJob(string name);
        ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(string cronJobName);
        ValueTask UpdateCronJobLastRanAt(string cronJobName);
        
    }
}