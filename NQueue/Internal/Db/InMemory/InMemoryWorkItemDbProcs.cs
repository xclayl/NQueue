using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.InMemory;

internal class InMemoryWorkItemDbProcs : IWorkItemDbProcs
{
    public readonly InMemoryDb Db = new();

    public ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo,
        bool duplicateProtection, string? internalJson, string? blockQueueName, string? externalLockIdWhenComplete)
    {
        queueName ??= Guid.NewGuid().ToString();
        return Db.EnqueueWorkItem(tran, url, queueName, debugInfo, duplicateProtection, internalJson, blockQueueName, externalLockIdWhenComplete);
    }

    public ValueTask<WorkItemInfo?> NextWorkItem(int shard) => Db.NextWorkItem();
    
    public ValueTask<WorkItemInfo?> NextWorkItem(string queueName, int shard) => Db.NextWorkItem(queueName);

    public ValueTask CompleteWorkItem(long workItemId, int shard, ILogger logger) => Db.CompleteWorkItem(workItemId);
    public ValueTask DelayWorkItem(long workItemId, int shard, ILogger logger) => Db.DelayWorkItem(workItemId);

    public ValueTask FailWorkItem(long workItemId, int shard, ILogger logger) => Db.FailWorkItem(workItemId);

    public ValueTask PurgeWorkItems(int shard) => Db.PurgeWorkItems();


    public ValueTask ReleaseExternalLock(string queueName, int maxShards, string externalLockId) => Db.ReleaseExternalLock(queueName, externalLockId);

    public ValueTask DeleteAllNQueueDataForUnitTests() => Db.DeleteAllNQueueDataForUnitTests();


    internal ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState() => Db.GetCronJobState();

    internal ValueTask<ICronTransaction> BeginTran() => Db.BeginTran();
}

class CronWorkItemTran : ICronTransaction
{
    private SemaphoreSlim _lock;
    private readonly InMemoryDb _inMemoryDb;
    private readonly List<CronJobInfo> _cronJobState;
    private readonly Func<long> _nextId;



    private readonly List<TempWorkItemForInMemory> _tempWorkItems = new();
    private readonly List<CronJobInfo> _tempCronJobState = new();
    private bool _lockAcquired;

    private record TempWorkItemForInMemory
    {
        public long WorkItemId { get; init; }
        public Uri Url { get; init; }
        public string QueueName { get; init; }
        public string? DebugInfo { get; init; }
        public bool IsIngested { get; init; }
        public string? Internal { get; init; }
        public bool DuplicateProtection { get; init; }
        public int FailCount { get; init; } = 0;
        public string? BlockQueueName { get; init; }
    }
    
    
    
    public CronWorkItemTran(SemaphoreSlim @lock, InMemoryDb inMemoryDb, List<CronJobInfo> cronJobState, Func<long> nextId)
    {
        _lock = @lock;
        _inMemoryDb = inMemoryDb;
        _cronJobState = cronJobState;
        _nextId = nextId;
    }

    public ValueTask DisposeAsync()
    {
        if (_lockAcquired)
            _lock.Release();
        _lockAcquired = false;
        return ValueTask.CompletedTask;
    }

    public async ValueTask CommitAsync()
    {
        if (!_lockAcquired)
            throw new Exception("In memory cron lock not acquired");
        
        foreach (var tempWorkItem in _tempWorkItems)
        {
            await _inMemoryDb.EnqueueWorkItem(null, tempWorkItem.Url, tempWorkItem.QueueName, tempWorkItem.DebugInfo, tempWorkItem.DuplicateProtection, tempWorkItem.Internal, null, null);
        }

        foreach (var tempCj in _tempCronJobState)
        {
            var cj = _cronJobState.SingleOrDefault(j => j.CronJobName == tempCj.CronJobName);
            if (cj != null)
                _cronJobState.Remove(cj);

            _cronJobState.Add(tempCj);
        }
        
        _tempWorkItems.Clear();
        _tempCronJobState.Clear();
        _lock.Release();
        _lockAcquired = false;
    }

    public ValueTask EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection)
    {
        
        if (!_lockAcquired)
            throw new Exception("In memory cron lock not acquired");
        
        queueName ??= Guid.NewGuid().ToString();
        _tempWorkItems.Add(new TempWorkItemForInMemory()
        {
            WorkItemId = _nextId(),
            DebugInfo = debugInfo,
            Url = url,
            QueueName = queueName,
            DuplicateProtection = duplicateProtection,
            BlockQueueName = null
        });
        return ValueTask.CompletedTask;
    }

    public async ValueTask CreateCronJob(string name)
    {
        if (_lockAcquired)
            throw new Exception("In memory cron lock should not be acquired");

        using var _ = await ALock.Wait(_lock);
        
        _tempCronJobState.Add(new CronJobInfo(name,
            new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        
    }

    public async ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(string cronJobName)
    {
        await _lock.WaitAsync();
        _lockAcquired = true;
        
        var cj = _tempCronJobState.SingleOrDefault(j => j.CronJobName == cronJobName);
        if (cj == null)
        {
            cj = _cronJobState.Single(j => j.CronJobName == cronJobName);
            _tempCronJobState.Add(cj);
        }
        
        return (cj.LastRanAt, true);
    }

    public ValueTask UpdateCronJobLastRanAt(string cronJobName)
    {
        if (!_lockAcquired)
            throw new Exception("In memory cron lock not acquired");
       
        var cj = _tempCronJobState.Single(j => j.CronJobName == cronJobName);
        _tempCronJobState.Remove(cj);
        var newCj = new CronJobInfo(cj.CronJobName, DateTimeOffset.Now);
        _tempCronJobState.Add(newCj);
    

        return ValueTask.CompletedTask;
    }
}

internal class ALock : IDisposable
{
    public static async ValueTask<IDisposable> Wait(SemaphoreSlim s)
    {
        await s.WaitAsync();
        return new ALock(s);
    }

    private readonly SemaphoreSlim _locked;

    private ALock(SemaphoreSlim locked)
    {
        _locked = locked;
    }

    public void Dispose()
    {
        _locked.Release();
    }
}