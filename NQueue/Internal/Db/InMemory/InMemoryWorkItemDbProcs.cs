using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.InMemory;

internal class InMemoryWorkItemDbProcs : IWorkItemDbProcs
{
    public readonly InMemoryDb Db = new();

    public ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo,
        bool duplicateProtection, string? internalJson) =>
        Db.EnqueueWorkItem(tran, url, queueName, debugInfo, duplicateProtection, internalJson);

    public ValueTask<WorkItemInfo?> NextWorkItem(int shard) => Db.NextWorkItem();

    public ValueTask CompleteWorkItem(int workItemId, int shard) => Db.CompleteWorkItem(workItemId);

    public ValueTask FailWorkItem(int workItemId, int shard) => Db.FailWorkItem(workItemId);

    public ValueTask PurgeWorkItems(int shard) => Db.PurgeWorkItems();

    public ValueTask DeleteAllNQueueDataForUnitTests() => Db.DeleteAllNQueueDataForUnitTests();


    internal ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState() => Db.GetCronJobState();

    internal ValueTask<ICronTransaction> BeginTran() => Db.BeginTran();
}

class CronWorkItemTran : ICronTransaction
{
    private SemaphoreSlim _lock;
    private readonly List<InMemoryDb.WorkItem> _workItems;
    private readonly List<CronJobInfo> _cronJobState;
    private readonly Func<int> _nextId;
    
    
    private readonly List<InMemoryDb.WorkItem> _tempWorkItems=new();
    private readonly List<CronJobInfo> _tempCronJobState = new();
    private bool _lockAcquired;

    public CronWorkItemTran(SemaphoreSlim @lock, List<InMemoryDb.WorkItem> workItems, List<CronJobInfo> cronJobState, Func<int> nextId)
    {
        _lock = @lock;
        _workItems = workItems;
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

    public ValueTask CommitAsync()
    {
        if (!_lockAcquired)
            throw new Exception("In memory cron lock not acquired");
        
        foreach (var tempWorkItem in _tempWorkItems)
        {
            if (!tempWorkItem.DuplicateProtection)
                _workItems.Add(tempWorkItem);
            else
            {
                var count = _workItems.Count(w => w.QueueName == tempWorkItem.QueueName
                                                  && w.Url == tempWorkItem.Url);
                if (count <= 1)
                    _workItems.Add(tempWorkItem);
            }
        }

        foreach (var tempCj in _tempCronJobState)
        {
            var cj = _cronJobState.SingleOrDefault(j => j.CronJobId == tempCj.CronJobId);
            if (cj != null)
                _cronJobState.Remove(cj);

            _cronJobState.Add(tempCj);
        }
        
        _tempWorkItems.Clear();
        _tempCronJobState.Clear();
        _lock.Release();
        _lockAcquired = false;
        return ValueTask.CompletedTask;
    }

    public ValueTask EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection)
    {
        
        if (!_lockAcquired)
            throw new Exception("In memory cron lock not acquired");
        
        queueName ??= Guid.NewGuid().ToString();
        _tempWorkItems.Add(new InMemoryDb.WorkItem
        {
            WorkItemId = _nextId(),
            DebugInfo = debugInfo,
            Url = url,
            QueueName = queueName,
            DuplicateProtection = true
        });
        return ValueTask.CompletedTask;
    }

    public async ValueTask<int> CreateCronJob(string name)
    {
        if (_lockAcquired)
            throw new Exception("In memory cron lock should not be acquired");

        using var _ = await ALock.Wait(_lock);
        
        var cronJobId = _nextId();
        _tempCronJobState.Add(new CronJobInfo(cronJobId, name,
            new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        return cronJobId;
    }

    public async ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(int cronJobId)
    {
        await _lock.WaitAsync();
        _lockAcquired = true;
        
        var cj = _tempCronJobState.SingleOrDefault(j => j.CronJobId == cronJobId);
        if (cj == null)
        {
            cj = _cronJobState.Single(j => j.CronJobId == cronJobId);
            _tempCronJobState.Add(cj);
        }
        
        return (cj.LastRanAt, true);
    }

    public ValueTask UpdateCronJobLastRanAt(int cronJobId)
    {
        if (!_lockAcquired)
            throw new Exception("In memory cron lock not acquired");
       
        var cj = _tempCronJobState.Single(j => j.CronJobId == cronJobId);
        _tempCronJobState.Remove(cj);
        var newCj = new CronJobInfo(cj.CronJobId, cj.CronJobName, DateTimeOffset.Now);
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