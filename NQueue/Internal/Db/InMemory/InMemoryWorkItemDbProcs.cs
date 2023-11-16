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

    public ValueTask<WorkItemInfo?> NextWorkItem() => Db.NextWorkItem();

    public ValueTask CompleteWorkItem(int workItemId) => Db.CompleteWorkItem(workItemId);

    public ValueTask FailWorkItem(int workItemId) => Db.FailWorkItem(workItemId);

    public ValueTask PurgeWorkItems() => Db.PurgeWorkItems();

    public ValueTask DeleteAllNQueueDataForUnitTests() => Db.DeleteAllNQueueDataForUnitTests();


    internal ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState() => Db.GetCronJobState();

    internal ValueTask<ICronTransaction> BeginTran() => Db.BeginTran();
}

class CronWorkItemTran : ICronTransaction
{
    private IDisposable? _lock;
    private readonly List<InMemoryDb.WorkItem> _workItems;
    private readonly List<CronJobInfo> _cronJobState;
    private readonly Func<int> _nextCronJobId;
    
    
    private readonly List<InMemoryDb.WorkItem> _tempWorkItems=new();
    private readonly List<CronJobInfo> _tempCronJobState = new(); 

    public CronWorkItemTran(IDisposable @lock, List<InMemoryDb.WorkItem> workItems, List<CronJobInfo> cronJobState, Func<int> nextCronJobId)
    {
        _lock = @lock;
        _workItems = workItems;
        _cronJobState = cronJobState;
        _nextCronJobId = nextCronJobId;
    }

    public ValueTask DisposeAsync()
    {
        _lock?.Dispose();
        _lock = null;
        return ValueTask.CompletedTask;
    }

    public ValueTask CommitAsync()
    {
        _workItems.AddRange(_tempWorkItems);
        _tempWorkItems.Clear();
        _lock?.Dispose();
        _lock = null;
        return ValueTask.CompletedTask;
    }

    public ValueTask EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection)
    {
        queueName ??= Guid.NewGuid().ToString();
        _tempWorkItems.Add(new InMemoryDb.WorkItem
        {
            DebugInfo = debugInfo,
            Url = url,
            QueueName = queueName,
        });
        return ValueTask.CompletedTask;
    }

    public ValueTask<int> CreateCronJob(string name)
    {
        var cronJobId = _nextCronJobId();
        _tempCronJobState.Add(new CronJobInfo(cronJobId, name, new DateTimeOffset(2000, 1 , 1, 0,0 ,0, TimeSpan.Zero)));
        return ValueTask.FromResult(cronJobId);
    }

    public ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(int cronJobId)
    {
        var cj = _cronJobState.Single(j => j.CronJobId == cronJobId);
        return ValueTask.FromResult((cj.LastRanAt, true));
    }

    public ValueTask UpdateCronJobLastRanAt(int cronJobId)
    {
        var cj = _cronJobState.Single(j => j.CronJobId == cronJobId);
        _cronJobState.Remove(cj);
        var newCj = new CronJobInfo(cj.CronJobId, cj.CronJobName, DateTimeOffset.Now);
        _cronJobState.Add(newCj);
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