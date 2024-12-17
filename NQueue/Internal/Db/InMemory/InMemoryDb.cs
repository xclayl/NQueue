using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.InMemory;

public class InMemoryDb
{
    
    public record class WorkItemForInMemory
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

    public class Queue
    {
        public string QueueName { get; init; }
        public DateTimeOffset? LockedUntil { get; init; }
        public List<long> BlockedBy { get; } = new();
    }
    
    
    
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly List<WorkItemForInMemory> _workItems = new();
    private readonly List<WorkItemForInMemory> _completedWorkItems = new();
    private readonly List<CronJobInfo> _cronJobState = new();
    private readonly List<Queue> _queues = new();
    private int _nextId = 23;



    public async ValueTask<IReadOnlyList<WorkItemForInMemory>> GetWorkItems()
    {
        using var _ = await ALock.Wait(_lock);
        return _workItems.ToList();
    }

    public async ValueTask<IReadOnlyList<WorkItemForInMemory>> GetCompletedWorkItems()
    {
        using var _ = await ALock.Wait(_lock);
        return _completedWorkItems.ToList();
    }

    public async ValueTask<IReadOnlyList<Queue>> GetQueues()
    {
        using var _ = await ALock.Wait(_lock);
        return _queues.ToList();
    }

    internal async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson, string? blockQueueName)
    {
        if (tran != null)
            throw new Exception("The in-memory NQueue implementation is not compatible with DB transactions.");

        using var _ = await ALock.Wait(_lock);
            
        queueName ??= Guid.NewGuid().ToString();
            
            
        if (duplicateProtection)
        {
            
            var count = _workItems.Count(w => w.QueueName == queueName
                                              && w.Url == url);
            
            if (count > 1)
                return;
        }

        _workItems.Add(new WorkItemForInMemory
        {
            WorkItemId = _nextId++,
            Url = url,
            QueueName = queueName,
            DebugInfo = debugInfo,
            IsIngested = false,
            Internal = internalJson,
            BlockQueueName = blockQueueName,
        });
            
                
            
    }

    internal async ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState()
    {
        using var _ = await ALock.Wait(_lock);
        return _cronJobState.ToList();
    }

    internal async ValueTask<ICronTransaction> BeginTran()
    {
        return new CronWorkItemTran(_lock, _workItems, _cronJobState, () => _nextId++);
    }

    internal async ValueTask<WorkItemInfo?> NextWorkItem(string? queueName = null)
    {
        using var _ = await ALock.Wait(_lock);

        foreach (var wi in _workItems.Where(w => !w.IsIngested))
        {
            if (_queues.All(q => q.QueueName != wi.QueueName))
            {
                _queues.Add(new Queue
                {
                    QueueName = wi.QueueName,
                    LockedUntil = null,
                });
            }

            if (wi.BlockQueueName != null)
            {
                var queueToBlock = _queues.Find(q => q.QueueName == wi.BlockQueueName);
                if (queueToBlock == null)
                    throw new Exception($"Queue {wi.QueueName} is not in the queue.");

                queueToBlock.BlockedBy.Add(wi.WorkItemId);
            }
        }

        var now = DateTimeOffset.Now;

        var next = _workItems.FirstOrDefault(w =>
        {
            if (queueName != null && w.QueueName != queueName)
                return false;
            
            var queue = _queues.Single(q => q.QueueName == w.QueueName);

            if (queue.LockedUntil != null && queue.LockedUntil > now)
                return false;
            
            if (queue.BlockedBy.Count > 0)
                return false;

            return true;
        });

        if (next == null)
            return null;
            
        var queue = _queues.Single(q => q.QueueName == next.QueueName);

        _queues.Remove(queue);
        _queues.Add(new Queue
        {
            QueueName = queue.QueueName,
            LockedUntil = now.AddHours(1)
        });

        return new WorkItemInfo(next.WorkItemId, next.Url.AbsoluteUri, next.Internal);
    }

    internal async ValueTask CompleteWorkItem(long workItemId)
    {
        using var _ = await ALock.Wait(_lock);

        var wi = _workItems.Single(w => w.WorkItemId == workItemId);

        var queue = _queues.Single(q => q.QueueName == wi.QueueName);

        if (wi.BlockQueueName != null)
        {
            var blockedQueue = _queues.Single(q => q.QueueName == wi.BlockQueueName);
            if (blockedQueue == null)
                throw new Exception($"Queue {wi.QueueName} is not in the queue.");
            
            blockedQueue.BlockedBy.RemoveAll(b => b == workItemId);
        }
            

        _workItems.Remove(wi);
        _completedWorkItems.Add(wi);

        _queues.Remove(queue);
        if (_workItems.Any(w => w.QueueName == queue.QueueName))
        {
            _queues.Add(new Queue
            {
                QueueName = queue.QueueName,
                LockedUntil = null
            });
        }
            
    }
    
    
    public async ValueTask DelayWorkItem(long workItemId)
    {
        using var _ = await ALock.Wait(_lock);
            
        var wi = _workItems.Single(w => w.WorkItemId == workItemId);
        var queue = _queues.Single(q => q.QueueName == wi.QueueName);
        
        _queues.Remove(queue);
        _queues.Add(new Queue
        {
            QueueName = queue.QueueName,
            LockedUntil = DateTimeOffset.Now,
        });

    }


    internal async ValueTask FailWorkItem(long workItemId)
    {
        using var _ = await ALock.Wait(_lock);
            
        var wi = _workItems.Single(w => w.WorkItemId == workItemId);
        var queue = _queues.Single(q => q.QueueName == wi.QueueName);
        
        _workItems.Remove(wi);
        _workItems.Add(wi with { FailCount = wi.FailCount + 1 });
        
        _queues.Remove(queue);
        _queues.Add(new Queue
        {
            QueueName = queue.QueueName,
            LockedUntil = DateTimeOffset.Now.AddMinutes(5),
        });
    }

    internal async ValueTask PurgeWorkItems()
    {
        using var _ = await ALock.Wait(_lock);

        _completedWorkItems.Clear();
    }

    internal async ValueTask DeleteAllNQueueDataForUnitTests()
    {
        using var _ = await ALock.Wait(_lock);

        _workItems.Clear();
        _completedWorkItems.Clear();
        _queues.Clear();
        _cronJobState.Clear();
    }
}
