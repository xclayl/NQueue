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
    
    private record WorkItemForInMemory
    {
        public long WorkItemId { get; init; }
        public Uri Url { get; init; }
        public string QueueName { get; init; }
        public string? DebugInfo { get; init; }
        public bool IsIngested { get; init; }
        public string? Internal { get; init; }
        // public bool DuplicateProtection { get; init; }
        public int FailCount { get; init; } = 0;
        public string? BlockQueueName { get; init; }
    }

    private record Queue
    {
        public string QueueName { get; init; }
        public long NextWorkItemId {get; init; }
        public DateTimeOffset LockedUntil { get; init; }
        public IReadOnlyList<long> BlockedBy { get; init; } = new List<long>();
        public string? ExternalLockId { get; init; }
    }
    
    private record BlockingMessage
    {
        public int BlockingMessageId { get; init; }
        public string QueueName { get; init; }
        public bool IsCreatingBlock {get; init; }
        public long BlockingWorkItemId { get; init; }
    }

    
    
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly List<WorkItemForInMemory> _workItems = new();
    private readonly List<WorkItemForInMemory> _completedWorkItems = new();
    private readonly List<CronJobInfo> _cronJobState = new();
    private readonly List<Queue> _queues = new();
    private readonly List<BlockingMessage> _blockingMessages = new();
    private int _nextId = 23;



    internal async ValueTask<IReadOnlyList<WorkItem>> GetWorkItems()
    {
        using var _ = await ALock.Wait(_lock);
        return _workItems
            .Select(w => new WorkItem(w.WorkItemId, w.Url, w.QueueName, w.DebugInfo, w.Internal, w.FailCount))
            .ToList();
    }

    internal async ValueTask<IReadOnlyList<WorkItem>> GetCompletedWorkItems()
    {
        using var _ = await ALock.Wait(_lock);
        return _completedWorkItems
            .Select(w => new WorkItem(w.WorkItemId, w.Url, w.QueueName, w.DebugInfo, w.Internal, w.FailCount))
            .ToList();
    }

    internal async ValueTask<IReadOnlyList<QueueInfo>> GetQueues()
    {
        using var _ = await ALock.Wait(_lock);
        return _queues
            .Select(q => new QueueInfo(q.QueueName, q.LockedUntil, q.ExternalLockId, q.BlockedBy.Count, 0))
            .ToList();
    }

    internal async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string queueName, string? debugInfo, bool duplicateProtection, string? internalJson, string? blockQueueName)
    {
        if (tran != null)
            throw new Exception("The in-memory NQueue implementation is not compatible with DB transactions.");

        using var _ = await ALock.Wait(_lock);

        if (blockQueueName != null && blockQueueName == queueName)
            throw new Exception("blockQueueName must different from queueName, otherwise deadlock will occur");
            
        
        
        
        if (duplicateProtection)
        {
            
            var count = _workItems.Count(w => w.QueueName == queueName
                                              && w.Url == url);
            if (count > 1)
                return;
        }


        var wi = new WorkItemForInMemory
        {
            WorkItemId = _nextId++,
            Url = url,
            QueueName = queueName,
            DebugInfo = debugInfo,
            IsIngested = false,
            Internal = internalJson,
            BlockQueueName = blockQueueName,
        };
        _workItems.Add(wi);

        if (blockQueueName != null)
        {
            _blockingMessages.Add(new BlockingMessage
            {
                BlockingMessageId = _nextId++,
                QueueName = blockQueueName,
                IsCreatingBlock = true,
                BlockingWorkItemId = wi.WorkItemId,
            });

            // var queueToBlock = _queues.SingleOrDefault(q => q.QueueName == blockQueueName);
            // if (queueToBlock != null)
            // {
            //     _queues.Remove(queueToBlock);
            //     _queues.Add(queueToBlock with
            //     {
            //         BlockedBy = queueToBlock.BlockedBy.Concat(new [] {wi.WorkItemId}).ToList(),
            //     });
            // }
        }
            
                
            
    }

    internal async ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState()
    {
        using var _ = await ALock.Wait(_lock);
        return _cronJobState.ToList();
    }

    internal async ValueTask<ICronTransaction> BeginTran()
    {
        return new CronWorkItemTran(_lock, this, _cronJobState, () => _nextId++);
    }

    internal async ValueTask<WorkItemInfo?> NextWorkItem(string? queueName = null)
    {
        using var _ = await ALock.Wait(_lock);

        foreach (var wi in _workItems.OrderBy(wi => wi.WorkItemId).Where(w => !w.IsIngested).ToList())
        {
            if (_queues.All(q => q.QueueName != wi.QueueName))
            {
                _queues.Add(new Queue
                {
                    QueueName = wi.QueueName,
                    LockedUntil = DateTimeOffset.Now,
                    NextWorkItemId = wi.WorkItemId
                });
            }

            // if (wi.BlockQueueName != null)
            // {
            //     var queueToBlock = _queues.Find(q => q.QueueName == wi.BlockQueueName);
            //     if (queueToBlock == null)
            //         throw new Exception($"Queue {wi.QueueName} is not in the queue.");
            //
            //     queueToBlock.BlockedBy.Add(wi.WorkItemId);
            // }
            
            _workItems.Remove(wi);
            _workItems.Add(wi with
            {
                IsIngested = true
            });
        }


        foreach (var blockingMessage in _blockingMessages.OrderBy(b => b.BlockingMessageId).ToList())
        {
            var bQueue = _queues.SingleOrDefault(q => q.QueueName == blockingMessage.QueueName);
            if (bQueue != null)
            {
                _queues.Remove(bQueue);
                if (blockingMessage.IsCreatingBlock)
                    _queues.Add(bQueue with
                    {
                        BlockedBy = bQueue.BlockedBy.Concat(new [] {blockingMessage.BlockingWorkItemId}).ToList()
                    });
                else
                    _queues.Add(bQueue with
                    {
                        BlockedBy = bQueue.BlockedBy.Where(b => b != blockingMessage.BlockingWorkItemId).ToList()
                    });
            }
            _blockingMessages.Remove(blockingMessage);
        }

        var now = DateTimeOffset.Now;

        // var next = _workItems.FirstOrDefault(w =>
        // {
        //     if (queueName != null && w.QueueName != queueName)
        //         return false;
        //     
        //     var queue = _queues.Single(q => q.QueueName == w.QueueName);
        //
        //     if (queue.LockedUntil != null && queue.LockedUntil > now)
        //         return false;
        //     
        //     if (queue.BlockedBy.Count > 0)
        //         return false;
        //     
        //     if (queue.ExternalLockId != null)
        //         return false;
        //
        //     return true;
        // });
        //
        // if (next == null)
        //     return null;
            
        var queue = _queues.FirstOrDefault(q => (queueName == null || q.QueueName == queueName)
            && q.LockedUntil < now
            && q.ExternalLockId == null
            && !q.BlockedBy.Any());

        if (queue == null)
            return null;
        
        _queues.Remove(queue);
        _queues.Add(queue with
        {
            LockedUntil = now.AddHours(1),
        });

        var next = _workItems.Single(wi => wi.WorkItemId == queue.NextWorkItemId);
        
        return new WorkItemInfo(next.WorkItemId, next.Url.AbsoluteUri, next.Internal);
        
    }

    internal async ValueTask CompleteWorkItem(long workItemId)
    {
        using var _ = await ALock.Wait(_lock);

        var wi = _workItems.Single(w => w.WorkItemId == workItemId);

        var queue = _queues.SingleOrDefault(q => q.QueueName == wi.QueueName);

        if (queue?.BlockedBy.Any() ?? false)
            throw new Exception("Cannot complete a blocked work item (it should be blocked)");

        if (queue != null)
        {
            var nextWorkItemId = _workItems
                .OrderBy(w => w.WorkItemId)
                .Where(w =>
                    w.QueueName == queue.QueueName
                    && w.WorkItemId != workItemId
                    && w.IsIngested
                )
                .Select(w => (long?) w.WorkItemId)
                .FirstOrDefault();

            if (nextWorkItemId == null)
            {
                if (queue.ExternalLockId != null)
                {
                    var noopWorkItem = new WorkItemForInMemory
                    {
                        WorkItemId = _nextId++,
                        Url = new Uri("noop:"),
                        QueueName = queue.QueueName,
                        DebugInfo = null,
                        IsIngested = true,
                        Internal = null,
                        BlockQueueName = wi.BlockQueueName,
                    };
                    _workItems.Add(noopWorkItem);
                    
                    _queues.Remove(queue);
                    _queues.Add(queue with
                    {
                        LockedUntil = DateTimeOffset.Now,
                        NextWorkItemId = noopWorkItem.WorkItemId,
                    });
                    
                    if (wi.BlockQueueName!= null)
                        _blockingMessages.Add(new BlockingMessage
                        {
                            BlockingMessageId = _nextId++,
                            QueueName = wi.BlockQueueName,
                            IsCreatingBlock = true,
                            BlockingWorkItemId = noopWorkItem.WorkItemId,
                        });
                }
                else
                {
                    _queues.Remove(queue);
                }
            }
            else
            {
                _queues.Remove(queue);
                _queues.Add(queue with
                {
                    LockedUntil = DateTimeOffset.Now,
                    NextWorkItemId = nextWorkItemId.Value,
                });
            }
        }
        
        _workItems.Remove(wi);
        _completedWorkItems.Add(wi);
        
        
        if (wi.BlockQueueName != null)
        {
            _blockingMessages.Add(new BlockingMessage
            {
                BlockingMessageId = _nextId++,
                QueueName = wi.BlockQueueName,
                IsCreatingBlock = false,
                BlockingWorkItemId = workItemId,
            });
            
            
            // var blockedQueue = _queues.Single(q => q.QueueName == wi.BlockQueueName);
            // if (blockedQueue == null)
            //     throw new Exception($"Queue {wi.QueueName} is not in the queue.");
            //
            // _queues.Remove(blockedQueue);
            // _queues.Add(blockedQueue with
            // {
            //     BlockedBy = blockedQueue.BlockedBy.Where(b => b != workItemId).ToList(),
            // });
        }
            
            
    }
    
    
    public async ValueTask DelayWorkItem(long workItemId)
    {
        using var _ = await ALock.Wait(_lock);
            
        var wi = _workItems.Single(w => w.WorkItemId == workItemId);
        var queue = _queues.Single(q => q.QueueName == wi.QueueName);
        
        _queues.Remove(queue);
        _queues.Add(queue with
        {
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
        _queues.Add(queue with
        {
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

    internal async ValueTask AcquireExternalLock(string queueName, string externalLockId)
    {
        using var _ = await ALock.Wait(_lock);

        var queue = _queues.SingleOrDefault(q => q.QueueName == queueName);
        if (queue != null)
        {
            if (queue.ExternalLockId != null)
                throw new Exception("Cannot acquire a lock when one is already granted.  An alternate pattern would be to create two child work-items instead, each with their own lock");
            
            _queues.Remove(queue);
            _queues.Add(queue with
            {
                ExternalLockId = externalLockId
            });
        }
        else
        {
            var noopWorkItem = new WorkItemForInMemory
            {
                WorkItemId = _nextId++,
                Url = new Uri("noop:"),
                QueueName = queueName,
                DebugInfo = null,
                IsIngested = true,
                Internal = null,
                BlockQueueName = null,
            };
            _workItems.Add(noopWorkItem);
                    
            _queues.Add(new Queue
            {
                QueueName = queueName,
                NextWorkItemId = noopWorkItem.WorkItemId,
                LockedUntil = DateTimeOffset.Now,
                ExternalLockId = externalLockId
            });
        }
    }

    internal async ValueTask ReleaseExternalLock(string queueName, string externalLockId)
    {
        using var _ = await ALock.Wait(_lock);
        
        var queue = _queues.SingleOrDefault(q => q.QueueName == queueName);
        if (queue != null)
        {
            if (queue.ExternalLockId != externalLockId)
                throw new Exception("ExternalLockId does not match");
            
            _queues.Remove(queue);
            _queues.Add(queue with
            {
                ExternalLockId = null
            });
        }
    }
}
