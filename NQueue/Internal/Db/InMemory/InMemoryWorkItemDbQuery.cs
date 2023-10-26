﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NQueue.Internal;
using NQueue.Internal.Db.InMemory;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.InMemory
{
    
    
    internal class InMemoryWorkItemDbQuery : IWorkItemDbQuery
    {
        internal class WorkItem
        {
            public int WorkItemId { get; set; }
            public Uri Url { get; set; }
            public string QueueName { get; set; }
            public string DebugInfo { get; set; }
            public bool IsIngested { get; set; }
        }

        private class Queue
        {
            public string QueueName { get; set; }
            public DateTimeOffset? LockedUntil { get; set; }
        }



        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private readonly List<WorkItem> _workItems = new List<WorkItem>();
        private readonly List<WorkItem> _completedWorkItems = new List<WorkItem>();
        private readonly List<CronJobInfo> _cronJobState = new List<CronJobInfo>();
        private readonly List<Queue> _queues = new List<Queue>();
        private int _nextId = 23;
        
        public async ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck()
        {
            using var _ = await ALock.Wait(_lock);
            
            throw new NotImplementedException();
            
            
        }

        public async ValueTask EnqueueWorkItem(Uri url, string queueName, string debugInfo, bool duplicateProtection)
        {

            using var _ = await ALock.Wait(_lock);
            
            queueName ??= Guid.NewGuid().ToString();
            
            
            if (duplicateProtection)
            {
                if (_workItems.Any(w => w.Url == url && w.QueueName == queueName))
                    return;
            }

            _workItems.Add(new WorkItem
            {
                WorkItemId = _nextId++,
                Url = url,
                QueueName = queueName,
                DebugInfo = debugInfo,
                IsIngested = false,
            });
            
                
            
        }

        public async ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState()
        {
            using var _ = await ALock.Wait(_lock);
            return _cronJobState.ToList();
        }

        public async ValueTask<IWorkItemDbTransaction> BeginTran()
        {
            await _lock.WaitAsync();
            return new CronWorkItemTran(_lock, _workItems, _cronJobState, () => _nextId++);
        }

        public async ValueTask<WorkItemInfo?> NextWorkItem()
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
            }

            var now = DateTimeOffset.Now;

            var next = _workItems.FirstOrDefault(w =>
            {
                var queue = _queues.Single(q => q.QueueName == w.QueueName);

                if (queue.LockedUntil != null && queue.LockedUntil > now)
                    return false;

                return true;
            });

            if (next == null)
                return null;
            
            var queue = _queues.Single(q => q.QueueName == next.QueueName);

            queue.LockedUntil = now.AddHours(1);

            return new WorkItemInfo(next.WorkItemId, next.Url.AbsoluteUri);
        }

        public async ValueTask CompleteWorkItem(int workItemId)
        {
            using var _ = await ALock.Wait(_lock);

            var wi = _workItems.Single(w => w.WorkItemId == workItemId);

            var queue = _queues.Single(q => q.QueueName == wi.QueueName);

            _workItems.Remove(wi);
            _completedWorkItems.Add(wi);

            if (_workItems.Any(w => w.QueueName == queue.QueueName))
            {
                queue.LockedUntil = null;
            }
            else
            {
                _queues.Remove(queue);
            }
            
        }

        public async ValueTask FailWorkItem(int workItemId)
        {
            using var _ = await ALock.Wait(_lock);
            
            var wi = _workItems.Single(w => w.WorkItemId == workItemId);

            var queue = _queues.Single(q => q.QueueName == wi.QueueName);
            queue.LockedUntil = DateTimeOffset.Now.AddMinutes(5);
        }

        public async ValueTask PurgeWorkItems()
        {
            using var _ = await ALock.Wait(_lock);

            _completedWorkItems.Clear();
        }
    }
}

class CronWorkItemTran : IWorkItemDbTransaction
{
    private IDisposable? _lock;
    private readonly List<InMemoryWorkItemDbQuery.WorkItem> _workItems;
    private readonly List<CronJobInfo> _cronJobState;
    private readonly Func<int> _nextCronJobId;
    
    
    private readonly List<InMemoryWorkItemDbQuery.WorkItem> _tempWorkItems=new List<InMemoryWorkItemDbQuery.WorkItem>();
    private readonly List<CronJobInfo> _tempCronJobState = new List<CronJobInfo>(); 

    public CronWorkItemTran(IDisposable @lock, List<InMemoryWorkItemDbQuery.WorkItem> workItems, List<CronJobInfo> cronJobState, Func<int> nextCronJobId)
    {
        _lock = @lock;
        _workItems = workItems;
        _cronJobState = cronJobState;
        _nextCronJobId = nextCronJobId;
    }

    public async ValueTask DisposeAsync()
    {
        _lock?.Dispose();
        _lock = null;
    }

    public async ValueTask CommitAsync()
    {
        _workItems.AddRange(_tempWorkItems);
        _tempWorkItems.Clear();
        _lock?.Dispose();
        _lock = null;
    }

    public async ValueTask EnqueueWorkItem(Uri url, string queueName, string debugInfo, bool duplicateProtection)
    {
        _tempWorkItems.Add(new InMemoryWorkItemDbQuery.WorkItem
        {
            DebugInfo = debugInfo,
            Url = url,
            QueueName = queueName,
        });
    }

    public async ValueTask<int> CreateCronJob(string name)
    {
        var cronJobId = _nextCronJobId();
        _tempCronJobState.Add(new CronJobInfo(cronJobId, name, new DateTimeOffset(2000, 1 , 1, 0,0 ,0, TimeSpan.Zero)));
        return cronJobId;
    }

    public async ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(int cronJobId)
    {
        var cj = _cronJobState.Single(j => j.CronJobId == cronJobId);
       return (cj.LastRanAt, true);
    }

    public async ValueTask UpdateCronJobLastRanAt(int cronJobId)
    {
        var cj = _cronJobState.Single(j => j.CronJobId == cronJobId);
        _cronJobState.Remove(cj);
        var newCj = new CronJobInfo(cj.CronJobId, cj.CronJobName, DateTimeOffset.Now);
        _cronJobState.Add(newCj);
    }
}

class ALock : IDisposable
{
    public static async ValueTask<IDisposable> Wait(SemaphoreSlim s)
    {
        await s.WaitAsync();
        return new ALock(s);
    }

    private readonly SemaphoreSlim _locked;

    public ALock(SemaphoreSlim locked)
    {
        _locked = locked;
    }

    public void Dispose()
    {
        _locked.Release();
    }
}