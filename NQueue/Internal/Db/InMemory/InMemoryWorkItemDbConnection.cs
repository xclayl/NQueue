using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.InMemory
{
    internal class InMemoryWorkItemDbConnection : IWorkItemDbConnection
    {
        private readonly InMemoryWorkItemDbProcs _procs = new InMemoryWorkItemDbProcs();

        public ValueTask<IWorkItemDbProcs> Get() => new ValueTask<IWorkItemDbProcs>(_procs);

        public ValueTask EnqueueWorkItem(DbTransaction tran, TimeZoneInfo tz, Uri url, string? queueName, string? debugInfo,
            bool duplicateProtection)
        {
            throw new Exception("The in-memory NQueue implementation is not compatible with DB transactions.");
        }
        
        public async ValueTask AsTran(Func<ICronTransaction, ValueTask> action)
        {
            await AsTran(async conn =>
            {
                await action(conn);
                return 0;
            });
        }
        
        public async ValueTask<T> AsTran<T>(Func<ICronTransaction, ValueTask<T>> action)
        {
            var tran = await _procs.BeginTran();
            var val = await action(tran);
            await tran.CommitAsync();
            return val;
        }
        
        
        public async ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState()
        {
            return await _procs.GetCronJobState();
        }

        public async ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck()
        {
            var s = await _procs.Db.GetWorkItems();

            var count = s.Count(wi => wi.FailCount >= 5);

            return (count == 0, count);
        }

        public ShardConfig ShardConfig { get; } = new(1);
        public IReadOnlyList<int> GetShardOrderForTesting() => new[] { 0 };
        public async IAsyncEnumerable<WorkItemForTests> GetWorkItemsForTests()
        {
            var workItems = await _procs.Db.GetWorkItems();
            foreach (var wi in workItems)
                yield return new WorkItemForTests(wi.WorkItemId, wi.Url, wi.QueueName, wi.DebugInfo, wi.Internal,
                    0, 1);
        }

        public async ValueTask<IReadOnlyList<QueueInfo>> GetQueuesForTesting()
        {
            return await _procs.Db.GetQueues();
        }

        public async IAsyncEnumerable<WorkItemForTests> GetCompletedWorkItemsForTests()
        {
            var workItems = await _procs.Db.GetCompletedWorkItems();
            foreach (var wi in workItems)
                yield return new WorkItemForTests(wi.WorkItemId, wi.Url, wi.QueueName, wi.DebugInfo, wi.Internal,
                    0, 1);
        }

        public ValueTask MakeConsistentForTests()
        {
            _procs.Db.MakeConsistent();
            return ValueTask.CompletedTask;
        }
    }
}