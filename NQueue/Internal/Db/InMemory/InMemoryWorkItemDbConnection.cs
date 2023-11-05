using System;
using System.Collections.Generic;
using System.Data.Common;
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
        
        
        public async ValueTask<ICronTransaction> BeginTran()
        {
            return await _procs.BeginTran();
        }
        
        
        public async ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState()
        {
            return await _procs.GetCronJobState();
        }

        public ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck()
        {
            throw new NotImplementedException();
        }

    }
}