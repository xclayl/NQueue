using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.InMemory
{
    internal class InMemoryWorkItemDbConnection : IWorkItemDbConnection
    {
        private readonly InMemoryWorkItemDbQuery _query = new InMemoryWorkItemDbQuery();

        public ValueTask<IWorkItemDbQuery> Get() => new ValueTask<IWorkItemDbQuery>(_query);

        public ValueTask EnqueueWorkItem(DbTransaction tran, TimeZoneInfo tz, Uri url, string? queueName, string? debugInfo,
            bool duplicateProtection)
        {
            throw new Exception("The in-memory NQueue implementation is not compatible with DB transactions.");
        }

    }
}