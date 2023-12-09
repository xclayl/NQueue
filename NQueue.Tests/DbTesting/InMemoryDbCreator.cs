using System.Data.Common;
using System.Threading.Tasks;
using NQueue.Internal.Db;
using NQueue.Internal.Db.InMemory;

namespace NQueue.Tests.DbTesting;

internal class InMemoryDbCreator : IDbCreator
{
    private readonly InMemoryWorkItemDbConnection _db = new();
    
    public ValueTask DisposeAsync() => default;

    public ValueTask<DbConnection?> CreateConnection() => ValueTask.FromResult<DbConnection?>(null);

    
    public ValueTask<IWorkItemDbConnection> CreateWorkItemDbConnection() => new(_db);
}