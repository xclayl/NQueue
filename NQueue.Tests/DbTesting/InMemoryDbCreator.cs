using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Tests.DbTesting;

public class InMemoryDbCreator : IDbCreator
{
    public ValueTask DisposeAsync() => default;

    public ValueTask<DbConnection?> CreateConnection() => ValueTask.FromResult<DbConnection?>(null);
}