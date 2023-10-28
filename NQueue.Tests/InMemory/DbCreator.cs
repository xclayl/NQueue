using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Tests.InMemory;

public class DbCreator : IDbCreator
{
    public ValueTask DisposeAsync() => default;

    public ValueTask<DbConnection?> CreateConnection() => ValueTask.FromResult<DbConnection?>(null);
}