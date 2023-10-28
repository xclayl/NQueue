using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Tests;

public interface IDbCreator : IAsyncDisposable
{
    ValueTask<DbConnection?> CreateConnection();
}