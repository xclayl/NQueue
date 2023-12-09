using System;
using System.Data.Common;
using System.Threading.Tasks;
using NQueue.Internal.Db;

namespace NQueue.Tests;

internal interface IDbCreator : IAsyncDisposable
{
    ValueTask<DbConnection?> CreateConnection();
    ValueTask<IWorkItemDbConnection> CreateWorkItemDbConnection();
}