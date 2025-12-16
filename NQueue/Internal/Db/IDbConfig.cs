using System;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NQueue.Internal.Db;

internal interface IDbConfig
{
    TimeZoneInfo TimeZone { get; }
    ValueTask WithDbConnection(Func<DbConnection, ValueTask> action);
    ValueTask WithDbConnectionAndRetries(Func<DbConnection, ValueTask> action, ILogger logger);
    ValueTask<T> WithDbConnection<T>(Func<DbConnection, ValueTask<T>> action);
    // ShardConfig ShardConfig { get; }
}
