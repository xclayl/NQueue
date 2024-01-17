using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db;

internal interface IDbConfig
{
    TimeZoneInfo TimeZone { get; }
    ValueTask WithDbConnection(Func<DbConnection, ValueTask> action);
    ValueTask<T> WithDbConnection<T>(Func<DbConnection, ValueTask<T>> action);
}