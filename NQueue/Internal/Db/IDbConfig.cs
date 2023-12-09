using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db;

internal interface IDbConfig
{
    TimeZoneInfo TimeZone { get; }
    ValueTask<DbConnection?> OpenDbConnection();
}