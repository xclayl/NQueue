using System;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using NQueue.Internal.Db;

namespace NQueue.Tests.DbTesting;

public class PostgresDbConfig : IDbConfig
{
    public TimeZoneInfo TimeZone { get; init; }
    public string Cnn { get; init; }
    
    public async ValueTask<DbConnection?> OpenDbConnection()
    {
        var cnn = new NpgsqlConnection(Cnn);
        await cnn.OpenAsync();
        return cnn;
    }
}


public class SqlServerDbConfig : IDbConfig
{
    public TimeZoneInfo TimeZone { get; init; }
    public string Cnn { get; init; }
    
    public async ValueTask<DbConnection?> OpenDbConnection()
    {
        var cnn = new SqlConnection(Cnn);
        await cnn.OpenAsync();
        return cnn;
    }
}