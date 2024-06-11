using System;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using NQueue.Internal.Db;

namespace NQueue.Tests.DbTesting;

internal class PostgresDbConfig : IDbConfig
{
    public TimeZoneInfo TimeZone { get; init; }
    public string Cnn { get; init; }
    
    public async ValueTask WithDbConnection(Func<DbConnection, ValueTask> action)
    {
        await using var cnn = new NpgsqlConnection(Cnn);
        await cnn.OpenAsync();
        await action(cnn);
    }
    
    public async ValueTask WithDbConnectionAndRetries(Func<DbConnection, ValueTask> action)
    {
        var tries = 0;
        const int MaxRetries = 10;
        while (tries <= MaxRetries)
        {
            try
            {
                await WithDbConnection(action);
                return;
            }
            catch (Exception e) when (tries < MaxRetries)
            {
                // do nothing
            }

            tries++;
            await Task.Delay(TimeSpan.FromSeconds(Math.Min(Math.Pow(2, tries), 30)));
        }
    }
    public async ValueTask<T> WithDbConnection<T>(Func<DbConnection, ValueTask<T>> action)
    {
        await using var cnn = new NpgsqlConnection(Cnn);
        await cnn.OpenAsync();
        return await action(cnn);
    }

    
  
}


internal class SqlServerDbConfig : IDbConfig
{
    public TimeZoneInfo TimeZone { get; init; }
    public string Cnn { get; init; }
    
    public async ValueTask WithDbConnection(Func<DbConnection, ValueTask> action)
    {
        await using var cnn = new SqlConnection(Cnn);
        await cnn.OpenAsync();
        await action(cnn);
    }

    public async ValueTask WithDbConnectionAndRetries(Func<DbConnection, ValueTask> action)
    {
        await WithDbConnection(action);
    }

    public async ValueTask<T> WithDbConnection<T>(Func<DbConnection, ValueTask<T>> action)
    {
        await using var cnn = new SqlConnection(Cnn);
        await cnn.OpenAsync();
        return await action(cnn);
    }
    
    
}