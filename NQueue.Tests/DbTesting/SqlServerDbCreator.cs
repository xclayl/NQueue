using System;
using System.Data.Common;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Data.SqlClient;
using NQueue.Internal.Db;
using NQueue.Internal.Db.Postgres;
using NQueue.Internal.Db.SqlServer;

namespace NQueue.Tests.DbTesting;

internal class SqlServerDbCreator : IDbCreator
{
    private const string Password = "ihSH3jqeVb7giIgOkohX";
    private bool _dbCreated = false;
    
    private readonly IContainer _postgreSqlContainer = new ContainerBuilder()
        .WithImage("mcr.microsoft.com/azure-sql-edge:latest")
        .WithEnvironment("MSSQL_SA_PASSWORD", Password)
        .WithEnvironment("ACCEPT_EULA", "1")
        .WithPortBinding(1433, true)
        .WithWaitStrategy(
            Wait.ForUnixContainer()
                .UntilPortIsAvailable(1433))
        .Build();

    
    public async ValueTask<IWorkItemDbConnection> CreateWorkItemDbConnection(ShardConfig? shardConfig = null)
    {
        await EnsureDbCreated();
        return new SqlServerWorkItemDbConnection(new SqlServerDbConfig
        {
            TimeZone = TimeZoneInfo.Local,
            Cnn = UserConnectionString("nqueue_test")
        });
    }
    
    private string OwnerConnectionString(string db) =>
        $"User ID=sa;Password={Password};Data Source=localhost,{_postgreSqlContainer.GetMappedPublicPort(1433)};Database={db};TrustServerCertificate=True";
    private string UserConnectionString(string user) =>
        $"User ID=NQueueUser;Password={Password};Data Source=localhost,{_postgreSqlContainer.GetMappedPublicPort(1433)};Database=nqueue_test;TrustServerCertificate=True";

    
    public ShardConfig DefaultShardConfig => new(1);
    
    private async ValueTask ExecuteNonQuery(string sql, string? db = null)
    {
        await using var connection = new SqlConnection(OwnerConnectionString(db ?? "master"));
        await using var command = connection.CreateCommand();

        connection.Open();
        command.Connection = connection;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }
    
    private async Task EnsureDbCreated()
    {
        if (!_dbCreated)
        {
            await _postgreSqlContainer.StartAsync();
            await ExecuteNonQuery("CREATE DATABASE nqueue_test");
            
            await ExecuteNonQuery($"CREATE LOGIN [NQueueUser] WITH PASSWORD = '{Password}';", "nqueue_test");
            await ExecuteNonQuery($"CREATE USER [NQueueUser] FOR LOGIN [NQueueUser];", "nqueue_test");
            await ExecuteNonQuery("IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE [name] = 'NQueue') EXEC ('CREATE SCHEMA [NQueue]')", "nqueue_test");
            await ExecuteNonQuery("ALTER AUTHORIZATION ON SCHEMA::[NQueue] TO [NQueueUser]", "nqueue_test");
            await ExecuteNonQuery("GRANT CREATE TABLE TO [NQueueUser]", "nqueue_test");
            await ExecuteNonQuery("GRANT CREATE PROCEDURE TO [NQueueUser]", "nqueue_test");
        }

        _dbCreated = true;
    }
    
    public async ValueTask<DbConnection?> CreateConnection()
    {
        await EnsureDbCreated();
        return new SqlConnection(UserConnectionString("nqueue_test"));
    }



    public ValueTask DisposeAsync() => _postgreSqlContainer.DisposeAsync();
}