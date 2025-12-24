using System;
using System.Data.Common;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Npgsql;
using NQueue.Internal.Db;
using NQueue.Internal.Db.Postgres;

namespace NQueue.Tests.DbTesting;

internal class PostgresDbCreator : IDbCreator
{
    private const string Password = "ihSH3jqeVb7giIgOkohX";
    private bool _dbCreated = false;
    
    private readonly IContainer _postgreSqlContainer = new ContainerBuilder()
        .WithImage("postgres:15-alpine3.17")
        .WithEnvironment("POSTGRES_PASSWORD", Password)
        .WithPortBinding(5432, true)
        .WithWaitStrategy(
            Wait.ForUnixContainer()
                .UntilPortIsAvailable(5432))
        .Build();

    

    public async ValueTask<IWorkItemDbConnection> CreateWorkItemDbConnection(ShardConfig? shardConfig = null)
    {
        await EnsureDbCreated();
        return new PostgresWorkItemDbConnection(new PostgresDbConfig
        {
            TimeZone = TimeZoneInfo.Local,
            Cnn = UserConnectionString("nqueue_test")
        }, false, shardConfig ?? DefaultShardConfig);
    }

    public ShardConfig DefaultShardConfig => new (1);

    private string OwnerConnectionString(string db) =>
        $"User ID=postgres;Password={Password};Host=localhost;Port={_postgreSqlContainer.GetMappedPublicPort(5432)};Database={db};Include Error Detail=true";
    private string UserConnectionString(string user) =>
        $"User ID=nqueue_user;Password={Password};Host=localhost;Port={_postgreSqlContainer.GetMappedPublicPort(5432)};Database=nqueue_test;Include Error Detail=true";

    
    private async ValueTask ExecuteNonQuery(string sql, string? db = null)
    {
        await using var connection = new NpgsqlConnection(OwnerConnectionString(db ?? "postgres"));
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
            await WaitForUp();
            await ExecuteNonQuery("CREATE DATABASE nqueue_test");
            await ExecuteNonQuery($"CREATE USER nqueue_user PASSWORD '{Password}';", "nqueue_test");
            await ExecuteNonQuery("CREATE SCHEMA IF NOT EXISTS nqueue AUTHORIZATION nqueue_user", "nqueue_test");
        }

        _dbCreated = true;
    }
    
    
    private async ValueTask WaitForUp()
    {
        var giveUp = DateTimeOffset.Now.AddSeconds(30);
        while (true)
            try
            {
                await ExecuteNonQuery("SELECT 1");
                return;
            }
            catch (PostgresException e) when (e.Message.Contains("the database system is starting up") && DateTimeOffset.Now < giveUp)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500));
            }
    }
    
    public async ValueTask<DbConnection?> CreateConnection()
    {
        await EnsureDbCreated();
        return new NpgsqlConnection(UserConnectionString("nqueue_test"));
    }



    public ValueTask DisposeAsync() => _postgreSqlContainer.DisposeAsync();
}