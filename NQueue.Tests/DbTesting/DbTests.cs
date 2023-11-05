using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace NQueue.Tests.DbTesting;

public enum DbType
{
    InMemory,
    Postgres,
    SqlServer
}

public class DbTests : IAsyncLifetime
{
    private readonly IReadOnlyDictionary<DbType, IDbCreator> _dbCreators;
    public Task InitializeAsync() => Task.CompletedTask;
    public Task DisposeAsync() => Task.WhenAll(_dbCreators.Select(async c => await c.Value.DisposeAsync()));


    public static readonly TheoryData<DbType> MyTheoryData = new();

    static DbTests()
    {
        MyTheoryData.Add(DbType.InMemory);
        MyTheoryData.Add(DbType.Postgres);
        MyTheoryData.Add(DbType.SqlServer);
    }

    public DbTests()
    {
        _dbCreators = MyTheoryData
            .Select(o => (DbType) o.Single())
            .Select(dbType =>
                dbType switch
                {
                    DbType.InMemory => (dbType, (IDbCreator) new InMemoryDbCreator()),
                    DbType.Postgres => (dbType, new PostgresDbCreator()),
                    DbType.SqlServer => (dbType, new SqlServerDbCreator()),
                    _ => throw new Exception($"Unknown type: {dbType}")
                }
            )
            .ToDictionary(r => r.Item1, r => r.Item2);
    }

    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task HappyPath(DbType dbType)
    {
        // arrange 
        await using var app = await SampleWebAppBuilder.Build(_dbCreators[dbType]);
        var nQueueClient = app.Application.Services.GetRequiredService<INQueueClient>();
        var guid = Guid.NewGuid();
        
        // act
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"));
        await app.FakeService.ProcessAll(app.Application.CreateClient,
            app.Application.Services.GetRequiredService<ILoggerFactory>());

        // assert
        var http = app.Application.CreateClient();
        using var r = await http.GetAsync(await nQueueClient.Localhost($"api/NQueue/GetMessage"));
        r.EnsureSuccessStatusCode();
        (await r.Content.ReadAsStringAsync()).Should().Be($"{guid}");
    }
}