using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NQueue.Testing;
using Xunit;

namespace NQueue.Tests.DbTesting;



public class DbTests : IAsyncLifetime
{
    public enum DbType
    {
        InMemory,
        Postgres,
        PostgresCitus,
        SqlServer
    }

    private readonly IReadOnlyDictionary<DbType, IDbCreator> _dbCreators = new Dictionary<DbType, IDbCreator>()
    {
        { DbType.InMemory, new InMemoryDbCreator() },
        { DbType.Postgres, new PostgresDbCreator() },
        { DbType.PostgresCitus, new PostgresCitusDbCreator() },
        { DbType.SqlServer, new SqlServerDbCreator() }
    };
    
    
    
    
    public Task InitializeAsync() => Task.CompletedTask;
    public Task DisposeAsync() => Task.WhenAll(_dbCreators.Select(async c => await c.Value.DisposeAsync()));


    public static readonly TheoryData<DbType> MyTheoryData = new();

    static DbTests()
    {
        var array = ((DbType[])Enum.GetValues(typeof(DbType))).ToList();
        array.ForEach(MyTheoryData.Add);
    }
  

    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task HappyPath(DbType dbType)
    {
        // arrange 
        var baseUrl = new Uri("http://localhost:8501");
        var fakeApp = new FakeWebApp();
        var fakeService = new NQueueHostedServiceFake(_dbCreators[dbType].CreateConnection, baseUrl);
        await fakeService.DeleteAllNQueueData();
        fakeApp.FakeService = fakeService;

        await using var app = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder => builder.ConfigureFakes(fakeApp, baseUrl)); 
        var nQueueClient = app.Services.GetRequiredService<INQueueClient>();
        var guid = Guid.NewGuid();
        
        // act
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"));
        await fakeApp.FakeService.ProcessAll(app.CreateClient,
            app.Services.GetRequiredService<ILoggerFactory>());

        // assert
        var http = app.CreateClient();
        using var r = await http.GetAsync(await nQueueClient.Localhost($"api/NQueue/GetMessage"));
        r.EnsureSuccessStatusCode();
        (await r.Content.ReadAsStringAsync()).Should().Be($"{guid}");
        fakeApp.FakeLogs.Where(l => l.LogLevel == LogLevel.Error).Should().BeEmpty();
    }
    
    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task ExecuteCronStoredProcs(DbType dbType)
    {
        // arrange 
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();
        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();
        
        
        (await dbCnn.GetCronJobState()).Should().BeEmpty();

        var health = await dbCnn.QueueHealthCheck();
        health.healthy.Should().BeTrue();
        health.countUnhealthy.Should().Be(0);

        await dbCnn.AsTran(async tran =>
        {
            var cronJobName = "cron name";
            await tran.CreateCronJob(cronJobName);
            var cron = await tran.SelectAndLockCronJob(cronJobName);
            cron.active.Should().BeTrue();
            await tran.UpdateCronJobLastRanAt(cronJobName);
        });
        
        
        

        (await dbCnn.GetCronJobState()).Should().NotBeEmpty();

    }

    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task ExecuteQueueStoredProcs(DbType dbType)
    {
        // arrange 
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();

        var item = await procs.NextWorkItem(0);
        item.Should().BeNull();


        await procs.EnqueueWorkItem(null, new Uri("http://localhost/api/NQueue/NoOp"), "queue-name", "debug-info", false,
            @"{""a"": 3}");
        
        

        foreach (var shard in Enumerable.Range(0, dbCnn.ShardCount))
        {
            var queuedItem = await procs.NextWorkItem(shard);
            if (queuedItem == null)
                continue;

            queuedItem.Url.Should().Be("http://localhost/api/NQueue/NoOp");
            queuedItem.Internal.Should().Be(@"{""a"": 3}");
            
            await procs.CompleteWorkItem(queuedItem.WorkItemId, shard);
            break;
        }
        
        await procs.EnqueueWorkItem(null, new Uri("http://localhost/api/NQueue/NoOp/2"), "queue-name", "debug-info", false,
            @"{""a"": 4}");


        foreach (var shard in Enumerable.Range(0, dbCnn.ShardCount))
        {
            var queuedItem = await procs.NextWorkItem(shard);
            if (queuedItem == null)
                continue;

            queuedItem.Url.Should().Be("http://localhost/api/NQueue/NoOp/2");
            queuedItem.Internal.Should().Be(@"{""a"": 4}");
            
            await procs.FailWorkItem(queuedItem.WorkItemId, shard);
            break;
        }

        foreach (var shard in Enumerable.Range(0, dbCnn.ShardCount))
        {
            await procs.PurgeWorkItems(shard);
        }



    }

    
}