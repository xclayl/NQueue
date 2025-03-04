using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
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
        // SqlServer
    }

    private readonly IReadOnlyDictionary<DbType, IDbCreator> _dbCreators = new Dictionary<DbType, IDbCreator>()
    {
        { DbType.InMemory, new InMemoryDbCreator() },
        { DbType.Postgres, new PostgresDbCreator() },
        { DbType.PostgresCitus, new PostgresCitusDbCreator() },
        // { DbType.SqlServer, new SqlServerDbCreator() }
    };

    private static int _id = 1;

    private readonly int _uniqueId = _id++;
    
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
    public async Task ProcessAllRegEx(DbType dbType)
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
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"),
            "abc_123");
        await fakeApp.FakeService.ProcessAll(new Regex(@"abc_\d*"), app.CreateClient,
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
    public async Task ProcessAllRegEx2(DbType dbType)
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
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"),
            "def_123");
        await fakeApp.FakeService.ProcessAll(new Regex(@"abc_\d*"), app.CreateClient,
            app.Services.GetRequiredService<ILoggerFactory>());

        // assert
        var http = app.CreateClient();
        using var r = await http.GetAsync(await nQueueClient.Localhost($"api/NQueue/GetMessage"));
        r.EnsureSuccessStatusCode();
        (await r.Content.ReadAsStringAsync()).Should().NotBe($"{guid}");
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
            @"{""a"": 3}", null);
        
        
        

        foreach (var shard in Enumerable.Range(0, dbCnn.ShardCount))
        {
            var queuedItem = await procs.NextWorkItem(shard);
            if (queuedItem == null)
                continue;

            queuedItem.Url.Should().Be("http://localhost/api/NQueue/NoOp");
            queuedItem.Internal.Should().Be(@"{""a"": 3}");
            
            await procs.DelayWorkItem(queuedItem.WorkItemId, shard, null);
            break;
        }

        

        foreach (var shard in Enumerable.Range(0, dbCnn.ShardCount))
        {
            var queuedItem = await procs.NextWorkItem(shard);
            if (queuedItem == null)
                continue;

            queuedItem.Url.Should().Be("http://localhost/api/NQueue/NoOp");
            queuedItem.Internal.Should().Be(@"{""a"": 3}");
            
            await procs.CompleteWorkItem(queuedItem.WorkItemId, shard, null);
            break;
        }
        
        await procs.EnqueueWorkItem(null, new Uri("http://localhost/api/NQueue/NoOp/2"), "queue-name", "debug-info", false,
            @"{""a"": 4}", null);


        foreach (var shard in Enumerable.Range(0, dbCnn.ShardCount))
        {
            var queuedItem = await procs.NextWorkItem(shard);
            if (queuedItem == null)
                continue;

            queuedItem.Url.Should().Be("http://localhost/api/NQueue/NoOp/2");
            queuedItem.Internal.Should().Be(@"{""a"": 4}");
            
            await procs.FailWorkItem(queuedItem.WorkItemId, shard, null);
            break;
        }

        foreach (var shard in Enumerable.Range(0, dbCnn.ShardCount))
        {
            await procs.PurgeWorkItems(shard);
        }



    }

    
    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task ExecuteBlockingStoredProcs(DbType dbType)
    {
        // arrange 
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();

        var item = await procs.NextWorkItem(0);
        item.Should().BeNull();


        var parentShard = CalculateShard("parent-queue", dbType);

        await procs.EnqueueWorkItem(null, new Uri("http://localhost/api/NQueue/NoOp"), "parent-queue", "debug-info", false,
            @"{""a"": 3}", null);
        
        
        

        var parentQueuedItem = await procs.NextWorkItem(parentShard);
        if (parentQueuedItem == null)
            throw new Exception("Could not find parent queue item");

     
        var childShard = CalculateShard("child-queue", dbType);
        await procs.EnqueueWorkItem(null, new Uri("http://localhost/api/NQueue/NoOp"), "child-queue", "debug-info", false,
            @"{""a"": 3}", "parent-queue");


        
       
        
        var childQueuedItem = await procs.NextWorkItem(childShard);
        childQueuedItem.Should().NotBeNull();
        
        await procs.DelayWorkItem(parentQueuedItem.WorkItemId, parentShard, null);
        
        parentQueuedItem = await procs.NextWorkItem(parentShard);
        parentQueuedItem.Should().BeNull();
        
        await procs.CompleteWorkItem(childQueuedItem.WorkItemId, childShard, null);
        

        
        parentQueuedItem = await procs.NextWorkItem(parentShard);
        parentQueuedItem.Should().NotBeNull();
        
        await procs.CompleteWorkItem(parentQueuedItem.WorkItemId, parentShard, null);
        

    }

    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task TooManyRequests(DbType dbType)
    {
        var endpoint = "api/NQueue/TooManyRequests";

        await ShouldRetryWithoutError(dbType, endpoint);
    }

    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task Retry(DbType dbType)
    {
        var endpoint = "api/NQueue/Retry";

        await ShouldRetryWithoutError(dbType, endpoint);
    }

    private async Task ShouldRetryWithoutError(DbType dbType, string endpoint)
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
        await nQueueClient.Enqueue(await nQueueClient.Localhost(endpoint));
        var enqueuedTime = DateTimeOffset.Now;
        await Task.Delay(TimeSpan.FromSeconds(1));
        await fakeApp.FakeService.ProcessOne(app.CreateClient,
            app.Services.GetRequiredService<ILoggerFactory>());

        // assert
        await using var cnn = await _dbCreators[dbType].CreateConnection();
        if (cnn == null)
        {
            var workItems = await fakeService.GetWorkItems().ToListAsync();
            workItems.Should().HaveCount(1);
            workItems.Single().FailCount.Should().Be(0);
            
            var completedWorkItems = await fakeService.GetCompletedWorkItems().ToListAsync();
            completedWorkItems.Should().BeEmpty();
        }
        else
        {
            await cnn.OpenAsync();
            {
                await using var cmd = cnn.CreateCommand();
                cmd.CommandText = "SELECT * FROM NQueue.WorkItem";
                await using var reader = await cmd.ExecuteReaderAsync();
                var workItems = new List<int>();
                while (await reader.ReadAsync())
                {
                    workItems.Add(1);
                }

                workItems.Should().HaveCount(1);
            }
            {
                await using var cmdComp = cnn.CreateCommand();
                cmdComp.CommandText = "SELECT * FROM NQueue.WorkItemCompleted";
                await using var readerComp = await cmdComp.ExecuteReaderAsync();
                var completedWorkItem = new List<int>();
                while (await readerComp.ReadAsync())
                {
                    completedWorkItem.Add(readerComp.GetInt32(0));
                }

                completedWorkItem.Should().BeEmpty();
            }

            {
                await using var cmdQueue = cnn.CreateCommand();
                cmdQueue.CommandText = "SELECT ErrorCount, LockedUntil FROM NQueue.Queue";
                await using var readerQueue = await cmdQueue.ExecuteReaderAsync();
                var queues = new List<(int, DateTimeOffset)>();
                while (await readerQueue.ReadAsync())
                {
                    queues.Add((readerQueue.GetInt32(0), readerQueue.GetDateTime(1)));
                }

                queues.Should().HaveCount(1);
                queues.Single().Item1.Should().Be(0);
                queues.Single().Item2.Should().BeAfter(enqueuedTime); // this is so that other queues will have a chance to run before this queue
            }
        }
        fakeApp.FakeLogs.Where(l => l.LogLevel == LogLevel.Error).Should().BeEmpty();
    }
    
    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task ProcessOneQueue(DbType dbType)
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
        var otherGuid = Guid.NewGuid();
        
        // act
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{otherGuid}"));
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"), "my-queue's name");
        await fakeApp.FakeService.ProcessOne("my-queue's name", app.CreateClient,
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
    public async Task LargeQueueName(DbType dbType)
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
        var otherGuid = Guid.NewGuid();
        
        // act
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{otherGuid}"));
        var queueName = new string('a', 500);
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"), queueName);
        await fakeApp.FakeService.ProcessOne(queueName, app.CreateClient,
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
    public async Task PausedQueuesIgnored(DbType dbType)
    {
        if (dbType == DbType.InMemory)
            return;
        
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
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"), "my-queue-name");

        await using (var cnn = await _dbCreators[dbType].CreateConnection())
        {
            await cnn.OpenAsync();
            await ExecuteProcedure(cnn, "NQueue.PauseQueue", 
                SqlParameter("my-queue-name"),
                SqlParameter(CalculateShard("my-queue-name", dbType)),
                SqlParameter(DateTimeOffset.Now.ToUniversalTime()));
        }
        
        await fakeApp.FakeService.ProcessAll(app.CreateClient,
            app.Services.GetRequiredService<ILoggerFactory>());

        // assert
        var http = app.CreateClient();
        using var r = await http.GetAsync(await nQueueClient.Localhost($"api/NQueue/GetMessage"));
        r.EnsureSuccessStatusCode();
        (await r.Content.ReadAsStringAsync()).Should().Be($"");
        fakeApp.FakeLogs.Where(l => l.LogLevel == LogLevel.Error).Should().BeEmpty();
    }
    
    
    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task KeepQueuePausedAfterCompletingWorkItem(DbType dbType)
    {
        if (dbType == DbType.InMemory)
            return;
        
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
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"), "my-queue-name");

        await using (var cnn = await _dbCreators[dbType].CreateConnection())
        {
            await cnn.OpenAsync();

            var workItems = new List<long>();
            await using (var cmd = cnn.CreateCommand())
            {
                cmd.CommandText = "SELECT WorkItemID FROM NQueue.WorkItem";
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    workItems.Add(reader.GetInt64(0));
                }
            }

            var workItemId = workItems.Single();
            
            await ExecuteProcedure(cnn, "NQueue.PauseQueue", 
                SqlParameter("my-queue-name"),
                SqlParameter(CalculateShard("my-queue-name", dbType)),
                SqlParameter(DateTimeOffset.Now.ToUniversalTime()));


            
            var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

            var procs = await dbCnn.Get();

            await procs.CompleteWorkItem(workItemId, CalculateShard("my-queue-name", dbType), null);
        }
        
        
        // assert
        
        await using (var cnn = await _dbCreators[dbType].CreateConnection())
        {
            await cnn.OpenAsync();
        
            await using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT IsPaused FROM NQueue.Queue";
            await using var reader = await cmd.ExecuteReaderAsync();
            var workItems = new List<bool>();
            while (await reader.ReadAsync())
            {
                workItems.Add(reader.GetBoolean(0));
            }

            workItems.Single().Should().Be(true);
        
        }
        
        fakeApp.FakeLogs.Where(l => l.LogLevel == LogLevel.Error).Should().BeEmpty();
    }
    
    
    
    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task PauseQueueBeforeExists(DbType dbType)
    {
        if (dbType == DbType.InMemory)
            return;
        
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

        await using (var cnn = await _dbCreators[dbType].CreateConnection())
        {
            await cnn.OpenAsync();

            await ExecuteProcedure(cnn, "NQueue.PauseQueue", 
                SqlParameter("my-queue-name"),
                SqlParameter(CalculateShard("my-queue-name", dbType)),
                SqlParameter(DateTimeOffset.Now.ToUniversalTime()));
        }
        
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"), "my-queue-name");
        
        
        await fakeApp.FakeService.ProcessAll(new Regex(@"abc_\d*"), app.CreateClient,
            app.Services.GetRequiredService<ILoggerFactory>());
        
        // assert
        
        var http = app.CreateClient();
        using var r = await http.GetAsync(await nQueueClient.Localhost($"api/NQueue/GetMessage"));
        r.EnsureSuccessStatusCode();
        (await r.Content.ReadAsStringAsync()).Should().Be($"");
        
        await using (var cnn = await _dbCreators[dbType].CreateConnection())
        {
            await cnn.OpenAsync();
        
            await using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT IsPaused FROM NQueue.Queue";
            await using var reader = await cmd.ExecuteReaderAsync();
            var workItems = new List<bool>();
            while (await reader.ReadAsync())
            {
                workItems.Add(reader.GetBoolean(0));
            }

            workItems.Single().Should().Be(true);
        
        }
        
        
        fakeApp.FakeLogs.Where(l => l.LogLevel == LogLevel.Error).Should().BeEmpty();
    }
    
    
    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task ResumeAPausedQueueBeforeQueueExisted(DbType dbType)
    {
        if (dbType == DbType.InMemory)
            return;
        
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

        await using (var cnn = await _dbCreators[dbType].CreateConnection())
        {
            await cnn.OpenAsync();

            await ExecuteProcedure(cnn, "NQueue.PauseQueue", 
                SqlParameter("my-queue-name"),
                SqlParameter(CalculateShard("my-queue-name", dbType)),
                SqlParameter(DateTimeOffset.Now.ToUniversalTime()));
        }
        
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"), "my-queue-name");
        
        await using (var cnn = await _dbCreators[dbType].CreateConnection())
        {
            await cnn.OpenAsync();

            await ExecuteNonQuery(cnn, "UPDATE NQueue.Queue SET IsPaused = FALSE");
        }

        
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());
        
        // assert
        
        var http = app.CreateClient();
        using var r = await http.GetAsync(await nQueueClient.Localhost($"api/NQueue/GetMessage"));
        r.EnsureSuccessStatusCode();
        (await r.Content.ReadAsStringAsync()).Should().Be($"{guid}");
        
        
        
        fakeApp.FakeLogs.Where(l => l.LogLevel == LogLevel.Error).Should().BeEmpty();
    }
    
    
    
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task CompletePausedQueue(DbType dbType)
    {
        if (dbType == DbType.InMemory)
            return;
        
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();
        
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
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/SetMessage/{guid}"), "my-queue-name");



        var workItem = await procs.NextWorkItem(CalculateShard("my-queue-name", dbType));
        workItem.Should().NotBeNull();
        
        await using (var cnn = await _dbCreators[dbType].CreateConnection())
        {
            await cnn.OpenAsync();
            await ExecuteProcedure(cnn, "NQueue.PauseQueue", 
                SqlParameter("my-queue-name"),
                SqlParameter(CalculateShard("my-queue-name", dbType)),
                SqlParameter(DateTimeOffset.Now.ToUniversalTime()));
        }
        
        
        await procs.CompleteWorkItem(workItem.WorkItemId, CalculateShard("my-queue-name", dbType), null);

        // assert
        // var http = app.CreateClient();
        // using var r = await http.GetAsync(await nQueueClient.Localhost($"api/NQueue/GetMessage"));
        // r.EnsureSuccessStatusCode();
        // (await r.Content.ReadAsStringAsync()).Should().Be($"");
        fakeApp.FakeLogs.Where(l => l.LogLevel == LogLevel.Error).Should().BeEmpty();
    }

    /// <summary>
    /// The work-item creates the lock and then runs again after releasing the lock to continue processing
    /// </summary>
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task AcquireExternalLockOnQueueThenContinueWorkItem(DbType dbType)
    {
        
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();

        // arrange 
        var baseUrl = new Uri("http://localhost:8501");
        var fakeApp = new FakeWebApp();
        var fakeService = new NQueueHostedServiceFake(_dbCreators[dbType].CreateConnection, baseUrl);
        await fakeService.DeleteAllNQueueData();
        fakeApp.FakeService = fakeService;

        await using var app = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder => builder.ConfigureFakes(fakeApp, baseUrl)); 
        var nQueueClient = app.Services.GetRequiredService<INQueueClient>();
        // var nQueueService = app.Services.GetRequiredService<INQueueService>();

        var resourceName = $"account123-{_uniqueId}";
        
        
        // act
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/AcquireExternalLockAndRunOnceMore?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my-queue-name")}"), "my-queue-name");
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());
        var lockId = await GetString(fakeService.BaseAddress, $"api/NQueue/GetLockId?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my-queue-name")}", app.CreateClient);

        
  
        
        // assert
        var workItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();

        workItems.Should().BeEmpty();
        
        // act
        await GetString(fakeService.BaseAddress, $"api/NQueue/ReleaseExternalLock?lockId={WebUtility.UrlEncode(lockId)}", app.CreateClient);
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());

        
        
        // assert
        workItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();

        workItems.Should().HaveCount(1);
    }



    /// <summary>
    /// The work-item creates the lock and then the next work-item in the queue runs after releasing the lock 
    /// </summary>
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task AcquireExternalLockOnQueueThenContinueNextWorkItem(DbType dbType)
    {
        
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();

        // arrange 
        var baseUrl = new Uri("http://localhost:8501");
        var fakeApp = new FakeWebApp();
        var fakeService = new NQueueHostedServiceFake(_dbCreators[dbType].CreateConnection, baseUrl);
        await fakeService.DeleteAllNQueueData();
        fakeApp.FakeService = fakeService;

        await using var app = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder => builder.ConfigureFakes(fakeApp, baseUrl)); 
        var nQueueClient = app.Services.GetRequiredService<INQueueClient>();
        // var nQueueService = app.Services.GetRequiredService<INQueueService>();
        
        var resourceName = $"account123-{_uniqueId}";
        
        // act
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/AcquireExternalLockAndDone?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my-queue-name")}"), "my-queue-name");
 
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/NoOp"), "my-queue-name");

        
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());

        var lockId = await GetString(fakeService.BaseAddress, $"api/NQueue/GetLockId?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my-queue-name")}", app.CreateClient);

  
        
        // assert
        var workItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();

        workItems.Should().HaveCount(1);
        
        // act
        await GetString(fakeService.BaseAddress, $"api/NQueue/ReleaseExternalLock?lockId={WebUtility.UrlEncode(lockId)}", app.CreateClient);
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());

        
        
        // assert
        workItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();

        workItems.Should().HaveCount(2);
    }

    /// <summary>
    /// The child work-item completes immediately, and then the parent work-item runs after releasing the lock
    /// is released
    /// </summary>
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task AcquireExternalLockOnQueueThenContinueWithParentWorkItem(DbType dbType)
    {
          
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();

        // arrange 
        var baseUrl = new Uri("http://localhost:8501");
        var fakeApp = new FakeWebApp();
        var fakeService = new NQueueHostedServiceFake(_dbCreators[dbType].CreateConnection, baseUrl);
        await fakeService.DeleteAllNQueueData();
        fakeApp.FakeService = fakeService;

        await using var app = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder => builder.ConfigureFakes(fakeApp, baseUrl)); 
        var nQueueClient = app.Services.GetRequiredService<INQueueClient>();
        // var nQueueService = app.Services.GetRequiredService<INQueueService>();
        
        var resourceName = $"account123-{_uniqueId}";
        
        // act
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/NoOp"), "my-queue-name");
        
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/AcquireExternalLockAndDone?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my-queue-name-child")}"), "my-queue-name-child", blockQueueName: "my-queue-name");
  
 

        
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());

        
        var lockId = await GetString(fakeService.BaseAddress, $"api/NQueue/GetLockId?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my-queue-name-child")}", app.CreateClient);

        
        // assert
        var completedWorkItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();
        var workItems = await fakeApp.FakeService.GetWorkItems().ToListAsync();

        await OutputTables(dbType);
        
        
        completedWorkItems.Should().HaveCount(1);
        workItems.Should().HaveCount(2);
        
        // act
        await GetString(fakeService.BaseAddress, $"api/NQueue/ReleaseExternalLock?lockId={WebUtility.UrlEncode(lockId)}", app.CreateClient);
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());

        
        
        // assert
        completedWorkItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();
        workItems = await fakeApp.FakeService.GetWorkItems().ToListAsync();
        
        completedWorkItems.Should().HaveCount(3);
        workItems.Should().BeEmpty();
    }

    private async ValueTask OutputTables(DbType dbType)
    {
        if (dbType == DbType.InMemory)
            return;
        
        await using var cnn = await _dbCreators[dbType].CreateConnection();
        await cnn.OpenAsync();

        {
            await using var cmd = cnn.CreateCommand();
            cmd.CommandText =
                "SELECT Name, NextWorkItemId, ErrorCount, LockedUntil, Shard, IsPaused, to_jsonb(BlockedBy) as BlockedByJson, ExternalLockId FROM NQueue.Queue";
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var a = reader.GetString(0);
            }
        }

        
        {
            await using var cmd = cnn.CreateCommand();
            cmd.CommandText =
                "SELECT wi.WorkItemId, wi.Url, wi.QueueName, wi.DebugInfo, wi.Internal, wi.Shard, wi.BlockingQueueName, wi.BlockingQueueShard FROM NQueue.WorkItem wi";
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var a = reader.GetInt64(0);
            }
        }

        
    }

    /// <summary>
    /// The external resource throws an exception, so rollback the lock on the work item.
    /// </summary>
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task AcquireExternalLockOnQueueThatThrowsAnException(DbType dbType)
    {

        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();

        // arrange 
        var baseUrl = new Uri("http://localhost:8501");
        var fakeApp = new FakeWebApp();
        var fakeService = new NQueueHostedServiceFake(_dbCreators[dbType].CreateConnection, baseUrl);
        await fakeService.DeleteAllNQueueData();
        fakeApp.FakeService = fakeService;

        await using var app = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder => builder.ConfigureFakes(fakeApp, baseUrl)); 
        var nQueueClient = app.Services.GetRequiredService<INQueueClient>();
        // var nQueueService = app.Services.GetRequiredService<INQueueService>();
        
        var resourceName = $"account123-{_uniqueId}";
        
        // act
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/AcquireExternalLockAndThrowException?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my,queue name ")}"), "my,queue name ");
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());

        //var lockId = await GetString(fakeService.BaseAddress, $"api/NQueue/GetLockId?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my,queue name ")}", app.CreateClient);

  
        
        // assert

        
        var queues = new List<string?>();
        
        if (dbType == DbType.InMemory)
        {
            var memQueues = await (await fakeService.GetInMemoryDb()).GetQueues();
            queues = memQueues.Select(q => q.ExternalLockId).ToList();
        }
        else
        {
            
            await using var cnn = await _dbCreators[dbType].CreateConnection();
            await cnn.OpenAsync();
        
            await using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT ExternalLockId FROM NQueue.Queue";
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                queues.Add(reader.IsDBNull(0) ? null : reader.GetString(0));
            }
        }
        

        queues.Single().Should().BeNull();
    }


    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task AcquireExternalLockWithSpecialChars(DbType dbType)
    {
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();

        // arrange 
        var baseUrl = new Uri("http://localhost:8501");
        var fakeApp = new FakeWebApp();
        var fakeService = new NQueueHostedServiceFake(_dbCreators[dbType].CreateConnection, baseUrl);
        await fakeService.DeleteAllNQueueData();
        fakeApp.FakeService = fakeService;

        await using var app = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder => builder.ConfigureFakes(fakeApp, baseUrl)); 
        var nQueueClient = app.Services.GetRequiredService<INQueueClient>();
        // var nQueueService = app.Services.GetRequiredService<INQueueService>();
        
        var resourceName = $"account123-{_uniqueId}";
        
        // act
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/AcquireExternalLockAndRunOnceMore?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my,queue name ")}"), "my,queue name ");
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());

        var lockId = await GetString(fakeService.BaseAddress, $"api/NQueue/GetLockId?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my,queue name ")}", app.CreateClient);

  
        
        // assert
        var workItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();

        workItems.Should().BeEmpty();
        
        // act
        await GetString(fakeService.BaseAddress, $"api/NQueue/ReleaseExternalLock?lockId={WebUtility.UrlEncode(lockId)}", app.CreateClient);
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());

        
        
        // assert
        workItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();

        workItems.Should().HaveCount(1);

    }

    
    /// <summary>
    /// The work-item creates the lock and then the next work-item in the queue runs after releasing the lock 
    /// </summary>
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task AcquireExternalLockOnQueueMultipleTimesNotAllowed(DbType dbType)
    {
        
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();

        // arrange 
        var baseUrl = new Uri("http://localhost:8501");
        var fakeApp = new FakeWebApp();
        var fakeService = new NQueueHostedServiceFake(_dbCreators[dbType].CreateConnection, baseUrl);
        await fakeService.DeleteAllNQueueData();
        fakeApp.FakeService = fakeService;

        await using var app = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder => builder.ConfigureFakes(fakeApp, baseUrl)); 
        var nQueueClient = app.Services.GetRequiredService<INQueueClient>();
        // var nQueueService = app.Services.GetRequiredService<INQueueService>();
        
        var resourceName = $"account123-{_uniqueId}";
        
        // act
        //await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/AcquireExternalLockAndDone?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my-queue-name")}"), "my-queue-name");
        // var lockId = await GetString(fakeService.BaseAddress, $"api/NQueue/GetLockId?resourceName={WebUtility.UrlEncode(resourceName)}&queueName={WebUtility.UrlEncode("my-queue-name")}", app.CreateClient);

        var lock1 = "";

        await nQueueClient.AcquireExternalLock("acount123", "my-queue-name", async lockId => lock1 = lockId);

        string lock2 = "";
        Func<Task> action = async () => await nQueueClient.AcquireExternalLock("acount567", "my-queue-name", async lockId => lock2 = lockId);

        await action.Should().ThrowAsync<Exception>();
        
    }


    /// <summary>
    /// Lock a queue that doesn't exist yet, then enqueue a work-item, then it runs after releasing the lock 
    /// </summary>
    [Theory]
    [MemberData(nameof(MyTheoryData))]
    public async Task AcquireExternalLockOnNewQueue(DbType dbType)
    {
      
        var dbCnn = await _dbCreators[dbType].CreateWorkItemDbConnection();

        var procs = await dbCnn.Get();
        await procs.DeleteAllNQueueDataForUnitTests();

        // arrange 
        var baseUrl = new Uri("http://localhost:8501");
        var fakeApp = new FakeWebApp();
        var fakeService = new NQueueHostedServiceFake(_dbCreators[dbType].CreateConnection, baseUrl);
        await fakeService.DeleteAllNQueueData();
        fakeApp.FakeService = fakeService;

        await using var app = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder => builder.ConfigureFakes(fakeApp, baseUrl)); 
        var nQueueClient = app.Services.GetRequiredService<INQueueClient>();
        // var nQueueService = app.Services.GetRequiredService<INQueueService>();

        var lockId1 = "";
        
        // act
        await nQueueClient.AcquireExternalLock("account123", "my-queue-name", async lockId => lockId1 = lockId);
        
        
        await nQueueClient.Enqueue(await nQueueClient.Localhost($"api/NQueue/NoOp"), "my-queue-name");
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());
     
        
        // assert
        var completedWorkItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();
        var workItems = await fakeApp.FakeService.GetWorkItems().ToListAsync();

        completedWorkItems.Should().BeEmpty();
        workItems.Should().HaveCount(2);
        
        // act
        await nQueueClient.ReleaseExternalLock(lockId1);
        
        await fakeApp.FakeService.ProcessAll(app.CreateClient, app.Services.GetRequiredService<ILoggerFactory>());

        
        
        // assert
        completedWorkItems = await fakeApp.FakeService.GetCompletedWorkItems().ToListAsync();
        workItems = await fakeApp.FakeService.GetWorkItems().ToListAsync();

        completedWorkItems.Should().HaveCount(2);
        workItems.Should().BeEmpty();
    }






    private async ValueTask<string> GetString(Uri baseUri, string relUrl, Func<HttpClient> httpClientFactory)
    {
        var url = new Uri(baseUri, relUrl);
        using var httpClient = httpClientFactory();
        using var resp = await httpClient.GetAsync(url);

        var content = await resp.Content.ReadAsStringAsync();
        
        if (!resp.IsSuccessStatusCode)
        {
            throw new Exception($"Error downloading content\r{content}");
        }

        return content;
    }


    private static async ValueTask ExecuteProcedure(DbConnection cnn, string procedure, params Func<DbCommand, DbParameter>[] ps)
    {
        await using var cmd = cnn.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = procedure;
        cmd.Parameters.AddRange(ps.Select(p => p(cmd)).ToArray());
        await cmd.ExecuteNonQueryAsync();
    }
    
    private static async ValueTask ExecuteNonQuery(DbConnection cnn, string sql, params Func<DbCommand, DbParameter>[] ps)
    {
        await using var cmd = cnn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddRange(ps.Select(p => p(cmd)).ToArray());
        await cmd.ExecuteNonQueryAsync();
    }
    
    private static Func<DbCommand, DbParameter> SqlParameter(string? val)
    {
        return (cmd) =>
        {
            var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.NVarChar, val?.Length ?? 1);
            p.DbType = System.Data.DbType.String;
            p.Value = val ?? (object)DBNull.Value;
            return p;
        };
    }
    private static Func<DbCommand, DbParameter> SqlParameter(int val)
    {
        return (cmd) =>
        {
            var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.Int);
            p.DbType = System.Data.DbType.Int32;
            p.Value = val;
            return p;
        };
    }        
    private static Func<DbCommand, DbParameter> SqlParameter(DateTimeOffset val)
    {
        return (cmd) =>
        {
            var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.DateTimeOffset);
            p.DbType = System.Data.DbType.DateTimeOffset;
            p.Value = val;
            return p;
        };
    }
    private static int CalculateShard(string queueName, DbType dbType)
    {
        if (dbType != DbType.PostgresCitus)
            return 0;
            
        using var md5 = MD5.Create();
        var bytes = md5.ComputeHash(Encoding.UTF8.GetBytes(queueName));

        var shard = bytes[0] >> 4 & 15;

        return shard;
    }
}