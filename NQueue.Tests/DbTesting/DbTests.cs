﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
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
            var workItems = await (await fakeService.GetInMemoryDb())!.GetWorkItems();
            workItems.Should().HaveCount(1);
            workItems.Single().FailCount.Should().Be(0);
            
            var completedWorkItems = await (await fakeService.GetInMemoryDb())!.GetCompletedWorkItems();
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