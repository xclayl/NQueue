﻿using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace NQueue.Sample.Controllers
{
    [ApiController]
    [Route("api/[controller]/[action]")]
    public class NQueueController : Controller
    {
        private readonly INQueueClient _client;
        private readonly INQueueService _service;
        private readonly IServer _server;
        private static volatile string? _msg;

        private readonly record struct MyStruct(string QueueName, string ResourceName);
        
        private static readonly ConcurrentDictionary<MyStruct, string> _lockIds = new();

        public NQueueController(INQueueClient client, INQueueService service, IServer server)
        {
            _client = client;
            _service = service;
            _server = server;
        }



        public IActionResult NoOp()
        {
            return Ok("Done");
        }

        [HttpGet("{msg}")]
        public void SetMessage([FromRoute] string msg)
        {
            _msg = msg;
        }

        [HttpGet]
        public string GetMessage()
        {
            return _msg;
        }
        
        [HttpGet]
        public IActionResult TooManyRequests()
        {
            return new StatusCodeResult(429);
        }
        
        [HttpGet]
        public IActionResult Retry()
        {
            return new StatusCodeResult(261); // totally made up code
        }

        public async Task<IActionResult> ErrorOp()
        {
            await Task.Delay(TimeSpan.FromMinutes(2));
            return Problem("a problem");
        }

        public async ValueTask<IActionResult> Enqueue()
        {
            using var _ = MyActivitySource.ActivitySource.StartActivity("hey");
            var a = _server.Features.Get<IServerAddressesFeature>().Addresses.ToList();
            
            await _client.Enqueue(await _client.Localhost("api/NQueue/NoOp"));
            return Ok("Enqueue Done");
        }

        public async ValueTask<IActionResult> TranEnqueue()
        {
            await using var dataSource = NpgsqlDataSource.Create("User Id=nqueueuser;Password=ihSH3jqeVb7giIgOkohX;Server=localhost;Port=15532;Database=NQueueSample;SslMode=Disable;");

            await using var cnn = await dataSource.OpenConnectionAsync();
            await using var tran = await cnn.BeginTransactionAsync();
            
            await _client.Enqueue(await _client.Localhost("api/NQueue/NoOp"), tran: tran);
            await tran.CommitAsync();
            return Ok("Enqueue Done");
        }

        public IActionResult PollNow()
        {
            _service.PollNow();
            return Ok("PollNow Done");
        }

        

        

        [HttpGet]
        public async ValueTask<string> GetLockId([FromQuery] string resourceName, [FromQuery] string queueName)
        {
            return _lockIds[new(queueName, resourceName)];
        }
        
        [HttpGet]
        public async ValueTask AcquireExternalLockAndDone([FromQuery] string resourceName, [FromQuery] string queueName)
        {
            await _client.AcquireExternalLock(resourceName, queueName, async (lockId) => _lockIds.TryAdd(new(queueName, resourceName), lockId));
        }
        [HttpGet]
        public async ValueTask<IActionResult> AcquireExternalLockAndRunOnceMore([FromQuery] string resourceName, [FromQuery] string queueName)
        {
            if (!_lockIds.ContainsKey(new(queueName, resourceName)))
            {
                await _client.AcquireExternalLock(resourceName, queueName, async (lockId) => _lockIds.TryAdd(new(queueName, resourceName), lockId));
                
                return new StatusCodeResult(261); // re-run     
            }
            
            return Ok(); // done 
        }
        [HttpGet]
        public async ValueTask<IActionResult> AcquireExternalLockAndThrowException([FromQuery] string resourceName, [FromQuery] string queueName)
        {
            await _client.AcquireExternalLock(resourceName, queueName, async (lockId) => throw new Exception("An exception occured"));
            
            return Ok("AcquireExternalLock Done");
        }
        [HttpGet]
        public async ValueTask<IActionResult> ReleaseExternalLock([FromQuery] string lockId)
        {
            await _client.ReleaseExternalLock(lockId);
            
            return Ok("ReleaseExternalLock Done");
        }

        [HttpGet]
        public async ValueTask<IActionResult> HealthCheck()
        {
            var h = await _service.HealthCheck();
            if (!h.healthy)
                return Problem(h.stateInfo);


            return Ok(h.stateInfo);
        }






    }
}