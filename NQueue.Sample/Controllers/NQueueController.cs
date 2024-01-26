using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
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

        public async ValueTask<IActionResult> HealthCheck()
        {
            var h = await _service.HealthCheck();
            if (!h.healthy)
                return Problem(h.stateInfo);


            return Ok(h.stateInfo);
        }






    }
}