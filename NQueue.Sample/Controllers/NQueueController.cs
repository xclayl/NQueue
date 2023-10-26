using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace NQueue.Sample.Controllers
{
    [ApiController()]
    [Route("api/[controller]/[action]")]
    public class NQueueController : Controller
    {
        private readonly INQueueClient _client;
        private readonly INQueueService _service;
        private readonly IServer _server;

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

        public IActionResult ErrorOp()
        {
            return Problem("a problem");
        }

        public async ValueTask<IActionResult> Enqueue()
        {
            var a = _server.Features.Get<IServerAddressesFeature>().Addresses.ToList();
            
            await _client.Enqueue(await _client.Localhost("NoOp"));
            return Ok("Enqueue Done");
        }

        public async ValueTask<IActionResult> TranEnqueue()
        {
            await using var dataSource = NpgsqlDataSource.Create("User Id=nqueueuser;Password=ihSH3jqeVb7giIgOkohX;Server=localhost;Port=15532;Database=NQueueSample;SslMode=Disable;");

            await using var cnn = await dataSource.OpenConnectionAsync();
            await using var tran = await cnn.BeginTransactionAsync();
            
            await _client.Enqueue(await _client.Localhost("NoOp"), tran: tran);
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