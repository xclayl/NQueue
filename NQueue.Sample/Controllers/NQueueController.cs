using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Mvc;

namespace NQueue.Sample.Controllers
{
    [ApiController()]
    [Route("api/[controller]/[action]")]
    public class NQueueController : Controller
    {
        private readonly INQueueClient _client;
        private readonly INQueueService _service;
        private readonly IServer _httpServer;

        public NQueueController(INQueueClient client, IServer httpServer, INQueueService service)
        {
            _client = client;
            _httpServer = httpServer;
            _service = service;
        }



        public IActionResult NoOp()
        {
            return Ok("Done");
        }

        public async ValueTask<IActionResult> Enqueue()
        {
            await _client.Enqueue(new Uri(FindLocalhost(), Url.Action("NoOp")));
            return Ok("Enqueue Done");
        }

        public IActionResult PollNow()
        {
            _service.PollNow();
            return Ok("PollNow Done");
        }








        private Uri FindLocalhost()
        {
            var urls =
                _httpServer.Features.Get<IServerAddressesFeature>().Addresses
                    .Select(a => new Uri(a))
                    .Select(u =>
                    {
                        if (u.Host == "[::]")
                        {
                            var b = new UriBuilder(u);
                            b.Host = "localhost";
                            u = b.Uri;
                        }

                        return u;
                    })
                    .ToList();

            return urls
                .OrderBy(u => u.Scheme == "http" ? 0 : 1)
                .First();
        }
    }
}