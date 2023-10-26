using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using NQueue.Internal;

namespace NQueue
{
    public interface INQueueClient
    {
        ValueTask Enqueue(Uri url, string? queueName = null, DbTransaction? tran = null, string? debugInfo = null,
            bool duplicatePrevention = false);

        ValueTask<Uri> Localhost(string relativeUri);
    }

    internal class NQueueClient : INQueueClient
    {
        private readonly ConfigFactory _configFactory;

        public NQueueClient(ConfigFactory configFactory)
        {
            _configFactory = configFactory;
        }


        public async ValueTask Enqueue(Uri url, string? queueName = null, DbTransaction? tran = null, string debugInfo = null,
            bool duplicatePrevention = false)
        {
            if (tran == null)
                await EnqueueWorkItem(url, queueName, debugInfo, duplicatePrevention);
            else
                await EnqueueWorkItem(tran, url, queueName, debugInfo, duplicatePrevention);
        }

        public async ValueTask<Uri> Localhost(string relativeUri)
        {
            return Localhost(relativeUri, await _configFactory.GetConfig());
        }
        internal static Uri Localhost(string relativeUri, NQueueServiceConfig config)
        {
            if (!config.LocalHttpAddresses.Any())
                throw new Exception(@"LocalHttpAddresses configuration is empty.  Set it using
services.AddNQueueHostedService((s, config) =>
{
     config.LocalHttpAddresses = s.GetService<IServer>()!.Features.Get<IServerAddressesFeature>()!.Addresses.ToList();
});

Or if this is in a test, 

var myFakeNQueueService = new NQueueHostedServiceFake(new Uri(""http://localhost:383838""));

Or 

var factory = new WebApplicationFactory<Program>();
myFakeNQueueService.BaseAddress = factory.Server.BaseAddress;
");
            
            var urls = 
                config.LocalHttpAddresses
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

            var bestBaseUri = urls
                .OrderBy(u => u.Scheme == "http" ? 0 : 1)
                .First();

            return new Uri(bestBaseUri, relativeUri);
        }


        private async ValueTask EnqueueWorkItem(Uri url, string? queueName, string? debugInfo,
            bool duplicatePrevention)
        {
            var config = await _configFactory.GetConfig();
            var conn = await config.GetWorkItemDbConnection();
            
            var query = await conn.Get();

            await query.EnqueueWorkItem(url, queueName, debugInfo, duplicatePrevention);
        }


        private async ValueTask EnqueueWorkItem(DbTransaction tran, Uri url, string? queueName, string? debugInfo,
            bool duplicatePrevention)
        {
            var config = await _configFactory.GetConfig();
            var wiConn = await config.GetWorkItemDbConnection();
            await wiConn.EnqueueWorkItem(tran, config.TimeZone, url, queueName, debugInfo, duplicatePrevention);
        }

    }
}