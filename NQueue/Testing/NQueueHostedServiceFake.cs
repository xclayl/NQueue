using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal;
using NQueue.Internal.Workers;

namespace NQueue.Testing
{
    
    public class NQueueHostedServiceFake
    {
        private readonly NQueueServiceConfig _config;
        private readonly NQueueServiceFake _service;

        public NQueueHostedServiceFake(Uri? baseUri = null) : this(() => ValueTask.FromResult((DbConnection?)null), baseUri)
        {
        }
        
        public NQueueHostedServiceFake(Func<ValueTask<DbConnection?>> cnnBuilder, Uri? baseUri = null)
        {
            _config = new NQueueServiceConfig
            {
                CreateDbConnection = cnnBuilder
            };
            if (baseUri != null)
                _config.LocalHttpAddresses = new[] { baseUri.AbsoluteUri };
            var configFactory = new ConfigFactory(_config);

            _service = new NQueueServiceFake(configFactory);

        }

        public INQueueClient Client => _service;
        public INQueueService Service => _service;

        public Uri BaseAddress
        {
            set => _config.LocalHttpAddresses = new List<string> { value.AbsoluteUri };
        }


        /// <summary>
        /// Wait and process all work items
        /// </summary>
        public async ValueTask ProcessAll(Func<HttpClient> client, ILoggerFactory loggerFactory)
        {
            var consumer = new WorkItemConsumer("testing",
                TimeSpan.Zero,
                await _config.GetWorkItemDbConnection(),
                new MyHttpClientFactory(client),
                _config,
                loggerFactory);
        
            
            var hasMore = true;
            while (hasMore)
            {
                hasMore = await consumer.ExecuteOne();
            }
        }
        
        /// <summary>
        /// Wait and process one (or zero) work items
        /// </summary>
        public async ValueTask ProcessOne(Func<HttpClient> client, ILoggerFactory loggerFactory)
        {
            var consumer = new WorkItemConsumer("testing",
                TimeSpan.Zero,
                await _config.GetWorkItemDbConnection(),
                new MyHttpClientFactory(client),
                _config,
                loggerFactory);
        
            
            await consumer.ExecuteOne();
        }


        private class MyHttpClientFactory : IHttpClientFactory
        {
            private readonly Func<HttpClient> _client;
        
            public MyHttpClientFactory(Func<HttpClient> client)
            {
                _client = client;
            }
        
            public HttpClient CreateClient(string name) => _client();
        }


        public async ValueTask DeleteAllNQueueData()
        {
            var db = await _config.GetWorkItemDbConnection();
            
            await (await db.Get()).DeleteAllNQueueDataForUnitTests();
        }
    }
}