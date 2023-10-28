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
        private readonly InternalWorkItemServiceState _state;
        private readonly NQueueServiceConfig _config;

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
            _state = new InternalWorkItemServiceState(new ConfigFactory(_config));
        }

        public Uri BaseAddress
        {
            set => _config.LocalHttpAddresses = new List<string> { value.AbsoluteUri };
        }

        public async ValueTask EnqueueWorkItem(Uri url, string? queueName, DbTransaction? tran, string? debugInfo,
            bool duplicatePrevention)
        {
            await _state.EnqueueWorkItem(url, queueName, tran, debugInfo, duplicatePrevention);
        }

        public async ValueTask PollNow(Func<HttpClient> client, ILoggerFactory loggerFactory)
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

        internal NQueueServiceConfig Config => _config;

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