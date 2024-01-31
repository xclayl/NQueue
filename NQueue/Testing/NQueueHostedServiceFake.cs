using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal;
using NQueue.Internal.Db.InMemory;
using NQueue.Internal.Model;
using NQueue.Internal.Workers;

namespace NQueue.Testing
{
    /// <summary>
    /// Instead of enabling background processing during tests, you can use this to control
    /// when background activity occurs.
    /// </summary>
    public class NQueueHostedServiceFake
    {
        private readonly NQueueServiceConfig _config;
        private readonly NQueueServiceFake _service;
        private readonly List<Uri> _externalBaseUrls = new();
        private readonly List<Uri> _externalUrlCalls = new();

        public NQueueHostedServiceFake(Uri? baseUri = null) : this(() => ValueTask.FromResult((DbConnection?)null), baseUri)
        {
        }
        
        public NQueueHostedServiceFake(Func<ValueTask<DbConnection?>> cnnBuilder, Uri? baseUri = null)
        {
            var fakeDbLock = new FakeDbConnectionLock();
            _config = new NQueueServiceConfig(fakeDbLock)
            {
                CreateDbConnection = cnnBuilder
            };
            if (baseUri != null)
                _config.LocalHttpAddresses = new[] { baseUri.AbsoluteUri };
            var configFactory = new ConfigFactory(_config);
            configFactory.SetDbLock(fakeDbLock);

            _service = new NQueueServiceFake(configFactory);

        }

        private class FakeDbConnectionLock : IDbConnectionLock
        {
            public void Dispose()
            {
                // TODO release managed resources here
            }

            public ValueTask<IDisposable> Acquire()
            {
                return ValueTask.FromResult<IDisposable>(new FakeDisposable());
            }

            private class FakeDisposable : IDisposable
            {
                public void Dispose()
                {
                    // TODO release managed resources here
                }
            }
        }

        public INQueueClient Client => _service;
        public INQueueService Service => _service;

        public Uri BaseAddress
        {
            set => _config.LocalHttpAddresses = new List<string> { value.AbsoluteUri };
        }

        /// <summary>
        /// Add base URLs to external systems so that this Fake does not hit them during tests.
        /// When this fake encounters a URL that matches one of the base URLs, it'll log it
        /// in the ExternalUrlsCalls property.
        /// </summary>
        public void AddExternalBaseUrls(IReadOnlyList<Uri> urls)
        {
            _externalBaseUrls.AddRange(urls);
        }

        /// <summary>
        /// When AddExternalBaseUrls is given URLs, then this list will be populated with any Work Item urls that
        /// match them (instead of making an HTTP request).
        /// </summary>
        public IList<Uri> ExternalUrlsCalls => _externalUrlCalls;
        

        /// <summary>
        /// Wait and process all work items.  Runs until there are no Work Items to immediately take.
        /// When this class is used for tests by calling AddNQueueHostedService(new NQueueHostedServiceFake(...)), then no
        /// background activity occurs. So this method exists to search
        /// for new WorkItems and process them.
        /// </summary>
        public async ValueTask ProcessAll(Func<HttpClient> client, ILoggerFactory loggerFactory)
        {
            var conn = await _config.GetWorkItemDbConnection();
            
            var hasMore = true;

            while (hasMore)
            {
                hasMore = false;
                
                var shardOrder = conn.GetShardOrderForTesting();
                foreach (var shard in shardOrder)
                {
                    var consumer = new WorkItemConsumer(1,
                        shard,
                        TimeSpan.Zero,
                        conn,
                        new MyHttpClientFactory(client),
                        _config,
                        loggerFactory);

                    var found = false;

                    while (await consumer.ExecuteOne(false, _externalBaseUrls, _externalUrlCalls))
                        found = true;

                    if (found)
                    {
                        await consumer.WaitUntilNoActivity();
                        hasMore = true;
                    }
                }
            }
        }
        
        /// <summary>
        /// Wait and process one (or zero) work items.
        /// When this class is used for tests by calling AddNQueueHostedService(new NQueueHostedServiceFake(...)), then no
        /// background activity occurs. So this method exists to search
        /// for new WorkItems and process one.
        /// </summary>
        public async ValueTask ProcessOne(Func<HttpClient> client, ILoggerFactory loggerFactory)
        {
            
            var conn = await _config.GetWorkItemDbConnection();

            var shardOrder = conn.GetShardOrderForTesting();
                
            
            
            foreach (var shard in shardOrder)
            {
              
                var consumer = new WorkItemConsumer(1,
                    shard,
                    TimeSpan.Zero,
                    conn,
                    new MyHttpClientFactory(client),
                    _config,
                    loggerFactory);


                if (await consumer.ExecuteOne(false, _externalBaseUrls, _externalUrlCalls))
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(5));
                    await consumer.WaitUntilNoActivity();
                    break;
                }
            }
            
            
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

        
        /// <summary>
        /// Deletes all nqueue data so that the next test can run with an empty queue.
        /// </summary>
        public async ValueTask DeleteAllNQueueData()
        {
            var db = await _config.GetWorkItemDbConnection();
            
            await (await db.Get()).DeleteAllNQueueDataForUnitTests();
        }


        /// <summary>
        /// I'm probably going to remove this.  Getting the list of workitems when testing is useful
        /// for all DB backends.
        /// For now, you can use this to access the Work Items created for verifying your tests.
        /// </summary>
        /// <returns></returns>
        public async ValueTask<InMemoryDb?> GetInMemoryDb()
        {
            var db = await _config.GetWorkItemDbConnection();
            return (await db.Get() as InMemoryWorkItemDbProcs)?.Db;
        }
    }
}