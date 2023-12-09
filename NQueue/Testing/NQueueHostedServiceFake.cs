using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal;
using NQueue.Internal.Db.InMemory;
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

                foreach (var shard in Enumerable.Range(0, conn.ShardCount))
                {
                    var consumer = new WorkItemConsumer(1,
                        shard,
                        TimeSpan.Zero,
                        conn,
                        new MyHttpClientFactory(client),
                        _config,
                        loggerFactory);

                    var found = false;

                    while (await consumer.ExecuteOne(false))
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
            foreach (var shard in Enumerable.Range(0, conn.ShardCount))
            {
              
                var consumer = new WorkItemConsumer(1,
                    shard,
                    TimeSpan.Zero,
                    conn,
                    new MyHttpClientFactory(client),
                    _config,
                    loggerFactory);


                if (await consumer.ExecuteOne(false))
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