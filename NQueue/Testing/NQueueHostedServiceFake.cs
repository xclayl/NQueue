using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal;
using NQueue.Internal.Db;
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
        
        
        /// <summary>
        /// Allows you to modify the HTTP request.  I've used this to add authentication in the past, but
        /// you can modify anything you like, even the RequestUri.
        /// </summary>
        public Func<HttpRequestMessage, ValueTask> ModifyHttpRequest 
        {
            get => _config.ModifyHttpRequest;
            set => _config.ModifyHttpRequest = value;
        }

        public Uri BaseAddress
        {
            get => _config.LocalHttpAddresses.Select(u => new Uri(u)).First();
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
        /// When this class is used for tests by calling AddNQueueHostedService(new NQueueHostedServiceFake(...)), then
        /// NQueueHostedServiceFake will not run background activity. So this method exists to search
        /// for new WorkItems and process them.
        /// </summary>
        public ValueTask ProcessAll(Func<HttpClient> client, ILoggerFactory loggerFactory)
        {
            return ProcessAllPriv(null, client, loggerFactory);
        }



        /// <summary>
        /// Wait and process all work items for queues that match.  Runs until there are no Work Items to immediately take.
        /// When this class is used for tests by calling AddNQueueHostedService(new NQueueHostedServiceFake(...)), then
        /// NQueueHostedServiceFake will not run background activity. So this method exists to search
        /// for new WorkItems and process them.
        /// </summary>
        public ValueTask ProcessAll(Regex queueNames, Func<HttpClient> client, ILoggerFactory loggerFactory)
        {
            return ProcessAllPriv(queueNames, client, loggerFactory);
        }
        private async ValueTask ProcessAllPriv(Regex? queueNames, Func<HttpClient> client, ILoggerFactory loggerFactory)
        {

            var conn = await _config.GetWorkItemDbConnection();

            var queueHistory = new List<string>();
            
            var queues = await GetActiveQueues(queueNames, conn, queueHistory);
            
            var hadActivity = false;
            
            while (queues.Any())
            {
                var queue = queues.First();
                queueHistory.Remove(queue.queueName);
                queueHistory.Add(queue.queueName);
         
                var consumer = new WorkItemConsumer(1,
                    queue.shard,
                    TimeSpan.Zero,
                    conn,
                    client(),
                    _config,
                    loggerFactory);

                var found = await consumer.ExecuteOne(queue.queueName, false, _externalBaseUrls, _externalUrlCalls);

                if (found)
                {
                    await consumer.WaitUntilNoActivity();
                    hadActivity = true;
                }


                queues.RemoveAt(0);

                if (!queues.Any())
                {
                    if (!hadActivity)
                        break;
                    
                    queues = await GetActiveQueues(queueNames, conn, queueHistory);
                    hadActivity = false;
                }
            }
        }

        private static async ValueTask<List<(string queueName, int shard)>> GetActiveQueues(Regex? queueNamesMatch, IWorkItemDbConnection conn, IReadOnlyList<string> queueHistory)
        {
            var now = DateTimeOffset.Now;

            var queues = await conn.GetQueuesForTesting();
            
            var lockedQueues = queues
                .Where(q => q.LockedUntil != null && q.LockedUntil > now || q.ExternalLockId != null)
                .Select(q => q.QueueName)
                .ToList();
            
            var queueNames = (await conn.GetWorkItemsForTests().ToListAsync())
                .Select(wi => (wi.QueueName, wi.Shard))
                .Distinct()
                .Where(qn => queueNamesMatch?.IsMatch(qn.QueueName) ?? true)
                .Where(qn => !lockedQueues.Contains(qn.QueueName))
                .ToList();

           var queueOrder = queueHistory // larger indexes means recently ran
                                .Select((queueName, i) => new {queueName, Order = i})
                                .ToDictionary(kv => kv.queueName, kv => kv.Order);

            queueNames = queueNames
                .OrderBy(q => queueOrder.GetValueOrDefault(q.QueueName, -1))
                .ToList();
            
            return queueNames;
        }
        
        /// <summary>
        /// Wait and process one (or zero) work items.
        /// When this class is used for tests by calling AddNQueueHostedService(new NQueueHostedServiceFake(...)), then
        /// NQueueHostedServiceFake will not run background activity. So this method exists to search
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
                    client(),
                    _config,
                    loggerFactory);


                if (await consumer.ExecuteOne(null, false, _externalBaseUrls, _externalUrlCalls))
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(5));
                    await consumer.WaitUntilNoActivity();
                    break;
                }
            }
            
            
        }


        /// <summary>
        /// Wait and process one (or zero) work items.
        /// When this class is used for tests by calling AddNQueueHostedService(new NQueueHostedServiceFake(...)), then
        /// NQueueHostedServiceFake will not run background activity. So this method exists to search
        /// for new WorkItems and process one.
        /// </summary>
        public async ValueTask ProcessOne(string queueName, Func<HttpClient> client, ILoggerFactory loggerFactory)
        {
            var conn = await _config.GetWorkItemDbConnection();

            var shardOrder = conn.GetShardOrderForTesting();
                
            
            
            foreach (var shard in shardOrder)
            {
              
                var consumer = new WorkItemConsumer(1,
                    shard,
                    TimeSpan.Zero,
                    conn,
                    client(),
                    _config,
                    loggerFactory);


                if (await consumer.ExecuteOne(queueName, false, _externalBaseUrls, _externalUrlCalls))
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



        internal async ValueTask<InMemoryDb?> GetInMemoryDb()
        {
            var db = await _config.GetWorkItemDbConnection();
            return (await db.Get() as InMemoryWorkItemDbProcs)?.Db;
        }


        public async IAsyncEnumerable<WorkItem> GetWorkItems()
        {
            var db = await _config.GetWorkItemDbConnection();

            await foreach (var wi in db.GetWorkItemsForTests())
                yield return WorkItem.From(wi);
        }
        public async IAsyncEnumerable<WorkItem> GetCompletedWorkItems()
        {
            var db = await _config.GetWorkItemDbConnection();

            await foreach (var wi in db.GetCompletedWorkItemsForTests())
                yield return WorkItem.From(wi);
        }
    }
}