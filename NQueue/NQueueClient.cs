using System;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using NQueue.Internal;

namespace NQueue
{
    
    /// <summary>
    /// Callback to provide the URL which will trigger the external process.  
    /// </summary>
    public delegate Uri ConvertExternalLockIdToUrl(string lockId);
    
    /// <summary>
    /// Used for enqueuing Work items.  This is thread-safe, and registered as a Singleton.
    /// </summary>
    public interface INQueueClient
    {
        /// <summary>
        /// Adds a Work Item to the queue.  If the DB tran is null, PollNow() is called, which will
        /// cause it to query the DB to look for Work Items to process. 
        /// </summary>
        /// <param name="url">The URL to call when consuming this Work Item</param>
        /// <param name="queueName">Name of the queue. If queue is null, Guid.NewGuid() is used. All Work Items in
        /// queues run serially, in FIFO order, including
        /// when errors occur (blocking Work Items in the same queue)</param>
        /// <param name="tran">Database transaction to use when adding the Work Item</param>
        /// <param name="debugInfo">A string you can use.  NQueue ignores this.  I've found it useful to
        /// identify the code that created a Work Item that failed, in order to help debug my code.</param>
        /// <param name="duplicatePrevention">If true, it won't add this Work Item if an identical Work Item
        /// Already exists.</param>
        /// <param name="blockQueueName">If set, prevents the queue from running again until this
        /// Work Item completes</param>
        ValueTask Enqueue(Uri url, string? queueName = null, DbTransaction? tran = null, string? debugInfo = null,
            bool duplicatePrevention = false, string? blockQueueName = null);

        /// <summary>
        /// Helper method to dynamically determine the local host URL.  Useful when calling Enqueue()
        /// To use this, call the following when configuring NQueue
        /// services.AddNQueueHostedService((s, config) =&gt;
        ///{
        ///     config.LocalHttpAddresses = s.GetRequiredService&lt;IServer&gt;().Features.Get&lt;IServerAddressesFeature&gt;()!.Addresses.ToList();
        ///});
        /// </summary>
        /// <param name="relativeUri"></param>
        /// <returns></returns>
        ValueTask<Uri> Localhost(string relativeUri);
        
        /// <summary>
        /// Creates a work-item where when it completes, a lock on the queue is granted (preventing the queue from running) until ReleaseExternalLock(lockId) is called.
        /// The work-item URL should trigger the external processing.
        /// </summary>
        /// <param name="resourceName">This acts like a 'key' to prevent double releasing the queue accidentally.
        /// You can pass an empty string if this is not a concern.</param>
        /// <param name="createNewWorkItemUrl">The URL that will trigger the external processing.
        /// The idea is you give this lock id
        /// to the 3rd party, which will make an HTTP call to an endpoint you create that executes
        /// ReleaseExternalLock(lockId).
        /// The lockId is basically the queueName & resourceName concatenated.</param>
        /// <param name="newWorkItemQueueName">Name of the queue to lock, which is also the queue to add the new work item.</param>
        /// <param name="tran">Database transaction to use when blocking the queue.</param>
        /// <param name="debugInfo">A string you can use.  NQueue ignores this.  I've found it useful to
        /// identify the code that created a Work Item that failed, in order to help debug my code.</param>
        /// <param name="blockQueueName">If you want this new work item to block a parent queue, provide the parent queue name here.</param>
        ValueTask BeginAcquireExternalLock(string resourceName, ConvertExternalLockIdToUrl createNewWorkItemUrl, string? newWorkItemQueueName = null, DbTransaction? tran = null,
            string? debugInfo = null, string? blockQueueName = null);
        
        /// <summary>
        /// Releases the lock on a queue so that it can run again.  
        /// </summary>
        /// <param name="lockId">The lockId given to triggerExternalCallback</param>
        ValueTask ReleaseExternalLock(string lockId);
    }

    internal class NQueueClient : INQueueClient
    {
        private readonly ConfigFactory _configFactory;
        private readonly IInternalWorkItemServiceState? _state;

        public NQueueClient(ConfigFactory configFactory, IInternalWorkItemServiceState? state)
        {
            _configFactory = configFactory;
            _state = state;
        }


        


        public async ValueTask<Uri> Localhost(string relativeUri)
        {
            return Localhost(relativeUri, await _configFactory.GetConfig());
        }


        private (string lockId, int maxShards) BuildExternalLockId(string resourceName, string queueName, int maxShards)
        {
            return ($"{maxShards}:{queueName.Length}.{queueName}.{resourceName}", maxShards);
        }

        private (string queueName, int maxShards) ExtractQueueNameFromExternalLockId(string lockId,
            NQueueServiceConfig config)
        {
            var colonIndex = lockId.IndexOf(':');
            var dotIndex = lockId.IndexOf('.');
            
            if (dotIndex < 0)
                throw new FormatException("Invalid external lock id format: " + lockId);

            int maxShards = 1;
            
            if (colonIndex > 0 && dotIndex > 0 && colonIndex < dotIndex) // new style lockId format
            {
                maxShards = int.Parse(lockId.Substring(0, colonIndex));
                
                lockId = lockId.Substring(colonIndex + 1);
                
                dotIndex = lockId.IndexOf('.');
            }
        
            
            var lenStr = lockId.Substring(0, dotIndex);
            
            if (!int.TryParse(lenStr, out var len))
                throw new FormatException("Invalid external lock id format: " + lockId);
            
            var queueName = lockId.Substring(dotIndex + 1, len);

            return (queueName, maxShards);
        }

        public async ValueTask ReleaseExternalLock(string lockId)
        {
            var config = await _configFactory.GetConfig();
            var conn = await config.GetWorkItemDbConnection();
            
            var query = await conn.Get();

            var queueInfo = ExtractQueueNameFromExternalLockId(lockId, config);
            
            await query.ReleaseExternalLock(queueInfo.queueName, queueInfo.maxShards, lockId);
            
            _state?.PollNow();
        }

        private static Uri Localhost(string relativeUri, NQueueServiceConfig config)
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

        public async ValueTask Enqueue(Uri url, string? queueName = null, DbTransaction? tran = null,
            string? debugInfo = null,
            bool duplicatePrevention = false, string? blockQueueName = null)
        {
            await PrivateEnqueue(url, queueName, tran, debugInfo, duplicatePrevention, blockQueueName, null);
        }
        
        
        public async ValueTask BeginAcquireExternalLock(string resourceName, ConvertExternalLockIdToUrl createNewWorkItemUrl,
            string? newWorkItemQueueName = null, DbTransaction? tran = null,
            string? debugInfo = null,
            string? blockQueueName = null)
        {
            newWorkItemQueueName ??= Guid.NewGuid().ToString();
            var config = await _configFactory.GetConfig();
            
            
            // if we're blocking a parent queue, the new work item must be on the same shard scheme. We want the whole work-item tree to finish.
            // The assumption is that the queue to block is currently running, so it's on
            // the Consuming shard scheme.
            // It doesn't make sense to block a random queue, b/c you don't know if it is running. (assumptions again)
            var maxShards = blockQueueName != null ? config.ShardConfig.ConsumingShardCount : config.ShardConfig.ProducingShardCount;
            
            
            var lockId = BuildExternalLockId(resourceName, newWorkItemQueueName, maxShards);  
            
            await PrivateEnqueue(createNewWorkItemUrl(lockId.lockId), newWorkItemQueueName, tran, debugInfo, false, blockQueueName, lockId.lockId);
        }

        private async ValueTask PrivateEnqueue(Uri url, string? queueName, DbTransaction? tran, string? debugInfo,
            bool duplicatePrevention, string? blockQueueName, string? externalLockIdWhenComplete)
        {
            var config = await _configFactory.GetConfig();
            var conn = await config.GetWorkItemDbConnection();
            
            var query = await conn.Get();
            

            using var _ = NQueueActivitySource.ActivitySource.StartActivity(ActivityKind.Producer);
            
            string? internalJson = null;
            if (Activity.Current != null)
            {
                Activity.Current.SetIdFormat(ActivityIdFormat.W3C);
                
                internalJson = JsonSerializer.Serialize(new
                {
                    otel = new
                    {
                        traceparent = Activity.Current.Id,
                        tracestate  = Activity.Current.TraceStateString,
                    }
                });
            }

            
            
            await query.EnqueueWorkItem(tran, url, queueName, debugInfo, duplicatePrevention, internalJson, blockQueueName, externalLockIdWhenComplete);
            
            if (tran == null)
                _state?.PollNow();
        }



    }
}