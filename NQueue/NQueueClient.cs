using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using NQueue.Internal;

namespace NQueue
{
    
    /// <summary>
    /// Callback to provide the generated lockId so that you can trigger the callback to Release the lock.
    /// </summary>
    public delegate ValueTask TriggerExternalLock(string lockId);
    
    /// <summary>
    /// Used for enqueuing Work items
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
        /// identify the code that created a Work Item that failed to help debug my code.</param>
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
        /// Locks a queue (preventing it from running) until ReleaseExternalLock(lockId) is called.
        /// </summary>
        /// <param name="resourceName">This acts like a 'key' to prevent double releasing the queue accidentally.
        /// You can pass an empty string if this is not a concern.</param>
        /// <param name="queueName">Name of the queue to lock.</param>
        /// <param name="triggerExternalCallback">Code to trigger 3rd party processing where you want this queue to
        /// wait until it is done.
        /// NQueue will give a lockId to triggerExternalCallback.  The idea is you give this lock id
        /// to the 3rd party, which will make an HTTP call to an endpoint you create that executes
        /// ReleaseExternalLock(lockId).
        /// The lockId is basically the queueName & resourceName concatenated.</param>
        ValueTask AcquireExternalLock(string resourceName, string queueName, TriggerExternalLock triggerExternalCallback);
        
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

        public async ValueTask AcquireExternalLock(string resourceName, string queueName, TriggerExternalLock triggerExternalCallback)
        {
           
            var config = await _configFactory.GetConfig();
            var conn = await config.GetWorkItemDbConnection();
            
            var query = await conn.Get();

            var externalLockId = BuildExternalLockId(resourceName, queueName);

            await query.AcquireExternalLock(queueName, externalLockId);

            try
            {
                await triggerExternalCallback(externalLockId);
            }
            catch
            {
                await query.ReleaseExternalLock(queueName, externalLockId);
                throw;
            }
        }

        private string BuildExternalLockId(string resourceName, string queueName)
        {
            return $"{queueName.Length}.{queueName}.{resourceName}";
        }
        private string ExtractQueueNameFromExternalLockId(string lockId)
        {
            var dotIndex = lockId.IndexOf('.');
            if (dotIndex < 0)
                throw new FormatException("Invalid external lock id format: " + lockId);
            
            var lenStr = lockId.Substring(0, dotIndex);
            
            if (!int.TryParse(lenStr, out var len))
                throw new FormatException("Invalid external lock id format: " + lockId);
            
            return lockId.Substring(dotIndex + 1, len);
        }

        public async ValueTask ReleaseExternalLock(string lockId)
        {
            var config = await _configFactory.GetConfig();
            var conn = await config.GetWorkItemDbConnection();
            
            var query = await conn.Get();

            var queueName = ExtractQueueNameFromExternalLockId(lockId);
            
            await query.ReleaseExternalLock(queueName, lockId);
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


        public async ValueTask Enqueue(Uri url, string? queueName = null, DbTransaction? tran = null, string? debugInfo = null,
            bool duplicatePrevention = false, string? blockQueueName = null)
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

            
            
            await query.EnqueueWorkItem(tran, url, queueName, debugInfo, duplicatePrevention, internalJson, blockQueueName);
            
            if (tran == null)
                _state?.PollNow();
        }


        

    }
}