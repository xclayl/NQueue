using System;
using System.Data.Common;
using System.Threading.Tasks;
using NQueue.Internal;

namespace NQueue
{
    public interface INQueueClient
    {
        Task Enqueue(Uri url, string? queueName = null, DbTransaction? tran = null, string? debugInfo = null,
            bool duplicatePrevention = false);
    }

    internal class NQueueClient : INQueueClient
    {
        private readonly ConfigFactory _configFactory;

        public NQueueClient(ConfigFactory configFactory)
        {
            _configFactory = configFactory;
        }


        public async Task Enqueue(Uri url, string? queueName = null, DbTransaction? tran = null, string debugInfo = null,
            bool duplicatePrevention = false)
        {
            if (tran == null)
                await EnqueueWorkItem(url, queueName, debugInfo, duplicatePrevention);
            else
                await EnqueueWorkItem(tran, url, queueName, debugInfo, duplicatePrevention);
        }
        

        private async Task EnqueueWorkItem(Uri url, string? queueName, string? debugInfo,
            bool duplicatePrevention)
        {
            var config = await _configFactory.GetConfig();
            var ctxFactory = new WorkItemContextFactory(config);
            
            var ctx = await ctxFactory.Get();

            await ctx.EnqueueWorkItem(url, queueName, debugInfo, duplicatePrevention);
        }


        private async Task EnqueueWorkItem(DbTransaction tran, Uri url, string? queueName, string? debugInfo,
            bool duplicatePrevention)
        {
            var config = await _configFactory.GetConfig();
            await WorkItemDbQuery.EnqueueWorkItem(tran, config.TimeZone, url, queueName, debugInfo, duplicatePrevention);
        }

    }
}