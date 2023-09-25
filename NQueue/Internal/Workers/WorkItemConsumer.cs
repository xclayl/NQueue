using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NQueue.Internal.Workers
{

    internal class WorkItemConsumer : AbstractTimerWorker
    {
        private readonly NQueueServiceConfig _config;
        private readonly WorkItemContextFactory _contextFactory;
        private readonly IHttpClientFactory _httpClientFactory;

        public WorkItemConsumer(string runnerName, TimeSpan pollInterval, WorkItemContextFactory contextFactory,
            IHttpClientFactory httpClientFactory, NQueueServiceConfig config, ILoggerFactory loggerFactory) : base(
            pollInterval,
            $"{typeof(WorkItemConsumer).FullName}.{runnerName}",
            config.TimeZone,
            loggerFactory
        )
        {
            _contextFactory = contextFactory;
            _httpClientFactory = httpClientFactory;
            _config = config;
        }

        protected internal override async ValueTask<bool> ExecuteOne()
        {
            var logger = CreateLogger();
            logger.Log(LogLevel.Information, "Looking for work");
            
            var query = await _contextFactory.Get();
            var request = await query.NextWorkItem();

            if (request == null)
            {
                await query.PurgeWorkItems();
                logger.Log(LogLevel.Information, "no work items found");
                return false;
            }

            try
            {
                using var httpClient = _httpClientFactory.CreateClient();

                var url = new Uri(request.Url);
                _config.ConfigureAuth(httpClient, url);
                using var resp = await httpClient.GetAsync(url);

                if (resp.IsSuccessStatusCode)
                {
                    await query.CompleteWorkItem(request.WorkItemId);
                    logger.Log(LogLevel.Information, "work item completed");
                    return true;
                }

                await query.FailWorkItem(request.WorkItemId);
                logger.Log(LogLevel.Information, "work item failed");
                return false;
            }
            catch (Exception e)
            {
                logger.LogError(e.ToString());
            }


            await query.FailWorkItem(request.WorkItemId);
            logger.Log(LogLevel.Information, "work item faulted");
            return false;
        }
    }
}