using System;
using System.Diagnostics;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal.Db;
using NQueue.Internal.Model;

namespace NQueue.Internal.Workers
{

    internal class WorkItemConsumer : AbstractTimerWorker
    {
        private readonly NQueueServiceConfig _config;
        private readonly IWorkItemDbConnection _workItemDbConnection;
        private readonly IHttpClientFactory _httpClientFactory;

        public WorkItemConsumer(string runnerName, TimeSpan pollInterval, IWorkItemDbConnection workItemDbConnection,
            IHttpClientFactory httpClientFactory, NQueueServiceConfig config, ILoggerFactory loggerFactory) : base(
            pollInterval,
            $"{typeof(WorkItemConsumer).FullName}.{runnerName}",
            config.TimeZone,
            loggerFactory
        )
        {
            _workItemDbConnection = workItemDbConnection;
            _httpClientFactory = httpClientFactory;
            _config = config;
        }

        protected internal override async ValueTask<bool> ExecuteOne()
        {
            var logger = CreateLogger();
            logger.Log(LogLevel.Information, "Looking for work");
            
            var query = await _workItemDbConnection.Get();
            var request = await query.NextWorkItem();

            if (request == null)
            {
                await query.PurgeWorkItems();
                logger.Log(LogLevel.Information, "no work items found");
                return false;
            }

            using var _ = StartWorkItemActivity(request);
            try
            {

                using var httpClient = _httpClientFactory.CreateClient();

                httpClient.Timeout = TimeSpan.FromHours(1);
                using var httpReq = new HttpRequestMessage();
                httpReq.RequestUri = new Uri(request.Url);
                httpReq.Method = HttpMethod.Get;
                await _config.ModifyHttpRequest(httpReq);
                using var resp = await httpClient.SendAsync(httpReq);

                if (resp.IsSuccessStatusCode)
                {
                    await query.CompleteWorkItem(request.WorkItemId);
                    logger.Log(LogLevel.Information, "work item completed");
                    return true;
                }

                await query.FailWorkItem(request.WorkItemId);
                logger.Log(LogLevel.Warning, $"work item, {httpReq.Method} {httpReq.RequestUri}, failed with status code {resp.StatusCode}");
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

        private static Activity? StartWorkItemActivity(WorkItemInfo request)
        {
            if (request.Internal != null)
            {
                var jsonObj = request.Internal.DeserializeAnonymousType(new { otel = new { traceparent = (string?)"", tracestate = (string?)"" } });
                if (jsonObj?.otel?.traceparent != null)
                {
                    if (ActivityContext.TryParse(jsonObj?.otel?.traceparent, jsonObj?.otel?.tracestate,
                            out ActivityContext context))
                    {
                        return NQueueActivitySource.ActivitySource.StartActivity(ActivityKind.Consumer, context);
                    }
                }
            }

            return null;
        }
    }
}