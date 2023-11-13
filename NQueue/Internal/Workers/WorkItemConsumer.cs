using System;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
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
        private readonly SemaphoreSlim _lock;
        private readonly int _maxQueueRunners;
        private long _currentQueueRunners = 0;
        private readonly SpinWait _testingSpinWait = new();

        public WorkItemConsumer(int maxQueueRunners, TimeSpan pollInterval, IWorkItemDbConnection workItemDbConnection,
            IHttpClientFactory httpClientFactory, NQueueServiceConfig config, ILoggerFactory loggerFactory) : base(
            pollInterval,
            typeof(WorkItemConsumer).FullName ?? "WorkItemConsumer",
            config.TimeZone,
            loggerFactory
        )
        {
            _workItemDbConnection = workItemDbConnection;
            _httpClientFactory = httpClientFactory;
            _config = config;
            _lock = new SemaphoreSlim(maxQueueRunners, maxQueueRunners);
            _maxQueueRunners = maxQueueRunners;
        }

        protected internal override async ValueTask<bool> ExecuteOne()
        {
            var logger = CreateLogger();
            logger.Log(LogLevel.Debug, "Looking for work");

            WorkItemInfo? request;
            
            var query = await _workItemDbConnection.Get();
            try
            {
                await _lock.WaitAsync();
                request = await query.NextWorkItem();
            }
            finally
            {
                _lock.Release();
            }

            if (request == null)
            {
                await query.PurgeWorkItems();
                logger.Log(LogLevel.Debug, "no work items found");
                return false;
            }
            

            ExecuteWorkItem(request, query, logger);

            return true;
        }

        public override void Dispose()
        {
            var deadline = DateTimeOffset.UtcNow.AddMinutes(5);
            
            // make sure tasks finish by taking all the locks
            Enumerable.Range(0, _maxQueueRunners).ToList().ForEach(_ =>
            {
                var timeout = deadline - DateTimeOffset.UtcNow;
                if (timeout > TimeSpan.Zero)
                    _lock.Wait(timeout);
            });
            _lock.Dispose();
        }


        private async void ExecuteWorkItem(WorkItemInfo request, IWorkItemDbProcs query, ILogger logger)
        {
            // "async void" is on purpose.  It means "fire and forget"

            
            Interlocked.Increment(ref _currentQueueRunners);
           
            await _lock.WaitAsync();
            try
            {

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
                    }
                    else
                    {
                        await query.FailWorkItem(request.WorkItemId);
                        logger.Log(LogLevel.Warning,
                            $"work item, {httpReq.Method} {httpReq.RequestUri}, failed with status code {resp.StatusCode}");
                    }

                }
                catch (Exception e)
                {
                    logger.LogError(e.ToString());
                    await query.FailWorkItem(request.WorkItemId);
                    logger.Log(LogLevel.Information, "work item faulted");
                }
            }
            finally
            {
                _lock.Release();
                Interlocked.Decrement(ref _currentQueueRunners);
            }
        
            
            PollNow();
        }

        internal async ValueTask WaitUntilNoActivity()
        {
            while (Interlocked.Read(ref _currentQueueRunners) > 0)
            {
                _testingSpinWait.SpinOnce();
                await Task.Delay(TimeSpan.FromMilliseconds(20));
            }
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