﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
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
        private readonly HttpClient _httpClient;
        private readonly SemaphoreSlim _lock;
        private readonly int _maxQueueRunners;
        private long _currentQueueRunners = 0;
        // private readonly SpinWait _testingSpinWait = new();
        private readonly int _shard;

        public WorkItemConsumer(int maxQueueRunners, int shard, TimeSpan pollInterval, IWorkItemDbConnection workItemDbConnection,
            HttpClient httpClient, NQueueServiceConfig config, ILoggerFactory loggerFactory) : base(
            pollInterval,
            typeof(WorkItemConsumer).FullName ?? "WorkItemConsumer",
            config.TimeZone,
            loggerFactory
        )
        {
            _shard = shard;
            _workItemDbConnection = workItemDbConnection;
            _httpClient = httpClient;
            _config = config;
            _lock = new SemaphoreSlim(maxQueueRunners, maxQueueRunners);
            _maxQueueRunners = maxQueueRunners;
            
            _httpClient.Timeout = TimeSpan.FromHours(1);
        }


        protected override string WorkerName => $"{_shard}";

        protected internal override async ValueTask<bool> ExecuteOne()
        {
            return await ExecuteOne(null, true, Array.Empty<Uri>(), Array.Empty<Uri>());
        }

        public async ValueTask<bool> ExecuteOne(string? queueName, bool runPurge, IReadOnlyList<Uri> testBaseUrls, IList<Uri> testCalls)
        {
            var logger = CreateLogger();
            logger.Log(LogLevel.Debug, "Looking for work {Shard}", _shard);

            WorkItemInfo? request;
            
            var query = await _workItemDbConnection.Get();
            try
            {
                await _lock.WaitAsync();
                request = queueName == null 
                    ? await query.NextWorkItem(_shard)
                    : await query.NextWorkItem(queueName, _shard);
            }
            finally
            {
                _lock.Release();
            }

            if (request == null)
            {
                if (runPurge)
                    try
                    {
                        logger.Log(LogLevel.Debug, "running purge {Shard}", _shard);
                        await _lock.WaitAsync();
                        await query.PurgeWorkItems(_shard);
                    }
                    finally
                    {
                        _lock.Release();
                    }


                logger.Log(LogLevel.Debug, "no work items found for shard {Shard}", _shard);
                return false;
            }
            

            logger.Log(LogLevel.Debug, "triggering work item {Shard} {WorkItemId}", _shard, request.WorkItemId);
            if (testBaseUrls.Any(u => request.Url.StartsWith(u.AbsoluteUri)))
                testCalls.Add(new Uri(request.Url));
            else if (request.Url == "noop:")
            { 
                await query.CompleteWorkItem(request.WorkItemId, _shard, logger);
            }
            else
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
            await Task.Yield(); // goal is to make async void "fire and forget" as quickly as possible.  
            
            Interlocked.Increment(ref _currentQueueRunners);
           
            await _lock.WaitAsync();
            try
            {
                logger.Log(LogLevel.Debug, "running work item {Shard} {WorkItemId}", _shard, request.WorkItemId);

                // using var _ = StartWorkItemActivity(request);
                try
                {
                    using var httpReq = new HttpRequestMessage();
                    httpReq.RequestUri = new Uri(request.Url);
                    httpReq.Method = HttpMethod.Get;
                    await _config.ModifyHttpRequest(httpReq);
                    using var resp = await _httpClient.SendAsync(httpReq);

                  
                    if (resp.StatusCode == HttpStatusCode.TooManyRequests || (int)resp.StatusCode == 261) // 261 is made up.  Here, it means "retry" - progress was made, but the Work Item is not complete yet
                    {
                        logger.Log(LogLevel.Debug, "work item, {HttpReqMethod} {HttpReqRequestUri}, returned status code {RespStatusCode}, rescheduling for later. {Shard}", httpReq.Method, httpReq.RequestUri, resp.StatusCode, _shard);
                        await query.DelayWorkItem(request.WorkItemId, _shard, logger);
                    } 
                    else  if (resp.IsSuccessStatusCode)
                    {
                        logger.LogDebug("work item completed {Shard}", _shard);
                        await query.CompleteWorkItem(request.WorkItemId, _shard, logger);
                    }
                    else
                    {
                        logger.Log(LogLevel.Debug, "work item, {HttpReqMethod} {HttpReqRequestUri}, failed with status code {RespStatusCode}. {Shard}", httpReq.Method, httpReq.RequestUri, resp.StatusCode, _shard);
                        await query.FailWorkItem(request.WorkItemId, _shard, logger);
                    }

                }
                catch (Exception e)
                {
                    logger.LogError(e.ToString());
                    await query.FailWorkItem(request.WorkItemId, _shard, logger);
                    logger.Log(LogLevel.Information, "work item faulted {Shard}", _shard);
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
            await Task.Delay(TimeSpan.FromMilliseconds(10));
            while (Interlocked.Read(ref _currentQueueRunners) > 0)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
            await Task.Delay(TimeSpan.FromMilliseconds(10));
            
            if (Interlocked.Read(ref _currentQueueRunners) > 0)
            {
                await WaitUntilNoActivity();
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