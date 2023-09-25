using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal.Workers;

namespace NQueue.Internal
{

    internal class InternalWorkItemBackgroundService
    {
        private readonly ConfigFactory _configFactory;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<InternalWorkItemBackgroundService> _logger;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IInternalWorkItemServiceState _serviceState;

        public InternalWorkItemBackgroundService(ILogger<InternalWorkItemBackgroundService> logger,
            ILoggerFactory loggerFactory, ConfigFactory configFactory, IServiceProvider serviceProvider,
            IHttpClientFactory httpClientFactory, IInternalWorkItemServiceState serviceState)
        {
            _logger = logger;
            _loggerFactory = loggerFactory;
            _configFactory = configFactory;
            _httpClientFactory = httpClientFactory;
            _serviceState = serviceState;
        }


        public async ValueTask ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("starting work request background services");

            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    var config = await _configFactory.GetConfig();

                    var ctxFactory = new WorkItemContextFactory(config);

                    while (!stoppingToken.IsCancellationRequested)
                    {
                        _logger.LogInformation("Loading work request workers");

                        using var reloadTrigger = new CancellationTokenSource();

                        using var comboCt =
                            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, reloadTrigger.Token);


                        var queueConsumers = Enumerable.Range(0, config.QueueRunners)
                            .Select(i => new WorkItemConsumer($"{i}", config.PollInterval, ctxFactory,
                                _httpClientFactory, config, _loggerFactory))
                            .ToList();

                        var workers = new List<IWorker>();

                        if (config.CronJobs.Any())
                        {
                            var cronWorker = new CronJobWorker(ctxFactory, config.TimeZone, _configFactory,
                                _loggerFactory);
                            workers.Add(cronWorker);
                        }
                        workers.AddRange(queueConsumers);

                        if (workers.Any())
                        {
                            _serviceState.Configure(workers);

                            await Task.WhenAll(workers.Select(w => w.ExecuteAsync(comboCt.Token).AsTask()));
                        }
                        else
                        {
                            await RunUntilStopped(stoppingToken);
                        }

                        _logger.LogInformation("Stopped all work request workers");
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e.ToString());
                    try
                    {
                        await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
                    }
                    catch (OperationCanceledException)
                    {
                        // do nothing
                    }
                }

            _logger.LogInformation("Shutting down work request workers");
        }

        private async ValueTask RunUntilStopped(CancellationToken stoppingToken)
        {
            while (true)
            {
                try
                {
                    await Task.Delay(5000, stoppingToken);
                }
                catch
                {
                    // ignore
                }
        
                if (stoppingToken.IsCancellationRequested)
                    break;
            }
        }
    }
}