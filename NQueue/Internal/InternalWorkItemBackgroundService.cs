using System;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal.Model;
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

            
            using var cnnLock = new DbConnectionLock();

            _configFactory.SetDbLock(cnnLock);
            
            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    var config = await _configFactory.GetConfig();

                    
                    var conn = await config.GetWorkItemDbConnection();

                    while (!stoppingToken.IsCancellationRequested)
                    {
                        _logger.LogInformation("Loading work request workers");

                        using var reloadTrigger = new CancellationTokenSource();

                        using var comboCt =
                            CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, reloadTrigger.Token);


                        using var workers = new DisposableList<IWorker>();

                        if (config.QueueRunners > 0)
                            Enumerable.Range(0, conn.ShardCount).ToList().ForEach(shard =>
                                workers.Add(new WorkItemConsumer(config.QueueRunners, shard, config.PollInterval, conn,
                                    _httpClientFactory, config, _loggerFactory))
                            );
                        


                        if (config.CronJobs.Any())
                            workers.Add(new CronJobWorker(conn, config.TimeZone, _configFactory,
                                _loggerFactory, _serviceState));
                        

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