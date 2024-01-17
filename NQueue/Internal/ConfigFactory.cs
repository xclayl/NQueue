using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cronos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NQueue.Internal.Model;

namespace NQueue.Internal
{




    internal class ConfigFactory : IDisposable
    {
        private readonly Func<IServiceProvider, NQueueServiceConfig, ValueTask>? _configBuilder;
        private NQueueServiceConfig? _config;
        private readonly SemaphoreSlim _initLock = new(1, 1);
        private readonly IServiceProvider? _serviceProvider;
        private volatile bool _appStarted = false;
        private volatile IDbConnectionLock? _dbLock;

        public ConfigFactory(Func<IServiceProvider, NQueueServiceConfig, ValueTask> configBuilder,
            IServiceProvider serviceProvider)
        {
            _configBuilder = configBuilder;
            _serviceProvider = serviceProvider;
            _serviceProvider.GetRequiredService<IHostApplicationLifetime>().ApplicationStarted
                .Register(() => _appStarted = true);
        }

        public ConfigFactory(NQueueServiceConfig config)
        {
            _config = config;
            _appStarted = true;
        }

        public async ValueTask<NQueueServiceConfig> GetConfig()
        {
            while (!_appStarted || _dbLock == null)
            {
                await Task.Delay(1_000);
            }
            
            await _initLock.WaitAsync();
            try
            {
                if (_config == null)
                {
                    _config = new NQueueServiceConfig(_dbLock);
                    await _configBuilder!(_serviceProvider!, _config);

                    AssertNoDuplicateCronJobNames(_config.CronJobs);
                    AssertValidUrls(_config.CronJobs);
                    AssertValidCronSpecs(_config.CronJobs);
                }

                return _config;
            }
            finally
            {
                _initLock.Release();
            }
        }

        private static void AssertValidCronSpecs(IReadOnlyList<NQueueCronJob> cronJobs)
        {
            foreach (var cronJob in cronJobs)
            {
                try
                {
                    var cronExpression = CronExpression.Parse(cronJob.CronSpec);
                }
                catch (CronFormatException e)
                {
                    throw new Exception("Invalid cron spec: " + cronJob.CronSpec, e);
                }
            }
        }

        private static void AssertValidUrls(IReadOnlyList<NQueueCronJob> cronJobs)
        {
            foreach (var cronJob in cronJobs)
            {
                var uri = new Uri(string.Format(cronJob.Url, DateTimeOffset.Now));
                if (uri.Scheme != "http" && uri.Scheme != "https")
                    throw new Exception(
                        $"The url, {uri}, must be either http or https. Found the scheme, {uri.Scheme}, for cron job {cronJob.Name}");
            }
        }

        private static void AssertNoDuplicateCronJobNames(IReadOnlyList<NQueueCronJob> cronJobs)
        {
            var duplicates =
                (from cronJob in cronJobs
                    group cronJob by cronJob.Name
                    into g
                    select new
                    {
                        Name = g.Key,
                        Count = g.Count()
                    })
                .Where(c => c.Count > 1)
                .ToList();

            if (duplicates.Any())
                throw new Exception(
                    $"Duplicate cron job names not allowed: {string.Join(",", duplicates.Select(d => d.Name))}");
        }

        public void Dispose()
        {
            _initLock?.Dispose();
        }

        public void SetDbLock(IDbConnectionLock cnnLock)
        {
            _dbLock = cnnLock;
        }
    }
}