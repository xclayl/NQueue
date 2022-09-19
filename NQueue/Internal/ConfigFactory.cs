using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cronos;

namespace NQueue.Internal
{




    internal class ConfigFactory : IDisposable
    {
        private readonly Func<IServiceProvider, NQueueServiceConfig, Task>? _configBuilder;
        private NQueueServiceConfig? _config;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private readonly IServiceProvider? _serviceProvider;

        public ConfigFactory(Func<IServiceProvider, NQueueServiceConfig, Task> configBuilder,
            IServiceProvider serviceProvider)
        {
            _configBuilder = configBuilder;
            _serviceProvider = serviceProvider;
        }

        public ConfigFactory(NQueueServiceConfig config)
        {
            _config = config;
        }

        public async Task<NQueueServiceConfig> GetConfig()
        {
            await _lock.WaitAsync();
            try
            {
                if (_config == null)
                {
                    _config = new NQueueServiceConfig();
                    await _configBuilder(_serviceProvider, _config);

                    AssertNoDuplicateCronJobNames(_config.CronJobs);
                    AssertValidUrls(_config.CronJobs);
                    AssertValidCronSpecs(_config.CronJobs);
                }

                return _config;
            }
            finally
            {
                _lock.Release();
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
            _lock?.Dispose();
        }
    }
}