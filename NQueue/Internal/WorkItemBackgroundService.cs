using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace NQueue.Internal
{

    internal class WorkItemBackgroundService : BackgroundService
    {
        private readonly IServiceProvider _services;

        public WorkItemBackgroundService(IServiceProvider services)
        {
            _services = services;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var scope = _services.CreateScope();

            var conf = scope.ServiceProvider.GetService<InternalConfig>();
            if (conf?.Enabled == true)
            {
                var scopedProcessingService =
                    scope.ServiceProvider
                        .GetRequiredService<InternalWorkItemBackgroundService>();

                await scopedProcessingService.ExecuteAsync(stoppingToken);
            }
            else
            {
                await RunUntilStopped(stoppingToken);
            }
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