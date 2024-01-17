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
        // private volatile bool _ready = false;

        public WorkItemBackgroundService(IServiceProvider services)
        {
            _services = services;
            // lifetime.ApplicationStarted.Register(() => _ready = true); 
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var scope = _services.CreateScope();

            var conf = scope.ServiceProvider.GetService<InternalConfig>();
            if (conf?.Enabled == true)
            {
                // while(!_ready)
                // {
                //     // waiting, b/c if we get the config too soon, IServerAddressesFeature will return no addresses
                //     // plus it is nice to get the http server running before our code runs
                //     await Task.Delay(1_000, stoppingToken);
                //     stoppingToken.ThrowIfCancellationRequested();
                // }
                
                var scopedProcessingService =
                    scope.ServiceProvider
                        .GetRequiredService<InternalWorkItemBackgroundService>();
                
                await scopedProcessingService.ExecuteAsync(stoppingToken);
            }
            else
            {
                await RunUntilStopped(stoppingToken, conf.IsUnitTest);
            }
        }

        private async ValueTask RunUntilStopped(CancellationToken stoppingToken, bool isUnitTest)
        {
            while (true)
            {
                try
                {
                    await Task.Delay(isUnitTest ? 10 : 5000, stoppingToken);
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