using System;
using System.Threading;
using System.Threading.Tasks;

namespace NQueue.Internal.Workers
{

    internal interface IWorker : IDisposable
    {
        ValueTask ExecuteAsync(CancellationToken stoppingToken);

        (bool healthy, string name, string state, string info) HealthCheck();
        void PollNow();
    }
}