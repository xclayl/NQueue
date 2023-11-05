using System;
using System.Data.Common;
using System.Threading.Tasks;
using NQueue.Internal;

namespace NQueue.Testing
{
    internal class NQueueServiceFake : NQueueClient, INQueueService
    {
        public NQueueServiceFake(ConfigFactory configFactory): base(configFactory)
        {
        }

        public ValueTask<(bool healthy, string stateInfo)> HealthCheck() => new((true, "Testing Service"));


        public void PollNow()
        {
            // ignore
        }

    }
}