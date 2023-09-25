﻿using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Testing
{
    internal class NQueueServiceFake : INQueueService, INQueueClient
    {
        private readonly NQueueHostedServiceFake _testServiceFake;

        public NQueueServiceFake(NQueueHostedServiceFake testServiceFake)
        {
            _testServiceFake = testServiceFake;
        }

        public ValueTask<(bool healthy, string stateInfo)> HealthCheck() => new ValueTask<(bool healthy, string stateInfo)>((true, "Testing Service"));

        public async ValueTask Enqueue(Uri url, string? queueName = null, DbTransaction? tran = null, string? debugInfo = null, bool duplicatePrevention = false)
        {
            await _testServiceFake.EnqueueWorkItem(url, queueName, tran, debugInfo, duplicatePrevention);
        }

        public void PollNow()
        {
            // ignore
        }

    }
}