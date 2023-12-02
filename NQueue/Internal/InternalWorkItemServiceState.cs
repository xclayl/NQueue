using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NQueue.Internal.Workers;

namespace NQueue.Internal
{


    internal interface IInternalWorkItemServiceState
    {
        ValueTask<(bool healthy, string stateInfo)> HealthCheck();
        void Configure(IReadOnlyList<IWorker> workers);

        void PollNow();
    }

    internal class InternalWorkItemServiceState : IInternalWorkItemServiceState
    {

        private volatile IReadOnlyList<IWorker> _workers = Array.Empty<IWorker>();
        private readonly ConfigFactory _configFactory;
        private readonly DateTimeOffset _createdAt = DateTimeOffset.Now;


        public InternalWorkItemServiceState(ConfigFactory configFactory)
        {
            _configFactory = configFactory;
        }

        public async ValueTask<(bool healthy, string stateInfo)> HealthCheck()
        {
            var workers = _workers;

            if (!workers.Any())
            {
                // give ourselves a minute to startup
                return (_createdAt.AddMinutes(1) > DateTimeOffset.Now, "Not initialized - no workers found to process cron jobs (via config.CronJobs) or work items (via config.QueueRunners)");
            }

            var sb = new StringBuilder();
            var healthy = workers.Any();

            var states = workers
                .Select(w => w.HealthCheck())
                .ToList();

            var config = await _configFactory.GetConfig();

            var conn = await config.GetWorkItemDbConnection();

            var queueState = await conn.QueueHealthCheck();
            
            states = states.Concat(new[]
                {
                    (queueState.healthy, "[NQueue].Queue table", queueState.healthy ? "GOOD" : "BAD",
                        $"{queueState.countUnhealthy} dead queue{(queueState.countUnhealthy == 1 ? "" : "s")}")
                })
                .ToList();


            var maxNameLen = states.Max(s => s.name.Length);
            var maxStateLen = states.Max(s => s.state.Length);

            foreach (var state in states)
            {
                healthy &= state.healthy;
                sb.AppendLine($"{state.name.PadRight(maxNameLen)} {state.state.PadRight(maxStateLen)} {state.info}");
            }




            return (healthy, sb.ToString());
        }


        public void Configure(IReadOnlyList<IWorker> workers)
        {
            if (!workers.Any())
                throw new ArgumentException("At least one worker is required.");
            _workers = workers;
        }


        public void PollNow()
        {
            var workers = _workers;
            foreach (var worker in workers)
            {
                worker.PollNow();
            }
        }
    }
}