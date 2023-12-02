using System.Threading.Tasks;
using NQueue.Internal;

namespace NQueue
{

    /// <summary>
    /// Used to influence or inspect the background services
    /// </summary>
    public interface INQueueService
    {
        /// <summary>
        /// Determines if there are any issues with the background processes
        /// </summary>
        /// <returns>true = healthy.  A string to determine what issues there might be</returns>
        ValueTask<(bool healthy, string stateInfo)> HealthCheck();
        /// <summary>
        /// Wakes up background threads to look for work.  This is very fast - feel free to call as many times as you'd like.
        /// When using NQueueHostedServiceFake, this method does nothing (b/c nothing will be running in the background).  Instead
        /// call NQueueHostedServiceFake.ProcessAll() or NQueueHostedServiceFake.ProcessOne().
        /// </summary>
        void PollNow();
    }

    internal class NQueueService : INQueueService
    {
        private readonly IInternalWorkItemServiceState _state;

        public NQueueService(IInternalWorkItemServiceState state)
        {
            _state = state;
        }


        public async ValueTask<(bool healthy, string stateInfo)> HealthCheck() => await _state.HealthCheck();

        /// <summary>
        /// Wakes up background threads to look for work.  This is very fast - feel free to call as many times as you'd like.
        /// When using NQueueHostedServiceFake, this method does nothing (b/c nothing will be running in the background).  Instead
        /// call NQueueHostedServiceFake.ProcessAll() or NQueueHostedServiceFake.ProcessOne().
        /// </summary>
        public void PollNow() => _state.PollNow();
    }
}