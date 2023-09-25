using System.Threading.Tasks;
using NQueue.Internal;

namespace NQueue
{

    public interface INQueueService
    {
        ValueTask<(bool healthy, string stateInfo)> HealthCheck();
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

        public void PollNow() => _state.PollNow();
    }
}