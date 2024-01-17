using System;
using System.Threading;
using System.Threading.Tasks;

namespace NQueue.Internal.Model;

internal interface IDbConnectionLock: IDisposable
{
    ValueTask<IDisposable> Acquire();
}

internal class DbConnectionLock : IDbConnectionLock
{
    private readonly SemaphoreSlim _lock = new(1, 1);

    public void Dispose()
    {
        _lock.Dispose();
    }

    public async ValueTask<IDisposable> Acquire()
    {
        await _lock.WaitAsync();
        return new AcquiredLock(_lock);
    }
    
    private class AcquiredLock : IDisposable
    {
        private readonly SemaphoreSlim _lock;

        public AcquiredLock(SemaphoreSlim @lock)
        {
            _lock = @lock;
        }

        public void Dispose() => _lock.Release();
    }
}