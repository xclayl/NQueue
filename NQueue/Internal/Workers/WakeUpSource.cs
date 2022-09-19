using System;
using System.Threading;

namespace NQueue.Internal.Workers
{
    internal class WakeUpSource
    {
        private volatile MyTokenSource? _lastCancellationTokenSource;
        private int _cancelled = 0;
        

        public ITokenSourceLike CreateTokenSource()
        {
            _lastCancellationTokenSource = new MyTokenSource();
            var wasCancelled = Interlocked.Exchange(ref _cancelled, 0) != 0;

            if (wasCancelled)
                _lastCancellationTokenSource.Cancel();
            
            return _lastCancellationTokenSource;

        }

        public void WakeUp()
        {
            Interlocked.Exchange(ref _cancelled, 1);
            _lastCancellationTokenSource?.Cancel();
        }

        private class MyTokenSource : ITokenSourceLike
        {
            private readonly object _lock = new object();
            private CancellationTokenSource? _source = new CancellationTokenSource();
            
            public void Cancel()
            {
                lock (_lock)
                {
                    _source?.Cancel();
                }
            }

            public void Dispose()
            {
                lock (_lock)
                {
                    _source?.Dispose();
                    _source = null;
                }
            }

            public CancellationToken Token
            {
                get
                {
                    lock (_lock)
                    {
                        if (_source == null)
                            throw new ObjectDisposedException(nameof(MyTokenSource));

                        return _source.Token;
                    }
                }
            }
        }
    }

    internal interface ITokenSourceLike : IDisposable
    {
        public CancellationToken Token { get; }
    }
}