using System;
using System.Threading;

namespace NQueue.Internal;

/// <summary>
/// Uses reference counting to track when to Dispose
/// </summary>
internal class SharedDisposableRef<T> where T : IDisposable
{
    private readonly T _item;
    private int _count = 0;

    
    
    private class SharedDisposableRefItem : ISharedDisposableRefItem<T>
    {
        private readonly SharedDisposableRef<T> _parent;
        public SharedDisposableRefItem(T item, SharedDisposableRef<T> parent)
        {
            Item = item;
            _parent = parent;
        }

        public T Item { get; }

        public void Dispose()
        {
            _parent.Release();
        }
    }

    private void Release()
    {
        var newCount = Interlocked.Decrement(ref _count);
        if (newCount == 0)
            _item.Dispose();
    }


    public SharedDisposableRef(T item)
    {
        _item = item;
    }

    public ISharedDisposableRefItem<T> Share()
    {
        Interlocked.Increment(ref _count);
        return new SharedDisposableRefItem(_item, this);
    }
}

internal interface ISharedDisposableRefItem<out T> : IDisposable where T: IDisposable
{
    T Item { get; }
}
