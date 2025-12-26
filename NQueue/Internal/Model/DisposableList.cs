using System;
using System.Collections.Generic;

namespace NQueue.Internal.Model;

internal class DisposableList<T> : List<T>, IDisposable where T : IDisposable
{
    
    public void Dispose()
    {
        ForEach(i => i.Dispose());
    }

}