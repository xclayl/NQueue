using System.Diagnostics;
using System.Reflection;

namespace NQueue;

internal static class NQueueActivitySource
{
    private static readonly AssemblyName AssemblyName 
        = typeof(NQueueActivitySource).Assembly.GetName();
    internal static readonly ActivitySource ActivitySource 
        = new (AssemblyName.Name, AssemblyName.Version.ToString());
}