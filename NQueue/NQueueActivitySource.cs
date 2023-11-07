using System.Diagnostics;
using System.Reflection;

namespace NQueue;

public static class NQueueActivitySource
{
    private static readonly AssemblyName AssemblyName 
        = typeof(NQueueActivitySource).Assembly.GetName();
    internal static readonly ActivitySource ActivitySource 
        = new (AssemblyName.Name, AssemblyName.Version.ToString());

    public static string Name => ActivitySource.Name;
    public static string Version => ActivitySource.Version.ToString();
}