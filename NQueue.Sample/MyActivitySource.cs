using System.Diagnostics;
using System.Reflection;

namespace NQueue.Sample;

internal static class MyActivitySource
{
    private static readonly AssemblyName AssemblyName 
        = typeof(MyActivitySource).Assembly.GetName();
    internal static readonly ActivitySource ActivitySource 
        = new (AssemblyName.Name, AssemblyName.Version.ToString());
}