using System.Diagnostics;
using System.Reflection;

namespace NQueue;

/// <summary>
/// Used internally, but needed to be public by the NQueue.Extensions.OpenTelemetry package.
/// I wouldn't use this directly.  It's probably not interesting to you anyways.  It
/// provides the name version to use with Open Telemetry.
/// </summary>
public static class NQueueActivitySource
{
    private static readonly AssemblyName AssemblyName 
        = typeof(NQueueActivitySource).Assembly.GetName();
    internal static readonly ActivitySource ActivitySource 
        = new (AssemblyName.Name, AssemblyName.Version.ToString());

    public static string Name => ActivitySource.Name;
    public static string Version => ActivitySource.Version.ToString();
}