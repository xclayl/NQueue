using NQueue;
using OpenTelemetry.Trace;

namespace OpenTelemetry.Instrumentation.NQueue;

public static class TracingExtensions
{
    /// <summary>
    /// Wires up Open Telemetry tracing for NQueue so that Consumers are linked to Producers
    /// </summary>
    public static TracerProviderBuilder AddNQueueSource(this TracerProviderBuilder builder)
    {
        return builder.AddSource(NQueueActivitySource.Name, NQueueActivitySource.Version);
    }
}