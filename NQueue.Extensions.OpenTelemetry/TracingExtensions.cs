using NQueue;
using OpenTelemetry.Trace;

namespace OpenTelemetry.Instrumentation.NQueue;

public static class TracingExtensions
{
    public static TracerProviderBuilder AddNQueueSource(this TracerProviderBuilder builder)
    {
        return builder.AddSource(NQueueActivitySource.Name, NQueueActivitySource.Version);
    }
}