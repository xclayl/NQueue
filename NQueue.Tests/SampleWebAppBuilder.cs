using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Logging;
using NQueue.Testing;
using OpenTelemetry.Logs;

namespace NQueue.Tests;

internal class FakeWebApp
{

    public readonly List<LogRecord> FakeLogs = new();
    public NQueueHostedServiceFake FakeService { get; set; }
    
 

}


internal static class FakeWebAppExtension
{
    public static void ConfigureFakes(this IWebHostBuilder builder, FakeWebApp fakeWebApp, Uri baseUrl)
    {
        

        builder.UseUrls(baseUrl.AbsoluteUri);
        builder.ConfigureServices(services =>
        {
            services.RemoveNQueueHostedService();
            services.AddNQueueHostedService(fakeWebApp.FakeService);
        });
        builder.ConfigureLogging(l =>
        {
            l.ClearProviders();
            l.AddOpenTelemetry(t =>
            {
                t.AddInMemoryExporter(fakeWebApp.FakeLogs);
            });
        });
    }
}