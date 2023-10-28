using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using NQueue.Testing;

namespace NQueue.Tests;

internal class SampleWebAppBuilder : IAsyncDisposable
{
    public SampleWebAppBuilder(WebApplicationFactory<Program> application, NQueueHostedServiceFake fakeService)
    {
        Application = application;
        FakeService = fakeService;
    }

    public WebApplicationFactory<Program> Application { get; }
    public NQueueHostedServiceFake FakeService { get; }
    
    internal static async ValueTask<SampleWebAppBuilder> Build(IDbCreator dbCreator)
    {
        
        var baseUrl = new Uri("http://localhost:8501");
        var fakeService = new NQueueHostedServiceFake(dbCreator.CreateConnection, baseUrl);
        await fakeService.DeleteAllNQueueData();
        
        return new SampleWebAppBuilder(new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.UseUrls(baseUrl.AbsoluteUri);
                builder.ConfigureServices(services =>
                {
                    services.RemoveNQueueHostedService();
                    services.AddNQueueHostedService(fakeService);
                });
            }), fakeService);
    }

    public async ValueTask DisposeAsync()
    {
        await Application.DisposeAsync();
    }
}