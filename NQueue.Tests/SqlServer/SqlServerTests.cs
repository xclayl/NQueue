using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace NQueue.Tests.SqlServer;

public sealed class SqlServerTests : IAsyncLifetime
{
    private readonly DbCreator _dbCreator = new();
    public Task InitializeAsync() => Task.CompletedTask;
    public Task DisposeAsync() => _dbCreator.DisposeAsync().AsTask();

    
    
    [Fact]
    public async Task Test()
    {
        await using var app = await SampleWebAppBuilder.Build(_dbCreator);

        var nqueueClient = app.Application.Services.GetService<INQueueClient>();
        await nqueueClient!.Enqueue(await nqueueClient.Localhost("api/NQueue/NoOp"));
        await app.FakeService.PollNow(app.Application.CreateClient, app.Application.Services.GetService<ILoggerFactory>());
        
        
    }
}