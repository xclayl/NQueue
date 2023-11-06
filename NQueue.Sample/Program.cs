using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using NQueue;
using NQueue.Sample;
using OpenTelemetry.Logs;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;


var builder = WebApplication.CreateBuilder(args);


builder.Services.AddRazorPages();
builder.Services.AddControllers();
            

builder.Services.AddHttpClient();
builder.Services.AddNQueueHostedService((s, config) =>
{
    config.CreateDbConnection = () =>
    {
        var cnnBuilder = new SqlConnectionStringBuilder
        {
            DataSource = "localhost,15533",
            InitialCatalog = "NQueueSample",
            UserID = "NQueueUser",
            Password = "ihSH3jqeVb7giIgOkohX"
        };
        cnnBuilder.Encrypt = !cnnBuilder.DataSource.StartsWith("localhost");
        return new ValueTask<DbConnection>(new SqlConnection(cnnBuilder.ToString()));
    };

    // var cnnBuilder = new NpgsqlConnectionStringBuilder()
    // {
    //     Host = "localhost",
    //     Database = "NQueueSample",
    //     Username = "nqueueuser",
    //     Password = "ihSH3jqeVb7giIgOkohX",
    //     Port = 15532
    // };
    // cnnBuilder.SslMode = cnnBuilder.Host == "localhost" ? SslMode.Disable : SslMode.VerifyFull;
    // config.CreateDbConnection = () => new ValueTask<DbConnection>(new NpgsqlConnection(cnnBuilder.ToString()));

    config.CronJobs = new[]
    {
        new NQueueCronJob("my-cron", "*/5 * * * *", "http://localhost:5000/api/NQueue/ErrorOp", "my-queue")
    };

    config.LocalHttpAddresses = s.GetService<IServer>().Features.Get<IServerAddressesFeature>().Addresses.ToList();
                
    return default;
});

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(MyActivitySource.ActivitySource.Name, MyActivitySource.ActivitySource.Version)
    .AddTelemetrySdk();

builder.Logging
    .ClearProviders()
    .AddOpenTelemetry(b =>
    {
        b.SetResourceBuilder(resourceBuilder)
            .AddConsoleExporter();
    });

builder.Services.AddOpenTelemetry()
    .WithTracing(b =>
    {
        b
            .AddSource(MyActivitySource.ActivitySource.Name)
            .SetResourceBuilder(resourceBuilder)
            .AddAspNetCoreInstrumentation()
            .AddConsoleExporter();
    });



var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
}
else
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

// app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();

app.UseAuthorization();

app.UseEndpoints(endpoints =>
{
    endpoints.MapRazorPages();
    endpoints.MapControllers();
});

app.Run();
