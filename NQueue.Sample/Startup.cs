using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace NQueue.Sample
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddRazorPages();
            services.AddControllers();
            

            services.AddHttpClient();
            services.AddNQueueHostedService((s, config) =>
            {
                // config.CreateDbConnection = () =>
                // {
                //     var cnnBuilder = new SqlConnectionStringBuilder
                //     {
                //         DataSource = "localhost,15533",
                //         InitialCatalog = "NQueueSample",
                //         UserID = "NQueueUser",
                //         Password = "ihSH3jqeVb7giIgOkohX"
                //     };
                //     cnnBuilder.Encrypt = !cnnBuilder.DataSource.StartsWith("localhost");
                //     return new ValueTask<DbConnection>(new SqlConnection(cnnBuilder.ToString()));
                // };

                var dataSource = NpgsqlDataSource.Create("User Id=nqueueuser;Password=ihSH3jqeVb7giIgOkohX;Server=localhost;Port=15532;Database=NQueueSample;SslMode=Disable;");
                config.CreateDbConnection = async () => await dataSource.OpenConnectionAsync();

                config.CronJobs = new[]
                {
                    new NQueueCronJob("my-cron", "*/5 * * * *", "http://localhost:5000/api/NQueue/ErrorOp", "my-queue")
                };
                
                return default;
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
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
        }
    }
}