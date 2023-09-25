﻿using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NQueue.Internal;
using NQueue.Testing;

namespace NQueue
{

    public static class NQueueServiceExtensions
    {
        public static IServiceCollection AddNQueueHostedService(this IServiceCollection services,
            Func<IServiceProvider, NQueueServiceConfig, ValueTask> configBuilder)
        {

            services.RemoveAll<InternalConfig>();
            services.AddSingleton(InternalConfig.AsEnabled);

            services.AddSingleton<InternalWorkItemBackgroundService>();
            services.AddSingleton<ConfigFactory>(s => new ConfigFactory(configBuilder, s));
            services.AddSingleton<INQueueService, NQueueService>();
            services.AddSingleton<INQueueClient, NQueueClient>();
            services.AddSingleton<IInternalWorkItemServiceState, InternalWorkItemServiceState>();

            services.AddHostedService<WorkItemBackgroundService>();

            return services;
        }

        /// <summary>
        /// Removes previously added hosted service.
        /// </summary>
        public static IServiceCollection RemoveNQueueHostedService(this IServiceCollection services)
        {
            services.RemoveAll<InternalConfig>();
            services.AddSingleton(InternalConfig.AsDisabled);

            services.RemoveAll<InternalWorkItemBackgroundService>();
            services.RemoveAll<ConfigFactory>();
            services.RemoveAll<INQueueService>();
            services.RemoveAll<INQueueClient>();
            services.RemoveAll<IInternalWorkItemServiceState>();


            return services;
        }

        /// <summary>
        /// Disables background processing so that you can control when polling happens in your tests.
        /// Call NQueueHostedServiceFake.PollNow() to process work items
        /// </summary>
        public static IServiceCollection AddNQueueHostedService(this IServiceCollection services,
            NQueueHostedServiceFake testServiceFake)
        {
            services.RemoveAll<InternalConfig>();
            services.AddSingleton(InternalConfig.AsDisabled);

            var internalTestService = new NQueueServiceFake(testServiceFake);

            services.AddSingleton<INQueueService>(internalTestService);
            services.AddSingleton<INQueueClient>(internalTestService);

            return services;
        }


    }
}