using System;
using System.Data.Common;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal;
using NQueue.Internal.Workers;

namespace NQueue.Testing
{

    public class NQueueHostedServiceFake
    {
        private readonly InternalWorkItemServiceState _state;
        private readonly NQueueServiceConfig _config;
        private readonly ILoggerFactory _loggerFactory = new MyLoggerFactory();

        public NQueueHostedServiceFake()
        {
            _config = new NQueueServiceConfig();
            _state = new InternalWorkItemServiceState(new ConfigFactory(_config));
        }

        public async ValueTask EnqueueWorkItem(Uri url, string? queueName, DbTransaction? tran, string? debugInfo,
            bool duplicatePrevention)
        {
            await _state.EnqueueWorkItem(url, queueName, tran, debugInfo, duplicatePrevention);
        }

        public async ValueTask PollNow(HttpClient client)
        {
            var consumer = new WorkItemConsumer("testing",
                TimeSpan.Zero,
                await _config.GetWorkItemDbConnection(),
                new MyHttpClientFactory(client),
                _config,
                _loggerFactory);
        
            var hasMore = true;
            while (hasMore)
            {
                hasMore = await consumer.ExecuteOne();
            }
        }

        private class MyHttpClientFactory : IHttpClientFactory
        {
            private readonly HttpClient _client;
        
            public MyHttpClientFactory(HttpClient client)
            {
                _client = client;
            }
        
            public HttpClient CreateClient(string name) => _client;
        }


        private class MyLoggerFactory : ILoggerFactory
        {
            public void Dispose()
            {
                // do nothing
            }

            public ILogger CreateLogger(string categoryName) => new MyLogger(categoryName);

            public void AddProvider(ILoggerProvider provider)
            {
                // do nothing
            }

            private class MyLogger : ILogger
            {
                private readonly string _categoryName;

                public MyLogger(string categoryName)
                {
                    _categoryName = categoryName;
                }

                public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                    Func<TState, Exception?, string> formatter)
                {
                    Console.WriteLine(formatter(state, exception));
                }

                public bool IsEnabled(LogLevel logLevel) => true;

                public IDisposable BeginScope<TState>(TState state) => new MyScope();

                private class MyScope : IDisposable
                {
                    public void Dispose()
                    {
                        // do nothing
                    }
                }
            }
        }
    }
}