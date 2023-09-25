using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace NQueue.Internal.Workers
{


    internal abstract class AbstractTimerWorker : IWorker
    {
        private readonly TimeSpan _interval;
        private readonly string _loggerCategoryName;
        private readonly Random _random = new Random();
        protected readonly TimeZoneInfo _tz;

        private volatile StrongBox<DateTimeOffset> _lastExecutionStarted = new StrongBox<DateTimeOffset>(DateTimeOffset.Now);

        // private volatile bool _hasMoreItems = false;
        private readonly WakeUpSource _wakeUp = new WakeUpSource();
        private readonly ILoggerFactory _loggerFactory;


        protected AbstractTimerWorker(TimeSpan interval, string loggerCategoryName, TimeZoneInfo tz,
            ILoggerFactory loggerFactory)
        {
            _interval = interval;
            _loggerCategoryName = loggerCategoryName;
            _tz = tz;
            _loggerFactory = loggerFactory;
        }

        protected ILogger CreateLogger() => _loggerFactory.CreateLogger(_loggerCategoryName);

        public async ValueTask ExecuteAsync(CancellationToken stoppingToken)
        {
            var logger = CreateLogger();

            logger.LogInformation("work request service starting");

            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    var startTime = DateTimeOffset.Now;
                    try
                    {
                        while (!stoppingToken.IsCancellationRequested)
                        {
                            _lastExecutionStarted = new StrongBox<DateTimeOffset>(DateTimeOffset.Now);
                            var runAgain = await ExecuteOne();
                            if (!runAgain)
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e.ToString());
                    }

                    var endTime = DateTimeOffset.Now;

                    var sleepTime = _interval - (endTime - startTime);
                    if (sleepTime > TimeSpan.FromSeconds(30))
                        sleepTime += TimeSpan.FromSeconds(_random.Next(-5, 5));

                    if (sleepTime > TimeSpan.Zero)
                    {
                        using var wakeUpTokenSource = _wakeUp.CreateTokenSource();
                        using var combo = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, wakeUpTokenSource.Token);
                        
                        logger.Log(LogLevel.Information, $"Pausing for work {sleepTime}");
                        await Task.Delay(sleepTime, combo.Token);
                    }
                }
                catch (TaskCanceledException)
                {
                    // do nothing
                }

            logger.LogInformation("work request service stopped");
        }

        public (bool healthy, string name, string state, string info) HealthCheck()
        {
            var lastRunBox = _lastExecutionStarted;
            var lastRun = lastRunBox.Value;
            var now = DateTimeOffset.Now;
            if ((now - lastRun) > _interval)
                return (false, _loggerCategoryName, "BAD", ToTz(lastRun, _tz).ToString("O"));


            return (true, _loggerCategoryName, "GOOD", ToTz(lastRun, _tz).ToString("O"));
        }

        public void PollNow()
        {
            _wakeUp.WakeUp();
        }

        protected DateTimeOffset ToTz(DateTimeOffset t, TimeZoneInfo tz)
        {
            return t.ToOffset(tz.GetUtcOffset(t));
        }

        protected internal abstract ValueTask<bool> ExecuteOne();
    }

}