using System;
using System.Threading;
using System.Threading.Tasks;
using NQueue.Internal.DbMigrations;

namespace NQueue.Internal
{



    internal class WorkItemContextFactory
    {
        private readonly string _cnn;
        private readonly TimeZoneInfo _tz;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private volatile bool _dbMigrationRan = false;

        internal WorkItemContextFactory(NQueueServiceConfig config)
        {
            _cnn = config.CheckedConnectionString;
            _tz = config.TimeZone;
        }

        internal async Task<WorkItemDbQuery> Get()
        {
            await EnsureDbMigrationRuns();
            return new WorkItemDbQuery(_cnn, _tz);
        }

        private async Task EnsureDbMigrationRuns()
        {
            if (!_dbMigrationRan)
            {
                await _lock.WaitAsync();
                try
                {
                    if (!_dbMigrationRan)
                    {
                        await DbMigrator.UpdateDbSchema(_cnn);

                        _dbMigrationRan = true;
                    }
                }
                finally
                {
                    _lock.Release();
                }
            }
        }
    }
}