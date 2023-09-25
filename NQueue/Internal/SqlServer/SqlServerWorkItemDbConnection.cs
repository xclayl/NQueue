using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NQueue.Internal.SqlServer.DbMigrations;

namespace NQueue.Internal.SqlServer
{



    internal class SqlServerWorkItemDbConnection : IWorkItemDbConnection
    {
        private readonly string _cnn;
        private readonly TimeZoneInfo _tz;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private volatile bool _dbMigrationRan = false;

        internal SqlServerWorkItemDbConnection(NQueueServiceConfig config)
        {
            _cnn = config.CheckedConnectionString;
            _tz = config.TimeZone;
        }

        public async ValueTask<IWorkItemDbQuery> Get()
        {
            await EnsureDbMigrationRuns();
            return new SqlServerWorkItemDbQuery(_cnn, _tz);
        }

        private async ValueTask EnsureDbMigrationRuns()
        {
            if (!_dbMigrationRan)
            {
                await _lock.WaitAsync();
                try
                {
                    if (!_dbMigrationRan)
                    {
                        await SqlServerDbMigrator.UpdateDbSchema(_cnn);

                        _dbMigrationRan = true;
                    }
                }
                finally
                {
                    _lock.Release();
                }
            }
        }

        public async ValueTask EnqueueWorkItem(DbTransaction tran, TimeZoneInfo tz, Uri url, string? queueName,
            string? debugInfo, bool duplicateProtection)
        {
            await SqlServerWorkItemDbQuery.EnqueueWorkItem(tran, tz, url, queueName, debugInfo, duplicateProtection);
        }
    }
}