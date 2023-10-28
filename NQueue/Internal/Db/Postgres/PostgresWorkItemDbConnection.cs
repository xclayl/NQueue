using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NQueue.Internal.Db.Postgres.DbMigrations;

namespace NQueue.Internal.Db.Postgres
{
    internal class PostgresWorkItemDbConnection : IWorkItemDbConnection
    {
        private readonly NQueueServiceConfig _config;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
        private volatile bool _dbMigrationRan = false;
    
        public PostgresWorkItemDbConnection(NQueueServiceConfig config)
        {
            _config = config;
        }
    
    
        public async ValueTask<IWorkItemDbQuery> Get()
        {
            await EnsureDbMigrationRuns();
            return new PostgresWorkItemDbQuery(_config);
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
                        await using var conn = await _config.OpenDbConnection();
                        await PostgresDbMigrator.UpdateDbSchema(conn);
    
                        _dbMigrationRan = true;
                    }
                }
                finally
                {
                    _lock.Release();
                }
            }
        }
    
        
        public async ValueTask EnqueueWorkItem(DbTransaction tran, TimeZoneInfo tz, Uri url, string? queueName, string? debugInfo,
            bool duplicateProtection)
        {
            await PostgresWorkItemDbQuery.EnqueueWorkItem(tran, tz, url, queueName, debugInfo, duplicateProtection);
        }
    }
}