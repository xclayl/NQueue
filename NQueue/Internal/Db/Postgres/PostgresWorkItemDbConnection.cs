using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NQueue.Internal.Db.Postgres.DbMigrations;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.Postgres
{
    internal class PostgresWorkItemDbConnection : PostgresAbstractWorkItemDb, IWorkItemDbConnection
    {
        private readonly NQueueServiceConfig _config;
        private readonly SemaphoreSlim _lock = new(1, 1);
        private volatile bool _dbMigrationRan;
    
        public PostgresWorkItemDbConnection(NQueueServiceConfig config): base(config.TimeZone)
        {
            _config = config;
        }
    
    
        public async ValueTask<IWorkItemDbProcs> Get()
        {
            await EnsureDbMigrationRuns();
            return new PostgresWorkItemDbProcs(_config);
        }

        public async ValueTask<ICronTransaction> BeginTran()
        {
            await EnsureDbMigrationRuns();
            var conn = await _config.OpenDbConnection();
            return new PostgresCronTransaction(await conn.BeginTransactionAsync(), conn, _config.TimeZone);
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
    
        
        public async ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState()
        {
            await EnsureDbMigrationRuns();
            await using var cnn = await _config.OpenDbConnection();
            var rows = ExecuteReader(
                "SELECT CronJobId, CronJobName, LastRanAt FROM NQueue.CronJob", 
                cnn,
                reader => new CronJobInfo(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    new DateTimeOffset(reader.GetDateTime(2), TimeSpan.Zero)
                ));

            return await rows.ToListAsync();
        }

        public int ShardCount => 1;


        public async ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck()
        {
            await EnsureDbMigrationRuns();
            await using var cnn = await _config.OpenDbConnection();
            var rows = ExecuteReader(
                "SELECT COUNT(*) FROM NQueue.Queue WHERE ErrorCount >= 5",
                cnn,
                reader => reader.GetInt32(0));

            var count = await rows.SingleAsync();

            return (count == 0, count);
        }

    }
}