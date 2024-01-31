using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NQueue.Internal.Db.SqlServer.DbMigrations;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.SqlServer
{



    internal class SqlServerWorkItemDbConnection : SqlServerAbstractWorkItemDb, IWorkItemDbConnection
    {
        private readonly IDbConfig _config;
        private readonly SemaphoreSlim _lock = new(1, 1);
        private volatile bool _dbMigrationRan;

        internal SqlServerWorkItemDbConnection(IDbConfig config): base(config.TimeZone)
        {
            _config = config;
        }

        public async ValueTask<IWorkItemDbProcs> Get()
        {
            await EnsureDbMigrationRuns();
            return new SqlServerWorkItemDbProcs(_config);
        }
        
        public async ValueTask AsTran(Func<ICronTransaction, ValueTask> action)
        {
            await AsTran(async conn =>
            {
                await action(conn);
                return 0;
            });
        }

        
        public async ValueTask<T> AsTran<T>(Func<ICronTransaction, ValueTask<T>> action)
        {
            await EnsureDbMigrationRuns();

            return await _config.WithDbConnection(async conn =>
            {
                await using var dbTran = await conn.BeginTransactionAsync();
                var tran = new SqlServerCronTransaction(dbTran, conn, _config.TimeZone);
                var val = await action(tran);
                await tran.CommitAsync();
                return val;
            });

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
                        await _config.WithDbConnection(async conn =>
                        {
                            await SqlServerDbMigrator.UpdateDbSchema(conn);
                        });

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

            return await _config.WithDbConnection(async cnn =>
            {
                var rows = ExecuteReader(
                    "SELECT [CronJobName], CONVERT(datetime, switchoffset ([LastRanAt], '+00:00')) AS LastRanAtUtc FROM [NQueue].CronJob",
                    cnn,
                    reader => new CronJobInfo(
                        reader.GetString(0),
                        new DateTimeOffset(reader.GetDateTime(1), TimeSpan.Zero)
                    ));

                return await rows.ToListAsync();
            });
        }


        public async ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck()
        {
            await EnsureDbMigrationRuns();

            return await _config.WithDbConnection(async cnn =>
            {
                var rows = ExecuteReader(
                    "SELECT COUNT(*) FROM [NQueue].[Queue] WHERE ErrorCount >= 5",
                    cnn,
                    reader => reader.GetInt32(0));

                var count = await rows.SingleAsync();

                return (count == 0, count);
            });
        }

        public int ShardCount => 1;
        public IReadOnlyList<int> GetShardOrderForTesting() => new[] { 0 };
    }
}