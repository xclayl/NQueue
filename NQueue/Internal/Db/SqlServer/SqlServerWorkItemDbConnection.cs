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
            throw new Exception(
                "The SQL Server implementation has become unmaintained. If you are interested in maintaining it, please let me know");
            
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

        public ShardConfig ShardConfig { get; } = new(1);
        public IReadOnlyList<int> GetShardOrderForTesting() => new[] { 0 };
        
        public async IAsyncEnumerable<WorkItemForTests> GetWorkItemsForTests()
        {
            await EnsureDbMigrationRuns();
            var list = await _config.WithDbConnection(async cnn =>
            {
                var rows = ExecuteReader(
                    "SELECT wi.WorkItemId, wi.Url, wi.QueueName, wi.DebugInfo, wi.Internal, wi.Shard FROM NQueue.WorkItem wi",
                    cnn,
                    reader => new WorkItemForTests(reader.GetInt64(0), 
                        new Uri(reader.GetString(1)), 
                        reader.GetString(2), 
                        reader.IsDBNull(3) ? null : reader.GetString(3),
                        reader.IsDBNull(4) ? null : reader.GetString(4),
                        reader.GetInt32(5)) );

                return await rows.ToListAsync();
            });

            foreach (var wi in list)
                yield return wi;
        }
        
        public async ValueTask<IReadOnlyList<QueueInfo>> GetQueuesForTesting()
        {
            await EnsureDbMigrationRuns();
            return await _config.WithDbConnection(async cnn =>
            {
                var rows = ExecuteReader(
                    "SELECT q.Name, q.LockedUntil, q.ExternalLockId, q.Shard FROM NQueue.Queue q",
                    cnn,
                    reader => new QueueInfo(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1), reader.IsDBNull(2) ? null : reader.GetString(2), 0, reader.GetInt32(3)));

                return await rows.ToListAsync();
            });
        }
        
        
        public async IAsyncEnumerable<WorkItemForTests> GetCompletedWorkItemsForTests()
        {
            await EnsureDbMigrationRuns();
            var list = await _config.WithDbConnection(async cnn =>
            {
                var rows = ExecuteReader(
                    "SELECT wi.WorkItemId, wi.Url, wi.QueueName, wi.DebugInfo, wi.Internal, wi.Shard FROM NQueue.WorkItemCompleted wi",
                    cnn,
                    reader => new WorkItemForTests(reader.GetInt64(0), 
                        new Uri(reader.GetString(1)), 
                        reader.GetString(2), 
                        reader.IsDBNull(3) ? null : reader.GetString(3),
                        reader.IsDBNull(4) ? null : reader.GetString(4),
                        reader.GetInt32(5)) );

                return await rows.ToListAsync();
            });

            foreach (var wi in list)
                yield return wi;

        }
    }
}