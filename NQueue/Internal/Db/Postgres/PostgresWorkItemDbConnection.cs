﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NQueue.Internal.Db.Postgres.DbMigrations;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.Postgres
{
    internal class PostgresWorkItemDbConnection : PostgresAbstractWorkItemDb, IWorkItemDbConnection
    {
        private readonly IDbConfig _config;
        private readonly SemaphoreSlim _lock = new(1, 1);
        private volatile bool _dbMigrationRan;

        private static IReadOnlyList<int>? RotatingShardOrderForTests = null;
    
        public PostgresWorkItemDbConnection(IDbConfig config, bool isCitus): base(config.TimeZone, isCitus)
        {
            _config = config;
        }
    
    
        public async ValueTask<IWorkItemDbProcs> Get()
        {
            await EnsureDbMigrationRuns();
            return new PostgresWorkItemDbProcs(_config, IsCitus);
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
                var tran = new PostgresCronTransaction(dbTran, conn, _config.TimeZone, IsCitus);
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
                            await PostgresDbMigrator.UpdateDbSchema(conn, IsCitus));
    
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
                    "SELECT CronJobName, LastRanAt FROM NQueue.CronJob",
                    cnn,
                    reader => new CronJobInfo(
                        reader.GetString(0),
                        new DateTimeOffset(reader.GetDateTime(1), TimeSpan.Zero)
                    ));

                return await rows.ToListAsync();
            });

        }

        public int ShardCount => IsCitus ? 16 : 1;


        public IReadOnlyList<int> GetShardOrderForTesting() => ShardCount == 1
            ? new[] { 0 }
            : RotateShardOrder();

        private IReadOnlyList<int> RotateShardOrder()
        {
            lock (typeof(PostgresWorkItemDbConnection))
            {
                if (RotatingShardOrderForTests == null || RotatingShardOrderForTests.Count != ShardCount)
                    RotatingShardOrderForTests = Enumerable.Range(0, ShardCount).ToList();
                else
                    RotatingShardOrderForTests = RotatingShardOrderForTests.Skip(1)
                        .Concat(new[] { RotatingShardOrderForTests.First() })
                        .ToList();

                return RotatingShardOrderForTests;
            }
        }


        public async ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck()
        {
            await EnsureDbMigrationRuns();
            return await _config.WithDbConnection(async cnn =>
            {
                var rows = ExecuteReader(
                    "SELECT COUNT(*) FROM NQueue.Queue WHERE ErrorCount >= 5",
                    cnn,
                    reader => reader.GetInt32(0));

                var count = await rows.SingleAsync();

                return (count == 0, count);
            });

        }

    }
}