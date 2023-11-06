using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.SqlServer
{

    internal class SqlServerCronTransaction : SqlServerAbstractWorkItemDb, ICronTransaction
    {
        private readonly DbTransaction _tran;
        private readonly DbConnection _conn;

        public SqlServerCronTransaction(DbTransaction tran, DbConnection conn, TimeZoneInfo tz) : base(tz)
        {
            _tran = tran;
            _conn = conn;
        }

        public async ValueTask DisposeAsync()
        {
            await _tran.DisposeAsync();
            await _conn.DisposeAsync();
        }

        public async ValueTask CommitAsync()
        {
            await _tran.CommitAsync();
        }

        public async ValueTask EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection)
        {
            await ExecuteNonQuery(
                _tran, 
                "EXEC [NQueue].[EnqueueWorkItem] @QueueName=@QueueName, @Url=@Url, @DebugInfo=@DebugInfo, @Now=@Now, @DuplicateProtection=@DuplicateProtection",
                SqlParameter("@QueueName", queueName),
                SqlParameter("@Url", url.ToString()),
                SqlParameter("@DebugInfo", debugInfo),
                SqlParameter("@Now", Now),
                SqlParameter("@DuplicateProtection", duplicateProtection)
            );
        }


        public async ValueTask<int> CreateCronJob(string name)
        {
            await ExecuteNonQuery(
                _tran, 
                "SELECT TOP 1 * FROM [NQueue].CronJob cj WITH (UPDLOCK,HOLDLOCK,TABLOCK);"
            );

            var rows = ExecuteReader(
                _tran, 
                @"IF NOT EXISTS(SELECT * FROM [NQueue].CronJob cj WHERE cj.CronJobName = @CronJobName) 
                    BEGIN;
                        INSERT INTO [NQueue].CronJob ([CronJobName], [LastRanAt], [Active]) VALUES (@CronJobName,'2000-01-01',1);
                    END;
                    SELECT cj.CronJobId FROM [NQueue].CronJob cj WHERE cj.CronJobName = @CronJobName",
                reader => reader.GetInt32(0),
                SqlParameter("@CronJobName", name)
            );

            return (await rows.ToListAsync()).Single();
        }

        public async ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(int cronJobId)
        {
            var rowEnumerable = ExecuteReader(
                _tran, 
                "SELECT CONVERT(datetime, switchoffset ([LastRanAt], '+00:00')) AS LastRanAtUtc, Active FROM [NQueue].CronJob cj WITH (UPDLOCK,HOLDLOCK) WHERE CronJobId=@CronJobId",
                reader => new
                {
                    LastRanAt = new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero),
                    Active = reader.GetBoolean(1),
                },
                SqlParameter("@CronJobId", cronJobId)
            );

            var rows = await rowEnumerable.ToListAsync();
            var row = rows.Single();
            return (row.LastRanAt, row.Active);
        }

        public async ValueTask UpdateCronJobLastRanAt(int cronJobId)
        {
            await ExecuteNonQuery(
                _tran,
                "UPDATE cj SET LastRanAt = @LastRanAt FROM [NQueue].CronJob cj WHERE CronJobId=@CronJobId",
                SqlParameter("@CronJobId", cronJobId),
                SqlParameter("@LastRanAt", Now)
            );
        }



    }
}