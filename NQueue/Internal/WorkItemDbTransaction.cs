using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace NQueue.Internal
{

    internal class WorkItemDbTransaction : AbstractWorkItemDb, IAsyncDisposable
    {
        private readonly DbTransaction _tran;

        public WorkItemDbTransaction(DbTransaction tran, TimeZoneInfo tz) : base(tz)
        {
            _tran = tran;
        }

        public async ValueTask DisposeAsync()
        {
            await _tran.DisposeAsync();
        }

        public async ValueTask CommitAsync()
        {
            await _tran.CommitAsync();
        }

        public async Task EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection)
        {
            await ExecuteNonQuery(
                "EXEC [NQueue].[EnqueueWorkItem] @QueueName=@QueueName, @Url=@Url, @DebugInfo=@DebugInfo, @Now=@Now, @DuplicateProtection=@DuplicateProtection",
                SqlParameter("@QueueName", queueName),
                SqlParameter("@Url", url.ToString()),
                SqlParameter("@DebugInfo", debugInfo),
                SqlParameter("@Now", Now),
                SqlParameter("@DuplicateProtection", duplicateProtection)
            );
        }


        public async Task<int> CreateCronJob(string name)
        {
            await ExecuteNonQuery(
                "SELECT TOP 1 * FROM [NQueue].CronJob cj WITH (UPDLOCK,HOLDLOCK,TABLOCK);"
            );

            var rows = ExecuteReader(
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

        public async Task<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(int cronJobId)
        {
            var rowEnumerable = ExecuteReader(
                "SELECT CAST(LastRanAt AT TIME ZONE 'UTC' AS DATETIME) AS LastRanAtUtc, Active FROM [NQueue].CronJob cj WITH (UPDLOCK,HOLDLOCK) WHERE CronJobId=@CronJobId",
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

        public async Task UpdateCronJobLastRanAt(int cronJobId)
        {
            await ExecuteNonQuery(
                "UPDATE cj SET LastRanAt = @LastRanAt FROM [NQueue].CronJob cj WHERE CronJobId=@CronJobId",
                SqlParameter("@CronJobId", cronJobId),
                SqlParameter("@LastRanAt", Now)
            );
        }

        private async Task ExecuteNonQuery(string sql, params DbParameter[] p)
        {
            await using var cmd = _tran.Connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = _tran;
            cmd.Parameters.AddRange(p);
            await cmd.ExecuteNonQueryAsync();
        }

        private async IAsyncEnumerable<T> ExecuteReader<T>(string sql, Func<DbDataReader, T> row,
            params DbParameter[] p)
        {
            await using var cmd = _tran.Connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = _tran;
            cmd.Parameters.AddRange(p);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                yield return row(reader);
        }
    }
}