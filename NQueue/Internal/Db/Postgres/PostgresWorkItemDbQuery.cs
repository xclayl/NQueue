using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.Postgres
{
    internal class PostgresWorkItemDbQuery : PostgresAbstractWorkItemDb, IWorkItemDbQuery
    {
        private readonly NQueueServiceConfig _config;
    
        public PostgresWorkItemDbQuery(NQueueServiceConfig config): base(config.TimeZone)
        {
            _config = config;
        }
        
        

        public async ValueTask<IWorkItemDbTransaction> BeginTran()
        {
            var conn = await _config.OpenDbConnection();
            return new PostgresWorkItemDbTransaction(await conn.BeginTransactionAsync(), conn, _tz);
        }

        
        public async ValueTask<WorkItemInfo?> NextWorkItem()
        {
            var rows = ExecuteReader("SELECT * FROM nqueue.NextWorkItem($1)",
                await _config.OpenDbConnection(),
                reader => new WorkItemInfo(
                    reader.GetInt32(reader.GetOrdinal("WorkItemId")),
                    reader.GetString(reader.GetOrdinal("Url"))
                ),
                SqlParameter(NowUtc)
            );

            var row = await rows.SingleOrDefaultAsync();

            return row;
        }


        public async ValueTask CompleteWorkItem(int workItemId)
        {
            await ExecuteProcedure(
                "nqueue.CompleteWorkItem",
                await _config.OpenDbConnection(),
                SqlParameter(workItemId),
                SqlParameter(NowUtc)
            );
        }

        public async ValueTask FailWorkItem(int workItemId)
        {
            await ExecuteProcedure(
                "nqueue.FailWorkItem",
                await _config.OpenDbConnection(),
                SqlParameter(workItemId),
                SqlParameter(NowUtc)
            );
        }

        // public async ValueTask ReplayRequest(int requestId)
        // {
        //     await ExecuteNonQuery(
        //         "EXEC [NQueue].[ReplayWorkItem] @RequestID=@WorkItemID, @Now=@Now",
        //         SqlParameter("@RequestID", requestId),
        //         SqlParameter("@Now", Now)
        //     );
        // }

        public async ValueTask PurgeWorkItems()
        {
            await ExecuteProcedure(
                "nqueue.PurgeWorkItems",
                await _config.OpenDbConnection(),
                SqlParameter(NowUtc)
            );
        }

        public async ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState()
        {
            var rows = ExecuteReader(
                "SELECT CronJobId, CronJobName, LastRanAt FROM NQueue.CronJob",
                await _config.OpenDbConnection(),
                reader => new CronJobInfo(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    new DateTimeOffset(reader.GetDateTime(2), TimeSpan.Zero)
                ));

            return await rows.ToListAsync();
        }


        public async ValueTask<(bool healthy, int countUnhealthy)> QueueHealthCheck()
        {
            var rows = ExecuteReader(
                "SELECT COUNT(*) FROM NQueue.Queue WHERE ErrorCount >= 5",
                await _config.OpenDbConnection(),
                reader => reader.GetInt32(0));

            var count = await rows.SingleAsync();

            return (count == 0, count);
        }

        
        
        public async ValueTask EnqueueWorkItem(Uri url, string? queueName, string? debugInfo, bool duplicateProtection)
        {
            await ExecuteProcedure(
                "nqueue.EnqueueWorkItem",
                await _config.OpenDbConnection(),
                SqlParameter(url.ToString()),
                SqlParameter(queueName),
                SqlParameter(debugInfo),
                SqlParameter(NowUtc),
                SqlParameter(duplicateProtection)
            );
        }

        public static async ValueTask EnqueueWorkItem(DbTransaction tran, TimeZoneInfo tz, Uri url, string? queueName, 
            string? debugInfo, bool duplicateProtection)
        {
            await ExecuteProcedure(
                tran,
                "nqueue.EnqueueWorkItem",
                SqlParameter(url.ToString()),
                SqlParameter(queueName),
                SqlParameter(debugInfo),
                SqlParameter(NowIn(tz)),
                SqlParameter(duplicateProtection)
            );
        }

    }
}