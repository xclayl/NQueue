using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.SqlServer
{

    internal class SqlServerWorkItemDbQuery : SqlServerAbstractWorkItemDb, IWorkItemDbQuery
    {
        private readonly NQueueServiceConfig _config;

        public SqlServerWorkItemDbQuery(NQueueServiceConfig config) : base(config.TimeZone)
        {
            _config = config;
        }


        public async ValueTask<IWorkItemDbTransaction> BeginTran()
        {
            var conn = await _config.OpenDbConnection();
            return new SqlServerWorkItemDbTransaction(await conn.BeginTransactionAsync(), conn, _tz);
        }



        public async ValueTask<WorkItemInfo?> NextWorkItem()
        {
            var rows = ExecuteReader("EXEC [NQueue].[NextWorkItem] @Now=@Now",
                await _config.OpenDbConnection(),
                reader => new WorkItemInfo(
                    reader.GetInt32(reader.GetOrdinal("WorkItemId")),
                    reader.GetString(reader.GetOrdinal("Url"))
                ),
                SqlParameter("@Now", Now)
            );

            var row = await rows.SingleOrDefaultAsync();

            return row;
        }


        public async ValueTask CompleteWorkItem(int workItemId)
        {
            await ExecuteNonQuery(
                "EXEC [NQueue].[CompleteWorkItem] @WorkItemID=@WorkItemID, @Now=@Now",
                await _config.OpenDbConnection(),
                SqlParameter("@WorkItemID", workItemId),
                SqlParameter("@Now", Now)
            );
        }

        public async ValueTask FailWorkItem(int workItemId)
        {
            await ExecuteNonQuery(
                "EXEC [NQueue].[FailWorkItem] @WorkItemID=@WorkItemID, @Now=@Now",
                await _config.OpenDbConnection(),
                SqlParameter("@WorkItemID", workItemId),
                SqlParameter("@Now", Now)
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
            await ExecuteNonQuery(
                "EXEC [NQueue].[PurgeWorkItems] @Now=@Now",
                await _config.OpenDbConnection(),
                SqlParameter("@Now", Now)
            );
        }


        public async ValueTask<IReadOnlyList<CronJobInfo>> GetCronJobState()
        {
            var rows = ExecuteReader(
                "SELECT [CronJobId], [CronJobName], CAST(LastRanAt AT TIME ZONE 'UTC' AS DATETIME) AS LastRanAtUtc FROM [NQueue].CronJob",
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
                "SELECT COUNT(*) FROM [NQueue].[Queue] WHERE ErrorCount >= 5",
                await _config.OpenDbConnection(),
                reader => reader.GetInt32(0));

            var count = await rows.SingleAsync();

            return (count == 0, count);
        }


        public async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection)
        {
            if (tran == null) 
                await ExecuteNonQuery(
                    "EXEC [NQueue].[EnqueueWorkItem] @QueueName=@QueueName, @Url=@Url, @DebugInfo=@DebugInfo, @Now=@Now, @DuplicateProtection=@DuplicateProtection",
                    await _config.OpenDbConnection(),
                    SqlParameter("@QueueName", queueName),
                    SqlParameter("@Url", url.ToString()),
                    SqlParameter("@DebugInfo", debugInfo),
                    SqlParameter("@Now", Now),
                    SqlParameter("@DuplicateProtection", duplicateProtection)
                );
            else
                await ExecuteNonQuery(
                    tran,
                    "EXEC [NQueue].[EnqueueWorkItem] @QueueName=@QueueName, @Url=@Url, @DebugInfo=@DebugInfo, @Now=@Now, @DuplicateProtection=@DuplicateProtection",
                    SqlParameter("@QueueName", queueName),
                    SqlParameter("@Url", url.ToString()),
                    SqlParameter("@DebugInfo", debugInfo),
                    SqlParameter("@Now", Now),
                    SqlParameter("@DuplicateProtection", duplicateProtection)
                );
        }


        public async ValueTask DeleteAllNQueueDataForUnitTests()
        {
            await using var db = await _config.OpenDbConnection();
            foreach (var table in new [] {"workitem", "workitemcompleted", "queue", "cronjob"})
            {
                await ExecuteNonQuery($"DELETE FROM nqueue.{table}", db);
            }
        }
    }
}