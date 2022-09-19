using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using NQueue.Internal.Model;

namespace NQueue.Internal
{

    internal class WorkItemDbQuery : AbstractWorkItemDb
    {
        private readonly string _cnn;

        public WorkItemDbQuery(string cnn, TimeZoneInfo tz) : base(tz)
        {
            _cnn = cnn;
        }


        public async Task<WorkItemDbTransaction> BeginTran()
        {
            var conn = new SqlConnection(_cnn);
            await conn.OpenAsync();
            return new WorkItemDbTransaction(await conn.BeginTransactionAsync(), _tz);
        }



        public async Task<WorkItemInfo?> NextWorkItem()
        {
            var rows = ExecuteReader("EXEC [NQueue].[NextWorkItem] @Now=@Now",
                _cnn,
                reader => new WorkItemInfo(
                    reader.GetInt32(reader.GetOrdinal("WorkItemId")),
                    reader.GetString(reader.GetOrdinal("Url"))
                ),
                SqlParameter("@Now", Now)
            );

            var row = await rows.SingleOrDefaultAsync();

            return row;
        }


        public async Task CompleteWorkItem(int workItemId)
        {
            await ExecuteNonQuery(
                "EXEC [NQueue].[CompleteWorkItem] @WorkItemID=@WorkItemID, @Now=@Now",
                _cnn,
                SqlParameter("@WorkItemID", workItemId),
                SqlParameter("@Now", Now)
            );
        }

        public async Task FailWorkItem(int workItemId)
        {
            await ExecuteNonQuery(
                "EXEC [NQueue].[FailWorkItem] @WorkItemID=@WorkItemID, @Now=@Now",
                _cnn,
                SqlParameter("@WorkItemID", workItemId),
                SqlParameter("@Now", Now)
            );
        }

        // public async Task ReplayRequest(int requestId)
        // {
        //     await ExecuteNonQuery(
        //         "EXEC [NQueue].[ReplayWorkItem] @RequestID=@WorkItemID, @Now=@Now",
        //         SqlParameter("@RequestID", requestId),
        //         SqlParameter("@Now", Now)
        //     );
        // }

        public async Task PurgeWorkItems()
        {
            await ExecuteNonQuery(
                "EXEC [NQueue].[PurgeWorkItems] @Now=@Now",
                _cnn,
                SqlParameter("@Now", Now)
            );
        }

        public async Task<IReadOnlyList<CronJobInfo>> GetCronJobState()
        {
            var rows = ExecuteReader(
                "SELECT [CronJobId], [CronJobName], CAST(LastRanAt AT TIME ZONE 'UTC' AS DATETIME) AS LastRanAtUtc FROM [NQueue].CronJob",
                _cnn,
                reader => new CronJobInfo(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    new DateTimeOffset(reader.GetDateTime(2), TimeSpan.Zero)
                ));

            return await rows.ToListAsync();
        }


        public async Task<(bool healthy, int countUnhealthy)> QueueHealthCheck()
        {
            var rows = ExecuteReader(
                "SELECT COUNT(*) FROM [NQueue].[Queue] WHERE ErrorCount >= 5",
                _cnn,
                reader => reader.GetInt32(0));

            var count = await rows.SingleAsync();

            return (count == 0, count);
        }


        public async Task EnqueueWorkItem(Uri url, string? queueName, string? debugInfo, bool duplicateProtection)
        {
            await ExecuteNonQuery(
                "EXEC [NQueue].[EnqueueWorkItem] @QueueName=@QueueName, @Url=@Url, @DebugInfo=@DebugInfo, @Now=@Now, @DuplicateProtection=@DuplicateProtection",
                _cnn,
                SqlParameter("@QueueName", queueName),
                SqlParameter("@Url", url.ToString()),
                SqlParameter("@DebugInfo", debugInfo),
                SqlParameter("@Now", Now),
                SqlParameter("@DuplicateProtection", duplicateProtection)
            );
        }

        public static async Task EnqueueWorkItem(DbTransaction tran, TimeZoneInfo tz, Uri url, string? queueName, 
            string? debugInfo, bool duplicateProtection)
        {
            await ExecuteNonQuery(
                tran,
                "EXEC [NQueue].[EnqueueWorkItem] @QueueName=@QueueName, @Url=@Url, @DebugInfo=@DebugInfo, @Now=@Now, @DuplicateProtection=@DuplicateProtection",
                SqlParameter("@QueueName", queueName),
                SqlParameter("@Url", url.ToString()),
                SqlParameter("@DebugInfo", debugInfo),
                SqlParameter("@Now", NowIn(tz)),
                SqlParameter("@DuplicateProtection", duplicateProtection)
            );
        }

        private static DateTimeOffset NowIn(TimeZoneInfo tz)
        {
            var nowLocal = DateTimeOffset.Now;
            return nowLocal.ToOffset(tz.GetUtcOffset(nowLocal));
        }

    }
}