using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.SqlServer
{

    internal class SqlServerWorkItemDbProcs : SqlServerAbstractWorkItemDb, IWorkItemDbProcs
    {
        private readonly NQueueServiceConfig _config;

        public SqlServerWorkItemDbProcs(NQueueServiceConfig config) : base(config.TimeZone)
        {
            _config = config;
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