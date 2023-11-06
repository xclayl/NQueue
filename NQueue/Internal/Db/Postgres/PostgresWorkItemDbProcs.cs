using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.Postgres
{
    internal class PostgresWorkItemDbProcs : PostgresAbstractWorkItemDb, IWorkItemDbProcs
    {
        private readonly NQueueServiceConfig _config;
    
        public PostgresWorkItemDbProcs(NQueueServiceConfig config): base(config.TimeZone)
        {
            _config = config;
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

        
        
        public async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection)
        {
            var internalJson = "{\"a\":3}";
            
            if (tran == null)
                await ExecuteProcedure(
                    "nqueue.EnqueueWorkItem",
                    await _config.OpenDbConnection(),
                    SqlParameter(url.ToString()),
                    SqlParameter(queueName),
                    SqlParameter(debugInfo),
                    SqlParameter(NowUtc),
                    SqlParameter(duplicateProtection),
                    SqlParameter(internalJson)
                );
            else
                await ExecuteProcedure(
                    tran,
                    "nqueue.EnqueueWorkItem",
                    SqlParameter(url.ToString()),
                    SqlParameter(queueName),
                    SqlParameter(debugInfo),
                    SqlParameter(NowUtc),
                    SqlParameter(duplicateProtection),
                    SqlParameter(internalJson)
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