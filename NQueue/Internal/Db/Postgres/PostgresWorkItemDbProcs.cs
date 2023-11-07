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
            await using var cnn = await _config.OpenDbConnection();
            var rows = ExecuteReader("SELECT * FROM nqueue.NextWorkItem($1)",
                cnn,
                reader => new WorkItemInfo(
                    reader.GetInt32(reader.GetOrdinal("WorkItemId")),
                    reader.GetString(reader.GetOrdinal("Url")),
                    !reader.IsDBNull(reader.GetOrdinal("Internal")) ? reader.GetString(reader.GetOrdinal("Internal")) : null
                ),
                SqlParameter(NowUtc)
            );

            var row = await rows.SingleOrDefaultAsync();

            return row;
        }


        public async ValueTask CompleteWorkItem(int workItemId)
        {
            await using var cnn = await _config.OpenDbConnection();
            await ExecuteProcedure(
                "nqueue.CompleteWorkItem",
                cnn,
                SqlParameter(workItemId),
                SqlParameter(NowUtc)
            );
        }

        public async ValueTask FailWorkItem(int workItemId)
        {
            await using var cnn = await _config.OpenDbConnection();
            await ExecuteProcedure(
                "nqueue.FailWorkItem",
                cnn,
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
            await using var cnn = await _config.OpenDbConnection();
            await ExecuteProcedure(
                "nqueue.PurgeWorkItems",
                cnn,
                SqlParameter(NowUtc)
            );
        }

        
        
        public async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson)
        {
            if (tran == null)
            {
                await using var cnn = await _config.OpenDbConnection();
                await ExecuteProcedure(
                    "nqueue.EnqueueWorkItem",
                    cnn,
                    SqlParameter(url.ToString()),
                    SqlParameter(queueName),
                    SqlParameter(debugInfo),
                    SqlParameter(NowUtc),
                    SqlParameter(duplicateProtection),
                    SqlParameter(internalJson)
                );
            }
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
            await ExecuteNonQuery("UPDATE NQueue.Queue SET NextWorkItemId = NULL", db);
            foreach (var table in new [] {"workitem", "workitemcompleted", "queue", "cronjob"})
            {
                await ExecuteNonQuery($"DELETE FROM nqueue.{table}", db);
            }
        }
    }
}