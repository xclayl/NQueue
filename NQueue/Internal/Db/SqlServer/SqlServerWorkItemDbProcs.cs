﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.SqlServer
{

    internal class SqlServerWorkItemDbProcs : SqlServerAbstractWorkItemDb, IWorkItemDbProcs
    {
        private readonly IDbConfig _config;

        public SqlServerWorkItemDbProcs(IDbConfig config) : base(config.TimeZone)
        {
            _config = config;
        }





        public async ValueTask<WorkItemInfo?> NextWorkItem(int shard)
        {
            await using var cnn = await _config.OpenDbConnection();
            var rows = ExecuteReader("EXEC [NQueue].[NextWorkItem] @Shard=@Shard, @Now=@Now",
                cnn,
                reader => new WorkItemInfo(
                    reader.GetInt32(reader.GetOrdinal("WorkItemId")),
                    reader.GetString(reader.GetOrdinal("Url")),
                    !reader.IsDBNull(reader.GetOrdinal("Internal")) ? reader.GetString(reader.GetOrdinal("Internal")) : null
                ),
                SqlParameter("@Shard", shard),
                SqlParameter("@Now", Now)
            );

            var row = await rows.SingleOrDefaultAsync();

            return row;
        }


        public async ValueTask CompleteWorkItem(int workItemId, int shard)
        {
            await using var cnn = await _config.OpenDbConnection();
            await ExecuteNonQuery(
                "EXEC [NQueue].[CompleteWorkItem] @WorkItemID=@WorkItemID, @Shard=@Shard, @Now=@Now",
                cnn,
                SqlParameter("@WorkItemID", workItemId),
                SqlParameter("@Shard", shard),
                SqlParameter("@Now", Now)
            );
        }

        public async ValueTask FailWorkItem(int workItemId, int shard)
        {
            await using var cnn = await _config.OpenDbConnection();
            await ExecuteNonQuery(
                "EXEC [NQueue].[FailWorkItem] @WorkItemID=@WorkItemID, @Shard=@Shard, @Now=@Now",
                cnn,
                SqlParameter("@WorkItemID", workItemId),
                SqlParameter("@Shard", shard),
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

        public async ValueTask PurgeWorkItems(int shard)
        {
            await using var cnn = await _config.OpenDbConnection();
            await ExecuteNonQuery(
                "EXEC [NQueue].[PurgeWorkItems] @Shard=@Shard, @Now=@Now",
                cnn,
                SqlParameter("@Shard", shard),
                SqlParameter("@Now", Now)
            );
        }



        public async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson)
        {
            if (tran == null)
            {
                await using var cnn = await _config.OpenDbConnection();
                await ExecuteNonQuery(
                    "EXEC [NQueue].[EnqueueWorkItem] @QueueName=@QueueName, @Url=@Url, @DebugInfo=@DebugInfo, @Now=@Now, @DuplicateProtection=@DuplicateProtection, @Internal=@Internal",
                    cnn,
                    SqlParameter("@QueueName", queueName),
                    SqlParameter("@Url", url.ToString()),
                    SqlParameter("@DebugInfo", debugInfo),
                    SqlParameter("@Now", Now),
                    SqlParameter("@DuplicateProtection", duplicateProtection),
                    SqlParameter("@Internal", internalJson)
                );
            }
            else
                await ExecuteNonQuery(
                    tran,
                    "EXEC [NQueue].[EnqueueWorkItem] @QueueName=@QueueName, @Url=@Url, @DebugInfo=@DebugInfo, @Now=@Now, @DuplicateProtection=@DuplicateProtection, @Internal=@Internal",
                    SqlParameter("@QueueName", queueName),
                    SqlParameter("@Url", url.ToString()),
                    SqlParameter("@DebugInfo", debugInfo),
                    SqlParameter("@Now", Now),
                    SqlParameter("@DuplicateProtection", duplicateProtection),
                    SqlParameter("@Internal", internalJson)
                );
        }


        public async ValueTask DeleteAllNQueueDataForUnitTests()
        {
            await using var db = await _config.OpenDbConnection();
            foreach (var table in new [] {"queue", "workitem", "workitemcompleted", "cronjob"})
            {
                await ExecuteNonQuery($"DELETE FROM nqueue.{table}", db);
            }
        }
    }
}