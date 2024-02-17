using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.Postgres
{
    internal class PostgresWorkItemDbProcs : PostgresAbstractWorkItemDb, IWorkItemDbProcs
    {
        private readonly IDbConfig _config;
    
        public PostgresWorkItemDbProcs(IDbConfig config, bool isCitus): base(config.TimeZone, isCitus)
        {
            _config = config;
        }
        
        
        
        public async ValueTask<WorkItemInfo?> NextWorkItem(int shard)
        {
            return await _config.WithDbConnection(async cnn =>
            {
                var rows = ExecuteReader("SELECT * FROM nqueue.NextWorkItem($1, $2)",
                    cnn,
                    reader => new WorkItemInfo(
                        reader.GetInt64(reader.GetOrdinal("WorkItemId")),
                        reader.GetString(reader.GetOrdinal("Url")),
                        !reader.IsDBNull(reader.GetOrdinal("Internal"))
                            ? reader.GetString(reader.GetOrdinal("Internal"))
                            : null
                    ),
                    SqlParameter(shard),
                    SqlParameter(NowUtc)
                );

                var row = await rows.SingleOrDefaultAsync();

                return row;

            });

        }


        public async ValueTask CompleteWorkItem(long workItemId, int shard)
        {
            await _config.WithDbConnection(async cnn =>
            {
                await ExecuteProcedure(
                    "nqueue.CompleteWorkItem",
                    cnn,
                    SqlParameter(workItemId),
                    SqlParameter(shard),
                    SqlParameter(NowUtc)
                );
            });
        }

        public async ValueTask DelayWorkItem(long workItemId, int shard)
        {
            await _config.WithDbConnection(async cnn =>
            {
                await ExecuteProcedure(
                    "nqueue.DelayWorkItem",
                    cnn,
                    SqlParameter(workItemId),
                    SqlParameter(shard),
                    SqlParameter(NowUtc)
                );
            });
        }

        public async ValueTask FailWorkItem(long workItemId, int shard)
        {
            await _config.WithDbConnection(async cnn =>
            {
                await ExecuteProcedure(
                    "nqueue.FailWorkItem",
                    cnn,
                    SqlParameter(workItemId),
                    SqlParameter(shard),
                    SqlParameter(NowUtc)
                );
            });
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
            await _config.WithDbConnection(async cnn =>
            {
                await ExecuteProcedure(
                    "nqueue.PurgeWorkItems",
                    cnn,
                    SqlParameter(shard),
                    SqlParameter(NowUtc)
                );
            });
        }

        
        
        public async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson)
        {
            if (tran == null)
            {
                await _config.WithDbConnection(async cnn =>
                {
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
                });
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
            await _config.WithDbConnection(async db =>
            {
                foreach (var table in new[] { "queue", "workitem", "workitemcompleted", "cronjob" })
                {
                    await ExecuteNonQuery($"DELETE FROM nqueue.{table}", db);
                }
            });
        }
    }
}