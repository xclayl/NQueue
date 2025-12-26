using System;
using System.Data.Common;
using System.IO.Hashing;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal.Model;

namespace NQueue.Internal.Db.Postgres
{
    internal class PostgresWorkItemDbProcs : PostgresAbstractWorkItemDb, IWorkItemDbProcs
    {
        private readonly IDbConfig _config;
    
        public PostgresWorkItemDbProcs(IDbConfig config, ShardConfig shardConfig): base(config.TimeZone, shardConfig)
        {
            _config = config;
        }
        
        
        
        public async ValueTask<WorkItemInfo?> NextWorkItem(int shard)
        {
	        return await NextWorkItem(shard, null);
        }
        
        
        public async ValueTask<WorkItemInfo?> NextWorkItem(string queueName, int shard)
        {
	        return await NextWorkItem(shard, queueName);
        }

        
        
        private async ValueTask<WorkItemInfo?> NextWorkItem(int shard, string? queueName)
        {
	        return await _config.WithDbConnection(async cnn =>
	        {
		        var rows = ExecuteReader("SELECT * FROM nqueue.NextWorkItem($1, $2, $3, $4)",
			        cnn,
			        reader => new WorkItemInfo(
				        reader.GetInt64(reader.GetOrdinal("o_WorkItemId")),
				        reader.GetString(reader.GetOrdinal("o_Url")),
				        !reader.IsDBNull(reader.GetOrdinal("o_Internal"))
					        ? reader.GetString(reader.GetOrdinal("o_Internal"))
					        : null
			        ),
			        SqlParameter(shard),
			        SqlParameter(ShardConfig.ConsumingShardCount),
			        SqlParameter(NowUtc),
			        SqlParameter(queueName)
		        );

		        var row = await rows.SingleOrDefaultAsync();

		        return row;

	        });
        }


        public async ValueTask CompleteWorkItem(long workItemId, int shard, ILogger logger)
        {
            await _config.WithDbConnectionAndRetries(async cnn =>
            {
                await ExecuteProcedure(
                    "nqueue.CompleteWorkItem",
                    cnn,
                    SqlParameter(workItemId),
                    SqlParameter(shard),
                    SqlParameter(ShardConfig.ConsumingShardCount),
                    SqlParameter(NowUtc)
                );
            }, logger);
        }

        public async ValueTask DelayWorkItem(long workItemId, int shard, ILogger logger)
        {
            await _config.WithDbConnectionAndRetries(async cnn =>
            {
                await ExecuteProcedure(
                    "nqueue.DelayWorkItem",
                    cnn,
                    SqlParameter(workItemId),
                    SqlParameter(shard),
                    SqlParameter(ShardConfig.ConsumingShardCount),
                    SqlParameter(NowUtc)
                );
            }, logger);
        }

        public async ValueTask FailWorkItem(long workItemId, int shard, ILogger logger)
        {
            await _config.WithDbConnectionAndRetries(async cnn =>
            {
                await ExecuteProcedure(
                    "nqueue.FailWorkItem",
                    cnn,
                    SqlParameter(workItemId),
                    SqlParameter(shard),
                    SqlParameter(ShardConfig.ConsumingShardCount),
                    SqlParameter(NowUtc)
                );
            }, logger);
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
                    SqlParameter(ShardConfig.ConsumingShardCount),
                    SqlParameter(NowUtc)
                );
            });
        }

        
        
        public async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson, string? blockQueueName, string? externalLockIdWhenComplete)
        {
	        
	        queueName ??= Guid.NewGuid().ToString();
            
	        // if we're blocking, the new work item must be on the same shard scheme. We want the whole work-item tree to finish.
	        // The assumption is that the queue to block is currently running, so it's on
	        // the Consuming shard scheme.
	        // It doesn't make sense to block a random queue, b/c you don't know if it is running. (assumptions again)
	        var maxShards = blockQueueName != null ? ShardConfig.ConsumingShardCount : ShardConfig.ProducingShardCount;
	        
	        var shard = CalculateShard(queueName, maxShards);

	        
            if (tran == null)
            {
                await _config.WithDbConnection(async cnn =>
                {
                    await ExecuteProcedure(
                        "nqueue.EnqueueWorkItem",
                        cnn,
                        SqlParameter(url.ToString()),
                        SqlParameter(queueName),
                        SqlParameter(shard),
                        SqlParameter(maxShards),
                        SqlParameter(debugInfo),
                        SqlParameter(NowUtc),
                        SqlParameter(duplicateProtection),
                        SqlParameter(internalJson), 
                        SqlParameter(blockQueueName != null ? CalculateShard(blockQueueName, maxShards) : null),
                        SqlParameter(blockQueueName),
                        SqlParameter(externalLockIdWhenComplete)
                    );
                });
            }
            else
                await ExecuteProcedure(
                    tran,
                    "nqueue.EnqueueWorkItem",
                    SqlParameter(url.ToString()),
                    SqlParameter(queueName),
                    SqlParameter(shard),
                    SqlParameter(maxShards),
                    SqlParameter(debugInfo),
                    SqlParameter(NowUtc),
                    SqlParameter(duplicateProtection),
                    SqlParameter(internalJson), 
                    SqlParameter(blockQueueName != null ? CalculateShard(blockQueueName, maxShards) : null),
                    SqlParameter(blockQueueName),
                    SqlParameter(externalLockIdWhenComplete)
                );
                
        }
        
        


        public async ValueTask ReleaseExternalLock(string queueName, int maxShards, string externalLockId)
        {
	      
	        var shard = CalculateShard(queueName, maxShards);
	        
	        await _config.WithDbConnection(async cnn =>
	        {
		        await ExecuteProcedure(
			        "nqueue.ReleaseExternalLock",
			        cnn,
			        SqlParameter(queueName),
			        SqlParameter(shard),
			        SqlParameter(maxShards),
			        SqlParameter(externalLockId),
			        SqlParameter(NowUtc)
		        );
	        });
        }

        
        
        
        private int CalculateShard(string queueName, int maxShards)
        {
	        if (maxShards == 1)
		        return 0;
            
	        if (maxShards == 16) // backwards compatible
	        {
		        using var md5 = MD5.Create();
		        var bytes = md5.ComputeHash(Encoding.UTF8.GetBytes(queueName));

		        return bytes[0] >> 4 & 15; 
	        }

	        var xxHash = new XxHash32();
	        xxHash.Append(Encoding.UTF8.GetBytes(queueName));
	        return GetShard(xxHash.GetCurrentHash(), maxShards);
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