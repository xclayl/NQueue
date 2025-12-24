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
            return await _config.WithDbConnection(async cnn =>
            {
                var rows = ExecuteReader("SELECT * FROM nqueue.NextWorkItem($1, $2, $3)",
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
                    SqlParameter(NowUtc)
                );

                var row = await rows.SingleOrDefaultAsync();

                return row;

            });

        }
        
        
        public async ValueTask<WorkItemInfo?> NextWorkItem(string queueName, int shard)
        {

	        var sql = @"
CREATE FUNCTION pg_temp.TestingNextWorkItem (
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
    pNow timestamp with time zone = NULL
) RETURNS TABLE(o_WorkItemId NQueue.WorkItem.WorkItemID%TYPE, o_Url NQueue.WorkItem.Url%TYPE, o_Internal NQueue.WorkItem.Internal%TYPE)
as $$
declare
	vQueueID NQueue.Queue.QueueID%TYPE;
	vWorkItemID NQueue.WorkItem.WorkItemID%TYPE;
	vBlockingMessageID NQueue.BlockingMessage.BlockingMessageID%TYPE;
	vBlockingQueueName NQueue.BlockingMessage.QueueName%TYPE;
	vBlockingIsCreatingBlock NQueue.BlockingMessage.IsCreatingBlock%TYPE;
	vBlockingWorkItemId NQueue.BlockingMessage.BlockingWorkItemId%TYPE;
	vBlockingShard NQueue.BlockingMessage.BlockingShard%TYPE;
	vBlockingExternalLockId NQueue.BlockingMessage.ExternalLockId%TYPE;
begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	IF pShard >= pMaxShards OR pShard < 0 THEN
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	-- SET NOCOUNT ON;


    PERFORM pg_advisory_xact_lock(-5839653868952364629 + pShard + 1);

	--------- import work items ---------

	WITH cte AS (
		SELECT
			wi.WorkItemId,
			wi.QueueName,
			wi.CreatedAt,
			ROW_NUMBER() OVER (Partition By wi.QueueName ORDER BY wi.WorkItemId) AS RN
		FROM NQueue.WorkItem wi
		WHERE
			wi.IsIngested = FALSE
			AND wi.Shard = pShard
			AND wi.MaxShards = pMaxShards
	)
	INSERT INTO NQueue.Queue (Name, NextWorkItemId, ErrorCount, LockedUntil, Shard, MaxShards, IsPaused, BlockedBy, ExternalLockId)
	SELECT cte.QueueName, cte.WorkItemId, 0, cte.CreatedAt, pShard, pMaxShards, FALSE, ARRAY[]::nqueue.block[], NULL
	FROM cte
	WHERE RN = 1
	ON CONFLICT (Shard, MaxShards, Name) DO NOTHING;

	UPDATE NQueue.WorkItem wi
	SET IsIngested = TRUE
	FROM
		NQueue.Queue q
	WHERE wi.QueueName = q.Name
		AND wi.IsIngested = FALSE
		AND wi.Shard = pShard
		AND wi.MaxShards = pMaxShards
		AND wi.Shard = q.Shard
		AND q.MaxShards = pMaxShards;


	--------- process blocking messages ---------


	SELECT b.BlockingMessageId, b.QueueName, b.IsCreatingBlock, b.BlockingWorkItemId, b.BlockingShard, b.ExternalLockId
	INTO   vBlockingMessageId,  vBlockingQueueName, vBlockingIsCreatingBlock, vBlockingWorkItemId, vBlockingShard, vBlockingExternalLockId
	FROM
		NQueue.BlockingMessage b
	WHERE
		b.QueueShard = pShard
		AND b.QueueMaxShards = pMaxShards
	ORDER BY
		b.BlockingMessageId
	LIMIT 1;


	WHILE vBlockingMessageId IS NOT NULL LOOP
		

		IF vBlockingWorkItemId IS NULL AND vBlockingShard IS NULL AND vBlockingExternalLockId IS NOT NULL THEN

		    SELECT q.QueueID
		    INTO   vQueueId
		    FROM NQueue.Queue q
		    WHERE q.Name = vBlockingQueueName 
				AND q.Shard = pShard
				AND q.MaxShards = pMaxShards;


		    IF vQueueId IS NOT NULL THEN
			    UPDATE NQueue.Queue q
			    SET ExternalLockId = vBlockingExternalLockId
			    WHERE q.QueueId = vQueueId
					AND q.Shard = pShard
					AND q.MaxShards = pMaxShards;
			ELSE		
				WITH wi AS (
				    INSERT INTO NQueue.WorkItem (Url, DebugInfo, CreatedAt, QueueName, IsIngested, Internal, Shard, MaxShards)
					VALUES ('noop:', NULL, pNow, vBlockingQueueName, TRUE, NULL, pShard, pMaxShards)
				    RETURNING WorkItemID
				)
				INSERT INTO NQueue.Queue (Name, NextWorkItemId, ErrorCount, LockedUntil, Shard, MaxShards, IsPaused, BlockedBy, ExternalLockId)
				SELECT vBlockingQueueName, wi.WorkItemID, 0, pNow, pShard, pMaxShards, FALSE, ARRAY[]::nqueue.block[], vBlockingExternalLockId
				FROM wi;
		    END IF;

		ELSIF vBlockingWorkItemId IS NOT NULL AND vBlockingShard IS NOT NULL AND vBlockingExternalLockId IS NULL THEN

			IF vBlockingIsCreatingBlock THEN
				UPDATE NQueue.Queue q
				SET BlockedBy = BlockedBy || (vBlockingWorkItemId, vBlockingShard)::nqueue.block
				WHERE q.Shard = pShard 
					AND q.MaxShards = pMaxShards
					AND q.Name = vBlockingQueueName;
			ELSE
				UPDATE NQueue.Queue q
				SET BlockedBy = array(
	                SELECT ((elem).WorkItemId, (elem).Shard)::nqueue.block
	                FROM unnest(q.BlockedBy) AS elem
	                WHERE NOT ((elem).WorkItemId = vBlockingWorkItemId AND (elem).Shard = vBlockingShard)
				)
				WHERE q.Shard = pShard 
					AND q.MaxShards = pMaxShards
					AND q.Name = vBlockingQueueName;
			END IF; 		

		ELSE 
			RAISE EXCEPTION 'That''s weird. vBlockingWorkItemId and vBlockingShard shouldn''t be NULL here, or vBlockingExternalLockId shouldn''t be NULL';
		END IF; 		



		DELETE FROM NQueue.BlockingMessage b WHERE b.QueueShard = pShard AND b.QueueMaxShards = pMaxShards AND b.BlockingMessageId = vBlockingMessageId;


		SELECT b.BlockingMessageId, b.QueueName, b.IsCreatingBlock, b.BlockingWorkItemId, b.BlockingShard, ExternalLockId
		INTO   vBlockingMessageId, vBlockingQueueName, vBlockingIsCreatingBlock, vBlockingWorkItemId, vBlockingShard, vBlockingExternalLockId
		FROM
			NQueue.BlockingMessage b
		WHERE
			b.QueueShard = pShard
			AND b.QueueMaxShards = pMaxShards
		ORDER BY
			b.BlockingMessageId
		LIMIT 1;

	END LOOP;

	


	--------- take work item ---------


	SELECT q.QueueId, q.NextWorkItemId
	INTO   vQueueID,  vWorkItemID
	FROM
		NQueue.Queue q
	WHERE
		q.LockedUntil < pNow
		AND q.ErrorCount < 5
		AND q.Shard = pShard
		AND q.MaxShards = pMaxShards
		AND q.IsPaused = FALSE
		AND q.ExternalLockId IS NULL
		AND cardinality(q.BlockedBy) = 0
		AND q.Name = pQueueName
	ORDER BY
		q.LockedUntil, q.NextWorkItemId
	LIMIT 1;



	IF vWorkItemID IS NOT NULL THEN
		UPDATE NQueue.WorkItem ur
		SET LastAttemptedAt = pNow
		WHERE ur.WorkItemId = vWorkItemID
			AND ur.Shard = pShard
			AND ur.MaxShards = pMaxShards;

		UPDATE NQueue.Queue ur
		SET LockedUntil = pNow + interval '1 hour'
		WHERE ur.QueueId = vQueueID
			AND ur.Shard = pShard
			AND ur.MaxShards = pMaxShards;
	END IF;

	return query
	SELECT r.WorkItemId, r.Url, r.Internal
		FROM NQueue.WorkItem r
		WHERE r.WorkItemId = vWorkItemID
			AND r.Shard = pShard
			AnD r.MaxShards = pMaxShards;


end; $$ language plpgsql;
";
	        return await _config.WithDbConnection(async cnn =>
	        {
		        await ExecuteNonQuery(sql, cnn);

		        var rows = ExecuteReader("SELECT * FROM pg_temp.TestingNextWorkItem($1, $2, $3, $4)",
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
			        SqlParameter(queueName),
			        SqlParameter(NowUtc)
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

        
        
        public async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson, string? blockQueueName)
        {
	        
	        queueName ??= Guid.NewGuid().ToString();
            
	        // if we're blocking, the new work item must be on the same shard scheme.
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
                        SqlParameter(blockQueueName)
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
                    SqlParameter(blockQueueName)
                );
                
        }
        
        

        public async ValueTask AcquireExternalLock(string queueName, int maxShards, string externalLockId, DbTransaction? tran, Func<ValueTask> action)
        {
	        if (tran == null)
	        {
		        await _config.WithDbConnection(async cnn =>
		        {
					await using var tran2 = await cnn.BeginTransactionAsync();
					await AcquireExternalLock(queueName, maxShards, externalLockId, tran2, action);
					await tran2.CommitAsync();
		        });
		        
		        return;
	        }
	        
	        
	        var shard = CalculateShard(queueName, maxShards); 
	      
     
	        await ExecuteProcedure(
		        tran,
		        "nqueue.AcquireExternalLock",
		        SqlParameter(queueName),
		        SqlParameter(shard),
		        SqlParameter(maxShards),
		        SqlParameter(externalLockId),
		        SqlParameter(NowUtc)
	        );   
	        

	        await action();
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