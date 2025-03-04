using System;
using System.Data.Common;
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
        
        
        public async ValueTask<WorkItemInfo?> NextWorkItem(string queueName, int shard)
        {

	        var sql = @"
CREATE FUNCTION pg_temp.TestingNextWorkItem (
	pShard NQueue.WorkItem.Shard%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
    pNow timestamp with time zone = NULL
) RETURNS TABLE(WorkItemId NQueue.WorkItem.WorkItemID%TYPE, Url NQueue.WorkItem.Url%TYPE, Internal NQueue.WorkItem.Internal%TYPE)
as $$
declare
	vQueueID NQueue.Queue.QueueID%TYPE;
	vWorkItemID NQueue.WorkItem.WorkItemID%TYPE;
	vBlockingMessageID NQueue.BlockingMessage.BlockingMessageID%TYPE;
	vBlockingQueueName NQueue.BlockingMessage.QueueName%TYPE;
	vBlockingIsCreatingBlock NQueue.BlockingMessage.IsCreatingBlock%TYPE;
	vBlockingWorkItemId NQueue.BlockingMessage.BlockingWorkItemId%TYPE;
	vBlockingShard NQueue.BlockingMessage.BlockingShard%TYPE;

begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
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
	)
	INSERT INTO NQueue.Queue (Name, NextWorkItemId, ErrorCount, LockedUntil, Shard, IsPaused, BlockedBy, ExternalLockId)
	SELECT cte.QueueName, cte.WorkItemId, 0, cte.CreatedAt, pShard, FALSE, ARRAY[]::nqueue.block[], NULL
	FROM cte
	WHERE RN = 1
	ON CONFLICT (Shard, Name) DO NOTHING;

	UPDATE NQueue.WorkItem wi
	SET IsIngested = TRUE
	FROM
		NQueue.Queue q
	WHERE wi.QueueName = q.Name
		AND wi.IsIngested = FALSE
		AND wi.Shard = pShard
		AND wi.Shard = q.Shard;


	--------- process blocking messages ---------


	SELECT b.BlockingMessageId, b.QueueName, b.IsCreatingBlock, b.BlockingWorkItemId, b.BlockingShard
	INTO   vBlockingMessageId,  vBlockingQueueName, vBlockingIsCreatingBlock, vBlockingWorkItemId, vBlockingShard
	FROM
		NQueue.BlockingMessage b
	WHERE
		b.QueueShard = pShard
	ORDER BY
		b.BlockingMessageId
	LIMIT 1;


	WHILE vBlockingMessageId IS NOT NULL LOOP
		
		IF vBlockingWorkItemId IS NULL OR vBlockingShard IS NULL THEN
			RAISE EXCEPTION 'That''s weird. vBlockingWorkItemId and vBlockingShard shouldn''t be NULL here.';
		END IF;

		IF vBlockingIsCreatingBlock THEN
			UPDATE NQueue.Queue q
			SET BlockedBy = BlockedBy || (vBlockingWorkItemId, vBlockingShard)::nqueue.block
			WHERE q.Shard = pShard 
				AND q.Name = vBlockingQueueName;
		ELSE
			UPDATE NQueue.Queue q
			SET BlockedBy = array(
                SELECT ((elem).WorkItemId, (elem).Shard)::nqueue.block
                FROM unnest(q.BlockedBy) AS elem
                WHERE NOT ((elem).WorkItemId = vBlockingWorkItemId AND (elem).Shard = vBlockingShard)
			)
			WHERE q.Shard = pShard 
				AND q.Name = vBlockingQueueName;
		END IF; 		



		DELETE FROM NQueue.BlockingMessage b WHERE b.QueueShard = pShard AND b.BlockingMessageId = vBlockingMessageId;


		SELECT b.BlockingMessageId, b.QueueName, b.IsCreatingBlock, b.BlockingWorkItemId, b.BlockingShard
		INTO   vBlockingMessageId,  vBlockingQueueName, vBlockingIsCreatingBlock, vBlockingWorkItemId, vBlockingShard
		FROM
			NQueue.BlockingMessage b
		WHERE
			b.QueueShard = pShard
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
			AND ur.Shard = pShard;

		UPDATE NQueue.Queue ur
		SET LockedUntil = pNow + interval '1 hour'
		WHERE ur.QueueId = vQueueID
			AND ur.Shard = pShard;
	END IF;

	return query
	SELECT r.WorkItemId, r.Url, r.Internal
		FROM NQueue.WorkItem r
		WHERE r.WorkItemId = vWorkItemID
			AND r.Shard = pShard;



end; $$ language plpgsql;
";
	        return await _config.WithDbConnection(async cnn =>
	        {
		        await ExecuteNonQuery(sql, cnn);

		        var rows = ExecuteReader("SELECT * FROM pg_temp.TestingNextWorkItem($1, $2, $3)",
			        cnn,
			        reader => new WorkItemInfo(
				        reader.GetInt64(reader.GetOrdinal("WorkItemId")),
				        reader.GetString(reader.GetOrdinal("Url")),
				        !reader.IsDBNull(reader.GetOrdinal("Internal"))
					        ? reader.GetString(reader.GetOrdinal("Internal"))
					        : null
			        ),
			        SqlParameter(shard),
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
                    SqlParameter(NowUtc)
                );
            });
        }

        
        
        public async ValueTask EnqueueWorkItem(DbTransaction? tran, Uri url, string? queueName, string? debugInfo, bool duplicateProtection, string? internalJson, string? blockQueueName)
        {
	        
	        queueName ??= Guid.NewGuid().ToString();
            
	        var shard = CalculateShard(queueName);

	        
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
                        SqlParameter(debugInfo),
                        SqlParameter(NowUtc),
                        SqlParameter(duplicateProtection),
                        SqlParameter(internalJson), 
                        SqlParameter(blockQueueName != null ? CalculateShard(blockQueueName) : null),
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
                    SqlParameter(debugInfo),
                    SqlParameter(NowUtc),
                    SqlParameter(duplicateProtection),
                    SqlParameter(internalJson), 
                    SqlParameter(blockQueueName != null ? CalculateShard(blockQueueName) : null),
                    SqlParameter(blockQueueName)
                );
                
        }
        
        

        public async ValueTask AcquireExternalLock(string queueName, string externalLockId)
        {
	        var shard = CalculateShard(queueName);
	        
	        await _config.WithDbConnection(async cnn =>
	        {
		        await ExecuteProcedure(
			        "nqueue.AcquireExternalLock",
			        cnn,
			        SqlParameter(queueName),
			        SqlParameter(shard),
			        SqlParameter(externalLockId),
			        SqlParameter(NowUtc)
		        );
	        });
        }

        public async ValueTask ReleaseExternalLock(string queueName, string externalLockId)
        {
	        var shard = CalculateShard(queueName);
	        
	        await _config.WithDbConnection(async cnn =>
	        {
		        await ExecuteProcedure(
			        "nqueue.ReleaseExternalLock",
			        cnn,
			        SqlParameter(queueName),
			        SqlParameter(shard),
			        SqlParameter(externalLockId),
			        SqlParameter(NowUtc)
		        );
	        });
        }

        
        
        
        private int CalculateShard(string queueName)
        {
	        if (!IsCitus)
		        return 0;
            
	        using var md5 = MD5.Create();
	        var bytes = md5.ComputeHash(Encoding.UTF8.GetBytes(queueName));

	        var shard = bytes[0] >> 4 & 15;

	        return shard;
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