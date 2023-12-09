using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.Postgres.DbMigrations;

internal class PostgresDbUpgrader03
{
    public async ValueTask Upgrade(DbTransaction tran, bool isCitus)
    {
         var sql = @"

ALTER TABLE NQueue.Queue DROP CONSTRAINT IF EXISTS FK_Queue_WorkItem_NextWorkItemId;

ALTER TABLE NQueue.Queue DROP CONSTRAINT IF EXISTS PK_Queue;
ALTER TABLE NQueue.WorkItem DROP CONSTRAINT IF EXISTS PK_WorkItem;
ALTER TABLE NQueue.WorkItemCompleted DROP CONSTRAINT IF EXISTS PK_WorkItemCompleted;
ALTER TABLE NQueue.CronJob DROP CONSTRAINT IF EXISTS PK_CronJob;

ALTER TABLE NQueue.Queue DROP CONSTRAINT IF EXISTS AK_Queue_Name;
ALTER TABLE NQueue.CronJob DROP CONSTRAINT IF EXISTS AK_CronJob_CronJobName;

DROP INDEX IF EXISTS NQueue.IX_NQueue_Queue_LockedUntil_NextWorkItemId;
DROP INDEX IF EXISTS NQueue.IX_NQueue_WorkItem_IsIngested_QueueName;
DROP INDEX IF EXISTS NQueue.IX_NQueue_WorkItemCompleted_IsIngested_QueueName;





ALTER TABLE NQueue.WorkItem ADD COLUMN IF NOT EXISTS Shard integer NULL;
ALTER TABLE NQueue.WorkItemCompleted ADD COLUMN IF NOT EXISTS Shard integer NULL;
ALTER TABLE NQueue.Queue ADD COLUMN IF NOT EXISTS Shard integer NULL;
ALTER TABLE NQueue.CronJob DROP COLUMN IF EXISTS CronJobId;

UPDATE NQueue.WorkItem
SET Shard = 0
WHERE Shard IS NULL;

UPDATE NQueue.WorkItemCompleted
SET Shard = 0
WHERE Shard IS NULL;

UPDATE NQueue.Queue
SET Shard = 0
WHERE Shard IS NULL;

ALTER TABLE NQueue.WorkItem ALTER COLUMN Shard SET NOT NULL;
ALTER TABLE NQueue.WorkItemCompleted ALTER COLUMN Shard SET NOT NULL;
ALTER TABLE NQueue.Queue ALTER COLUMN Shard SET NOT NULL;

ALTER TABLE NQueue.Queue ADD CONSTRAINT PK_Queue PRIMARY KEY (Shard, QueueId);
ALTER TABLE NQueue.WorkItem ADD CONSTRAINT PK_WorkItem PRIMARY KEY (Shard, WorkItemId);
ALTER TABLE NQueue.WorkItemCompleted ADD CONSTRAINT PK_WorkItemCompleted PRIMARY KEY (Shard, WorkItemId);
ALTER TABLE NQueue.CronJob ADD CONSTRAINT PK_CronJob PRIMARY KEY (CronJobName);

ALTER TABLE NQueue.Queue ADD CONSTRAINT AK_Queue_Name UNIQUE (Shard, Name);


CREATE UNIQUE INDEX IF NOT EXISTS IX_NQueue_Queue_LockedUntil_NextWorkItemId
ON NQueue.Queue (Shard,LockedUntil,NextWorkItemId,QueueId)
INCLUDE (ErrorCount);

CREATE UNIQUE INDEX IF NOT EXISTS IX_NQueue_WorkItem_IsIngested_QueueName
ON NQueue.WorkItem (Shard, IsIngested, QueueName, WorkItemId)
INCLUDE (CreatedAt);

CREATE UNIQUE INDEX IF NOT EXISTS IX_NQueue_WorkItemCompleted_IsIngested_QueueName
ON NQueue.WorkItemCompleted (Shard, CompletedAt);

ALTER TABLE NQueue.Queue ADD CONSTRAINT FK_Queue_WorkItem_NextWorkItemId FOREIGN KEY(Shard, NextWorkItemId)
REFERENCES NQueue.WorkItem (Shard, WorkItemId);




DROP PROCEDURE IF EXISTS NQueue.CompleteWorkItem(
	pWorkItemID NQueue.WorkItem.WorkItemId%TYPE,
	pNow timestamp with time zone
);

CREATE OR REPLACE PROCEDURE NQueue.CompleteWorkItem(
	pWorkItemID NQueue.WorkItem.WorkItemId%TYPE,
	pShard integer,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare
    vQueueName NQueue.WorkItem.QueueName%TYPE;
	vNextWorkItemID NQueue.WorkItem.WorkItemId%TYPE;
    vNextCreatedAt NQueue.WorkItem.CreatedAt%TYPE;
begin


	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;


    PERFORM pg_advisory_xact_lock(-5839653868952364629 + pShard + 1);


    SELECT wi.QueueName
    INTO   vQueueName
    FROM NQueue.WorkItem wi
    WHERE wi.WorkItemId = pWorkItemID 
		AND wi.Shard = pShard;


    IF vQueueName IS NOT NULL THEN


        SELECT WorkItemID,    wi.CreatedAt
        INTO vNextWorkItemID, vNextCreatedAt
        FROM NQueue.WorkItem wi
        WHERE
            wi.QueueName = vQueueName
			AND wi.Shard = pShard
            AND wi.WorkItemId != pWorkItemID
            AND wi.IsIngested = TRUE
        ORDER BY wi.WorkItemId
        LIMIT 1;


        IF vNextWorkItemID IS NULL THEN
            DELETE FROM NQueue.Queue q
            WHERE q.Name = vQueueName
				AND q.Shard = pShard;
        ELSE
            UPDATE NQueue.Queue q
            SET LockedUntil = vNextCreatedAt,
                NextWorkItemId = vNextWorkItemID,
                ErrorCount = 0
            WHERE q.Name = vQueueName
				AND q.Shard = pShard;
        END IF;
    END IF;


    WITH del_cte AS (
        DELETE FROM NQueue.WorkItem
        WHERE WorkItemId = pWorkItemID
			AND Shard = pShard
        RETURNING
            WorkItemId,
            Url,
            DebugInfo,
            CreatedAt,
            LastAttemptedAt,
            QueueName,
            pNow AS CompletedAt,
			Internal,
			Shard
    )
    INSERT INTO NQueue.WorkItemCompleted
               (WorkItemId
               ,Url
               ,DebugInfo
               ,CreatedAt
               ,LastAttemptedAt
               ,QueueName
               ,CompletedAt
               ,Internal
			   ,Shard)
    SELECT * FROM del_cte a;

end; $$;


DROP PROCEDURE IF EXISTS NQueue.EnqueueWorkItem(
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE,
	pNow timestamp with time zone,
	pDuplicateProtection boolean,
	pInternal text
);

CREATE OR REPLACE PROCEDURE NQueue.EnqueueWorkItem (
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE default NULL,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE default NULL,
	pNow timestamp with time zone default NULL,
	pDuplicateProtection boolean default NULL,
	pInternal text default NULL
)
language plpgsql
as $$
declare
	vDupeWorkItemID NQueue.WorkItem.WorkItemID%TYPE;
	vDupeWorkItemID2 NQueue.WorkItem.WorkItemID%TYPE;
	vDupeLastAttemptedAt NQueue.WorkItem.LastAttemptedAt%TYPE;
	vIsSharded boolean;
	vShard NQueue.WorkItem.Shard%TYPE;
begin
	vIsSharded := %%IsSharded%%;
	
	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	-- DECLARE @Now datetimeoffset(7) = sysdatetimeoffset() AT TIME ZONE 'GMT Standard Time';
	-- select * from sys.time_zone_info

	IF pQueueName IS NULL THEN
		pQueueName := gen_random_uuid()::text;
	END IF;

	vShard = 0;
	IF vIsSharded = TRUE THEN
		-- this makes for 16 shards (0 - 15):
		vShard := ('x'||substr(md5(pQueueName),1,1))::bit(4)::int;
	END IF;

	IF pDuplicateProtection IS NULL THEN
		pDuplicateProtection := FALSE;
	END IF;

	IF pDuplicateProtection = TRUE THEN

		SELECT wi.WorkItemId
		INTO   vDupeWorkItemID
		FROM NQueue.WorkItem wi
		WHERE
				wi.LastAttemptedAt IS NULL
				AND wi.QueueName = pQueueName
				AND wi.Url = pUrl
				AND wi.Shard = vShard
		LIMIT 1;

		IF vDupeWorkItemID IS NOT NULL THEN

			SELECT wi.WorkItemID,    LastAttemptedAt
			INTO   vDupeWorkItemID2, vDupeLastAttemptedAt
			FROM NQueue.WorkItem wi -- WITH (REPEATABLEREAD) -- [NQueue].[NextWorkItem] won't be allowed to take this record until the transaction completes
			WHERE wi.WorkItemId = vDupeWorkItemID	
				AND wi.Shard = vShard
			FOR UPDATE;

			IF vDupeWorkItemID2 IS NOT NULL AND vDupeLastAttemptedAt IS NULL THEN
				RETURN;
			END IF;

		END IF;

	END IF;


	INSERT INTO NQueue.WorkItem (Url, DebugInfo, CreatedAt, QueueName, IsIngested, Internal, Shard)
	VALUES (pUrl, pDebugInfo, pNow, pQueueName, FALSE, pInternal::jsonb, vShard);

END; $$;





DROP PROCEDURE IF EXISTS NQueue.FailWorkItem (
	pWorkItemID NQueue.WorkItem.WorkItemID%TYPE,
	pNow timestamp with time zone
);

CREATE OR REPLACE PROCEDURE NQueue.FailWorkItem (
	pWorkItemID NQueue.WorkItem.WorkItemID%TYPE,
	pShard integer,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare

begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	UPDATE NQueue.Queue q
	SET ErrorCount = q.ErrorCount + 1, LockedUntil = pNow + interval '5 minutes'
	FROM NQueue.WorkItem r
	WHERE r.QueueName = q.Name
        AND r.WorkItemId = pWorkItemID
		AND r.Shard = pShard
		AND r.Shard = q.Shard;

end; $$;


DROP PROCEDURE IF EXISTS NQueue.NextWorkItem;

CREATE OR REPLACE FUNCTION NQueue.NextWorkItem (
	pShard integer,
	pNow timestamp with time zone = NULL
) RETURNS TABLE(WorkItemId NQueue.WorkItem.WorkItemID%TYPE, Url NQueue.WorkItem.Url%TYPE, Internal NQueue.WorkItem.Internal%TYPE)
as $$
declare
	vQueueID NQueue.Queue.QueueID%TYPE;
	vWorkItemID NQueue.WorkItem.WorkItemID%TYPE;

begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	-- SET NOCOUNT ON;


    PERFORM pg_advisory_xact_lock(-5839653868952364629 + pShard + 1);

	-- import work items

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
	INSERT INTO NQueue.Queue (Name, NextWorkItemId, ErrorCount, LockedUntil, Shard)
	SELECT cte.QueueName, cte.WorkItemId, 0, cte.CreatedAt, pShard
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



	-- take work item


	SELECT q.QueueId, q.NextWorkItemId
	INTO   vQueueID,  vWorkItemID
	FROM
		NQueue.Queue q
	WHERE
		q.LockedUntil < pNow
		AND q.ErrorCount < 5
		AND q.Shard = pShard
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




DROP PROCEDURE IF EXISTS NQueue.PurgeWorkItems;
CREATE OR REPLACE PROCEDURE NQueue.PurgeWorkItems (
	pShard integer,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare

begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	DELETE
	FROM NQueue.WorkItemCompleted c
	WHERE c.WorkItemId IN (
	    SELECT i.WorkItemId
	    FROM NQueue.WorkItemCompleted i
	    WHERE i.CompletedAt < pNow - interval '14 days'
			AND i.Shard = pShard
	    LIMIT 1000
    )
	AND c.Shard = pShard;
end; $$;




CREATE OR REPLACE PROCEDURE NQueue.ReplayWorkItem (
	pWorkItemID NQueue.WorkItem.WorkItemID%TYPE,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare

begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	INSERT INTO NQueue.WorkItem
           (Url
			,DebugInfo
			,CreatedAt
			,QueueName
			,IsIngested
			,Internal
			,Shard)
     SELECT
		   c.Url,
		   c.DebugInfo,
		   pNow,
		   c.QueueName,
		   FALSE,
		   NULL,
		   c.Shard
	FROM NQueue.WorkItemCompleted c
	WHERE c.WorkItemId = pWorkItemID;

end; $$

        ";
        
        
        await AbstractWorkItemDb.ExecuteNonQuery(tran, sql.Replace("%%IsSharded%%", isCitus ? "TRUE" : "FALSE"));

     
	    if (isCitus)
	    {
		    var distributeSql = @"
SELECT create_distributed_table('nqueue.workitem', 'shard');
SELECT create_distributed_table('nqueue.queue', 'shard', colocate_with => 'nqueue.workitem');
SELECT create_distributed_table('nqueue.workitemcompleted', 'shard', colocate_with => 'nqueue.workitem');
SELECT create_distributed_table('nqueue.cronjob', 'cronjobname');
";
		    await AbstractWorkItemDb.ExecuteNonQuery(tran, distributeSql);
	    }
    }
}