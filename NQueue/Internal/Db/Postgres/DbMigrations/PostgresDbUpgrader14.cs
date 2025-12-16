using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.Postgres.DbMigrations;

public class PostgresDbUpgrader14
{

	public async ValueTask Upgrade(DbTransaction tran, bool isCitus)
	{
		var sql = @"

            ALTER TABLE NQueue.Queue             DROP CONSTRAINT IF EXISTS FK_Queue_WorkItem_NextWorkItemId;

            ALTER TABLE NQueue.Queue    	DROP CONSTRAINT AK_Queue_Name;
			DROP INDEX IF EXISTS NQueue.IX_NQueue_Queue_LockedUntil_NextWorkItemId;
			DROP INDEX IF EXISTS NQueue.ix_nqueue_workitem_isingested_queuename;
			DROP INDEX IF EXISTS NQueue.ix_nqueue_workitemcompleted_completedat;

            ALTER TABLE NQueue.Queue                DROP CONSTRAINT PK_Queue;
            ALTER TABLE NQueue.WorkItem             DROP CONSTRAINT PK_WorkItem;
            ALTER TABLE NQueue.WorkItemCompleted    DROP CONSTRAINT PK_WorkItemCompleted;
            ALTER TABLE NQueue.BlockingMessage    	DROP CONSTRAINT PK_BlockingMessage;

			
			ALTER TABLE NQueue.Queue 				ADD COLUMN IF NOT EXISTS MaxShards integer NULL;
			ALTER TABLE NQueue.WorkItem 			ADD COLUMN IF NOT EXISTS MaxShards integer NULL;
			ALTER TABLE NQueue.WorkItemCompleted 	ADD COLUMN IF NOT EXISTS MaxShards integer NULL;
			ALTER TABLE NQueue.BlockingMessage 		ADD COLUMN IF NOT EXISTS QueueMaxShards integer NULL;
			


			--ALTER TABLE NQueue.WorkItem 			ADD COLUMN IF NOT EXISTS BlockingQueueMaxShards integer NULL;
			--ALTER TABLE NQueue.WorkItemCompleted	ADD COLUMN IF NOT EXISTS BlockingQueueMaxShards integer NULL;
		

			UPDATE NQueue.Queue
			SET MaxShards = CASE WHEN %%IsSharded%% THEN 16 ELSE 1 END
			WHERE MaxShards IS NULL;
		
			UPDATE NQueue.WorkItem
			SET MaxShards = CASE WHEN %%IsSharded%% THEN 16 ELSE 1 END
			WHERE MaxShards IS NULL;
		
			UPDATE NQueue.WorkItemCompleted
			SET MaxShards = CASE WHEN %%IsSharded%% THEN 16 ELSE 1 END
			WHERE MaxShards IS NULL;
		
			UPDATE NQueue.BlockingMessage
			SET QueueMaxShards = CASE WHEN %%IsSharded%% THEN 16 ELSE 1 END
			WHERE QueueMaxShards IS NULL;


			--UPDATE NQueue.WorkItem
			--SET BlockingQueueMaxShards = CASE WHEN %%IsSharded%% THEN 16 ELSE 1 END
			--WHERE BlockingQueueMaxShards IS NULL AND BlockingQueueName IS NOT NULL;
		
			--UPDATE NQueue.WorkItemCompleted
			--SET BlockingQueueMaxShards = CASE WHEN %%IsSharded%% THEN 16 ELSE 1 END
			--WHERE BlockingQueueMaxShards IS NULL AND BlockingQueueName IS NOT NULL;

			ALTER TABLE NQueue.Queue 				ALTER COLUMN MaxShards SET NOT NULL;
			ALTER TABLE NQueue.WorkItem 			ALTER COLUMN MaxShards SET NOT NULL;
			ALTER TABLE NQueue.WorkItemCompleted 	ALTER COLUMN MaxShards SET NOT NULL;
			ALTER TABLE NQueue.BlockingMessage 		ALTER COLUMN QueueMaxShards SET NOT NULL;


            ALTER TABLE NQueue.Queue                ADD CONSTRAINT PK_Queue             PRIMARY KEY (Shard, MaxShards, QueueId);
            ALTER TABLE NQueue.WorkItem             ADD CONSTRAINT PK_WorkItem          PRIMARY KEY (Shard, MaxShards, WorkItemId);
            ALTER TABLE NQueue.WorkItemCompleted    ADD CONSTRAINT PK_WorkItemCompleted PRIMARY KEY (Shard, MaxShards, WorkItemId);
            ALTER TABLE NQueue.BlockingMessage    	ADD CONSTRAINT PK_BlockingMessage 	PRIMARY KEY (QueueShard, QueueMaxShards, BlockingMessageId);

		
	           
			ALTER TABLE NQueue.Queue ADD CONSTRAINT AK_Queue_Name UNIQUE (Shard, MaxShards, Name);

	        CREATE UNIQUE INDEX IF NOT EXISTS IX_NQueue_WorkItem_IsIngested_QueueName
	            ON NQueue.WorkItem (Shard, MaxShards, IsIngested, QueueName, WorkItemId)
	            INCLUDE (CreatedAt);

            CREATE UNIQUE INDEX IF NOT EXISTS IX_NQueue_WorkItemCompleted_CompletedAt
                ON NQueue.WorkItemCompleted (Shard, MaxShards, CompletedAt, WorkItemId);

			CREATE UNIQUE INDEX IF NOT EXISTS IX_NQueue_Queue_LockedUntil_NextWorkItemId
			ON NQueue.Queue (
			    Shard,
			    MaxShards,
			    LockedUntil,
			    NextWorkItemId,
			    queueid
			)
			WHERE
			    ErrorCount < 5
			    AND IsPaused = FALSE
			    AND ExternalLockId IS NULL
			    AND cardinality(BlockedBy) = 0;


                ALTER TABLE NQueue.Queue ADD CONSTRAINT FK_Queue_WorkItem_NextWorkItemId 
                    FOREIGN KEY(Shard, MaxShards, NextWorkItemId)
                    REFERENCES NQueue.WorkItem (Shard, MaxShards, WorkItemId);
        ";
		await AbstractWorkItemDb.ExecuteNonQueryForMigration(tran, sql.Replace("%%IsSharded%%", isCitus ? "TRUE" : "FALSE"));

		
		
		
        
        
        sql = @"


DROP PROCEDURE IF EXISTS NQueue.CompleteWorkItem(
	pWorkItemID NQueue.WorkItem.WorkItemId%TYPE,
	pShard integer,
	pNow timestamp with time zone 
);

DROP PROCEDURE IF EXISTS NQueue.CompleteWorkItem(
	pWorkItemID NQueue.WorkItem.WorkItemId%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone 
);

DROP PROCEDURE IF EXISTS NQueue.EnqueueWorkItem (
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName varchar,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE,
	pNow timestamp with time zone,
	pDuplicateProtection boolean,
	pInternal text
);

DROP PROCEDURE IF EXISTS NQueue.EnqueueWorkItem (
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE,
	pNow timestamp with time zone,
	pDuplicateProtection boolean,
	pInternal text
);

DROP PROCEDURE IF EXISTS NQueue.EnqueueWorkItem2 (
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE,
	pNow timestamp with time zone,
	pDuplicateProtection boolean,
	pInternal text
);

DROP PROCEDURE IF EXISTS NQueue.EnqueueWorkItem2 (
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE,
	pNow timestamp with time zone,
	pDuplicateProtection boolean,
	pInternal text,
	pBlockingQueueShard NQueue.Queue.Shard%TYPE,
	pBlockingQueueName NQueue.Queue.Name%TYPE
);

DROP PROCEDURE IF EXISTS NQueue.EnqueueWorkItem (
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE,
	pNow timestamp with time zone,
	pDuplicateProtection boolean,
	pInternal text,
	pBlockingQueueShard NQueue.Queue.Shard%TYPE,
	pBlockingQueueName NQueue.Queue.Name%TYPE
);


DROP PROCEDURE IF EXISTS NQueue.EnqueueWorkItem (
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE,
	pNow timestamp with time zone,
	pDuplicateProtection boolean,
	pInternal text,
	pBlockingQueueShard NQueue.Queue.Shard%TYPE,
	pBlockingQueueName NQueue.Queue.Name%TYPE
);


DROP PROCEDURE IF EXISTS NQueue.FailWorkItem (
	pWorkItemID NQueue.WorkItem.WorkItemID%TYPE,
	pShard integer,
	pNow timestamp with time zone
);

DROP PROCEDURE IF EXISTS NQueue.FailWorkItem (
	pWorkItemID NQueue.WorkItem.WorkItemID%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone
);

DROP FUNCTION IF EXISTS NQueue.NextWorkItem (
	pNow timestamp with time zone
);

DROP FUNCTION IF EXISTS NQueue.NextWorkItem (
	pShard integer,
	pNow timestamp with time zone
);

DROP FUNCTION IF EXISTS NQueue.NextWorkItem (
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone
);

DROP PROCEDURE IF EXISTS NQueue.PurgeWorkItems (
	pShard integer,
	pNow timestamp with time zone
);

DROP PROCEDURE IF EXISTS NQueue.PurgeWorkItems (
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone 
);

DROP PROCEDURE IF EXISTS NQueue.ReplayWorkItem (
	pWorkItemID NQueue.WorkItem.WorkItemID%TYPE,
	pNow timestamp with time zone
);

DROP PROCEDURE IF EXISTS NQueue.ReplayWorkItem (
	pWorkItemID NQueue.WorkItem.WorkItemID%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone 
);

DROP PROCEDURE IF EXISTS NQueue.DelayWorkItem(
	pWorkItemID NQueue.WorkItem.WorkItemId%TYPE,
	pShard integer,
	pNow timestamp with time zone
);

DROP PROCEDURE IF EXISTS NQueue.DelayWorkItem(
	pWorkItemID NQueue.WorkItem.WorkItemId%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone 
);

DROP PROCEDURE IF EXISTS NQueue.PauseQueue(
	pQueueName NQueue.Queue.Name%TYPE,
	pShard NQueue.Queue.Shard%TYPE,
	pNow timestamp with time zone 
);

DROP PROCEDURE IF EXISTS NQueue.PauseQueue(
	pQueueName NQueue.Queue.Name%TYPE,
	pShard NQueue.Queue.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone 
);

DROP PROCEDURE IF EXISTS NQueue.AcquireExternalLock(
	pQueueName NQueue.Queue.Name%TYPE,
	pShard NQueue.Queue.Shard%TYPE,
	pExternalLockId NQueue.Queue.ExternalLockId%TYPE,
	pNow timestamp with time zone 
);

DROP PROCEDURE IF EXISTS NQueue.AcquireExternalLock(
	pQueueName NQueue.Queue.Name%TYPE,
	pShard NQueue.Queue.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pExternalLockId NQueue.Queue.ExternalLockId%TYPE,
	pNow timestamp with time zone
);

DROP PROCEDURE IF EXISTS NQueue.ReleaseExternalLock(
	pQueueName NQueue.Queue.Name%TYPE,
	pShard NQueue.Queue.Shard%TYPE,
	pExternalLockId NQueue.Queue.ExternalLockId%TYPE,
	pNow timestamp with time zone 
);

DROP PROCEDURE IF EXISTS NQueue.ReleaseExternalLock(
	pQueueName NQueue.Queue.Name%TYPE,
	pShard NQueue.Queue.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pExternalLockId NQueue.Queue.ExternalLockId%TYPE,
	pNow timestamp with time zone 
);



-- All the routines:

CREATE OR REPLACE PROCEDURE NQueue.CompleteWorkItem(
	pWorkItemID NQueue.WorkItem.WorkItemId%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare
    vQueueName NQueue.WorkItem.QueueName%TYPE;
	vNextWorkItemID NQueue.WorkItem.WorkItemId%TYPE;
    vNextCreatedAt NQueue.WorkItem.CreatedAt%TYPE;
    vQueueIsPaused NQueue.Queue.IsPaused%TYPE;
    vQueueBlockedBy NQueue.Queue.BlockedBy%TYPE;
    vQueueExternalLockId NQueue.Queue.ExternalLockId%TYPE;



    vWorkItemUrl NQueue.WorkItem.Url%TYPE; 
    vWorkItemDebugInfo NQueue.WorkItem.DebugInfo%TYPE; 
    vWorkItemCreatedAt NQueue.WorkItem.CreatedAt%TYPE; 
    vWorkItemLastAttemptedAt NQueue.WorkItem.LastAttemptedAt%TYPE; 
    vWorkItemQueueName NQueue.WorkItem.QueueName%TYPE; 
	vWorkItemInternal NQueue.WorkItem.Internal%TYPE; 
    vWorkItemBlockingQueueName NQueue.WorkItem.BlockingQueueName%TYPE; 
    vWorkItemBlockingQueueShard NQueue.WorkItem.BlockingQueueShard%TYPE; 
begin


	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	IF pShard >= pMaxShards OR pShard < 0 THEN
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;

    PERFORM pg_advisory_xact_lock(-5839653868952364629 + pShard + 1);


    SELECT wi.QueueName, q.IsPaused, q.BlockedBy, q.ExternalLockId,	          wi.BlockingQueueName, wi.BlockingQueueShard
    INTO   vQueueName, vQueueIsPaused, vQueueBlockedBy, vQueueExternalLockId, vWorkItemBlockingQueueName, vWorkItemBlockingQueueShard
    FROM NQueue.WorkItem wi
	JOIN NQueue.Queue q ON wi.QueueName = q.Name AND wi.Shard = q.Shard
    WHERE wi.WorkItemId = pWorkItemID 
		AND wi.Shard = pShard
		AND wi.MaxShards = pMaxShards;


	IF cardinality(vQueueBlockedBy) != 0 THEN
		RAISE EXCEPTION 'Cannot complete a blocked work item (it should be blocked)';
	END IF;


    IF vQueueName IS NOT NULL THEN


        SELECT WorkItemID,    wi.CreatedAt
        INTO vNextWorkItemID, vNextCreatedAt
        FROM NQueue.WorkItem wi
        WHERE
            wi.QueueName = vQueueName
			AND wi.Shard = pShard
			and wi.MaxShards = pMaxShards
            AND wi.WorkItemId != pWorkItemID
            AND wi.IsIngested = TRUE
        ORDER BY wi.WorkItemId
        LIMIT 1;


        IF vNextWorkItemID IS NULL THEN
			IF vQueueIsPaused = TRUE OR vQueueExternalLockId IS NOT NULL THEN
				WITH wi AS(
				    INSERT INTO NQueue.WorkItem (Url, DebugInfo, CreatedAt, QueueName, IsIngested, Internal, Shard, MaxShards, BlockingQueueShard, BlockingQueueName)
					VALUES ('noop:', NULL, pNow, vQueueName, TRUE, NULL, pShard, pMaxShards, vWorkItemBlockingQueueShard, vWorkItemBlockingQueueName)
				    RETURNING WorkItemID
				)
				UPDATE NQueue.Queue q
		           SET LockedUntil = pNow,
		               NextWorkItemId = (SELECT wi.WorkItemId FROM wi),
		               ErrorCount = 0
		           WHERE q.Name = vQueueName
					AND q.Shard = pShard
					AND q.MaxShards = pMaxShards;

				
				IF vWorkItemBlockingQueueName IS NOT NULL THEN 
					INSERT INTO NQueue.BlockingMessage (QueueShard, QueueMaxShards, QueueName, IsCreatingBlock, BlockingWorkItemId, BlockingShard)
					SELECT vWorkItemBlockingQueueShard, pMaxShards, vWorkItemBlockingQueueName, TRUE, q.NextWorkItemId, q.Shard
					FROM NQueue.Queue q
					WHERE q.Name = vQueueName
					AND q.Shard = pShard
					AND q.MaxShards = pMaxShards;
				END IF;
			ELSE
	            DELETE FROM NQueue.Queue q 
	            WHERE q.Name = vQueueName
					AND q.Shard = pShard
					AND q.MaxShards = pMaxShards;
			END IF;
        ELSE
            UPDATE NQueue.Queue q
            SET LockedUntil = vNextCreatedAt,
                NextWorkItemId = vNextWorkItemID,
                ErrorCount = 0
            WHERE q.Name = vQueueName
				AND q.Shard = pShard
				AND q.MaxShards = pMaxShards;
        END IF;
    END IF;


	DELETE FROM NQueue.WorkItem
    WHERE WorkItemId = pWorkItemID
		AND Shard = pShard
		AND MaxShards = pMaxShards
    RETURNING
        Url,
        DebugInfo,
        CreatedAt,
        LastAttemptedAt,
        QueueName,
        --pNow AS CompletedAt,
		Internal
	INTO 
        vWorkItemUrl,
        vWorkItemDebugInfo,
        vWorkItemCreatedAt,
        vWorkItemLastAttemptedAt,
        vWorkItemQueueName,
		vWorkItemInternal;



    INSERT INTO NQueue.WorkItemCompleted
               (WorkItemId
               ,Url
               ,DebugInfo
               ,CreatedAt
               ,LastAttemptedAt
               ,QueueName
               ,CompletedAt
               ,Internal
			   ,Shard
			   ,MaxShards
			   ,BlockingQueueName
			   ,BlockingQueueShard
			   )
	VALUES (
		pWorkItemID,  
		vWorkItemUrl,
        vWorkItemDebugInfo,
        vWorkItemCreatedAt,
        vWorkItemLastAttemptedAt,
        vWorkItemQueueName,
		pNow,
		vWorkItemInternal,
		pShard,
		pMaxShards,
		vWorkItemBlockingQueueName,
		vWorkItemBlockingQueueShard
	); 


	IF vWorkItemBlockingQueueName IS NOT NULL THEN
		INSERT INTO NQueue.BlockingMessage (QueueShard, QueueMaxShards, QueueName, IsCreatingBlock, BlockingWorkItemId, BlockingShard)
		VALUES (vWorkItemBlockingQueueShard, pMaxShards, vWorkItemBlockingQueueName, FALSE, pWorkItemID, pShard);
	END IF;
	

end; $$;



CREATE OR REPLACE PROCEDURE NQueue.EnqueueWorkItem (
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE default NULL,
	pNow timestamp with time zone default NULL,
	pDuplicateProtection boolean default NULL,
	pInternal text default NULL,
	pBlockingQueueShard NQueue.Queue.Shard%TYPE default NULL,
	pBlockingQueueName NQueue.Queue.Name%TYPE default NULL
)
language plpgsql
as $$
declare
	vDupeWorkItemID NQueue.WorkItem.WorkItemID%TYPE;
	vDupeWorkItemID2 NQueue.WorkItem.WorkItemID%TYPE;
	vDupeLastAttemptedAt NQueue.WorkItem.LastAttemptedAt%TYPE;
	vWorkItemId NQueue.WorkItem.WorkItemID%TYPE;
begin
	
	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	IF pShard >= pMaxShards OR pShard < 0 THEN
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;

	IF pBlockingQueueShard IS NULL AND pBlockingQueueName IS NOT NULL
		OR pBlockingQueueShard IS NOT NULL AND pBlockingQueueName IS NULL THEN
		RAISE EXCEPTION 'Either pBlockingQueueShard and pBlockingQueueName must all be NULL or all NOT NULL';
	END IF;
	IF pBlockingQueueName IS NOT NULL AND pBlockingQueueName = pQueueName THEN
		RAISE EXCEPTION 'pBlockingQueueName must different from pQueueName, otherwise deadlock will occur';
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
				AND wi.Shard = pShard
				AND wi.MaxShards = pMaxShards
		LIMIT 1;

		IF vDupeWorkItemID IS NOT NULL THEN

			SELECT wi.WorkItemID,    LastAttemptedAt
			INTO   vDupeWorkItemID2, vDupeLastAttemptedAt
			FROM NQueue.WorkItem wi -- WITH (REPEATABLEREAD) -- [NQueue].[NextWorkItem] won't be allowed to take this record until the transaction completes
			WHERE wi.WorkItemId = vDupeWorkItemID	
				AND wi.Shard = pShard
				AND wi.MaxShards = pMaxShards
			FOR UPDATE;

			IF vDupeWorkItemID2 IS NOT NULL AND vDupeLastAttemptedAt IS NULL THEN
				RETURN;
			END IF;

		END IF;

	END IF;


	INSERT INTO NQueue.WorkItem (Url, DebugInfo, CreatedAt, QueueName, IsIngested, Internal, Shard, MaxShards, BlockingQueueName, BlockingQueueShard)
	VALUES (pUrl, pDebugInfo, pNow, pQueueName, FALSE, pInternal::jsonb, pShard, pMaxShards, pBlockingQueueName, pBlockingQueueShard)
	RETURNING WorkItemId INTO vWorkItemId;


	IF pBlockingQueueName IS NOT NULL THEN
		INSERT INTO NQueue.BlockingMessage (QueueShard, QueueMaxShards, QueueName, IsCreatingBlock, BlockingWorkItemId, BlockingShard)
		VALUES (pBlockingQueueShard, pMaxShards, pBlockingQueueName, TRUE, vWorkItemId, pShard);
	END IF;
	

END; $$;



CREATE OR REPLACE PROCEDURE NQueue.FailWorkItem (
	pWorkItemID NQueue.WorkItem.WorkItemID%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare

begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	IF pShard >= pMaxShards OR pShard < 0 THEN
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;

	UPDATE NQueue.Queue q
	SET ErrorCount = q.ErrorCount + 1, LockedUntil = pNow + interval '5 minutes'
	FROM NQueue.WorkItem r
	WHERE r.QueueName = q.Name
        AND r.WorkItemId = pWorkItemID
		AND r.Shard = pShard
		AND r.MaxShards = pMaxShards
		AND r.Shard = q.Shard
		AND q.MaxShards = pMaxShards;

end; $$;


CREATE OR REPLACE FUNCTION NQueue.NextWorkItem (
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
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


	SELECT b.BlockingMessageId, b.QueueName, b.IsCreatingBlock, b.BlockingWorkItemId, b.BlockingShard
	INTO   vBlockingMessageId,  vBlockingQueueName, vBlockingIsCreatingBlock, vBlockingWorkItemId, vBlockingShard
	FROM
		NQueue.BlockingMessage b
	WHERE
		b.QueueShard = pShard
		AND b.QueueMaxShards = pMaxShards
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



		DELETE FROM NQueue.BlockingMessage b WHERE b.QueueShard = pShard AND b.QueueMaxShards = pMaxShards AND b.BlockingMessageId = vBlockingMessageId;


		SELECT b.BlockingMessageId, b.QueueName, b.IsCreatingBlock, b.BlockingWorkItemId, b.BlockingShard
		INTO   vBlockingMessageId, vBlockingQueueName, vBlockingIsCreatingBlock, vBlockingWorkItemId, vBlockingShard
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



CREATE OR REPLACE PROCEDURE NQueue.PurgeWorkItems (
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare

begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	IF pShard >= pMaxShards OR pShard < 0 THEN
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;

	DELETE
	FROM NQueue.WorkItemCompleted c
	WHERE c.WorkItemId IN (
	    SELECT i.WorkItemId
	    FROM NQueue.WorkItemCompleted i
	    WHERE i.CompletedAt < pNow - interval '14 days'
			AND i.Shard = pShard
			AND i.MaxShards = pMaxShards
	    LIMIT 1000
    )
	AND c.Shard = pShard
	AND c.MaxShards = pMaxShards;
end; $$;




CREATE OR REPLACE PROCEDURE NQueue.ReplayWorkItem (
	pWorkItemID NQueue.WorkItem.WorkItemID%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare

begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	IF pShard >= pMaxShards OR pShard < 0 THEN	
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;

	INSERT INTO NQueue.WorkItem
           (Url
			,DebugInfo
			,CreatedAt
			,QueueName
			,IsIngested
			,Internal
			,Shard
			,MaxShards)
     SELECT
		   c.Url,
		   c.DebugInfo,
		   pNow,
		   c.QueueName,
		   FALSE,
		   NULL,
		   c.Shard,
		   c.MaxShards
	FROM NQueue.WorkItemCompleted c
	WHERE c.WorkItemId = pWorkItemID
		AND c.Shard = pShard
		AND c.MaxShards = pMaxShards;

end; $$;












CREATE OR REPLACE PROCEDURE NQueue.DelayWorkItem(
	pWorkItemID NQueue.WorkItem.WorkItemId%TYPE,
	pShard NQueue.WorkItem.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
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

	IF pShard >= pMaxShards OR pShard < 0 THEN
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;

    PERFORM pg_advisory_xact_lock(-5839653868952364629 + pShard + 1);

    SELECT wi.QueueName
    INTO   vQueueName
    FROM NQueue.WorkItem wi
    WHERE wi.WorkItemId = pWorkItemID 
		AND wi.Shard = pShard
		AND wi.MaxShards = pMaxShards;


    IF vQueueName IS NOT NULL THEN

	    UPDATE NQueue.Queue q
	    SET LockedUntil = pNow,
			ErrorCount = 0
	    WHERE q.Name = vQueueName
			AND q.Shard = pShard
			AND q.MaxShards = pMaxShards;

    END IF;


end; $$;





CREATE OR REPLACE PROCEDURE NQueue.PauseQueue(
	pQueueName NQueue.Queue.Name%TYPE,
	pShard NQueue.Queue.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare
    vQueueId NQueue.Queue.QueueID%TYPE;
    vWorkItemID NQueue.WorkItem.WorkItemId%TYPE;
begin


	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	IF pShard >= pMaxShards OR pShard < 0 THEN
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;

    PERFORM pg_advisory_xact_lock(-5839653868952364629 + pShard + 1);

    SELECT q.QueueID
    INTO   vQueueId
    FROM NQueue.Queue q
    WHERE q.Name = pQueueName 
		AND q.Shard = pShard
		AND q.MaxShards = pMaxShards;


    IF vQueueId IS NOT NULL THEN
	    UPDATE NQueue.Queue q
	    SET IsPaused = TRUE
	    WHERE q.QueueId = vQueueId
			AND q.Shard = pShard
			AND q.MaxShards = pMaxShards;
	ELSE		
		WITH wi AS (
		    INSERT INTO NQueue.WorkItem (Url, DebugInfo, CreatedAt, QueueName, IsIngested, Internal, Shard, MaxShards)
			VALUES ('noop:', NULL, pNow, pQueueName, TRUE, NULL, pShard, pMaxShards)
		    RETURNING WorkItemID
		)
		INSERT INTO NQueue.Queue (Name, NextWorkItemId, ErrorCount, LockedUntil, Shard, MaxShards, IsPaused, BlockedBy)
		SELECT pQueueName, wi.WorkItemID, 0, pNow, pShard, pMaxShards, TRUE, ARRAY[]::nqueue.block[]
		FROM wi;
    END IF;


end; $$;





CREATE OR REPLACE PROCEDURE NQueue.AcquireExternalLock(
	pQueueName NQueue.Queue.Name%TYPE,
	pShard NQueue.Queue.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pExternalLockId NQueue.Queue.ExternalLockId%TYPE,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare
    vQueueId NQueue.Queue.QueueID%TYPE;
    vWorkItemID NQueue.WorkItem.WorkItemId%TYPE;
	vExternalLockId NQueue.Queue.ExternalLockId%TYPE;
begin

	
	IF pExternalLockId IS NULL THEN
		RAISE EXCEPTION 'pExternalLockId must be NOT NULL';
	END IF;

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	IF pShard >= pMaxShards OR pShard < 0 THEN
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;

    PERFORM pg_advisory_xact_lock(-5839653868952364629 + pShard + 1);

    SELECT q.QueueID, q.ExternalLockId
    INTO   vQueueId, vExternalLockId
    FROM NQueue.Queue q
    WHERE q.Name = pQueueName 
		AND q.Shard = pShard
		AND q.MaxShards = pMaxShards;

	IF vExternalLockId IS NOT NULL THEN
		RAISE EXCEPTION 'Cannot acquire a lock when one is already granted.  An alternate pattern would be to create two child work-items instead, each with their own lock';
	END IF;

    IF vQueueId IS NOT NULL THEN
	    UPDATE NQueue.Queue q
	    SET ExternalLockId = pExternalLockId
	    WHERE q.QueueId = vQueueId
			AND q.Shard = pShard
			AND q.MaxShards = pMaxShards;
	ELSE		
		WITH wi AS (
		    INSERT INTO NQueue.WorkItem (Url, DebugInfo, CreatedAt, QueueName, IsIngested, Internal, Shard, MaxShards)
			VALUES ('noop:', NULL, pNow, pQueueName, TRUE, NULL, pShard, pMaxShards)
		    RETURNING WorkItemID
		)
		INSERT INTO NQueue.Queue (Name, NextWorkItemId, ErrorCount, LockedUntil, Shard, MaxShards, IsPaused, BlockedBy, ExternalLockId)
		SELECT pQueueName, wi.WorkItemID, 0, pNow, pShard, pMaxShards, FALSE, ARRAY[]::nqueue.block[], pExternalLockId
		FROM wi;
    END IF;


end; $$;


CREATE OR REPLACE PROCEDURE NQueue.ReleaseExternalLock(
	pQueueName NQueue.Queue.Name%TYPE,
	pShard NQueue.Queue.Shard%TYPE,
	pMaxShards NQueue.WorkItem.MaxShards%TYPE,
	pExternalLockId NQueue.Queue.ExternalLockId%TYPE,
	pNow timestamp with time zone = NULL
)
language plpgsql
as $$
declare
    vQueueId NQueue.Queue.QueueID%TYPE;
	vExternalLockId NQueue.Queue.ExternalLockId%TYPE;
begin

	
	IF pExternalLockId IS NULL THEN
		RAISE EXCEPTION 'pExternalLockId must be NOT NULL';
	END IF;

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	IF pShard >= pMaxShards OR pShard < 0 THEN
		RAISE EXCEPTION 'pShard must be between 0 and pMaxShards - 1, inclusive';
	END IF;


    PERFORM pg_advisory_xact_lock(-5839653868952364629 + pShard + 1);

    SELECT q.QueueID, q.ExternalLockId
    INTO   vQueueId, vExternalLockId
    FROM NQueue.Queue q
    WHERE q.Name = pQueueName 
		AND q.Shard = pShard
		AND q.MaxShards = pMaxShards;


    IF vQueueId IS NOT NULL THEN
		IF vExternalLockId IS NULL OR vExternalLockId != pExternalLockId THEN
			RAISE EXCEPTION 'ExternalLockId does not match';
		END IF;

	    UPDATE NQueue.Queue q
	    SET ExternalLockId = NULL
	    WHERE q.QueueId = vQueueId
			AND q.Shard = pShard
			AND q.MaxShards = pMaxShards;
    END IF;


end; $$;

        ";
        
        
        await AbstractWorkItemDb.ExecuteNonQueryForMigration(tran, sql.Replace("%%IsSharded%%", isCitus ? "TRUE" : "FALSE"));
        


	}
    
}