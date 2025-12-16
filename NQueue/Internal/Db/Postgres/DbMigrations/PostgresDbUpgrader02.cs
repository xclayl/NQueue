using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.Postgres.DbMigrations;

internal class PostgresDbUpgrader02
{
    public async ValueTask Upgrade(DbTransaction tran)
    { 
        var sql = @"
ALTER TABLE NQueue.WorkItem ADD COLUMN IF NOT EXISTS Internal JSONB;
ALTER TABLE NQueue.WorkItemCompleted ADD COLUMN IF NOT EXISTS Internal JSONB;






CREATE OR REPLACE PROCEDURE NQueue.CompleteWorkItem(
	pWorkItemID NQueue.WorkItem.WorkItemId%TYPE,
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


    PERFORM pg_advisory_xact_lock(-5839653868952364629);


    SELECT wi.QueueName
    INTO   vQueueName
    FROM NQueue.WorkItem wi
    WHERE wi.WorkItemId = pWorkItemID;


    IF vQueueName IS NOT NULL THEN


        SELECT WorkItemID,    wi.CreatedAt
        INTO vNextWorkItemID, vNextCreatedAt
        FROM NQueue.WorkItem wi
        WHERE
            wi.QueueName = vQueueName
            AND wi.WorkItemId != pWorkItemID
            AND wi.IsIngested = TRUE
        ORDER BY wi.WorkItemId
        LIMIT 1;


        IF vNextWorkItemID IS NULL THEN
            DELETE FROM NQueue.Queue q
            WHERE q.Name = vQueueName;
        ELSE
            UPDATE NQueue.Queue q
            SET LockedUntil = vNextCreatedAt,
                NextWorkItemId = vNextWorkItemID,
                ErrorCount = 0
            WHERE q.Name = vQueueName;
        END IF;
    END IF;


    WITH del_cte AS (
        DELETE FROM NQueue.WorkItem
        WHERE WorkItemId = pWorkItemID
        RETURNING
            WorkItemId,
            Url,
            DebugInfo,
            CreatedAt,
            LastAttemptedAt,
            QueueName,
            pNow AS CompletedAt,
			Internal
    )
    INSERT INTO NQueue.WorkItemCompleted
               (WorkItemId
               ,Url
               ,DebugInfo
               ,CreatedAt
               ,LastAttemptedAt
               ,QueueName
               ,CompletedAt
               ,Internal)
    SELECT * FROM del_cte a;

end; $$;


DROP PROCEDURE IF EXISTS NQueue.EnqueueWorkItem(
	pUrl NQueue.WorkItem.Url%TYPE,
	pQueueName NQueue.WorkItem.QueueName%TYPE,
	pDebugInfo NQueue.WorkItem.DebugInfo%TYPE,
	pNow timestamp with time zone,
	pDuplicateProtection boolean);

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
begin
	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	-- DECLARE @Now datetimeoffset(7) = sysdatetimeoffset() AT TIME ZONE 'GMT Standard Time';
	-- select * from sys.time_zone_info

	IF pQueueName IS NULL THEN
		pQueueName := gen_random_uuid()::text;
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
		LIMIT 1;

		IF vDupeWorkItemID IS NOT NULL THEN

			SELECT wi.WorkItemID,    LastAttemptedAt
			INTO   vDupeWorkItemID2, vDupeLastAttemptedAt
			FROM NQueue.WorkItem wi -- WITH (REPEATABLEREAD) -- [NQueue].[NextWorkItem] won't be allowed to take this record until the transaction completes
			WHERE wi.WorkItemId = vDupeWorkItemID
			FOR UPDATE;

			IF vDupeWorkItemID2 IS NOT NULL AND vDupeLastAttemptedAt IS NULL THEN
				RETURN;
			END IF;

		END IF;

	END IF;


	INSERT INTO NQueue.WorkItem (Url, DebugInfo, CreatedAt, QueueName, IsIngested, Internal)
	VALUES (pUrl, pDebugInfo, pNow, pQueueName, FALSE, pInternal::jsonb);

END; $$;






CREATE OR REPLACE PROCEDURE NQueue.FailWorkItem (
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

	UPDATE NQueue.Queue q
	SET ErrorCount = q.ErrorCount + 1, LockedUntil = pNow + interval '5 minutes'
	FROM NQueue.WorkItem r
	WHERE r.QueueName = q.Name
        AND r.WorkItemId = pWorkItemID;

end; $$;


DROP FUNCTION IF EXISTS NQueue.NextWorkItem;


CREATE OR REPLACE FUNCTION NQueue.NextWorkItem (
	pNow timestamp with time zone = NULL
) RETURNS TABLE(WorkItemId NQueue.WorkItem.WorkItemID%TYPE, Url NQueue.WorkItem.Url%TYPE, Internal NQueue.WorkItem.Internal%TYPE)
as $$
declare
	vQueueID NQueue.WorkItem.WorkItemID%TYPE;
	vWorkItemID NQueue.Queue.QueueID%TYPE;

begin

	IF pNow IS NULL THEN
		pNow := CURRENT_TIMESTAMP;
	END IF;

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	-- SET NOCOUNT ON;


    PERFORM pg_advisory_xact_lock(-5839653868952364629);

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
	)
	INSERT INTO NQueue.Queue (Name, NextWorkItemId, ErrorCount, LockedUntil)
	SELECT cte.QueueName, cte.WorkItemId, 0, cte.CreatedAt
	FROM cte
	WHERE RN = 1
	ON CONFLICT (Name) DO NOTHING;


	UPDATE NQueue.WorkItem wi
	SET IsIngested = TRUE
	FROM
		NQueue.Queue q
	WHERE wi.QueueName = q.Name
		AND wi.IsIngested = FALSE;



	-- take work item


	SELECT q.QueueId, q.NextWorkItemId
	INTO   vQueueID,  vWorkItemID
	FROM
		NQueue.Queue q
	WHERE
		q.LockedUntil < pNow
		AND q.ErrorCount < 5
	ORDER BY
		q.LockedUntil, q.NextWorkItemId
	LIMIT 1;



	IF vWorkItemID IS NOT NULL THEN
		UPDATE NQueue.WorkItem ur
		SET LastAttemptedAt = pNow
		WHERE ur.WorkItemId = vWorkItemID;

		UPDATE NQueue.Queue ur
		SET LockedUntil = pNow + interval '1 hour'
		WHERE ur.QueueId = vQueueID;
	END IF;

	return query
	SELECT r.WorkItemId, r.Url, r.Internal
		FROM NQueue.WorkItem r
		WHERE r.WorkItemId = vWorkItemID;


end; $$ language plpgsql;




CREATE OR REPLACE PROCEDURE NQueue.PurgeWorkItems (
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
	    LIMIT 1000
    );
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
			,Internal)
     SELECT
		   c.Url,
		   c.DebugInfo,
		   pNow,
		   c.QueueName,
		   FALSE,
		   NULL
	FROM NQueue.WorkItemCompleted c
	WHERE c.WorkItemId = pWorkItemID;

end; $$

        ";
        
        
        await AbstractWorkItemDb.ExecuteNonQueryForMigration(tran, sql);
    }
    
    
    
}