using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.Postgres.DbMigrations;

public class PostgresDbUpgrader05
{
    public async ValueTask Upgrade(DbTransaction tran, bool isCitus)
    {
        var sql = @"


CREATE OR REPLACE PROCEDURE NQueue.DelayWorkItem(
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

	    UPDATE NQueue.Queue q
	    SET LockedUntil = pNow
	    WHERE q.Name = vQueueName
			AND q.Shard = pShard;

    END IF;


end; $$;

        ";

        
        
        await AbstractWorkItemDb.ExecuteNonQueryForMigration(tran, sql.Replace("%%IsSharded%%", isCitus ? "TRUE" : "FALSE"));
        
    }
}