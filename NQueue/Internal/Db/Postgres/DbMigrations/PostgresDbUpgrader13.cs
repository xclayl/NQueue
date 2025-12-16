using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.Postgres.DbMigrations;

public class PostgresDbUpgrader13
{
	public async ValueTask Upgrade(DbTransaction tran, bool isCitus)
	{
		var sql = @"

			DROP INDEX IF EXISTS NQueue.IX_NQueue_Queue_LockedUntil_NextWorkItemId;
			
			CREATE UNIQUE INDEX IF NOT EXISTS IX_NQueue_Queue_LockedUntil_NextWorkItemId
			ON NQueue.Queue (
			    Shard,
			    LockedUntil,
			    NextWorkItemId,
			    queueid
			)
			WHERE
			    ErrorCount < 5
			    AND IsPaused = FALSE
			    AND ExternalLockId IS NULL
			    AND cardinality(BlockedBy) = 0;
        ";
		await AbstractWorkItemDb.ExecuteNonQueryForMigration(tran, sql.Replace("%%IsSharded%%", isCitus ? "TRUE" : "FALSE"));



	}
}