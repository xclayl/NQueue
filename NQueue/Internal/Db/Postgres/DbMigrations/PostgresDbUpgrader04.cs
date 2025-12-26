using System.Data.Common;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.Postgres.DbMigrations;

internal class PostgresDbUpgrader04
{
    public async ValueTask Upgrade(DbTransaction tran, bool isCitus)
    {
        var sql = @"

DROP INDEX IF EXISTS NQueue.IX_NQueue_WorkItemCompleted_IsIngested_QueueName;


CREATE UNIQUE INDEX IF NOT EXISTS IX_NQueue_WorkItemCompleted_CompletedAt
ON NQueue.WorkItemCompleted (Shard, CompletedAt, WorkItemId);


        ";

        
        
        await AbstractWorkItemDb.ExecuteNonQueryForMigration(tran, sql.Replace("%%IsSharded%%", isCitus ? "TRUE" : "FALSE"));
        
    }
}