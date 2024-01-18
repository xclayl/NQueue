using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.Postgres.DbMigrations
{
    internal class PostgresDbMigrator
    {
        
        
        public static async ValueTask UpdateDbSchema(DbConnection conn, bool isCitus)
        {
            
            await using var tran = await conn.BeginTransactionAsync();
            await AbstractWorkItemDb.ExecuteNonQuery(tran, @"do $$ BEGIN 
PERFORM pg_advisory_xact_lock(-5839653868952364629);
end; $$");
            
            
            var currentVersion = 0;

            while (currentVersion != 4)
            {
                
                var dbObjects = await AbstractWorkItemDb.ExecuteReader(
                    tran,
                    @"                   
                    select 'schema' AS type, s.SCHEMA_NAME::text AS name from INFORMATION_SCHEMA.SCHEMATA s where s.SCHEMA_NAME = 'nqueue'
                    UNION
                    select 'table' AS type, t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = 'nqueue'
                    UNION
                    select 'routine' AS type, r.ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES r WHERE r.ROUTINE_SCHEMA = 'nqueue'
                    UNION
                    select 'column' AS type,  c.table_name::text || '.' || c.column_name::text AS column_name FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = 'nqueue'
                    UNION
                    select 'index' AS type, i.tablename::text || '.' || i.indexname::text || ': ' || i.indexdef FROM pg_indexes i WHERE i.schemaname = 'nqueue';
                    ", 
                        reader => new PostgresSchemaInfo(
                        reader.GetString(0),
                        reader.GetString(1)
                    )
                ).ToListAsync();


                if (dbObjects.Count == 0 || dbObjects.Count == 1 && dbObjects.Single().Type == "schema" && dbObjects.Single().Name == "nqueue")
                {
                    // version "0" DB. Upgrade to version 1

                    currentVersion = 0;
                }
                else if (PostgresSchemaInfo.IsVersion04(dbObjects))
                {
                    currentVersion = 4;
                }
                else if (PostgresSchemaInfo.IsVersion03(dbObjects))
                {
                    currentVersion = 3;
                }
                else if (PostgresSchemaInfo.IsVersion02(dbObjects))
                {
                    currentVersion = 2;
                }
                else if (PostgresSchemaInfo.IsVersion01(dbObjects))
                {
                    currentVersion = 1;
                }
                else
                {
                    throw new Exception("The NQueue schema has an unknown structure.  One option is to move all tables & stored procedures to another schema so that NQueue can recreate them from scratch.");
                }


                if (currentVersion == 0)
                {
                    await new PostgresDbUpgrader01().Upgrade(tran);
                }
                if (currentVersion == 1)
                {
                    await new PostgresDbUpgrader02().Upgrade(tran);
                }
                if (currentVersion == 2)
                {
                    await new PostgresDbUpgrader03().Upgrade(tran, isCitus);
                }
                if (currentVersion == 3)
                {
                    await new PostgresDbUpgrader04().Upgrade(tran, isCitus);
                }
            }

            await tran.CommitAsync();
            
        }
    }
}