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

            var attempts = 0;
            var highestVersionDetected = 0;

            while (currentVersion != 7)
            {
                attempts++;
                if (attempts > 100)
                    throw new Exception($"Unable to migrate the nqueue database schema.  Last version detected: {highestVersionDetected} (so there is probably an issue migrating to version {highestVersionDetected + 1})");

                var dbObjects = await AbstractWorkItemDb.ExecuteReader(
                    tran,
                    @"                   
                    select 'schema' AS type, s.SCHEMA_NAME::text AS name, NULL::text AS data_type from INFORMATION_SCHEMA.SCHEMATA s where s.SCHEMA_NAME = 'nqueue'
                    UNION
                    select 'table' AS type, t.TABLE_NAME, NULL::text AS data_type FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = 'nqueue'
                    UNION
                    select 'routine' AS type, r.ROUTINE_NAME, NULL::text AS data_type FROM INFORMATION_SCHEMA.ROUTINES r WHERE r.ROUTINE_SCHEMA = 'nqueue'
                    UNION
                    select 'column' AS type, c.table_name::text || '.' || c.column_name::text AS column_name, c.data_type::text FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = 'nqueue'
                    UNION
                    select 'index' AS type, i.tablename::text || '.' || i.indexname::text || ': ' || i.indexdef, NULL::text AS data_type FROM pg_indexes i WHERE i.schemaname = 'nqueue';
                    ", 
                        reader => new PostgresSchemaInfo(
                        reader.GetString(0),
                        reader.GetString(1),
                        reader.IsDBNull(2) ? null : reader.GetString(2)
                    )
                ).ToListAsync();


                if (dbObjects.Count == 0 || dbObjects.Count == 1 && dbObjects.Single().Type == "schema" && dbObjects.Single().Name == "nqueue")
                {
                    // version "0" DB. Upgrade to version 1

                    currentVersion = 0;
                }
                else if (PostgresSchemaInfo.IsVersion07(dbObjects))
                {
                    currentVersion = 7;
                }
                else if (PostgresSchemaInfo.IsVersion06(dbObjects))
                {
                    currentVersion = 6;
                }
                else if (PostgresSchemaInfo.IsVersion05(dbObjects))
                {
                    currentVersion = 5;
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

                highestVersionDetected = Math.Max(highestVersionDetected, currentVersion);

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
                if (currentVersion == 4)
                {
                    await new PostgresDbUpgrader05().Upgrade(tran, isCitus);
                }
                if (currentVersion == 5)
                {
                    await new PostgresDbUpgrader06().Upgrade(tran, isCitus);
                }
                if (currentVersion == 6)
                {
                    await new PostgresDbUpgrader07().Upgrade(tran, isCitus);
                }
            }

            await tran.CommitAsync();
            
        }
    }
}