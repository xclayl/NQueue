﻿using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.SqlServer.DbMigrations
{
    internal class SqlServerDbMigrator
    {
        
        
        
        public static async ValueTask UpdateDbSchema(DbConnection conn)
        {
            await using var tran = await conn.BeginTransactionAsync();
            await AbstractWorkItemDb.ExecuteNonQuery(tran, @"
DECLARE @result int;  
EXEC @result = sp_getapplock @Resource = 'NQueue-schema-upgrade', @LockMode = 'Exclusive', @LockTimeout = 30000; 
IF @result < 0  
BEGIN  
	THROW 51000, 'NQueue schema upgrade lock not granted.', 1;  
END 
");
            
            
            var currentVersion = 0;

            while (currentVersion != 3)
            {
                
                var dbObjects = await AbstractWorkItemDb.ExecuteReader(
                    tran, 
                    @"
                    select 'schema' AS [type], s.SCHEMA_NAME AS [name] from INFORMATION_SCHEMA.SCHEMATA s where s.SCHEMA_NAME = 'NQueue'
                    UNION
                    select 'table' AS [type], t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = 'NQueue'
                    UNION
                    select 'routine' AS [type], r.ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES r WHERE r.ROUTINE_SCHEMA = 'NQueue'
                    UNION
                    select 'column' AS type, c.TABLE_NAME + '.' + c.COLUMN_NAME AS column_name FROM INFORMATION_SCHEMA.COLUMNS c WHERE c.TABLE_SCHEMA = 'nqueue';
                    ", reader => new SqlServerSchemaInfo(
                        reader.GetString(0),
                        reader.GetString(1)
                    )
                ).ToListAsync();
                /*
select 'index' as type, a.table_name + '.' + a.index_name + ': (' + STRING_AGG(a.col_name, ',')  WITHIN GROUP ( ORDER BY a.key_ordinal) + ')' as val
FROM (
	select o.name as table_name, i.name as index_name, c.name as col_name, ic.key_ordinal		
	from sys.indexes i
	inner join sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id
	inner join sys.columns c on ic.column_id = c.column_id and ic.object_id = c.object_id
	inner join sys.objects o on i.object_id = o.object_id
	inner join sys.schemas s on o.schema_id = s.schema_id
	where s.name = 'nqueue'
)  a
group by a.table_name, a.index_name
                 */


                if (dbObjects.Count == 0 || dbObjects.Count == 1 && dbObjects.Single().Type == "schema" && dbObjects.Single().Name == "NQueue")
                {
                    // version "0" DB. Upgrade to version 1

                    currentVersion = 0;
                }
                else if (SqlServerSchemaInfo.IsVersion03(dbObjects))
                {
                    currentVersion = 3;
                }
                else if (SqlServerSchemaInfo.IsVersion02(dbObjects))
                {
                    currentVersion = 2;
                }
                else if (SqlServerSchemaInfo.IsVersion01(dbObjects))
                {
                    currentVersion = 1;
                }
                else
                {
                    throw new Exception("The NQueue schema has an unknown structure.  One option is to move all tables & stored procedures to another schema so that NQueue can recreate them from scratch.  ALTER SCHEMA ... TRANSFER might be useful here.");
                }


                if (currentVersion == 0)
                {
                    await new SqlServerDbUpgrader01().Upgrade(tran);
                }
                if (currentVersion == 1)
                {
                    await new SqlServerDbUpgrader02().Upgrade(tran);
                }
                if (currentVersion == 2)
                {
                    await new SqlServerDbUpgrader03().Upgrade(tran);
                }
            }

            await tran.CommitAsync();
        }

    }
}