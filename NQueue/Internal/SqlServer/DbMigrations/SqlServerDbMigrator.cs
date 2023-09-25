using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace NQueue.Internal.SqlServer.DbMigrations
{
    internal class SqlServerDbMigrator
    {
        
        
        
        public static async ValueTask UpdateDbSchema(string cnn)
        {
            await using var conn = new SqlConnection(cnn);
            await conn.OpenAsync();
            await using var tran = await conn.BeginTransactionAsync();
            await SqlServerAbstractWorkItemDb.ExecuteNonQuery(tran, @"
DECLARE @result int;  
EXEC @result = sp_getapplock @Resource = 'NQueue-schema-upgrade', @LockMode = 'Exclusive', @LockTimeout = 30000; 
IF @result < 0  
BEGIN  
	THROW 51000, 'NQueue schema upgrade lock not granted.', 1;  
END 
");
            
            
            var currentVersion = 0;

            while (currentVersion != 1)
            {
                
                var dbObjects = await SqlServerAbstractWorkItemDb.ExecuteReader(@"
                    select 'schema' AS [type], s.SCHEMA_NAME AS [name] from INFORMATION_SCHEMA.SCHEMATA s where s.SCHEMA_NAME = 'NQueue'
                    UNION
                    select 'table' AS [type], t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES t WHERE t.TABLE_SCHEMA = 'NQueue'
                    UNION
                    select 'routine' AS [type], r.ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES r WHERE r.ROUTINE_SCHEMA = 'NQueue';
                    ", cnn,
                        reader => new SqlServerSchemaInfo(
                        reader.GetString(0),
                        reader.GetString(1)
                    )
                ).ToListAsync();


                if (dbObjects.Count == 0 || dbObjects.Count == 1 && dbObjects.Single().Type == "schema" && dbObjects.Single().Name == "NQueue")
                {
                    // version "0" DB. Upgrade to version 1

                    currentVersion = 0;
                }
                else if (new SqlServerDbUpgrader01().IsMyVersion(dbObjects))
                {
                    currentVersion = 1;
                }
                else
                {
                    throw new Exception("The NQueue schema has an unknown structure.  One option is to move all tables & stored procedures to another schema so that NQueue can recreate them from scratch.  ALTER SCHEMA ... TRANSFER might be useful here.");
                }


                if (currentVersion == 0)
                {
                    await new SqlServerDbUpgrader01().Upgrade(cnn);
                }
            }

            await tran.CommitAsync();
        }

    }
}