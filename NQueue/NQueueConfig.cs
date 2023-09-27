using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Net.Http;
using System.Threading.Tasks;
using NQueue.Internal;
using NQueue.Internal.Db.Postgres;
using NQueue.Internal.Db.SqlServer;

namespace NQueue
{

    public class NQueueServiceConfig
    {
        // public bool RunWorkers { get; set; } = true;
        public int QueueRunners { get; set; } = 1;
        public TimeSpan PollInterval { get; set; } = TimeSpan.FromMinutes(5);
        public Action<HttpClient, Uri> ConfigureAuth { get; set; } = (h, u) => { };
        public Func<ValueTask<DbConnection>> CreateDbConnection { get; set; } = null!;
        public IReadOnlyList<NQueueCronJob> CronJobs = new List<NQueueCronJob>();

        internal async ValueTask<DbConnection> OpenDbConnection()
        {
            if (CreateDbConnection == null)
                throw new Exception("Please configure CreateDbConnection in AddNQueueHostedService()");
            var conn = await CreateDbConnection();
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            return conn;
        }

        public TimeZoneInfo TimeZone { get; set; } = TimeZoneInfo.Local;

        internal async ValueTask<IWorkItemDbConnection> GetWorkItemDbConnection()
        {
            var dbType = await DetectServerType();
            switch (dbType)
            {
                case DbServerTypes.SqlServer: return new SqlServerWorkItemDbConnection(this);
                // case DbServerTypes.Postgres: return new PostgresWorkItemDbConnection(this);
                default: throw new Exception($"Unknown DB Server type: {dbType}");
            }

        }

        private async ValueTask<DbServerTypes> DetectServerType()
        {
            await using var conn = await OpenDbConnection();
            // Console.WriteLine(conn.ServerVersion);
            // 16.00.5100 for SQL Server
            // 16.0 for Postgres
            
            // TODO
            return DbServerTypes.SqlServer;
            
            
            // conn.ServerVersion;
            
            // SQL Server:
            // select distinct schema_name from INFORMATION_SCHEMA.SCHEMATA;
            // select @@VERSION;
            
            // Postgres: 
            // select distinct schema_name from INFORMATION_SCHEMA.SCHEMATA;
            // select character_value from information_schema.sql_implementation_info WHERE implementation_info_name = 'DBMS NAME';
        }

        private enum DbServerTypes
        {
            SqlServer,
            Postgres,
        }

    }
}