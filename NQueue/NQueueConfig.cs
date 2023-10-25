using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using NQueue.Internal;
using NQueue.Internal.Db;
using NQueue.Internal.Db.InMemory;
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
        public Func<ValueTask<DbConnection?>> CreateDbConnection { get; set; } = null!;
        public IReadOnlyList<NQueueCronJob> CronJobs = new List<NQueueCronJob>();

        private volatile IWorkItemDbConnection? _workItemDbConnection;
        private readonly InMemoryWorkItemDbConnection _inMemoryWorkItemDbConnection = new InMemoryWorkItemDbConnection();


        internal async ValueTask<DbConnection?> OpenDbConnection()
        {
            if (CreateDbConnection == null)
                throw new Exception("Please configure CreateDbConnection in AddNQueueHostedService()");
            var conn = await CreateDbConnection();
            if (conn == null)
                return null;
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();
            return conn;
        }

        public TimeZoneInfo TimeZone { get; set; } = TimeZoneInfo.Local;

        internal async ValueTask<IWorkItemDbConnection> GetWorkItemDbConnection()
        {
            if (_workItemDbConnection != null)
                return _workItemDbConnection;
            
            var dbType = await DetectServerType();
            switch (dbType)
            {
                case DbServerTypes.SqlServer: return _workItemDbConnection = new SqlServerWorkItemDbConnection(this);
                case DbServerTypes.Postgres: return _workItemDbConnection = new PostgresWorkItemDbConnection(this);
                case DbServerTypes.InMemory: return _workItemDbConnection = _inMemoryWorkItemDbConnection;
                default: throw new Exception($"Unknown DB Server type: {dbType}");
            }

        }

        private async ValueTask<DbServerTypes> DetectServerType()
        {
            await using var conn = await OpenDbConnection();
            if (conn == null)
                return DbServerTypes.InMemory;
            // Console.WriteLine(conn.ServerVersion);
            // 16.00.5100 for SQL Server
            // 16.0 for Postgres

            var points = new Dictionary<DbServerTypes, double>()
            {
                { DbServerTypes.SqlServer, 0 },
                { DbServerTypes.Postgres, 0 },
            };

            if (conn.ServerVersion?.Count(c => c == '.') == 2)
                points[DbServerTypes.SqlServer] += 0.1;
            if (conn.ServerVersion?.Count(c => c == '.') == 1)
                points[DbServerTypes.Postgres] += 0.1;

            var schemas = await 
                AbstractWorkItemDb.ExecuteReader("select distinct schema_name from INFORMATION_SCHEMA.SCHEMATA", conn, reader => reader.GetString(0))
                    .ToListAsync();
            
            if (schemas.Contains("dbo"))
                points[DbServerTypes.SqlServer] += 0.9;
            if (schemas.Contains("sys"))
                points[DbServerTypes.SqlServer] += 0.5;
            if (schemas.Contains("public"))
                points[DbServerTypes.Postgres] += 0.3;
            if (schemas.Contains("pg_catalog"))
                points[DbServerTypes.Postgres] += 1;

            var order = points.OrderByDescending(kv => kv.Value).Select(kv => kv.Key).ToList();

            foreach (var dbServerType in order)
            {
                switch (dbServerType)
                {
                    case DbServerTypes.Postgres:
                        if (await IsPostgres(conn))
                            return DbServerTypes.Postgres;
                        break;
                    case DbServerTypes.SqlServer:
                        if (await IsSqlServer(conn))
                            return DbServerTypes.SqlServer;
                        break;
                    default:
                        throw new Exception($"Unknown type: {dbServerType}");
                }
            }

            throw new Exception("DB Type not detected");
            
                
        }

        private static async ValueTask<bool> IsPostgres(DbConnection conn)
        {
            try
            {
                var val = (await
                    AbstractWorkItemDb
                        .ExecuteReader(
                            "select character_value from information_schema.sql_implementation_info WHERE implementation_info_name = 'DBMS NAME'",
                            conn, reader => reader.GetString(0))
                        .ToListAsync())
                    .SingleOrDefault();

                return val == "PostgreSQL";
            }
            catch (Exception)
            {
                return false;
            }
        }
        private static async ValueTask<bool> IsSqlServer(DbConnection conn)
        {
            try
            {
                var val = (await
                        AbstractWorkItemDb
                            .ExecuteReader(
                                "select @@VERSION",
                                conn, reader => reader.GetString(0))
                            .ToListAsync())
                    .SingleOrDefault();

                return val?.StartsWith("Microsoft") ?? false;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private enum DbServerTypes
        {
            SqlServer,
            Postgres,
            InMemory
        }

    }
}