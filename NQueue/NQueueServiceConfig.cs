﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NQueue.Internal.Db;
using NQueue.Internal.Db.InMemory;
using NQueue.Internal.Db.Postgres;
using NQueue.Internal.Db.SqlServer;
using NQueue.Internal.Model;

namespace NQueue
{

    public class NQueueServiceConfig : IDbConfig
    {
        private readonly IDbConnectionLock _dbLock;
        
        /// <summary>
        /// Maximum number of Work Items to be processed in parallel (per Shard).  0 = disables queue processing.
        /// Feel free to use a ridiculous number, like 1_000_000.
        /// If this is zero, Cron Jobs will not be processed (by this app)
        /// Default = 1
        /// </summary>
        public int QueueRunners { get; set; } = 1;

        /// <summary>
        /// The amount of time to wait until querying the DB again to look for Work Items.
        /// If a Work Item was found previously, this is ignored, and NQueue immediately
        /// queries for another Work Item.
        /// Default = 30s
        /// </summary>
        public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(30);
        
        /// <summary>
        /// Allows you to modify the HTTP request.  I've used this to add authentication in the past, but
        /// you can modify anything you like, even the RequestUri.
        /// </summary>
        public Func<HttpRequestMessage, ValueTask> ModifyHttpRequest { get; set; } = (_) => ValueTask.CompletedTask;
        
        /// <summary>
        /// Used to create the DB Connection to the database to look for Work Items, or anything else like
        /// cron job management.
        /// Default = null (in-memory queues, usually for testing)
        /// </summary>
        public Func<ValueTask<DbConnection?>> CreateDbConnection { get; set; } = () => ValueTask.FromResult((DbConnection?) null);
        
        /// <summary>
        /// A list of cron jobs which create Work Items on a schedule.
        /// These will not create duplicate Work Items
        /// in case the Work Item takes longer than the cron interval.
        /// You probably want to set the TimeZone property as well.
        /// If config.QueueRunners is zero, Cron Jobs will not be processed (by this app).
        /// </summary>
        public IReadOnlyList<NQueueCronJob> CronJobs = new List<NQueueCronJob>();

        private volatile IReadOnlyCollection<string> _localHttpAddresses = new List<string>();
        /// <summary>
        /// Used to enable the nQueueClient.Localhost() method.
        /// You probably want this code:
        ///     config.LocalHttpAddresses = s.GetRequiredService&lt;IServer&gt;().Features.Get&lt;IServerAddressesFeature&gt;().Addresses.ToList();
        /// </summary>
        public IReadOnlyCollection<string> LocalHttpAddresses
        {
            get => _localHttpAddresses;
            set => _localHttpAddresses = value;
        }

        private volatile IWorkItemDbConnection? _workItemDbConnection;
        private readonly InMemoryWorkItemDbConnection _inMemoryWorkItemDbConnection = new();

        internal NQueueServiceConfig(IDbConnectionLock dbLock)
        {
            _dbLock = dbLock;
        }


        /// <summary>
        /// Whether NQueue should have a single connection to the DB at a time.
        /// When false (defoult), NQueue will open DB Connections as need, unrestricted.
        /// When true, NQueue will only open DB Connections if no other DB Connection is open.
        /// This can be useful if the DB has a small number of limited connections allowed,
        /// especially through a shored connection pooling, like PG Bouncer.
        /// </summary>
        public bool SingleDbConnectionAtATime { get; set; }

        private async ValueTask<T> WithDbConnectionForDetection<T>(Func<DbConnection?, ValueTask<T>> action)
        {
            if (CreateDbConnection == null)
                throw new Exception("This should never happen, CreateDbConnection is null.");
            
            using var connectionLock = SingleDbConnectionAtATime ? await _dbLock.Acquire() : null;

            await using var conn = await CreateDbConnection();
            if (conn == null)
                return await action(null);
            if (conn!.State != ConnectionState.Open)
                await conn.OpenAsync();
            
            return await action(conn);
        }


        async ValueTask IDbConfig.WithDbConnection(Func<DbConnection, ValueTask> action)
        {
            await WithDbConnectionPriv(async conn =>
            {
                await action(conn);
                return 0;
            });
        }
        async ValueTask IDbConfig.WithDbConnectionAndRetries(Func<DbConnection, ValueTask> action, ILogger logger)
        {
            var tries = 0;
            const int MaxRetries = 10;
            while (tries <= MaxRetries)
            {
                try
                {
                    await WithDbConnectionPriv(async conn =>
                    {
                        await action(conn);
                        return 0;
                    });
                    return;
                }
                catch (Exception e) when (tries < MaxRetries)
                {
                    logger.LogWarning(e.ToString());
                }

                tries++;
                await Task.Delay(TimeSpan.FromSeconds(Math.Min(Math.Pow(2, tries), 30)));
            }
        }
        
        async ValueTask<T> IDbConfig.WithDbConnection<T>(Func<DbConnection, ValueTask<T>> action)
        {
            return await WithDbConnectionPriv(async conn => await action(conn));
        }

        private async ValueTask<T> WithDbConnectionPriv<T>(Func<DbConnection, ValueTask<T>> action)
        {
            if (CreateDbConnection == null)
                throw new Exception("This should never happen, CreateDbConnection is null.");

            using var connectionLock = SingleDbConnectionAtATime ? await _dbLock.Acquire() : null;

            await using var conn = await CreateDbConnection();
            if (conn == null)
                throw new Exception("This should never happen, CreateDbConnection returned a null.");
            if (conn!.State != ConnectionState.Open)
                await conn.OpenAsync();
            
            return await action(conn);
        }

        /// <summary>
        /// Timezone to understand when the cron jobs should run.
        /// For SQL Server, it'll use the time zone to store DateTimeOffsets in the DB (to make them more readable).  
        /// Default = TimeZoneInfo.Local
        /// </summary>
        public TimeZoneInfo TimeZone { get; set; } = TimeZoneInfo.Local;

        internal async ValueTask<IWorkItemDbConnection> GetWorkItemDbConnection()
        {
            if (_workItemDbConnection != null)
                return _workItemDbConnection;
            
            var dbType = await DetectServerType();
            switch (dbType)
            {
                case DbServerTypes.SqlServer: return _workItemDbConnection = new SqlServerWorkItemDbConnection(this);
                case DbServerTypes.Postgres: return _workItemDbConnection = new PostgresWorkItemDbConnection(this, false);
                case DbServerTypes.PostgresCitus: return _workItemDbConnection = new PostgresWorkItemDbConnection(this, true);
                case DbServerTypes.InMemory: return _workItemDbConnection = _inMemoryWorkItemDbConnection;
                default: throw new Exception($"Unknown DB Server type: {dbType}");
            }

        }

        private async ValueTask<DbServerTypes> DetectServerType()
        {
            return await WithDbConnectionForDetection(async conn =>
            {
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

                if (conn.ServerVersion.Count(c => c == '.') == 2)
                    points[DbServerTypes.SqlServer] += 0.1;
                if (conn.ServerVersion.Count(c => c == '.') == 1)
                    points[DbServerTypes.Postgres] += 0.1;

                var schemas = await
                    AbstractWorkItemDb.ExecuteReader("select distinct schema_name from INFORMATION_SCHEMA.SCHEMATA",
                            conn, reader => reader.GetString(0))
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
                            {
                                if (await IsPostgresCitus(conn))
                                    return DbServerTypes.PostgresCitus;
                                else
                                    return DbServerTypes.Postgres;
                            }

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


            });
            
                
        }
        
        
        private static async ValueTask<bool> IsPostgresCitus(DbConnection conn)
        {
            /*
               SELECT * FROM pg_available_extensions WHERE name = 'citus';

               SELECT * FROM pg_extension WHERE extname = 'citus';
            */
            
            var extension = (await
                    AbstractWorkItemDb.ExecuteReader(
                            "SELECT extname FROM pg_extension WHERE extname = 'citus'",
                            conn, reader => reader.GetString(0))
                        .ToListAsync())
                .SingleOrDefault();

            return extension == "citus";
        }

        private static async ValueTask<bool> IsPostgres(DbConnection conn)
        {
           
            try
            {
                var val = (await
                        AbstractWorkItemDb.ExecuteReader(
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
            PostgresCitus,
            InMemory
        }

    }
}