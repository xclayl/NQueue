using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace NQueue.Internal.Db
{

    internal abstract class AbstractWorkItemDb
    {
        protected readonly TimeZoneInfo _tz;

        protected AbstractWorkItemDb(TimeZoneInfo tz)
        {
            _tz = tz;
        }

        protected DateTimeOffset Now
        {
            get
            {
                var nowLocal = DateTimeOffset.Now;
                return nowLocal.ToOffset(_tz.GetUtcOffset(nowLocal));
            }
        }

        internal static Func<DbCommand, DbParameter> SqlParameter(string name, string? val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.NVarChar, val?.Length ?? 1);
                p.ParameterName = name;
                p.DbType = DbType.String;
                p.Value = val ?? (object)DBNull.Value;
                return p;
            };
        }

        internal static Func<DbCommand, DbParameter> SqlParameter(string name, DateTimeOffset val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.DateTimeOffset);
                p.ParameterName = name;
                p.DbType = DbType.DateTimeOffset;
                p.Value = val;
                return p;
            };
        }

        internal static Func<DbCommand, DbParameter> SqlParameter(string name, int val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.Int);
                p.ParameterName = name;
                p.DbType = DbType.Int32;
                p.Value = val;
                return p;
            };
        }

        internal static Func<DbCommand, DbParameter> SqlParameter(string name, bool val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.Bit);
                p.ParameterName = name;
                p.DbType = DbType.Boolean;
                p.Value = val;
                return p;
            };
        }
        
        
        public static async ValueTask ExecuteNonQuery(string sql, DbConnection conn, params Func<DbCommand, DbParameter>[] ps)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddRange(ps.Select(p => p(cmd)).ToArray());
            await cmd.ExecuteNonQueryAsync();
        }

        public static async IAsyncEnumerable<T> ExecuteReader<T>(string sql, DbConnection conn, Func<DbDataReader, T> row,
            params Func<DbCommand, DbParameter>[] ps)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddRange(ps.Select(p => p(cmd)).ToArray());
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                yield return row(reader);
        }

        public static async ValueTask ExecuteNonQuery(DbTransaction tran, string sql, params Func<DbCommand, DbParameter>[] ps)
        {
            await using var cmd = tran.Connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = tran;
            cmd.Parameters.AddRange(ps.Select(p => p(cmd)).ToArray());
            await cmd.ExecuteNonQueryAsync();
        }
        
        
        public static async IAsyncEnumerable<T> ExecuteReader<T>(DbTransaction tran, string sql, Func<DbDataReader, T> row,
            params Func<DbCommand, DbParameter>[] ps)
        {
            await using var cmd = tran.Connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = tran;
            cmd.Parameters.AddRange(ps.Select(p => p(cmd)).ToArray());
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                yield return row(reader);
        }
        
        
        protected static DateTimeOffset NowIn(TimeZoneInfo tz)
        {
            var nowLocal = DateTimeOffset.Now;
            return nowLocal.ToOffset(tz.GetUtcOffset(nowLocal));
        }
    }
}