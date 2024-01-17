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
        private readonly TimeZoneInfo _tz;

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

        
        
        protected static async ValueTask ExecuteNonQuery(string sql, DbConnection conn, params Func<DbCommand, DbParameter>[] ps)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.Parameters.AddRange(ps.Select(p => p(cmd)).ToArray());
            await cmd.ExecuteNonQueryAsync();
        }
        
        protected static async ValueTask ExecuteProcedure(string procedure, DbConnection conn, params Func<DbCommand, DbParameter>[] ps)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = procedure;
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
            await using var cmd = tran.Connection!.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = tran;
            cmd.Parameters.AddRange(ps.Select(p => p(cmd)).ToArray());
            await cmd.ExecuteNonQueryAsync();
        }

        public static async ValueTask ExecuteProcedure(DbTransaction tran, string procedure, params Func<DbCommand, DbParameter>[] ps)
        {
            await using var cmd = tran.Connection!.CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = procedure;
            cmd.Transaction = tran;
            cmd.Parameters.AddRange(ps.Select(p => p(cmd)).ToArray());
            await cmd.ExecuteNonQueryAsync();
        }
        
        
        public static async IAsyncEnumerable<T> ExecuteReader<T>(DbTransaction tran, string sql, Func<DbDataReader, T> row,
            params Func<DbCommand, DbParameter>[] ps)
        {
            await using var cmd = tran.Connection!.CreateCommand();
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