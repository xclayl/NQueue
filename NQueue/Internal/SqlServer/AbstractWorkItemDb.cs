using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace NQueue.Internal.SqlServer
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

        public static DbParameter SqlParameter(string name, string? val)
        {
            var p = new SqlParameter(name, SqlDbType.NVarChar, val?.Length ?? 1);
            p.Value = val ?? (object) DBNull.Value;
            return p;
        }

        public static DbParameter SqlParameter(string name, DateTimeOffset val)
        {
            var p = new SqlParameter(name, SqlDbType.DateTimeOffset);
            p.Value = val;
            return p;
        }

        public static DbParameter SqlParameter(string name, int val)
        {
            var p = new SqlParameter(name, SqlDbType.Int);
            p.Value = val;
            return p;
        }

        public static DbParameter SqlParameter(string name, bool val)
        {
            var p = new SqlParameter(name, SqlDbType.Bit);
            p.Value = val;
            return p;
        }
        
        
        public static async ValueTask ExecuteNonQuery(string sql, string cnn, params DbParameter[] p)
        {
            await using var conn = new SqlConnection(cnn);
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddRange(p);
            await cmd.ExecuteNonQueryAsync();
        }

        public static async IAsyncEnumerable<T> ExecuteReader<T>(string sql, string cnn, Func<DbDataReader, T> row,
            params DbParameter[] p)
        {
            await using var conn = new SqlConnection(cnn);
            await conn.OpenAsync();
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddRange(p);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                yield return row(reader);
        }

        public static async ValueTask ExecuteNonQuery(DbTransaction tran, string sql, params DbParameter[] p)
        {
            await using var cmd = tran.Connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.Transaction = tran;
            cmd.Parameters.AddRange(p);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}