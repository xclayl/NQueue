using System;
using System.Data;
using System.Data.Common;

namespace NQueue.Internal.Db.Postgres
{
    internal abstract class PostgresAbstractWorkItemDb : AbstractWorkItemDb
    {
        TimeZoneInfo _utc = TimeZoneInfo.Utc;
        protected readonly bool IsCitus;
        
        protected PostgresAbstractWorkItemDb(TimeZoneInfo tz, bool isCitus) : base(tz)
        {
            IsCitus = isCitus;
        }
        
        
        protected DateTimeOffset NowUtc => TimeZoneInfo.ConvertTime(Now, _utc);


        internal static Func<DbCommand, DbParameter> SqlParameter(string? val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.NVarChar, val?.Length ?? 1);
                p.DbType = DbType.String;
                p.Value = val ?? (object)DBNull.Value;
                return p;
            };
        }

        internal static Func<DbCommand, DbParameter> SqlParameter(DateTimeOffset val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.DateTimeOffset);
                p.DbType = DbType.DateTimeOffset;
                p.Value = val;
                return p;
            };
        }

        internal static Func<DbCommand, DbParameter> SqlParameter(int val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.Int);
                p.DbType = DbType.Int32;
                p.Value = val;
                return p;
            };
        }

        internal static Func<DbCommand, DbParameter> SqlParameter(int? val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.Int);
                p.DbType = DbType.Int32;
                p.Value = val ?? (object)DBNull.Value;
                return p;
            };
        }

        internal static Func<DbCommand, DbParameter> SqlParameter(long val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); 
                p.DbType = DbType.Int64;
                p.Value = val;
                return p;
            };
        }

        internal static Func<DbCommand, DbParameter> SqlParameter(bool val)
        {
            return (cmd) =>
            {
                var p = cmd.CreateParameter(); // new SqlParameter(name, SqlDbType.Bit);
                p.DbType = DbType.Boolean;
                p.Value = val;
                return p;
            };
        }
    }
}