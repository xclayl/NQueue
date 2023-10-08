using System;
using System.Data;
using System.Data.Common;

namespace NQueue.Internal.Db.SqlServer
{
    internal class SqlServerAbstractWorkItemDb : AbstractWorkItemDb
    {
        public SqlServerAbstractWorkItemDb(TimeZoneInfo tz) : base(tz)
        {
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
    }
}