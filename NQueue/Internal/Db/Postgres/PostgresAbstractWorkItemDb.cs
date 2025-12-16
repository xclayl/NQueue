using System;
using System.Buffers.Binary;
using System.Data;
using System.Data.Common;

namespace NQueue.Internal.Db.Postgres
{
    internal abstract class PostgresAbstractWorkItemDb : AbstractWorkItemDb
    {
        TimeZoneInfo _utc = TimeZoneInfo.Utc;
        protected readonly ShardConfig ShardConfig;
        
        protected PostgresAbstractWorkItemDb(TimeZoneInfo tz, ShardConfig shardConfig) : base(tz)
        {
            ShardConfig = shardConfig;
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
        
        internal int GetShard(byte[] hashBytes, int maxShards)
        {
            if (maxShards <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxShards));

            return (int)(ToUInt32(hashBytes) % (uint)maxShards);
            
        }
        
        uint ToUInt32(byte[] hashBytes)
        {
            if (hashBytes == null || hashBytes.Length != 4)
                throw new ArgumentException("xxHash32 must be exactly 4 bytes");

            return BinaryPrimitives.ReadUInt32LittleEndian(hashBytes);
        }
    }
}