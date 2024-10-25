using System;
using System.Data.Common;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.Postgres
{
    internal class PostgresCronTransaction: PostgresAbstractWorkItemDb, ICronTransaction
    {
        private readonly DbTransaction _tran;
        private readonly DbConnection _conn;

        public PostgresCronTransaction(DbTransaction tran, DbConnection conn, TimeZoneInfo tz, bool isCitus): base(tz, isCitus)
        {
            _tran = tran;
            _conn = conn;
        }
        
        
        public async ValueTask DisposeAsync()
        {
            await _tran.DisposeAsync();
            await _conn.DisposeAsync();
        }

        public async ValueTask CommitAsync()
        {
            await _tran.CommitAsync();
        }

        public async ValueTask EnqueueWorkItem(Uri url, string? queueName, string debugInfo, bool duplicateProtection)
        {
            queueName ??= Guid.NewGuid().ToString();
            
            var shard = CalculateShard(queueName);
            
            await ExecuteProcedure(
                _tran, 
                "nqueue.EnqueueWorkItem2",
                SqlParameter(url.ToString()),
                SqlParameter(queueName),
                SqlParameter(shard),
                SqlParameter(debugInfo),
                SqlParameter(NowUtc),
                SqlParameter(duplicateProtection),
                SqlParameter(null)
            );
        }

        private int CalculateShard(string queueName)
        {
            if (!IsCitus)
                return 0;
            
            using var md5 = MD5.Create();
            var bytes = md5.ComputeHash(Encoding.UTF8.GetBytes(queueName));

            var shard = bytes[0] >> 4 & 15;

            return shard;
        }


        public async ValueTask CreateCronJob(string name)
        {
            // await ExecuteNonQuery(
            //     _tran, 
            //     "LOCK TABLE NQueue.CronJob IN SHARE ROW EXCLUSIVE MODE;"
            // );  
            
            await ExecuteNonQuery(
                _tran, 
                "INSERT INTO NQueue.CronJob (CronJobName, LastRanAt, Active) VALUES ($1,'2000-01-01'::timestamp,TRUE) ON CONFLICT (CronJobName) DO NOTHING;",
                SqlParameter(name)
            );

        }

        public async ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(string cronJobName)
        {
            var rowEnumerable = ExecuteReader(
                _tran, 
                "SELECT LastRanAt, Active FROM NQueue.CronJob cj WHERE CronJobName=$1 FOR UPDATE",
                reader => new
                {
                    LastRanAt = new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero),
                    Active = reader.GetBoolean(1),
                },
                SqlParameter(cronJobName)
            );

            var rows = await rowEnumerable.ToListAsync();
            var row = rows.Single();
            return (row.LastRanAt, row.Active);
        }

        public async ValueTask UpdateCronJobLastRanAt(string cronJobName)
        {
            await ExecuteNonQuery(
                _tran,
                "UPDATE NQueue.CronJob cj SET LastRanAt = $2 WHERE CronJobName=$1",
                SqlParameter(cronJobName),
                SqlParameter(NowUtc)
            );
        }

    }
}