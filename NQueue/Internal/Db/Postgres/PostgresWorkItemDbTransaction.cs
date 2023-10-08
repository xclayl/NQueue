using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace NQueue.Internal.Db.Postgres
{
    internal class PostgresWorkItemDbTransaction: PostgresAbstractWorkItemDb, IWorkItemDbTransaction
    {
        private readonly DbTransaction _tran;
        private readonly DbConnection _conn;

        public PostgresWorkItemDbTransaction(DbTransaction tran, DbConnection conn, TimeZoneInfo tz): base(tz)
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
            await ExecuteProcedure(
                _tran, 
                "nqueue.EnqueueWorkItem",
                SqlParameter(url.ToString()),
                SqlParameter(queueName),
                SqlParameter(debugInfo),
                SqlParameter(NowUtc),
                SqlParameter(duplicateProtection)
            );
        }


        public async ValueTask<int> CreateCronJob(string name)
        {
            await ExecuteNonQuery(
                _tran, 
                "LOCK TABLE NQueue.CronJob IN SHARE ROW EXCLUSIVE MODE;"
            );  
            
            await ExecuteNonQuery(
                _tran, 
                "INSERT INTO NQueue.CronJob (CronJobName, LastRanAt, Active) VALUES ($1,'2000-01-01'::timestamp,TRUE) ON CONFLICT (CronJobName) DO NOTHING;",
                SqlParameter(name)
            );

            var rows = ExecuteReader(
                _tran, 
                @"SELECT cj.CronJobId FROM NQueue.CronJob cj WHERE cj.CronJobName = $1",
                reader => reader.GetInt32(0),
                SqlParameter(name)
            );

            return (await rows.ToListAsync()).Single();
        }

        public async ValueTask<(DateTimeOffset lastRan, bool active)> SelectAndLockCronJob(int cronJobId)
        {
            var rowEnumerable = ExecuteReader(
                _tran, 
                "SELECT LastRanAt, Active FROM NQueue.CronJob cj WHERE CronJobId=$1 FOR UPDATE",
                reader => new
                {
                    LastRanAt = new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero),
                    Active = reader.GetBoolean(1),
                },
                SqlParameter(cronJobId)
            );

            var rows = await rowEnumerable.ToListAsync();
            var row = rows.Single();
            return (row.LastRanAt, row.Active);
        }

        public async ValueTask UpdateCronJobLastRanAt(int cronJobId)
        {
            await ExecuteNonQuery(
                _tran,
                "UPDATE NQueue.CronJob cj SET LastRanAt = $2 WHERE CronJobId=$1",
                SqlParameter(cronJobId),
                SqlParameter(NowUtc)
            );
        }

    }
}