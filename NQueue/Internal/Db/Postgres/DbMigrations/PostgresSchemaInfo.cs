using System;
using System.Collections.Generic;
using System.Linq;

namespace NQueue.Internal.Db.Postgres.DbMigrations;

internal record class PostgresSchemaInfo(string Type, string Name)
{
    
    public static bool IsVersion01(IReadOnlyList<PostgresSchemaInfo> actual)
    {
        actual = actual.Where(o => o.Type != "index").ToList();
        
        var expectedTables = new[]
        {
            "cronjob",
            "queue",
            "workitem",
            "workitemcompleted",
        };

        var expectedSps =
            new[]
            {
                "failworkitem",
                "nextworkitem",
                "purgeworkitems",
                "replayworkitem",
                "enqueueworkitem",
                "completeworkitem",
            };


        if (expectedTables.Any(t => !actual.Any(d => d.Type.Equals("table", StringComparison.InvariantCultureIgnoreCase) && d.Name.Equals(t, StringComparison.InvariantCultureIgnoreCase))))
            return false;
        if (expectedSps.Any(r => !actual.Any(d => d.Type.Equals("routine", StringComparison.InvariantCultureIgnoreCase) && d.Name.Equals(r, StringComparison.InvariantCultureIgnoreCase))))
            return false;

        return true;
    }


    private static IReadOnlyList<PostgresSchemaInfo> Version02 => new PostgresSchemaInfo[]
    {
        new("column", "CronJob.Active"),
        new("column", "CronJob.CronJobId"),
        new("column", "CronJob.CronJobName"),
        new("column", "CronJob.LastRanAt"),
        new("column", "Queue.ErrorCount"),
        new("column", "Queue.LockedUntil"),
        new("column", "Queue.Name"),
        new("column", "Queue.NextWorkItemId"),
        new("column", "Queue.QueueId"),
        new("column", "WorkItem.CreatedAt"),
        new("column", "WorkItem.DebugInfo"),
        new("column", "WorkItem.Internal"),
        new("column", "WorkItem.IsIngested"),
        new("column", "WorkItem.LastAttemptedAt"),
        new("column", "WorkItem.QueueName"),
        new("column", "WorkItem.Url"),
        new("column", "WorkItem.WorkItemId"),
        new("column", "WorkItemCompleted.CompletedAt"),
        new("column", "WorkItemCompleted.CreatedAt"),
        new("column", "WorkItemCompleted.DebugInfo"),
        new("column", "WorkItemCompleted.Internal"),
        new("column", "WorkItemCompleted.LastAttemptedAt"),
        new("column", "WorkItemCompleted.QueueName"),
        new("column", "WorkItemCompleted.Url"),
        new("column", "WorkItemCompleted.WorkItemId"),
        new("routine", "CompleteWorkItem"),
        new("routine", "EnqueueWorkItem"),
        new("routine", "FailWorkItem"),
        new("routine", "NextWorkItem"),
        new("routine", "PurgeWorkItems"),
        new("routine", "ReplayWorkItem"),
        new("schema", "NQueue"),
        new("table", "CronJob"),
        new("table", "Queue"),
        new("table", "WorkItem"),
        new("table", "WorkItemCompleted"),
    };


    private static bool IsVersion(IReadOnlyList<PostgresSchemaInfo> actual, IReadOnlyList<PostgresSchemaInfo> expected)
    {
        if (expected.Count != actual.Count)
            return false;

        var items = expected.OrderBy(e => e.Type).ThenBy(e => e.Name)
            .Zip(actual.OrderBy(a => a.Type).ThenBy(a => a.Name), (e, a) => new
            {
                Expected = e,
                Actual = a
            })
            .ToList();

        return items.All(i => i.Expected.Type.Equals(i.Actual.Type, StringComparison.InvariantCultureIgnoreCase) 
                              && i.Expected.Name.Equals(i.Actual.Name, StringComparison.InvariantCultureIgnoreCase) );
    }

    public static bool IsVersion02(IReadOnlyList<PostgresSchemaInfo> actual)
    {
        actual = actual.Where(o => o.Type != "index").ToList();
        
        var expected = Version02;

        return IsVersion(actual, expected);
    }

    
    
    private static IReadOnlyList<PostgresSchemaInfo> Version03 => new PostgresSchemaInfo[]
    {
        new("column", "CronJob.Active"),
        new("column", "CronJob.CronJobName"),
        new("column", "CronJob.LastRanAt"),
        new("column", "Queue.ErrorCount"),
        new("column", "Queue.LockedUntil"),
        new("column", "Queue.Name"),
        new("column", "Queue.NextWorkItemId"),
        new("column", "Queue.QueueId"),
        new("column", "Queue.Shard"),
        new("column", "WorkItem.CreatedAt"),
        new("column", "WorkItem.DebugInfo"),
        new("column", "WorkItem.Internal"),
        new("column", "WorkItem.IsIngested"),
        new("column", "WorkItem.LastAttemptedAt"),
        new("column", "WorkItem.QueueName"),
        new("column", "WorkItem.Url"),
        new("column", "WorkItem.WorkItemId"),
        new("column", "WorkItem.Shard"),
        new("column", "WorkItemCompleted.CompletedAt"),
        new("column", "WorkItemCompleted.CreatedAt"),
        new("column", "WorkItemCompleted.DebugInfo"),
        new("column", "WorkItemCompleted.Internal"),
        new("column", "WorkItemCompleted.LastAttemptedAt"),
        new("column", "WorkItemCompleted.QueueName"),
        new("column", "WorkItemCompleted.Url"),
        new("column", "WorkItemCompleted.WorkItemId"),
        new("column", "WorkItemCompleted.Shard"),
        new("routine", "CompleteWorkItem"),
        new("routine", "EnqueueWorkItem"),
        new("routine", "FailWorkItem"),
        new("routine", "NextWorkItem"),
        new("routine", "PurgeWorkItems"),
        new("routine", "ReplayWorkItem"),
        new("schema", "NQueue"),
        new("table", "CronJob"),
        new("table", "Queue"),
        new("table", "WorkItem"),
        new("table", "WorkItemCompleted"),
        new("index", "queue.pk_queue: CREATE UNIQUE INDEX pk_queue ON nqueue.queue USING btree (shard, queueid)"),
        new("index", "workitem.pk_workitem: CREATE UNIQUE INDEX pk_workitem ON nqueue.workitem USING btree (shard, workitemid)"),
        new("index", "workitemcompleted.pk_workitemcompleted: CREATE UNIQUE INDEX pk_workitemcompleted ON nqueue.workitemcompleted USING btree (shard, workitemid)"),
        new("index", "cronjob.pk_cronjob: CREATE UNIQUE INDEX pk_cronjob ON nqueue.cronjob USING btree (cronjobname)"),
        new("index", "queue.ak_queue_name: CREATE UNIQUE INDEX ak_queue_name ON nqueue.queue USING btree (shard, name)"),
        new("index", "queue.ix_nqueue_queue_lockeduntil_nextworkitemid: CREATE UNIQUE INDEX ix_nqueue_queue_lockeduntil_nextworkitemid ON nqueue.queue USING btree (shard, lockeduntil, nextworkitemid, queueid) INCLUDE (errorcount)"),
        new("index", "workitem.ix_nqueue_workitem_isingested_queuename: CREATE UNIQUE INDEX ix_nqueue_workitem_isingested_queuename ON nqueue.workitem USING btree (shard, isingested, queuename, workitemid) INCLUDE (createdat)"),
        new("index", "workitemcompleted.ix_nqueue_workitemcompleted_isingested_queuename: CREATE UNIQUE INDEX ix_nqueue_workitemcompleted_isingested_queuename ON nqueue.workitemcompleted USING btree (shard, completedat)"),

        
    };
    public static bool IsVersion03(IReadOnlyList<PostgresSchemaInfo> actual)
    {
        var expected = Version03;
       
        return IsVersion(actual, expected);
    }
    
    
    
    
    private static IReadOnlyList<PostgresSchemaInfo> Version04 => new PostgresSchemaInfo[]
    {
        new("column", "CronJob.Active"),
        new("column", "CronJob.CronJobName"),
        new("column", "CronJob.LastRanAt"),
        new("column", "Queue.ErrorCount"),
        new("column", "Queue.LockedUntil"),
        new("column", "Queue.Name"),
        new("column", "Queue.NextWorkItemId"),
        new("column", "Queue.QueueId"),
        new("column", "Queue.Shard"),
        new("column", "WorkItem.CreatedAt"),
        new("column", "WorkItem.DebugInfo"),
        new("column", "WorkItem.Internal"),
        new("column", "WorkItem.IsIngested"),
        new("column", "WorkItem.LastAttemptedAt"),
        new("column", "WorkItem.QueueName"),
        new("column", "WorkItem.Url"),
        new("column", "WorkItem.WorkItemId"),
        new("column", "WorkItem.Shard"),
        new("column", "WorkItemCompleted.CompletedAt"),
        new("column", "WorkItemCompleted.CreatedAt"),
        new("column", "WorkItemCompleted.DebugInfo"),
        new("column", "WorkItemCompleted.Internal"),
        new("column", "WorkItemCompleted.LastAttemptedAt"),
        new("column", "WorkItemCompleted.QueueName"),
        new("column", "WorkItemCompleted.Url"),
        new("column", "WorkItemCompleted.WorkItemId"),
        new("column", "WorkItemCompleted.Shard"),
        new("routine", "CompleteWorkItem"),
        new("routine", "EnqueueWorkItem"),
        new("routine", "FailWorkItem"),
        new("routine", "NextWorkItem"),
        new("routine", "PurgeWorkItems"),
        new("routine", "ReplayWorkItem"),
        new("schema", "NQueue"),
        new("table", "CronJob"),
        new("table", "Queue"),
        new("table", "WorkItem"),
        new("table", "WorkItemCompleted"),
        new("index", "queue.pk_queue: CREATE UNIQUE INDEX pk_queue ON nqueue.queue USING btree (shard, queueid)"),
        new("index", "workitem.pk_workitem: CREATE UNIQUE INDEX pk_workitem ON nqueue.workitem USING btree (shard, workitemid)"),
        new("index", "workitemcompleted.pk_workitemcompleted: CREATE UNIQUE INDEX pk_workitemcompleted ON nqueue.workitemcompleted USING btree (shard, workitemid)"),
        new("index", "cronjob.pk_cronjob: CREATE UNIQUE INDEX pk_cronjob ON nqueue.cronjob USING btree (cronjobname)"),
        new("index", "queue.ak_queue_name: CREATE UNIQUE INDEX ak_queue_name ON nqueue.queue USING btree (shard, name)"),
        new("index", "queue.ix_nqueue_queue_lockeduntil_nextworkitemid: CREATE UNIQUE INDEX ix_nqueue_queue_lockeduntil_nextworkitemid ON nqueue.queue USING btree (shard, lockeduntil, nextworkitemid, queueid) INCLUDE (errorcount)"),
        new("index", "workitem.ix_nqueue_workitem_isingested_queuename: CREATE UNIQUE INDEX ix_nqueue_workitem_isingested_queuename ON nqueue.workitem USING btree (shard, isingested, queuename, workitemid) INCLUDE (createdat)"),
        new("index", "workitemcompleted.ix_nqueue_workitemcompleted_completedat: CREATE UNIQUE INDEX ix_nqueue_workitemcompleted_completedat ON nqueue.workitemcompleted USING btree (shard, completedat, workitemid)"),

        
    };
    public static bool IsVersion04(IReadOnlyList<PostgresSchemaInfo> actual)
    {
        var expected = Version04;
       
        return IsVersion(actual, expected);
    }
    
    
    
    private static IReadOnlyList<PostgresSchemaInfo> Version05 => new PostgresSchemaInfo[]
    {
        new("column", "CronJob.Active"),
        new("column", "CronJob.CronJobName"),
        new("column", "CronJob.LastRanAt"),
        new("column", "Queue.ErrorCount"),
        new("column", "Queue.LockedUntil"),
        new("column", "Queue.Name"),
        new("column", "Queue.NextWorkItemId"),
        new("column", "Queue.QueueId"),
        new("column", "Queue.Shard"),
        new("column", "WorkItem.CreatedAt"),
        new("column", "WorkItem.DebugInfo"),
        new("column", "WorkItem.Internal"),
        new("column", "WorkItem.IsIngested"),
        new("column", "WorkItem.LastAttemptedAt"),
        new("column", "WorkItem.QueueName"),
        new("column", "WorkItem.Url"),
        new("column", "WorkItem.WorkItemId"),
        new("column", "WorkItem.Shard"),
        new("column", "WorkItemCompleted.CompletedAt"),
        new("column", "WorkItemCompleted.CreatedAt"),
        new("column", "WorkItemCompleted.DebugInfo"),
        new("column", "WorkItemCompleted.Internal"),
        new("column", "WorkItemCompleted.LastAttemptedAt"),
        new("column", "WorkItemCompleted.QueueName"),
        new("column", "WorkItemCompleted.Url"),
        new("column", "WorkItemCompleted.WorkItemId"),
        new("column", "WorkItemCompleted.Shard"),
        new("routine", "CompleteWorkItem"),
        new("routine", "EnqueueWorkItem"),
        new("routine", "FailWorkItem"),
        new("routine", "DelayWorkItem"),
        new("routine", "NextWorkItem"),
        new("routine", "PurgeWorkItems"),
        new("routine", "ReplayWorkItem"),
        new("schema", "NQueue"),
        new("table", "CronJob"),
        new("table", "Queue"),
        new("table", "WorkItem"),
        new("table", "WorkItemCompleted"),
        new("index", "queue.pk_queue: CREATE UNIQUE INDEX pk_queue ON nqueue.queue USING btree (shard, queueid)"),
        new("index", "workitem.pk_workitem: CREATE UNIQUE INDEX pk_workitem ON nqueue.workitem USING btree (shard, workitemid)"),
        new("index", "workitemcompleted.pk_workitemcompleted: CREATE UNIQUE INDEX pk_workitemcompleted ON nqueue.workitemcompleted USING btree (shard, workitemid)"),
        new("index", "cronjob.pk_cronjob: CREATE UNIQUE INDEX pk_cronjob ON nqueue.cronjob USING btree (cronjobname)"),
        new("index", "queue.ak_queue_name: CREATE UNIQUE INDEX ak_queue_name ON nqueue.queue USING btree (shard, name)"),
        new("index", "queue.ix_nqueue_queue_lockeduntil_nextworkitemid: CREATE UNIQUE INDEX ix_nqueue_queue_lockeduntil_nextworkitemid ON nqueue.queue USING btree (shard, lockeduntil, nextworkitemid, queueid) INCLUDE (errorcount)"),
        new("index", "workitem.ix_nqueue_workitem_isingested_queuename: CREATE UNIQUE INDEX ix_nqueue_workitem_isingested_queuename ON nqueue.workitem USING btree (shard, isingested, queuename, workitemid) INCLUDE (createdat)"),
        new("index", "workitemcompleted.ix_nqueue_workitemcompleted_completedat: CREATE UNIQUE INDEX ix_nqueue_workitemcompleted_completedat ON nqueue.workitemcompleted USING btree (shard, completedat, workitemid)"),

        
    };
    public static bool IsVersion05(IReadOnlyList<PostgresSchemaInfo> actual)
    {
        var expected = Version05;
       
        return IsVersion(actual, expected);
    }
}