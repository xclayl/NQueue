using System;
using System.Collections.Generic;
using System.Linq;

namespace NQueue.Internal.Db;

internal record class DbSchemaInfo(string Type, string Name)
{


    public static bool IsVersion01(IReadOnlyList<DbSchemaInfo> actual)
    {
   
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


    private static IReadOnlyList<DbSchemaInfo> Version02 => new DbSchemaInfo[]
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


    private static bool IsVersion(IReadOnlyList<DbSchemaInfo> actual, IReadOnlyList<DbSchemaInfo> expected)
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

    public static bool IsVersion02(IReadOnlyList<DbSchemaInfo> actual)
    {
        var expected = DbSchemaInfo.Version02;

        return IsVersion(actual, expected);
    }

    
    
    private static IReadOnlyList<DbSchemaInfo> Version03 => new DbSchemaInfo[]
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
    };
    public static bool IsVersion03(IReadOnlyList<DbSchemaInfo> actual)
    {
        var expected = DbSchemaInfo.Version03;

        return IsVersion(actual, expected);
    }
};