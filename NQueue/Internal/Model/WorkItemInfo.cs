using System;

namespace NQueue.Internal.Model
{

    internal record WorkItemInfo(long WorkItemId, string Url, string? Internal);

    internal record WorkItemInfoWithQueueName(long WorkItemId, string Url, string QueueName, string? Internal, int Shard);

    internal record QueueInfo(string QueueName, DateTimeOffset? LockedUntil, string? ExternalLockId, int BlockedByCount, int Shard, int MaxShards);


    public readonly record struct WorkItem(
        long WorkItemId,
        Uri Url,
        string QueueName,
        string? DebugInfo,
        string? Internal,
        int FailCount)
    {
        internal static WorkItem From(WorkItemForTests wi)
        {
            return new WorkItem(wi.WorkItemId,
                wi.Url,
                wi.QueueName,
                wi.DebugInfo,
                wi.Internal,
                0);
        }
    }
    
    
    internal readonly record struct WorkItemForTests(
        long WorkItemId,
        Uri Url,
        string QueueName,
        string? DebugInfo,
        string? Internal,
        int Shard,
        int MaxShards);
}