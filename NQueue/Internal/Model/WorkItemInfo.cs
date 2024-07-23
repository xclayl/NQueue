using System;

namespace NQueue.Internal.Model
{

    internal record WorkItemInfo(long WorkItemId, string Url, string? Internal);

    internal record WorkItemInfoWithQueueName(long WorkItemId, string Url, string QueueName, string? Internal);

    internal record QueueInfo(string QueueName, DateTimeOffset? LockedUntil);
}