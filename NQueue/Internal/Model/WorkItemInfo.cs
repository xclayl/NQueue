namespace NQueue.Internal.Model
{

    internal class WorkItemInfo
    {
        public WorkItemInfo(int workItemId, string url)
        {
            WorkItemId = workItemId;
            Url = url;
        }

        public int WorkItemId { get;  }
        public string Url { get; }
    }

}