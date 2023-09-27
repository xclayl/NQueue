# NQueue
NQueue *runs background code* in a dotnet ASPNet Core application in a *persistent* way by storing the work items in *SQL Server*.

In contrast to alternatives:
* Kafka - Kafka is more about replicating data, while NQueue "makes sure X happens", where "X" might be "shut down a VM in the cloud".
* Redis Pub/Sub - Redis Pub/Sub is more about ephemeral user notification, while NQueue makes sure every work item executes successfully.
* Hangfire - Hangfire is the closest cousin.  Both persist "jobs" or "work items" in SQL Server and poll for the next item, however NQueue is less general purpose and therefore simpler to debug and fix issues in production environments.
 