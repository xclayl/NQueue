# NQueue
NQueue *runs background code* in a dotnet ASPNet Core application in a *persistent* way by storing the work items in *SQL Server*.

In contrast to alternatives:
* Kafka - Kafka is more about replicating data, while NQueue "makes sure X happens", where "X" might be "shut down a VM in the cloud".
* Redis Pub/Sub - Redis Pub/Sub is more about ephemeral user notification, while NQueue makes sure every work item executes successfully via polling the DB.
* Hangfire - Hangfire is the closest cousin.  Both persist "jobs" or "work items" in SQL Server and poll for the next item, however NQueue is less general purpose and therefore simpler to debug and fix issues in production environments.

## Getting Started
    CREATE USER [NQueueUser] WITH PASSWORD = 'a_$trong_p4ssword'
    
    GO
    
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE [name] = 'NQueue') EXEC ('CREATE SCHEMA [NQueue]')
    
    GO
    
    ALTER AUTHORIZATION ON SCHEMA::[NQueue] TO [NQueueUser]
    
    GO
    
    GRANT CREATE TABLE TO [NQueueUser]
    
    GO
    
    GRANT CREATE PROCEDURE TO [NQueueUser]
    
    GO

In "ConfigureServices()" add the following code

    services.AddHttpClient();
    services.AddNQueueHostedService((s, config) =>
    {
        config.ConnectionString = new SqlConnectionStringBuilder
        {
            DataSource = "(local)",
            InitialCatalog = "NQueueTest",
            UserID = "NQueueUser",
            Password = "a_$trong_p4ssword"
        }.ToString();

        return Task.CompletedTask;
    });

Enqueue your first work item:

    private readonly INQueueClient _nQueueClient; // populated by IOC
    public async Task MyMethod() {
        await _nQueueClient.Enqueue(new Uri("http://localhost:5332/MyEndpoint"));
    }

In the near future, a GET to http://localhost:5332/MyEndpoint will be made 