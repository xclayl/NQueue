
# SQL Server implementation is unmaintained
If you are interested in maintaining it, please let me know.


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

        return default;
    });

Enqueue your first work item:

    private readonly INQueueClient _nQueueClient; // populated by IOC
    public async Task MyMethod() {
        await _nQueueClient.Enqueue(new Uri("http://localhost:5332/MyEndpoint"));
    }

In the near future, a GET to http://localhost:5332/MyEndpoint will be made