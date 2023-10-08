## Getting Started
    CREATE USER NQueueUser PASSWORD 'ihSH3jqeVb7giIgOkohX';
    CREATE SCHEMA IF NOT EXISTS nqueue AUTHORIZATION NQueueUser;



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
