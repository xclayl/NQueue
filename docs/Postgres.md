## Getting Started
    CREATE DATABASE nqueue_test;
    -- in the new database:
    CREATE USER nqueue_user PASSWORD 'ihSH3jqeVb7giIgOkohX';
    CREATE SCHEMA IF NOT EXISTS nqueue AUTHORIZATION nqueue_user;



In "ConfigureServices()" add the following code

    services.AddHttpClient();
    services.AddNQueueHostedService((s, config) =>
    {
        var cnnBuilder = new NpgsqlConnectionStringBuilder()
        {
            DataSource = "localhost",
            InitialCatalog = "nqueue_test",
            UserID = "nqueue_user",
            Password = "a_$trong_p4ssword"
        };
        cnnBuilder.SslMode = cnnBuilder.Host == "localhost" ? SslMode.Disable : SslMode.VerifyFull;
        config.CreateDbConnection = () => new ValueTask<DbConnection>(new NpgsqlConnection(cnnBuilder.ToString()));

        return default;
    });

Enqueue your first work item:

    private readonly INQueueClient _nQueueClient; // populated by IOC
    public async Task MyMethod() {
        await _nQueueClient.Enqueue(new Uri("http://localhost:5332/MyEndpoint"));
    }

In the near future, a GET to http://localhost:5332/MyEndpoint will be made
