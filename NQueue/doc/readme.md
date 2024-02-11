Implementation of the outbox pattern in dotnet.



## Use Postgres

```sql
    CREATE DATABASE my_database;
    -- in the new database:
    CREATE USER nqueueuser PASSWORD 'ihSH3jqeVb7giIgOkohX';
    CREATE SCHEMA IF NOT EXISTS nqueue AUTHORIZATION nqueueuser;
```

```csharp
  services.AddHttpClient();
  services.AddNQueueHostedService((s, config) =>
    {
      var cnnBuilder = new NpgsqlConnectionStringBuilder()
      {
          Host = "localhost",
          Database = "my_database",
          Username = "nqueueuser",
          Password = "ihSH3jqeVb7giIgOkohX",
      };
      cnnBuilder.SslMode = cnnBuilder.Host == "localhost" ? SslMode.Disable : SslMode.VerifyFull;
      config.CreateDbConnection = () => new ValueTask<DbConnection>(new NpgsqlConnection(cnnBuilder.ToString()));
      config.LocalHttpAddresses = s.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>().Addresses.ToList();
      return default;
    });
```

Enqueue your first work item:
```csharp
    private readonly INQueueClient _nQueueClient; // populated by IOC
    public async Task MyMethod() {
        await _nQueueClient.Enqueue(await _nQueueClient.Localhost("/MyEndpoint"));
    }
```

## Use SQL Server

*Warning:* This no longer is working.  If you're interested in maintaining it, please let me know.

```csharp
  services.AddNQueueHostedService((s, config) =>
    {
      config.CreateDbConnection = () =>
      {
          var cnnBuilder = new SqlConnectionStringBuilder
          {
              DataSource = "localhost,15533",
              InitialCatalog = "NQueueSample",
              UserID = "NQueueUser",
              Password = "ihSH3jqeVb7giIgOkohX"
          };
          cnnBuilder.Encrypt = !cnnBuilder.DataSource.StartsWith("localhost");
          return new ValueTask<DbConnection>(new SqlConnection(cnnBuilder.ToString()));
      };   
      return default;
    });
```
