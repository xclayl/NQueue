﻿Implementation of the outbox pattern in dotnet.


## Use SQL Server

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



## Use Postgres

```csharp
  services.AddNQueueHostedService((s, config) =>
    {
      var cnnBuilder = new NpgsqlConnectionStringBuilder()
      {
          Host = "localhost",
          Database = "NQueueSample",
          Username = "nqueueuser",
          Password = "ihSH3jqeVb7giIgOkohX",
      };
      cnnBuilder.SslMode = cnnBuilder.Host == "localhost" ? SslMode.Disable : SslMode.VerifyFull;
      config.CreateDbConnection = () => new ValueTask<DbConnection>(new NpgsqlConnection(cnnBuilder.ToString()));
      return default;
    });
```