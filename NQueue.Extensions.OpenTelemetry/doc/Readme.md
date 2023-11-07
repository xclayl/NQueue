Program.cs file


```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(b =>
    {
        b
            ...
            .AddNQueueSource()
            ...;
    });
```
