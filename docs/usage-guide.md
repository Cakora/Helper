# Usage Guide

## Feature Index
- Configure database endpoints (`DatabaseOptions`, `MigrationRunnerOptions`)
- Register DAL/Core services (single provider or source/destination pairs)
- Execute commands with `IDatabaseHelper` (sync + async, scalar/query/stream)
- Insert in bulk via `IBulkWriteHelper` or provider writers
- Wrap work in transactions (`ITransactionManager`, multi-table rollback)
- Optional EF Core integration (`UseTransaction`, `WriteSqlServerBulkAsync`)
- Provider-specific recipes live under `docs/providers` (per-provider ADO + EF guides with sync/async samples).

---

## 1. Configure Providers

### Single provider (`DatabaseOptions`)
```json
{
  "Database": {
    "Provider": "SqlServer",
    "ConnectionString": "Server=.;Database=HelperDb;Trusted_Connection=True;",
    "CommandTimeoutSeconds": 60,
    "ConnectionTimeoutSeconds": 15,
    "WrapProviderExceptions": true
  }
}
```

```csharp
var databaseOptions = builder.Configuration
    .GetSection(DatabaseOptions.SectionName)
    .Get<DatabaseOptions>()
    ?? throw new InvalidOperationException("Missing Database section.");

builder.Services.AddDataAccessLayer(databaseOptions);
builder.Services.AddCoreBusiness();
```

Need to disable validation globally (for smoke tests or load harnesses) or point-in-time? `AddCoreBusiness` exposes an optional configuration callback:

```csharp
builder.Services.AddCoreBusiness(options =>
{
    options.Enabled = builder.Environment.IsProduction(); // enable only in prod
});
```

You can still override validation per-call by passing `forceEnabled` to `IValidationService.Validate/ValidateAndThrow`, so instrumentation code can bypass validation even when it's globally on.

Prefer configuration files? Add a `Validation` section next to your database settings and bind it automatically:

```json
{
  "Validation": {
    "Enabled": true
  }
}
```

```csharp
builder.Services.AddCoreBusiness(builder.Configuration);
```

Need certain rules to run everywhere by default? Extend the JSON:

```json
{
  "Validation": {
    "Enabled": true,
    "DefaultRuleSets": "Strict"
  }
}
```

Now every `IValidationService.Validate/ValidateAndThrow` call automatically runs the `Strict` rule set unless the caller explicitly supplies a different list.

### Provider-specific business workflows

Prefer to keep the provider-to-workflow mapping obvious instead of relying on convention? Wire the workflow explicitly right after `AddCoreBusiness`:

```csharp
builder.Services
    .AddCoreBusiness()
    .AddMigrationWorkflow(databaseOptions);           // reads DatabaseOptions.Provider internally

// Or branch manually without DatabaseOptions:
builder.Services.AddMigrationWorkflow(DatabaseProvider.SqlServer);
```

`AddMigrationWorkflow` exposes a `ServiceLifetime` parameter (defaults to scoped) so you can choose whether the workflow is scoped, singleton, or transient. Each call resolves to one of the built-in workflow classes (`SqlServerMigrationWorkflow`, `PostgresMigrationWorkflow`, `OracleMigrationWorkflow`). If you need a custom workflow, register your own implementation before calling the helper or replace the helper with a direct `services.AddScoped<IMigrationWorkflow, CustomWorkflow>()` line. The key is that the registration now lives in one obvious extension method instead of being buried in unrelated DI code.

### Choose Your DAL Surface

- **ADO-only:** call `builder.Services.AddDataAccessLayer(databaseOptions);`. Stop there if you only need `IDatabaseHelper`, transactions, telemetry, bulk, etc.
- **EF-only:** call `AddDataAccessLayer` (to reuse the shared infrastructure) and then `AddEcmEntityFrameworkSupport(databaseOptions);`. Skip the helper call entirely if you never want the EF surface.
- **Hybrid (default):** call both methods. The helper registers validation/transactions/bulk infrastructure immediately, and you can opt into telemetry, resilience, or detailed logging per host when needed.

See `docs/dal-usage-modes.md` for a deeper walk-through of both ADO-only and EF-with-ADO connection sharing scenarios.

Optional services (telemetry, resilience retries, verbose logging) are controlled via the optional `configureServices` delegate on `AddDataAccessLayer`. All three default to `false`, so flip them only in hosts that truly need the extra behavior. Example:

```csharp
services.AddDataAccessLayer(
    databaseOptions,
    configureServices: dal =>
    {
        dal.EnableTelemetry = true;
        dal.EnableDetailedLogging = environment.IsDevelopment();
    });
```

Validation still runs on every command unless a request explicitly opts out. When a harness needs to bypass validators (for example while replaying intentionally malformed payloads), set `DbCommandRequest.SkipValidation = true` on that single request. The rest of the application continues to benefit from validation.

### Source + Destination (`MigrationRunnerOptions`)
```json
{
  "MigrationRunner": {
    "Source": { "Provider": "SqlServer", "SqlServer": { "Server": "localhost", "Database": "HelperSource" } },
    "Destination": { "Provider": "PostgreSql", "Postgres": { "Host": "localhost", "Database": "HelperDest", "Username": "postgres", "Password": "postgres" } },
    "Bulk": { "BatchSize": 2000 }
  }
}
```

```csharp
var runnerOptions = builder.Configuration
    .GetSection(MigrationRunnerOptions.SectionName)
    .Get<MigrationRunnerOptions>()
    ?? throw new DalConfigurationException("Missing MigrationRunner configuration.");

builder.Services
    .AddMigrationRunnerServices(runnerOptions);
```

`AddMigrationRunnerServices` converts the source/destination JSON into `EndpointRegistration` objects, registers the DAL + EF + CoreBusiness stacks for each provider, and exposes `EndpointRuntimeOptions` (`IOptionsMonitor<EndpointRuntimeOptions>`) so the rest of the app can read connection strings and provider metadata per role. Need to tweak a single endpoint (for example disable EF or customize validation)? Build two `EndpointRegistration` instances manually (optionally passing `configureValidation`) and call the overload that accepts registrations. Configure logging separately via `builder.Logging` or your logging provider of choice.

---

## 2. Execute Commands

### Async
```csharp
var db = scope.ServiceProvider.GetRequiredService<IDatabaseHelper>();

var request = new DbCommandRequest
{
    CommandText = "dbo.GetTodo",
    CommandType = CommandType.StoredProcedure,
    Parameters = DbParameterCollectionBuilder.FromAnonymous(new { Id = todoId }),
    TraceName = "todo.get"
};

DbQueryResult<IReadOnlyList<TodoDto>> result = await db.QueryAsync(
    request,
    reader => new TodoDto(reader.GetGuid(0), reader.GetString(1)),
    cancellationToken);

// Stored procedure with explicit parameter directions
var procRequest = new DbCommandRequest
{
    CommandText = "pkg_orders.process",
    CommandType = CommandType.StoredProcedure,
    Parameters =
    [
        // INPUT parameter with DbType
        DbParameterCollectionBuilder.Input("p_order_id", orderId, DbType.Int32),

        // INPUT collection (e.g., CSV -> TreatAsList)
        new DbParameterDefinition
        {
            Name = "p_tags",
            Values = tagsArray,
            TreatAsList = true,
            ProviderTypeName = "_text" // PostgreSQL array type hint
        },

        // OUTPUT parameter (string)
        DbParameterCollectionBuilder.Output("p_status", DbType.String, size: 32),

        // INPUT/OUTPUT parameter
        DbParameterCollectionBuilder.InputOutput("p_amount", amount, DbType.Decimal, precision: 18, scale: 2),

        // RETURN value
        DbParameterCollectionBuilder.ReturnValue("p_return_code", DbType.Int32),

        // Oracle REF CURSOR (special helper)
        OracleParameterHelper.RefCursor("p_cursor")
    ],
    TraceName = "orders.process"
};

var procResult = await db.ExecuteAsync(procRequest, cancellationToken);
var status = (string?)procResult.OutputParameters["p_status"];
var returnCode = (int?)procResult.OutputParameters["p_return_code"];

> The sample above intentionally mixes every supported parameter direction (input, list/array, output, input/output, return value, REF CURSOR). The binding behavior is regression-tested in `tests/DataAccessLayer.Tests/DbCommandFactoryTests` (`StoredProcedureParameterMatrix_BindsAllDirections`), so you can rely on the same definitions for Oracle, PostgreSQL, and SQL Server stored procedures.
```

### Sync
```csharp
DbExecutionResult execution = db.Execute(new DbCommandRequest
{
    CommandText = "UPDATE TodoItems SET IsCompleted = 1 WHERE Id = @Id",
    Parameters =
    [
        new DbParameterDefinition { Name = "Id", Value = todoId, DbType = DbType.Guid }
    ]
});
Console.WriteLine($"Rows affected: {execution.RowsAffected}");

// Same stored-proc parameter mix, but synchronous
var parameters = new[]
{
    DbParameterCollectionBuilder.Input("p_order_id", orderId, DbType.Int32),
    new DbParameterDefinition
    {
        Name = "p_tags",
        Values = tagsArray,
        TreatAsList = true,
        ProviderTypeName = "_text" // replace with SYS.ODCIVARCHAR2LIST when targeting Oracle
    },
    DbParameterCollectionBuilder.Output("p_status", DbType.String, size: 32),
    DbParameterCollectionBuilder.InputOutput("p_amount", amount, DbType.Decimal, precision: 18, scale: 2),
    DbParameterCollectionBuilder.ReturnValue("p_return_code", DbType.Int32),
    OracleParameterHelper.RefCursor("p_cursor")
};

var storedProcResult = db.ExecuteStoredProcedure("pkg_orders.process", parameters);
var syncStatus = (string?)storedProcResult.OutputParameters["p_status"];
var syncReturnCode = (int?)storedProcResult.OutputParameters["p_return_code"];
```

### Streaming (no buffering)
```csharp
await foreach (var todo in db.StreamAsync(
    new DbCommandRequest { CommandText = "SELECT Id, Title FROM TodoItems" },
    reader => new TodoSummary(reader.GetGuid(0), reader.GetString(1)),
    cancellationToken))
{
    Console.WriteLine(todo.Title);
}
```

- **DataTable / DataSet**

```csharp
// Async DataTable
var tableResult = await db.LoadDataTableAsync(
    new DbCommandRequest { CommandText = "SELECT * FROM Sales.Customers" },
    cancellationToken);

DataTable customers = tableResult.Data;

// Sync DataSet (multiple result sets)
var setResult = db.LoadDataSet(new DbCommandRequest { CommandText = "EXEC dbo.GetCustomersAndOrders" });
DataSet resultSets = setResult.Data;
```

`LoadDataTable`/`LoadDataSet` work for every provider (SQL Server, PostgreSQL, Oracle) and the integration tests cover both sync and async flows. Projection helpers allocate a single contiguous buffer via `GC.AllocateUninitializedArray` and fill it through `MemoryMarshal` spans, so converting buffered results to DTO lists is effectively allocation-free beyond the final array.

Once you have a buffered shape, project it to your DTOs using the mapper factory that is already registered in DI:

```csharp
var mapperFactory = scope.ServiceProvider.GetRequiredService<IRowMapperFactory>();

// DataTable -> List<CustomerDto>
var customers = tableResult.Data.MapRows<CustomerDto>(mapperFactory);

// DataSet -> List<OrderDto> (named table + column overrides)
var orders = resultSets.MapRows<OrderDto>(
    "Orders",
    mapperFactory,
    new RowMapperRequest
    {
        PropertyToColumnMap = new Dictionary<string, string>
        {
            [nameof(OrderDto.Id)] = "OrderId",
            [nameof(OrderDto.Total)] = "LineTotal"
        }
    });
```

`DataTableMappingExtensions` keeps the projection path lightweight (it streams through `DataTable.CreateDataReader()` and reuses whatever mapper strategy you configured—reflection, IL emit, source-generated) so converting to lists is effectively as fast as calling `QueryAsync<T>` directly. See `docs/mapping-guide.md` for deeper coverage of strategies, column maps, and custom delegates.

#### Cross-endpoint user synchronization

The `MigrationRunner` project includes a concrete example that reads users from one provider and upserts them into another without duplicating DAL code:

1. **Shared contract:** `Shared/Entities/UserProfile.cs` defines the fields for both databases. `DataAccessLayer.Database.ECM.Models.Configurations.UserProfileConfiguration` maps it to the `Users` table, so SQL Server, PostgreSQL, and Oracle reuse the same schema.
2. **Provider-specific wiring:** `Program.cs` builds two `EndpointRegistration` objects (source/destination) and calls `builder.Services.AddMigrationRunnerServices(...)`. The helper wires both DAL stacks, their EF support, and exposes `ISourceDbContextFactory` / `IDestinationDbContextFactory`. Each gateway therefore uses the appropriate provider/connection string with no manual branching.
3. **Business logic once:** `MigrationRunner/Infrastructure/UserSynchronizationService.cs` calls `ISourceUserDataGateway.GetUsersAsync`, copies the result, and invokes `IDestinationUserDataGateway.UpsertAsync`. Because the gateways abstract the provider, the service only cares about transformation logic (trim strings, set `IsActive`, etc.).
4. **Hosted workflow:** `MigrationHostedService` resolves `IUserSynchronizationService` after running migrations. A single `dotnet run` migrates both schemas and synchronizes user rows.

To create your own sync, follow the same structure: add a shared entity, expose it from `EcmDbContextBase`, implement a gateway over `IEcmDbContextFactory` (or `IDatabaseHelper` if you prefer straight ADO), and inject the source/destination versions via the endpoint helpers so providers remain declarative.

#### Column aliases & provider-specific conversions

Use `RowMapperRequest.PropertyToColumnMap` when provider columns differ from your DTO property names (Oracle uppercase, PostgreSQL snake_case, etc.). The mapper handles type coercion (`NUMBER(1)` -> `bool`, string timestamps -> `DateTime`) via `Convert.ChangeType`, so you rarely need manual casts.

```csharp
var mapperRequest = new RowMapperRequest
{
    PropertyToColumnMap = new Dictionary<string, string>
    {
        [nameof(CustomerDto.Id)] = "CUSTOMER_ID",
        [nameof(CustomerDto.IsPreferred)] = "IS_PREFERRED"
    }
};

var customers = tableResult.Data.MapRows<CustomerDto>(mapperFactory, mapperRequest);
```

Need special logic (e.g., Oracle stores `Y/N`)? Supply a delegate mapper for that query or projection:

```csharp
var customers = await db.QueryAsync(
    request,
    reader => new CustomerDto
    {
        Id = reader.GetInt32(0),
        IsPreferred = reader.GetString(3) == "Y",
        CreatedUtc = reader.GetDateTime(4).ToUniversalTime()
    });
```

### DbDataReader materialization (class, dictionary, DataTable)

Sometimes you need raw control over the reader (e.g., to stream multiple result sets manually). Lease the reader and use the `DbDataReaderMappingExtensions` helpers:

```csharp
await using var lease = await db.ExecuteReaderAsync(
    new DbCommandRequest { CommandText = "SELECT Id, Name FROM Sales.Customers" },
    cancellationToken);

var mapperFactory = scope.ServiceProvider.GetRequiredService<IRowMapperFactory>();

// Class list (reuses mapper strategy + column maps)
var customers = lease.Reader.MapRows<CustomerDto>(mapperFactory);

// Dictionary rows (column name -> value)
var dictionaryRows = lease.Reader.MapDictionaries(mapperFactory);

// Clone the current result set into a DataTable
var customerTable = lease.Reader.ToDataTable("CustomersSnapshot");
```

Each helper consumes the remaining rows in the active result set, so request a new reader (or use `LoadDataTable*`/`Query*`) if you need multiple shapes.

---

### Command pooling lifecycle

Most apps never interact with `IDbCommandFactory` directly, but it is helpful to understand how the DAL names the lifecycle hooks now that we replaced the old “rent/lease” verbs:

- `GetCommand(DbConnection, DbCommandRequest)` and `GetCommandAsync(...)` return a pooled `DbCommand` that has already been configured (connection, transaction, command text, type, timeout, and parameters). The async overload only awaits `PrepareAsync`, so the cancellation token is forwarded to the underlying provider if you opt into prepared statements.
- `ReturnCommand(DbCommand)` must be called once the command is finished executing. The helper and `DbReaderScope` do this for you automatically; call it yourself only when you bypass the helper and orchestrate commands manually.
- When command pooling is enabled, returning a command clears parameters and places it back into the pool so the next caller can reuse both the command and its parameter objects. When pooling is disabled the factory disposes the command after calling `ReturnCommand`.

If you need to hook into the pool directly (for custom diagnostics, profiling, etc.) the pattern is:

```csharp
var command = commandFactory.GetCommand(connection, request);
try
{
    await command.ExecuteNonQueryAsync(cancellationToken);
}
finally
{
    commandFactory.ReturnCommand(command);
}
```

Following this pattern keeps the pool healthy and avoids the “where was this command registered?” confusion noted in previous iterations.

---

### Switching providers at runtime

When a host needs to talk to different providers (SQL Server locally, Oracle in the cloud, etc.) you no longer have to rewire Startup. Bind the `ActiveDataSource` section once, and pass that object to whichever helper you need:

```csharp
var active = configuration.GetSection("ActiveDataSource").Get<ActiveDataSourceOptions>()
    ?? throw new InvalidOperationException();

services.AddDataAccessLayer(active);             // ADO pipeline
services.AddEcmEntityFrameworkSupport(active);   // Optional EF helpers sharing the same provider
```

`ActiveDataSourceOptions` accepts either a raw connection string or the provider-specific profile (`SqlServer`, `Postgres`, or `Oracle`). The DAL builds the correct `DatabaseOptions` on the fly, so “active provider” can be flipped per environment without touching the calling code. This works equally well for ADO-only workloads and EF-assisted scenarios.

---

## 3. Bulk Inserts

### Helper
```csharp
var mapping = BulkMapping
    .ForTable("dbo.Customers")
    .Columns(
        BulkColumn.Create("CustomerId", row => row.Id, isKey: true),
        BulkColumn.Create("Name", row => row.Name),
        BulkColumn.Create("CreatedUtc", row => row.CreatedUtc));

var operation = new BulkOperation<CustomerRow>(mapping, new BulkOptions
{
    BatchSize = 1000,
    OverrideOptions = destinationEndpoint.Database
});

await bulkWriteHelper.ExecuteAsync(operation, customers, cancellationToken);
```

### EF + LINQ
```csharp
var pending = await dbContext.Orders
    .Where(o => o.Status == OrderStatus.Pending)
    .Select(o => new OrderRow(o.Id, o.Amount, o.CreatedUtc))
    .ToListAsync(cancellationToken);

await bulkWriteHelper.ExecuteAsync(operation, pending, cancellationToken);
```

See `docs/bulk-operations.md` for provider-specific options and sync alternatives.

---

## 4. Transactions (single or multi-table)

```csharp
await using var scope = await transactionManager.BeginAsync(cancellationToken: cancellationToken);

try
{
    await db.ExecuteAsync(cmd1 with { Connection = scope.Connection, Transaction = scope.Transaction }, cancellationToken);
    await db.ExecuteAsync(cmd2 with { Connection = scope.Connection, Transaction = scope.Transaction }, cancellationToken);
    await scope.CommitAsync(cancellationToken);
}
catch
{
    await scope.RollbackAsync(cancellationToken);
    throw;
}
```

- Set `TransactionScopeOption.RequiresNew` / `Suppress` as needed.
- Bulk writers respect the same scope (pass the scope’s connection/transaction to the writer when you construct it).
- Multi-database coordination is currently archived. If you need the previous coordinator, see `Archive/MultiDbTransactions/` and re-enable it intentionally.

See `docs/transactions.txt` for advanced scenarios (savepoints, archived multi-db notes, EF `UseTransaction`).

---

## 5. EF Core Integration

- `DbContext.Database.UseTransaction(scope.Transaction)` shares the DAL transaction with EF.
- `WithAmbientConnection` extension (in `DataAccessLayer.EF.DbContextExtensions`) lets you map EF queries to the same scope without reopening connections.
- `WriteSqlServerBulkAsync` delegates to the DAL bulk infrastructure so EF migrations/batch jobs can bulk insert without switching APIs.
- Cross-database orchestration is currently disabled; consult `Archive/MultiDbTransactions/` if you need to restore the old coordinator.
