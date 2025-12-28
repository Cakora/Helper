# ADO.NET Helper – Generic SQL Operations

## Feature Index
- Command construction (`DbCommandRequest`, parameter builders, stored procedures)
- Core operations (`Execute*`, `Query*`, readers, buffering, streaming)
- Mapping strategies and column maps
- Bulk insert helpers
- Performance knobs (pooling, retries, streaming, SqlBuilder safety)

Use `IDatabaseHelper` for any provider-neutral SQL scenario. All APIs have sync and async variants and accept `DbCommandRequest` describing SQL text, parameters, timeout, and telemetry labels.

## Building Commands
- Use `DbParameterCollectionBuilder.FromAnonymous(...)` for quick parameter lists.
- For OUT params, use `DbParameterCollectionBuilder.Output(...)`/`InputOutput(...)`/`ReturnValue(...)`.
- Include `CommandType.StoredProcedure` when invoking stored procedures.

## Common Operations
1. **ExecuteNonQuery / ExecuteAsync** – Inserts/updates/deletes.
2. **ExecuteScalar / ExecuteScalarAsync** – Return first column of first row.
3. **ExecuteReader / ExecuteReaderAsync** – obtain a `DbReaderScope` when you need raw `DbDataReader` control.
4. **Query / QueryAsync** – materialize records via mapper delegate.
5. **LoadDataTable / LoadDataSet** – fill `DataTable`/`DataSet` for reporting.
6. **StreamAsync** – sequential `IAsyncEnumerable<T>` to avoid buffering.
7. **Output Parameters** – inspect `DbExecutionResult.OutputParameters`.
8. **Transactions** – create via `ITransactionManager`, pass `Connection`/`Transaction` from `ITransactionScope`.
9. **Savepoints** – call `BeginSavepoint`, `RollbackToSavepoint`, `ReleaseSavepoint` on the scope to guard critical sections.
10. **Mapping** – either supply a custom mapper delegate or call `QueryAsync<T>(request, mapperRequest)` to let the built-in `IRowMapperFactory` pick the right strategy (Reflection/IL/Dictionary/Dynamic/Source-generated). Use `RowMapperRequest` when you need per-call overrides (column map, casing, strategy).
- **Source-generated mapping** – decorate entities with `[GeneratedMapper("MyMapperName")]` and request `MapperStrategy.SourceGenerator` for the fastest property binding (code emitted at compile time).
- **Bulk Inserts** – use provider-specific writers:
  - `SqlServerBulkWriter<T>` + `SqlServerBulkWriterOptions<T>` for `SqlBulkCopy`.
  - `PostgresBulkWriter<T>` for `COPY ... FROM STDIN (FORMAT BINARY)`.
  - `OracleBulkWriter<T>` for array-binding INSERT/PLSQL operations.

## Performance Settings
- Configure `DatabaseOptions.CommandPool` to tune pooling:
  - `EnableCommandPooling` (default true) keeps provider commands alive for reuse.
  - `EnableParameterPooling` (default false) reuses `DbParameter` instances for high-churn workloads.
  - `MaximumRetainedCommands` / `MaximumRetainedParameters` clamp per-provider pool sizes.
- Set `DbCommandRequest.PrepareCommand = true` when you want pooled commands to stay prepared for hot SQL statements.
- Configure `DatabaseOptions.Resilience` for retries/telemetry:
  - `EnableCommandRetries`/`CommandRetryCount`/`CommandBaseDelayMilliseconds` control Polly-based retries for helper calls.
  - `EnableTransactionRetries`/`TransactionRetryCount` manage transaction commits/rollbacks (single-database scopes).
- Use the streaming APIs for large payloads:
  - `StreamColumnAsync/StreamColumn` pipe BLOBs directly into a `Stream` (uses `CommandBehavior.SequentialAccess`).
  - `StreamTextAsync/StreamText` send CLOB/text columns into a `TextWriter` without intermediate allocations.

### Automatic Mapping Example
```csharp
var mapperRequest = new RowMapperRequest
{
    PropertyToColumnMap = new Dictionary<string, string>
    {
        [nameof(CustomerDto.Id)] = "customer_id",
        [nameof(CustomerDto.Email)] = "email_address"
    }
};

var customers = await db.QueryAsync<CustomerDto>(
    new DbCommandRequest { CommandText = "SELECT customer_id, email_address FROM sales.customers" },
    mapperRequest,
    cancellationToken);
```
`RowMapperFactory` is configured via `DbHelperOptions` (defaults: Reflection + case-insensitive matching). Override in `AddDataAccessLayer(..., configureHelper: opts => opts.DefaultMapperStrategy = MapperStrategy.IlEmit);`

## Example
```csharp
var request = new DbCommandRequest
{
    CommandText = "UPDATE Accounts SET Balance = Balance - @Amount WHERE Id = @Id",
    Parameters =
    [
        DbParameterCollectionBuilder.Input("Amount", 50m, DbType.Decimal),
        DbParameterCollectionBuilder.Input("Id", accountId, DbType.Guid)
    ]
};

await db.ExecuteAsync(request, cancellationToken);
```

## Dynamic SQL Builder + SqlSafety
Use `SqlBuilder` when you need dynamic filters/pagination without hand-crafted concatenation. Every interpolation hole becomes a DAL parameter, so user input never touches the SQL string.

```csharp
var extraWhere = search.IncludeBetaUsers ? "u.IsBeta = 1" : "1=1";

var builder = SqlBuilder
    .Select("u.Id", "u.Email", "u.CreatedOn")
    .From("dbo.Users u")
    .Where($"u.ClientId = {clientId}")
    .Where(!string.IsNullOrWhiteSpace(region), $"u.Region = {region}")
    .WhereRaw(SqlSafety.EnsureClause(extraWhere, nameof(extraWhere))) // vetted fragment
    .Paginate(pageNumber: page, pageSize: pageSize);

// Pick an ORDER BY expression from a whitelist to avoid injection vectors.
var allowedSorts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    ["created"] = "u.CreatedOn DESC",
    ["email"] = "u.Email ASC"
};
var sortExpression = allowedSorts.TryGetValue(sortBy ?? string.Empty, out var value)
    ? value
    : allowedSorts["created"];
builder.OrderBy(SqlSafety.EnsureClause(sortExpression, nameof(sortExpression)));

var request = builder.ToCommandRequest(databaseOptions);
var users = await db.QueryAsync<UserDto>(request, cancellationToken: ct);
```

`SqlSafety.EnsureClause` trims and rejects comment/statement terminators (`--`, `/* */`, `;`). Use it whenever you must pass a raw fragment (e.g., feature-flagged filters). Prefer `Where(FormattableString)` for anything involving values; the builder will emit `DbParameterDefinition` entries automatically (including `IN` lists).

**Repository pattern sample**

```csharp
public async Task<IReadOnlyList<AuditEntry>> SearchAsync(
    AuditSearch search,
    CancellationToken cancellationToken)
{
    var builder = SqlBuilder
        .Select("a.Id", "a.EventType", "a.Actor", "a.CreatedUtc")
        .From("security.Audit a")
        .Where($"a.TenantId = {search.TenantId}")
        .Where(search.FromUtc.HasValue, $"a.CreatedUtc >= {search.FromUtc}")
        .Where(search.ToUtc.HasValue, $"a.CreatedUtc <= {search.ToUtc}")
        .Paginate(search.PageNumber, search.PageSize);

    if (!string.IsNullOrWhiteSpace(search.SortBy))
    {
        builder.OrderBy(SqlSafety.EnsureClause(search.SortBy, nameof(search.SortBy)));
    }

    var request = builder.ToCommandRequest(_options);
    var result = await _db.QueryAsync(request, MapAudit, cancellationToken).ConfigureAwait(false);
    return result.Data;
}
```

**Safety Checklist**
- Use interpolated `Where` for anything containing values (builder creates parameters).
- Gate `WhereRaw` / `OrderBy` behind whitelists or call `SqlSafety.EnsureClause` before passing the fragment.
- Never concatenate untrusted strings; if you need complex logic, build it server-side (views, TVFs, stored procs).
- Log `request.TraceName` instead of raw SQL unless diagnostic mode is explicitly enabled.

### Parameter Builder Cheat Sheet
- **Anonymous object (fast & readable)**  
  ```csharp
  var parameters = DbParameterCollectionBuilder.FromAnonymous(new
  {
      Id = accountId,
      Amount = 50m,
      Tags = new[] { "finance", "approved" } // auto TreatAsList
  });
  ```
- **Explicit array (full control over DbType/size/direction)**  
  ```csharp
  var parameters = new[]
  {
      DbParameterCollectionBuilder.Input("Id", accountId, DbType.Guid),
      DbParameterCollectionBuilder.Input("Amount", 50m, DbType.Decimal),
      DbParameterCollectionBuilder.InputList("Tags", new[] { "finance", "approved" }),
      DbParameterCollectionBuilder.Output("Status", DbType.String, size: 16)
  };
  ```
Both produce `DbParameterDefinition` collections that preserve direction, DbType, provider type names, custom converters, etc., so downstream providers behave consistently.

## Stored Procedure with Output
```csharp
var request = new DbCommandRequest
{
    CommandText = "dbo.Transfer",
    Parameters =
    [
        DbParameterCollectionBuilder.Input("SourceId", sourceId, DbType.Int32),
        DbParameterCollectionBuilder.Input("TargetId", targetId, DbType.Int32),
        DbParameterCollectionBuilder.Input("Amount", amount, DbType.Decimal),
        DbParameterCollectionBuilder.ReturnValue()
    ]
}.AsStoredProcedure();

var result = await db.ExecuteAsync(request, cancellationToken);
var statusCode = result.OutputParameters["ReturnValue"];
```
Use `.AsStoredProcedure()` on any request to avoid repeating `CommandType = CommandType.StoredProcedure` boilerplate and keep fluent builders focused on the important bits (parameters, timeouts, etc.).

### Convenience Helpers
For quick stored-proc execution:
```csharp
await db.ExecuteStoredProcedureAsync(
    "dbo.Transfer",
    [
        DbParameterCollectionBuilder.Input("SourceId", sourceId, DbType.Int32),
        DbParameterCollectionBuilder.Input("TargetId", targetId, DbType.Int32),
        DbParameterCollectionBuilder.Input("Amount", amount, DbType.Decimal)
    ],
    cancellationToken);

var result = await db.QueryStoredProcedureAsync(
    "dbo.GetHistory",
    reader => new HistoryDto
    {
        Id = reader.GetInt32(0),
        Amount = reader.GetDecimal(1)
    },
    cancellationToken: cancellationToken);

await using var lease = await db.ExecuteStoredProcedureReaderAsync(
    "dbo.GetRawHistory",
    cancellationToken: cancellationToken);
while (await lease.Reader.ReadAsync(cancellationToken))
{
    // consume DbDataReader directly
}
```

## Telemetry & Redacted Logging

- Set `DbCommandRequest.TraceName` to control what appears in logs/scopes without exposing SQL. When no trace name is supplied the DAL logs `[redacted]` by default.
- Use `services.AddDataAccessLayer(options, helper => helper.Telemetry.IncludeCommandTextInLogs = true);` only in trusted environments when you need to log SQL statements verbatim.
- All helper APIs emit OpenTelemetry spans through `helper.Telemetry.ActivitySourceName` (default `Helper.DataAccessLayer.Database`). Bulk writers participate in the same trace tree, so SqlBulkCopy/COPY/array-bind flows show up next to regular commands.

## Transactions (Sync + Async)
See `docs/transactions.txt` for end-to-end multi-procedure samples showing commit/rollback in both modes.

## Feature Coverage Snapshot
- **ExecuteNonQuery / ExecuteScalar / Query / Stream / DataTable / DataSet / ExecuteReader**: all shipped via `IDatabaseHelper`.
- **Stored Procedures & OUT params**: use `CommandType.StoredProcedure` or the helper wrappers; outputs captured via `DbExecutionResult.OutputParameters`.
- **Parameter helpers**: `DbParameterCollectionBuilder` + `StructuredParameterBuilder` cover input/output/list/tvp/array-bind scenarios.
- **Transactions + Savepoints**: coordinated via `ITransactionManager`/`ISavepointManager`. Multi-db coordination is archived; reintroduce it only if your deployment requires the older behavior (`Archive/MultiDbTransactions/`).
- **Bulk writers & mapper engines**: `SqlServerBulkWriter`, `PostgresBulkWriter`, `OracleBulkWriter`, plus mapper strategies surfaced through `IRowMapperFactory`/`RowMapperRequest` (reflection/IL/dictionary/dynamic/source-gen) and `BulkMapping<T>` projections.
- **Pooling**: command pooling on by default; parameter pooling opt-in via `DatabaseOptions.CommandPool`.
- For complete configuration (pooling/resilience), see `docs/configuration.md` for `appsettings.json` examples.
