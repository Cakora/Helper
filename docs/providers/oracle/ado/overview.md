# ADO.NET Helper – Oracle Operations

## Feature Index
- Provider capabilities (REF CURSORs, savepoints, array binding, LOB streaming)
- Stored procedure pattern with REF CURSOR output
- Mapping/numeric conversion tips

Provider: `DatabaseProvider.Oracle`

## Capabilities
- Full OUT/INOUT/RETURN value support via `OracleParameter` semantics.
- REF CURSOR handling: call `OracleParameterHelper.RefCursor("cursorName")` to create the output parameter, then consume the `DbDataReader`.
- Savepoints translate to `SAVEPOINT/Rollback` statements (release is implicit on commit).
- Array binding: set `TreatAsList = true` or provide explicit `Values` collections to leverage Oracle array DML.
- LOB/BLOB helpers: use `OracleLobHelper.StreamBlobAsync` / `StreamClobAsync` to read/write large payloads without buffering everything in memory.
- NUMBER conversions: use `OracleTypeConverters.ToInt32/ToInt64/ToDecimal` when you need explicit range validation for Oracle NUMBER columns.
- Boolean conversions: the DAL registers `OracleBooleanMappingProfile` automatically so `NUMBER(1)` / `CHAR(1)` / `Y/N` columns project into CLR `bool` properties without custom code. Add your own `IMappingProfile` if you need different semantics.
- Date/time conversions: `OracleDateTimeMappingProfile` maps Oracle `DATE` / `TIMESTAMP` payloads (including REF CURSORs and DataTables) into UTC `DateTimeOffset` or `DateTime` values when your DTOs expect those types.
- Flexible projections: `MapperStrategy.Dictionary` / `MapperStrategy.Dynamic` (via `DataMapperFactory`) consume REF CURSOR payloads without predefined DTOs.

## Stored Procedure with REF CURSOR
```csharp
var request = new DbCommandRequest
{
    CommandText = "pkg_reports.get_customer_history",
    CommandType = CommandType.StoredProcedure,
    Parameters =
    [
        DbParameterCollectionBuilder.Input("p_customer_id", customerId, DbType.Int32),
        new DbParameterDefinition
        {
            Name = "p_cursor",
            Direction = ParameterDirection.Output,
            DbType = DbType.Object,
            ProviderTypeName = "RefCursor",
            IsNullable = true
        }
    ]
};

var result = await helper.ExecuteAsync(request, cancellationToken);
await using var reader = await helper.StreamAsync(
    request,
    r => new CustomerHistory
    {
        OrderId = r.GetInt32(0),
        Amount = r.GetDecimal(1),
        OrderedOn = r.GetDateTime(2)
    },
    cancellationToken).GetAsyncEnumerator(cancellationToken);
```

## Array Bind Example
```csharp
var request = new DbCommandRequest
{
    CommandText = "pkg_inventory.adjust_bulk",
    CommandType = CommandType.StoredProcedure,
    Parameters =
    [
        OracleParameterHelper.Array("p_ids", ids, DbType.Int32),
        OracleParameterHelper.Array("p_deltas", deltas, DbType.Decimal)
    ]
};

await helper.ExecuteAsync(request, cancellationToken);
```

## Transactions & Savepoints
```csharp
await using var tx = await transactionManager.BeginAsync(options: oracleOptions);
await tx.BeginSavepointAsync("Segment1");
try
{
    await helper.ExecuteAsync(procA with { Connection = tx.Connection, Transaction = tx.Transaction }, cancellationToken);
    await helper.ExecuteAsync(procB with { Connection = tx.Connection, Transaction = tx.Transaction }, cancellationToken);
    await tx.CommitAsync(cancellationToken);
}
catch
{
    await tx.RollbackToSavepointAsync("Segment1");
    await tx.RollbackAsync(cancellationToken);
    throw;
}
```

## REF CURSOR Convenience Helper
```csharp
var request = new DbCommandRequest
{
    CommandText = "pkg_reports.get_customer_history",
    CommandType = CommandType.StoredProcedure,
    Parameters =
    [
        DbParameterCollectionBuilder.Input("p_customer_id", customerId, DbType.Int32),
        OracleParameterHelper.RefCursor("p_cursor")
    ]
};

await using var lease = await helper.ExecuteRefCursorAsync(request, "p_cursor", cancellationToken);
while (await lease.Reader.ReadAsync(cancellationToken))
{
    // map rows directly from the REF CURSOR
}
```
`ExecuteRefCursor`/`ExecuteRefCursorAsync` run the procedure, capture the REF CURSOR output, and return a `DbReaderScope` so you can stream rows without casting to `OracleRefCursor` yourself.

## Conventions
- Oracle normalizes identifiers to uppercase unless quoted; when mapping by name, call `reader.GetOrdinal("COLUMN_NAME")` or normalize property names to uppercase.
- Use `CommandBehavior.SequentialAccess` when streaming large CLOB/BLOB fields to reduce memory usage.
- For REF CURSOR output parameters use `OracleParameterHelper.RefCursor("cursor")`. When array binding, use `OracleParameterHelper.Array` to generate the parameter definition and rely on the driver to convert to Oracle array types.
- When working with dictionary/dynamic projections, pass the row through `OracleColumnNormalizer.Normalize` to map uppercase database identifiers to CLR-friendly casing.
- Global timeout settings live in `DatabaseOptions`: `ConnectionTimeoutSeconds` rewrites the Oracle connection string when the helper/EF context opens connections, and `CommandTimeoutSeconds` becomes the default for every `OracleCommand` unless you override it per request.
- Marking parameters with `TreatAsList = true` (or using `DbParameterCollectionBuilder.InputList`) automatically feeds Oracle array binding, so stored procedures expecting PL/SQL table types receive the values without custom plumbing.

## Coming Later
- `OracleBulkWriter<T>` already performs array binding with configurable batch size; supply parameter names/DbType hints and a value selector to load arrays per execution.
- Direct helpers for `OracleBulkCopy` and pipelined table functions.
- Reflection/IL/source-gen mappers will encapsulate uppercase normalization rules automatically.
- Use `StructuredParameterBuilder.OracleArray` for simple array binding scenarios today.
