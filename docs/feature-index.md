# DAL Feature Index

> Need a “how-to”? Start with `docs/usage-guide.md`. The sections below describe what the DAL implements and where to find each feature in the codebase.

## Core Features

### Enable/Disable Guides
- Optional subsystems are opted into via the `configureServices` delegate passed to `AddDataAccessLayer`. Use it to enable telemetry, disable retries, or turn on verbose logging per host. Validation stays on globally; set `DbCommandRequest.SkipValidation = true` when a specific request must bypass validators.
- Deployment playbooks for ADO-only hosts and EF+ADO hybrids live in `docs/dal-usage-modes.md`.

### DbHelper Folder Layout
- `Common/DbHelper/Infrastructure` – connection factory/pool, parameter binder, command factory, and resilience strategy abstractions that every helper uses.
- `Common/DbHelper/Execution`, `Transactions`, `Telemetry`, `Providers`, `Mapping` – purpose-specific helpers layered on top of the infrastructure primitives.
- `Common/DbHelper/EF/*` – Entity Framework helpers (bulk extensions, compiled queries, interceptors) with `EF/Migrations` containing `IMigrationService` + `MigrationService`.
- `Common/DbHelper/Bulk`, `Configuration`, `Validation` – option objects and helper types that can be swapped per application.

### 1. Database Helper (ADO.NET surface)
- **Purpose:** single entry point for commands, scalars, queries, streaming, DataTable/DataSet loads.
- **Key types:** `IDatabaseHelper`, `DatabaseHelper`, `DbCommandRequest`, `DbExecutionResult`, `DbQueryResult<T>`, `DbReaderScope`.
- **Where:** `DataAccessLayer/Common/DbHelper`, `DataAccessLayer/Common/DbHelper/Execution`.

### 2. Transactions
- **Ambient scopes:** `ITransactionManager`, `TransactionScope`, `TransactionScopeAmbient`.
- **Savepoints:** `ISavepointManager` (SQL Server/PostgreSQL/Oracle specific statements).
- **Docs/tests:** `docs/transactions.txt`, `TransactionManagerTests`, `DatabaseHelperIntegrationTests`.

### 3. Bulk Operations
- **Helper:** `IBulkWriteHelper` orchestrates provider engines.
- **Providers:** `SqlServerBulkWriter`, `PostgresBulkWriter`, `OracleBulkWriter`.
- **Mapping primitives:** `BulkMapping<T>`, `BulkColumn`, `BulkOperation<T>`, `BulkOptions`.
- **Docs/tests:** `docs/bulk-operations.md`, `BulkWriteHelperTests`, provider-specific tests.

### 4. Row Mapping & Projection
- `RowMapperFactory`, `RowMapperRequest`, `IRowMapper<T>` handle DTO materialization.
- `DataTableMappingExtensions` projects buffered `DataTable`/`DataSet` results to DTOs without rewriting mappers (see tests `DataTableMappingExtensionsTests`).
- **Perf note:** the extensions preallocate `T[]` via `GC.AllocateUninitializedArray` and use `MemoryMarshal.CreateSpan` to stream mapper output with zero per-row allocations.
- `DbDataReaderMappingExtensions` lets you take an ambient `DbDataReader` (e.g., from `ExecuteReaderAsync`) and map it to lists, dictionaries, or a `DataTable` snapshot on demand.
- `RowMapperRequest.PropertyToColumnMap` keeps provider-specific column names (Oracle uppercase, PostgreSQL snake_case) aligned with DTO properties, and mapper coercion handles type differences such as `NUMBER(1)` -> `bool` or string timestamps -> `DateTime`.
- See `docs/mapping-guide.md` for end-to-end mapping patterns, provider tips, and troubleshooting.
- Provider-specific documentation lives under `docs/providers/<provider>` with ADO + EF guides.
- Register custom `IMappingProfile` implementations via DI when you need centralized value converters (e.g., Oracle `Y/N` -> bool).
- Built-in profiles: Oracle deployments automatically register `OracleBooleanMappingProfile` (NUMBER(1)/CHAR -> bool) and `OracleDateTimeMappingProfile` (DATE/TIMESTAMP -> UTC DateTime/DateTimeOffset); all providers use `EnumMappingProfile` so numeric/string status columns hydrate strongly-typed enums. Add your own profiles to extend/override defaults.
- Bulk projection reuses the same mapping metadata so column names stay centralized.

### 5. EF Core Extensions
- `DataAccessLayer.EF` namespace: `UseAmbientTransaction`, `WithAmbientConnection`, bulk helpers (`WriteSqlServerBulkAsync`).
- Ensures EF contexts share connections/transactions with the DAL primitives.

### 6. Optional / Manual Features
- **Staging/hospital logic:** implemented in CoreBusiness/migration orchestration; DAL provides the plumbing (staging tables remain read/write).
- **Provider-specific workflows:** `CoreBusiness/Workflows` contains the SQL Server/Oracle/PostgreSQL migration workflows. Register them explicitly via `CoreBusiness/DependencyInjection/ServiceCollectionExtensions.Workflows.cs` (`AddMigrationWorkflow` overloads).

## Configuration & Runtime Options

- Global options live in `Shared.Configuration.DatabaseOptions` (provider, connection string, pooling, resilience, diagnostics).
- Per-endpoint options (`MigrationRunnerOptions`) flow through `AddMigrationRunnerServices` (or explicit `EndpointRegistration` helpers).
- `DalServiceRegistrationOptions` (the optional delegate on `AddDataAccessLayer`) controls telemetry, resilience, and detailed logging. Bulk engines, EF helpers, and transactions are always registered; remove their folders to ban them entirely.
- `EndpointRuntimeOptions` exposes runtime values (connection string, timeouts, bulk batch size) via `IOptionsMonitor`.
- ECM EF Core contexts live under `DataAccessLayer/Database/ECM/DbContexts`. `IEcmDbContextFactory` resolves the provider-specific context (`EcmSqlServerDbContext`, `EcmPostgresDbContext`, `EcmOracleDbContext`) while keeping the base API consistent.
- See `docs/configuration.md`, `docs/migration-runner.md`, and `docs/usage-guide.md` for examples.
- `docs/configuration.md` and `docs/usage-guide.md` include ready-made presets for ADO-only, EF-only, and hybrid deployments so you can enable only the features you need.

## Input / Output Reference

### Command Execution (ADO.NET)
- **Input:** `DbCommandRequest` (`CommandText`, `CommandType`, `Parameters`, optional `Connection`/`Transaction`, `OverrideOptions`, `TraceName`).
- **Output:** `DbExecutionResult` – rows affected, scalar value, output parameters dictionary.
- **Sync/Async:** `Execute` / `ExecuteAsync`.

### Query Materialization
- **Input:** `DbCommandRequest` + mapper (`Func<DbDataReader,T>` or `RowMapperRequest`).
- **Output:** `DbQueryResult<IReadOnlyList<T>>`, `DbQueryResult<DataTable>`, `DbQueryResult<DataSet>`.
- **Sync/Async:** `Query`, `QueryAsync`, `LoadDataTable`, `LoadDataTableAsync`, etc.

### Streaming
- **Input:** `DbCommandRequest` + mapper.
- **Output:** `IAsyncEnumerable<T>` yielded via `StreamAsync`.
- **Notes:** Sequential access, allocation-free pipeline (no output parameters).

### Reader Lease
- **Input:** `DbCommandRequest`.
- **Output:** `DbReaderScope` (caller controls reader/command/connection disposal).

### Bulk Insert/Update
- **Input:** `BulkOperation<T>` (mapping, options, override provider) or provider-specific writer options.
- **Output:** `BulkExecutionResult` (rows inserted, batches). Providers also expose `Write`/`WriteAsync`.
- **Shared engines:** The same provider engines back both `IBulkWriteHelper` (ADO) and the EF extensions in `DataAccessLayer.EF.BulkExtensions` (e.g., `WriteSqlServerBulkAsync`). Delete `Common/DbHelper/Bulk` if your deployment must forbid bulk writers.

### EF / LINQ Pipelines
- **Input:** `DbContext` queries + DAL bulk helpers or `UseTransaction`.
- **Output:** Same as bulk inserts or LINQ projections.
- **Docs:** `docs/bulk-operations.md`, `docs/usage-guide.md`.

### Multi-Database Coordination
- This capability is currently archived. Refer to `Archive/MultiDbTransactions/` if you need to restore the previous coordinator.

## Validation & Tests
- `DatabaseHelperIntegrationTests` cover command/query/streaming + transaction reuse.
- `BulkWriteHelperTests` and provider writer tests cover bulk paths.
- `MigrationRunner.Tests` confirm source/destination DI wiring and runtime options.
