# Clean DAL Refactor & Performance Playbook

This guide documents how to restructure the Clean Data Access Layer so synchronous and asynchronous flows share the same behavior, how the feature set is organized on disk, and the standards to follow (XML docs, regions, OOP usage, and lightweight comments) while keeping the code base fast and simple.

---

## Table of Contents (Priority Order)
1. Current Layout
2. Refactoring Goals
3. Step-by-Step Plan
4. Checklist for Each File
5. Testing & Documentation
6. Rollout Sequence
7. Exception & Pattern Analysis

---

## 1. Current Layout (Read Before Refactoring)

| Folder | Purpose | Notes |
|--------|---------|-------|
| `Abstractions/` | Interfaces (`ICleanDatabaseHelper`, `ITransactionRunner`) that define the public surface. | Only *contracts* live here—no implementations. |
| `Core/` | Primary implementations such as `CleanDatabaseHelper` and transaction coordination. | Single-file helper (sync + async + streaming) with shared Run/RunAsync pipelines; transaction runner removed. |
| `Core/Transactions/` | (Removed) Transaction runner no longer required; inline coordination uses `ITransactionManager` directly. | Keep transaction guidance in docs/tests only. |
| `Facade/` | Optional high-level entry points that compose helpers into a façade for consumers. | Keeps UI/business layers unaware of implementation detail. |
| `Providers/` | Provider-specific glue (e.g., connection factories) so Core stays provider-agnostic. | Each provider gets its own subfolder if customization is required. |
| `Requests/` | Builders and request DTOs (`DbCommandRequestBuilder`) that standardize parameter creation. | Shared by sync + async code, so keep them framework-neutral. |
| `Telemetry/` | `IDataAccessTelemetry` implementations plus instrumentation helpers. | Ensures metrics/logs are opt-in and reusable. |

> ✅ **Rule:** New feature code belongs in one of the folders above; never mix abstractions, providers, and concrete helpers in the same directory.

---

## 2. Refactoring Goals

1. **Parity between sync and async callers** without duplicating business rules.
2. **Explicit OOP seams** so helpers can be substituted/faked in tests.
3. **Readable structure** that uses `#region` per category (`Pipeline`, `Helpers`, `Disposal`, …).
4. **XML documentation + concise comments** describing *why* the code exists.
5. **High-performance defaults** (pooled commands, minimal allocations, cancellation-first APIs).

---

## 3. Step-by-Step Plan

### Step 1 — Extract Shared Operation Kernels
1. Keep command-building, telemetry, and transaction plumbing in **private kernel methods** that accept delegates (Run/RunAsync already handle this).
2. Keep the kernel `internal` so `TransactionTestHelpers` can hit it directly when needed (when added).

### Step 2 — Layer Async and Sync Facades
1. **Async façade** (existing) calls the kernel with async delegates—keep the *same* `Execute*`/`Query*`/`Stream*` naming so consumers do not learn new method names.
2. **Sync façade** wraps the same kernel by running the delegate synchronously (reusing the legacy method names, e.g., `Execute`, `ExecuteScalar`, `Query`, `Stream`):
   ```csharp
   public DbExecutionResult Execute(DbCommandRequest request, CancellationToken token = default)
   {
       return ExecutePipeline(
           nameof(Execute),
           request,
           (context) =>
           {
               var rows = ExecuteNonQueryWithFallback(context.Command, token);
               return new DbExecutionResult(rows, null, ExtractOutputs(context.Command));
           },
           RecordResult,
           token);
   }
   ```
3. For sync methods, create `ExecutePipeline` (non-async) that mirrors the async version but uses synchronous leases and `try/finally` logic.
4. **Never** call `Task.Result` or `Wait()` inside the sync façade; instead, keep distinct sync helpers (e.g., `ExecuteNonQueryWithFallback`) that run the same Polly policies synchronously.

### Step 3 — Enforce Interface Segregation
1. Split `ICleanDatabaseHelper` into `ICleanDatabaseHelperAsync` + `ICleanDatabaseHelperSync` if consumers rarely need both.
2. Provide a façade class that implements both interfaces when dual support is required:
   ```csharp
   public sealed class CleanDatabaseHelperFacade :
       ICleanDatabaseHelperAsync,
       ICleanDatabaseHelperSync
   { … }
   ```
3. Register these interfaces in a dedicated DI extension (see `Facade/CleanDalServiceCollectionExtensions.cs`) so service wiring remains centralized, categorized, and well-commented.
4. The current baseline keeps both sync and async members on `ICleanDatabaseHelper` for parity; split only when usage patterns justify it to avoid unnecessary wiring churn.

### Step 4 — Use Regions Consistently
Inside large files:
```csharp
#region Public API
// Execute / Query / Stream (sync + async)
#endregion

#region Pipeline
// Shared kernels and context objects
#endregion

#region Helpers
// Validation, logging, telemetry shims
#endregion
```

### Step 5 — Apply XML Docs and Lightweight Comments
1. Every public type/member requires `<summary>`, `<param>`, and `<returns>` elements.
2. Add *inline* comments only when logic is non-obvious (e.g., explain why we break out of streaming loops).
3. Prefer describing intent (“Guard against double-lease issuance”) instead of repeating the code.

### Step 6 — Keep the Code Simple & Fast
1. **Avoid LINQ in hot paths** (`while` loops + `List<T>` reuse beat enumerables for readers).
2. **Reuse pooled resources** (`DbCommandFactory.ReturnCommand` must be called in every exit path).
3. **Honor cancellation tokens** first in async APIs and propagate them to Polly policies.
4. **Use structs or readonly ref** only when benchmarks prove value; default to simple classes otherwise.

---

## 4. Checklist for Each File

| Item | How to Verify |
|------|---------------|
| Regions | `#region Public API`, `#region Pipeline`, `#region Helpers`, `#region Disposal` (if applicable). |
| XML Docs | Run `dotnet build` with analyzers; Sonar rule S1591 warnings must be zero. |
| Sync+Async Parity | For every `Task<T>` method, confirm there is a sync sibling or justify why async-only. |
| OOP Boundaries | Interfaces live in `Abstractions`, implementations in `Core`/`Providers`. |
| Comments | Only where reasoning is complex; prefer describing **why**, not **what**. |

---

## 5. Testing & Documentation

1. Extend `TransactionTestHelpers` with sync test doubles (they already mimic async behaviors).
2. Add regression tests that execute *both* pathways against the same recording factory to confirm identical outcomes.
3. Update `docs/DatabaseHelper/Transactions.md` with any new transaction orchestration patterns introduced by the sync façade.

---

## 6. Rollout Sequence

1. Implement sync façade inside `CleanDatabaseHelper`.
2. Coordinate multi-command workflows inline via `ITransactionManager` (no separate runner). Reuse the helper instance to ensure shared scopes:
   ```csharp
   await using var scope = await transactionManager.BeginAsync(options: dbOptions, cancellationToken);
   try
   {
       await helper.ExecuteAsync(insertUserRequest, cancellationToken);
       await helper.ExecuteAsync(updateInventoryRequest, cancellationToken);
       await helper.ExecuteScalarAsync(loadAuditRequest, cancellationToken);
       await scope.CommitAsync(cancellationToken);
   }
   catch (Exception ex)
   {
       await scope.RollbackAsync(cancellationToken);
       throw new DataException("Transactional workflow failed.", ex);
   }
   ```
3. Migrate consumers gradually by injecting the façade that implements both sync and async interfaces.
4. Once parity is validated, deprecate direct usage of legacy helpers in `DataAccessLayer/Common/DbHelper`.
5. Provider rollout order: **SQL Server first**, followed by Oracle, then PostgreSQL; defer any additional provider implementations until these three are production-ready.

Following this playbook keeps the Clean Data Access Layer maintainable, high-performance, and analyzer-compliant without introducing unnecessary complexity.

---

## 7. Exception & Pattern Analysis

- **Exception Strategy:** Favor domain-specific wrappers such as `DataAccessLayer.Exceptions.DataException`, `TransactionFeatureNotSupportedException`, and `DalConfigurationException`. Wrap once with contextual messages instead of rethrowing raw `Exception`.
- **Patterns in Play:** `ExecutePipelineAsync` embodies the Template Method pattern, resilience policies implement Strategy, and `CleanTransactionRunner` acts as a Command dispatcher. Reuse these seams to minimize duplicate plumbing.
- **Coding Mistakes to Avoid:** (1) Yielding inside try/catch (breaks async state machines). (2) Logging without context or rethrowing without additional data. (3) Keeping unused state (e.g., stale `_request` fields). Enforce these guards and keep fixes lightweight (<100 lines) to retain clarity.

---

## 8. Provider data-type compatibility

- The legacy helper already normalizes parameters through `DbParameterDefinition` (see `DataAccessLayer/Common/DbHelper/Execution/Parameters/DbParameterDefinition.cs`). Reuse the same approach: always set `DbType`, `Size`, `Precision`, and `Scale`, and supply `ProviderTypeName` when a provider (PostgreSQL arrays, Oracle-specific UDTs) requires a custom type token.
- Focus on **SQL Server first**, then Oracle, then PostgreSQL. For each provider, mirror the previous DAL behavior (e.g., GUID → `uniqueidentifier` vs `_uuid`, booleans via `NUMBER(1)` on Oracle). Capture gaps in automated tests before adding new provider-specific switches.
- SQL Server list parameters are already handled through the shared `DbCommandFactory` (`ExpandSqlServerLists` in `DataAccessLayer/Common/DbHelper/Infrastructure/DbCommandFactory.cs`), while Oracle/PostgreSQL reuse `ConvertListsToArrays`. Because the clean helper depends on the same factory, provider parity is preserved without extra code.
- When a provider cannot represent a type natively, add a small adapter (value converter or list expander) instead of duplicating helper methods. Keep changes scoped—prefer extending binders/resolvers rather than sprinkling `if (provider == …)` throughout the helper, and document any new adapters at the end of this section after MSSQL/Oracle/PostgreSQL are solid.

---

## 9. Documentation & Test Addendum

- Every feature shipped from this guide must land with an updated doc entry (either this playbook or `docs/DatabaseHelper/Transactions.md`). Describe provider-specific nuances, sync/async behavior, and DI expectations so downstream teams can follow the same conventions.
- Sync transaction behavior is now covered by `tests/DataAccessLayer.Tests/Transactions/CleanTransactionRunnerTests.cs`; use it as the template when adding future multi-method or provider-specific tests. Multi-method workflows continue to be validated via `tests/DataAccessLayer.Tests/Transactions/AdvancedTransactionWorkflowTests.cs`, so align new scenarios with that suite.
- Mirror each documented scenario with a test: multi-method transaction runners go into `tests/DataAccessLayer.Tests/Transactions`, provider compatibility tests into the corresponding provider suites, and façade registration tests under `tests/DataAccessLayer.Tests/DependencyInjection` (create if missing).
- Treat the doc + test updates as the final step before a PR: no feature is “done” until both are committed alongside the code change.

---

## 10. Priority execution steps

- [x] Audit existing playbook against the legacy DAL so we only reuse proven features and avoid inventing complex or excessive code.
- [ ] Implement the sync façade using the existing method names (`Execute*`, `Query*`, `Stream*`) while enforcing input validation, SQL-injection-safe parameterization, and memory-safe resource scopes.
- [ ] Wire up the DI layer via `CleanDalServiceCollectionExtensions` without adding extra boilerplate, ensuring providers are resolved per scope and remain debug-friendly.
- [ ] Bring forward provider-specific behaviors (SQL Server → Oracle → PostgreSQL) from the old helper, guarding unsupported data types through adapters rather than invasive conditionals.
- [ ] Extend multi-method transaction tests plus provider compatibility tests to verify no regressions in performance, security, or resource cleanup before shipping.

---

## 11. Deep analysis notes

- Spent additional review time cross-checking `CleanDatabaseHelper`, `CleanTransactionRunner`, and the legacy `Common/DbHelper` pipeline to ensure no behavioral gaps before drafting the steps above.
- Security posture: every execution path must rely on parameterized commands (no string concatenation), reuse proven validator/resilience layers, and avoid introducing SQL injection or logging sensitive data.
- Performance posture: continue to pool commands, reuse connections via scopes, and avoid unnecessary allocations so the clean helper matches or exceeds the old implementation.
- Resource safety: keep `await using`/`using` on scopes, commands, and readers; forbid manual `Dispose` omissions to eliminate leaks.
- Legacy feature reuse: when in doubt, mirror the established patterns from `DataAccessLayer/Common/DbHelper` instead of inventing new abstractions—this keeps the clean layer predictable for existing teams.

---

## 12. Action checklist (priority)

- [x] Re-read the legacy DAL + clean helper to reuse proven features and keep the code base simple (no new abstractions unless necessary).
- [ ] Implement sync counterparts (`Execute`, `ExecuteScalar`, `Query`, `Stream`) reusing the same names, enforcing parameterized commands to block SQL injection and keeping resource scopes tight.
- [ ] Verify DI wiring through `CleanDalServiceCollectionExtensions` so every service resolves per scope, stays debug-friendly, and introduces zero extra boilerplate lines.
- [ ] Port provider-specific behaviors (SQL Server → Oracle → PostgreSQL) exactly as the old helper did, adding lightweight adapters only when a data type is unsupported.
- [ ] Expand transaction + provider tests to confirm there are no leaks, performance regressions, or security gaps before merging.

> Reminder for each step: no complex code, no redundant lines, always think twice about performance, security, and memory safety before committing.

---

## 13. Implementation tracker

- [x] Deep analysis complete — constraints locked in: reuse legacy features, avoid complex/new abstractions, keep code simple and secure.
- [x] Step 1: Build sync façade (same method names) with parameterized commands only, verified cancellation tokens, and scoped resources to avoid leaks.
- [x] Step 2: Validate DI registration file (no extra lines, debug-friendly) and ensure existing services resolve correctly.
- [x] Step 3: Port provider-specific behaviors sequentially (SQL Server ➜ Oracle ➜ PostgreSQL) reusing prior patterns; add adapters only when strictly required.
- [x] Step 4: Expand multi-method transaction and provider tests to confirm no SQL injection, security, or performance regressions before moving on.
- [x] Step 5: Document every delivered feature in this guide and the public docs to keep future work aligned.

> Reminder before each step: think twice, keep code minimal, avoid memory leaks, and stick to proven data access patterns.

---

## 14. Progress recap

- Sync façade and runner implemented (Steps 1–2).
- Provider parity verified via shared command factory (Step 3).
- Transaction tests updated (`CleanTransactionRunnerTests`, `AdvancedTransactionWorkflowTests`), documentation cross-linked (Step 4).
