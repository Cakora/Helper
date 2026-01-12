# Clean DAL Analysis (Iteration Log)

This log captures incremental passes over the current clean DAL to prep a refactor that separates sync/async flows, removes duplication, and keeps provider support (SQL Server, Oracle, PostgreSQL) intact.

## Pass 1 — Structure & Surface
- `ICleanDatabaseHelper` now mixes sync and async members; splitting into `ICleanDatabaseHelperAsync`/`ICleanDatabaseHelperSync` (with an optional façade that implements both) would simplify consumer intent and DI wiring.
- `CleanDatabaseHelper` houses both flows in a single file; consider partials or separate classes under `Core/Async` and `Core/Sync` to keep hot-path changes small and focused.
- `CleanTransactionRunner` is cohesive but only depends on the combined helper interface; once split, keep overloads that accept either or both interfaces to reduce coupling.

## Pass 2 — Duplication & Pipelines
- Execute/ExecuteScalar/Query (sync and async) repeat the same pattern; extract small private helpers that accept a delegate to reduce six near-identical code blocks and enforce telemetry/logging consistency.
- Streaming paths are well-isolated via `PipelineLease`, but the async streaming block lives far from the async section; grouping streams with their sync/async cohorts will improve readability after the split.
- Resilience helpers (`Execute*WithFallback*`) are already shared; keep them in a dedicated partial or helper to avoid scattering Polly policy usage across files.

## Pass 3 — Resource & Provider Safety
- Pipeline disposal is correct when the lease isn’t issued; streaming relies on callers to exhaust/Dispose the returned enumerable. Add guardrails (e.g., analyzer notes or docs) to avoid leaking scoped connections when a consumer forgets to dispose the streaming enumerator.
- Transaction application is scope-first, request-second; document that provider-specific transaction objects are honored to prevent mismatched `DbConnection`/`DbTransaction` combos (especially for Oracle/PostgreSQL).
- Command creation uses the shared factory, so provider-specific behaviors (list expansion, type mapping) remain centralized. Preserve this by keeping provider glue in `Providers/` instead of branching inside the helper.

## Pass 4 — Proposed Layout (post-split)
- `Abstractions/` — `ICleanDatabaseHelperAsync`, `ICleanDatabaseHelperSync`, `ITransactionRunner`.
- `Core/Async/` — `CleanDatabaseHelper.Async.cs` (async public API + async pipeline hooks).
- `Core/Sync/` — `CleanDatabaseHelper.Sync.cs` (sync public API + sync pipeline hooks).
- `Core/Shared/` — pipeline/context/lease types, validation, resilience helpers, telemetry/logging helpers.
- `Transactions/` — `CleanTransactionRunner` (optionally partial if async/sync paths diverge).
- `Facade/` — DI extensions registering either async-only, sync-only, or dual façade.
- `Requests/`, `Telemetry/`, `Providers/` — remain provider-agnostic; add provider-specific shims only under `Providers/`.

## Pass 5 — Refactor Guardrails
- Keep method names and behavior parity with the legacy helper to minimize regression risk.
- Avoid sync-over-async; maintain distinct sync helpers that call sync database APIs.
- Ensure every exit path returns pooled commands and disposes scopes; keep streaming lease disposal paths mirrored between sync/async.
- Current state: interfaces are split and the helper is partial across Async/Sync/Shared files; DI now exposes all three interfaces to consumers.

Next action: decide on interface split and partial class layout, then implement the shared pipeline extraction to eliminate repeated Execute*/Query* shells without increasing overall line count.

## Current Completion Status
- ✅ Interfaces split (`ICleanDatabaseHelperAsync`/`Sync`) with combined façade.
- ✅ CleanDatabaseHelper partitioned into `Async.cs`, `Sync.cs`, and shared pipeline/helpers.
- ✅ DI exposes all three helper interfaces.
- ✅ Execute*/Query* shells deduplicated through shared sync/async helpers while keeping pipeline logic centralized.
- ⏭️ Next priority: validate disposal/telemetry parity for streams and run clean build/tests after removing stale obj/bin to clear duplicate assembly attributes.
