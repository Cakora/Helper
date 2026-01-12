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

## Pass 6 — Single-File Helper
- Collapsed the helper into `Core/CleanDatabaseHelper.cs` so sync, async, and streaming flows are readable top-to-bottom.
- Renamed internal concepts to “scopes” and “contexts” to avoid the old lease/rent wording.
- Kept Oracle sync-path fallback and PostgreSQL OUT-parameter emulation inline for clarity.
- Telemetry now sits next to execution, and cleanup runs through the same pipeline for sync/async.

## Pass 4 — Proposed Layout (post-split)
- `Abstractions/` — `ICleanDatabaseHelperAsync`, `ICleanDatabaseHelperSync`.
- `Core/` — `CleanDatabaseHelper.cs` (sync + async + streaming in one file).
- `Facade/` — DI extensions registering either async-only, sync-only, or dual façade.
- `Requests/`, `Telemetry/`, `Providers/` — remain provider-agnostic; add provider-specific shims only under `Providers/`.

## Pass 5 — Refactor Guardrails
- Keep method names and behavior parity with the legacy helper to minimize regression risk.
- Avoid sync-over-async; maintain distinct sync helpers that call sync database APIs.
- Ensure every exit path returns pooled commands and disposes scopes; keep streaming lease disposal paths mirrored between sync/async.
- Current state: interfaces are split and the helper lives in a single file with shared Run/RunAsync helpers; DI still exposes all three interfaces to consumers.

Next action: add parity tests (sync/async + streaming), document inline transaction samples, and verify provider coverage.

## Current Completion Status
- ✅ Interfaces split (`ICleanDatabaseHelperAsync`/`Sync`) with combined façade.
- ✅ CleanDatabaseHelper collapsed into `Core/CleanDatabaseHelper.cs` with sync, async, and streaming flows together.
- ✅ DI exposes all three helper interfaces.
- ✅ Execute*/Query*/Stream shells share a common Run/RunAsync pipeline with inline telemetry and cleanup.
- ✅ Runner abstraction removed; transaction guidance is inline via `ITransactionManager`.
- ✅ Postgres OUT-parameter emulation and Oracle sync-path fallbacks retained.
- ⏭️ Next priority: add inline transaction examples/tests and verify stream disposal/telemetry parity plus provider coverage tests.

## Readability & Debuggability Plan (High Priority)
- Problem: navigation overhead across partials/pipeline types makes the helper hard to follow and debug.
- Goal: reduce cognitive hops, prefer descriptive names over “pipeline/scope”, and keep call graphs shallow.
- Steps:
  1. Collapse helper partials into a single cohesive file with clear sections (Async, Sync, Core helpers) or keep partials but duplicate the “Shared” scaffolding inline so you can read top-to-bottom. **Completed.**
  2. Rename internals with common terminology: `PipelineScope` → `CommandScope`, `StreamingScope` → `ReaderScope`, `ScopeIssued` → `ScopeActive`, `Context` → `CommandContext`.
  3. Inline small helpers where they obscure flow (e.g., `RunExecution*` wrappers) and favor direct calls inside Execute/Query so debugging shows the real stack. **Run/RunAsync remain as the shared core.**
  4. Add a compact “flow” comment block at the top of the helper describing the happy path and error path (one comment, no regions).
  5. Keep telemetry/logging calls adjacent to the work (avoid bouncing to helpers unless shared across sync/async).
  6. Merge folder depth where possible: `Core/{Async,Sync,Shared}` → `Core/` with one file, plus `Infrastructure/` for scope/context types if separation is still desired. **Completed.**
  7. Validate by stepping through a debug session to ensure minimal file hops and clear variable names, then re-run targeted tests.

## Sync/Async Parity Check Plan
- Verify method coverage: every async API has a sync twin with identical semantics (timeouts, behaviors, telemetry).
- Align naming and structure: present sync then async (or vice versa) in the same file to make parity obvious.
- Keep shared helpers minimal: only validation/telemetry/type creation should be shared; execution paths stay inline for readability.
- Add a parity checklist comment in the helper and a doc snippet showing one sync/async pair side by side.
