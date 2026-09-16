# Changelog

All notable changes are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While pre-1.0, a **minor** version may contain breaking changes.

## [0.1.3] - 2026-09-16

### Added

- `PostgresRunStoreOptions.ensureSchema` (default `true`). Set it to `false` where
  a migration role creates the run tables and the application role may not run
  DDL: a least-privilege role with only `SELECT`/`INSERT`/`UPDATE`/`DELETE` cannot
  run `CREATE TABLE` or `CREATE INDEX`, so the store must not attempt `migrate()`.

## [0.1.2] - 2026-09-16

Publish to GitHub Packages. The published name is scoped to the owner, so the
import specifier changes.

### Changed

- **Breaking:** the package is now `@elderengineer/pg-workflow` (was
  `pg-workflow`), required by the GitHub Packages npm registry. Update imports
  and the `.npmrc` registry entry accordingly.
- `repository.url` corrected to the canonical HTTPS URL; GitHub rejects a
  publish whose `repository` does not match the repository.
- `Release` now publishes the tarball to GitHub Packages (`npm.pkg.github.com`)
  before attaching it to the GitHub release.

## [0.1.1] - 2026-09-15

Dev toolchain refresh. No runtime changes.

### Changed

- Development dependencies: jest 29 → 30, eslint 9 → 10, TypeScript 5 → 6,
  `@types/node` 20 → 26, `@types/jest` 29 → 30,
  `eslint-config-prettier` 9 → 10, `actions/checkout` 4 → 7,
  `actions/setup-node` 7.
- TypeScript stays on v6 (not v7): `ts-jest@29` requires `typescript <7` and
  `typescript-eslint@8` requires `<6.1`. Dependabot is configured to ignore
  `typescript >= 7` until both support it.
- Added explicit `rootDir: "."` to `tsconfig.json`, required by TypeScript 6
  with declaration emit on (TS5011 under ts-jest).

## [0.1.0] - 2026-09-15

The first release. Breaking by nature — it is the initial extraction.

### Added

- **Durable run state.** `RunStore` with `InMemoryRunStore` (tests/dev) and
  `PostgresRunStore` (tables `pg_workflow_run`, `pg_workflow_step`, prefix
  overridable). `engine.trigger` returns the run id.
- **Idempotency ledger.** A step's outgoing hops are recorded _before_ it
  publishes them, so a redelivered job re-forwards instead of re-running the
  step body.
- **Versioned workflows.** `register(workflow, { version })`; a run dispatches to
  the version it started on, so a rolling deploy cannot mix step graphs.
- **Run cancellation and retention.** `engine.cancelRun(runId)` and
  `store.prune({ olderThanDays })`.
- **Optional transactional steps.** `workOptions: { transactional: true }` runs a
  step inside the job's database transaction; run-state writes join it, and
  `context.transaction` exposes it to steps so database side effects commit or
  roll back with the step.
- **Admin CLI.** `pg-workflow-admin.mjs`: `stats`, `show`, `retry`,
  `reschedule`, `cancel`, `redrive`, `run`, `prune`.
- **Design notes and benchmarks.** `docs/` (architecture, context, cluster,
  debugging, pg-boss internals, examples, intro) and `bench/` with the
  state-in-run-row vs state-in-message measurement.
- **CI.** Build, lint, format, unit tests and Postgres integration on every
  push/PR, plus Dependabot.
- **Release tooling.** A tag-driven GitHub release workflow (`Release`) that runs
  the suite and attaches the packed tarball. `pg` is an optional peer dependency
  for the Postgres store.

### Changed

- **Breaking:** the queue envelope is now
  `{ runId, workflow, version, step, attempt, data, options, timezone }`.
  `context.state` lives in the run store, not in the job message.
- **Breaking:** `WorkflowEngine` options are `{ store, hooks }`; `onError` /
  `onStepSuccess` moved under `hooks`.
- **Breaking:** `LocalDayTime` resolves via `Intl` in an IANA timezone and
  defaults to UTC (previously the host's local time).
- **Breaking:** `Queue.subscribe` subscribers receive an optional delivery scope.
- `DurationInput` accepts a `Duration`, a number (ms), an ISO-8601 string, or a
  Temporal-like object; years/months are rejected.

### Fixed

- **jsonb writes to the run store.** A JS array passed straight to
  node-postgres is serialized as a Postgres array literal, not JSON — empty
  arrays were silently stored as `{}` and arrays of objects raised
  `invalid input syntax for type json`. Parameters are now `JSON.stringify`-ed
  and cast `::jsonb`.
- **Post-step failures were silent.** Errors raised after a step body
  (recording, saving, re-publishing) now go through `hooks.onError` instead of
  becoming a bare retry.

[unreleased]: https://github.com/elderengineer/pg-workflow/commits/master
