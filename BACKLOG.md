# Backlog

This backlog is the implementation source of truth for `regime-loader`.

The repository loads reusable daily market-state inputs from open/public sources, preserves source history, performs strict incremental updates during normal execution, and publishes deterministic immutable Gold feature snapshots through a Bronze -> Silver -> Gold architecture.

Last reviewed: 2026-08-24

## Delivery Policy

- One `PR-XX` entry equals one logical implementation pull request.
- PRs are sized for two weak coding agents working in parallel: one infrastructure boundary, provider family, transformation boundary, publication concern, or operational concern per PR.
- Every PR has `Status`, `Updated`, `PR`, `Git branch`, `Git status`, `Agent lane`, `Depends on`, `Commit`, and `Design patterns`.
- Delivery statuses: `Planned`, `In Progress`, `Blocked`, `Ready`, `Merged`.
- Git statuses: `not-started (branch absent)`, `active-clean`, `active-dirty: <paths>`, `pushed-ci-failing`, `pushed-ci-green`, `merged`.
- Every `Description` requirement `R<n>` has exactly one matching `Acceptance` item `A<n>`. Counts must match.
- Implement only the selected PR. Do not pull future-PR scope forward.
- Required unit/integration tests are offline. Live provider tests use `@pytest.mark.network` and are excluded from required gates.
- Production dataframe operations are Polars-first; no production pandas dependency.
- Runtime `lake/` is ignored by Git.
- `README.md`, `ARCHITECTURE.md`, `AGENTS.md`, and this backlog must not intentionally contradict one another.

## Git Workflow Contract

Every implementation PR uses:

```text
Git branch: pr-XX/<kebab-case-description>
Commit:     type(pr-XX): <lowercase imperative description>
```

Allowed Conventional Commit types:

```text
feat fix docs test refactor perf build ci chore
```

Rules:

- Branch and commit scope contain the same `pr-XX` as the backlog entry.
- Every non-generated commit subject uses Conventional Commit format exactly `type(pr-XX): <description>` with an allowed type and the same PR identifier as the branch.
- Branch from dependency-complete `main` only after every `Depends on` PR is merged.
- Before every commit, verify the active branch is exactly the `Git branch` declared by the backlog PR.
- Before push: required local quality gate passes and `git status --short` is empty.
- A pushed branch with any failing required checks has Git status `pushed-ci-failing`.
- Before `Ready`: remote `lint`, `type`, `unit`, `integration`, `coverage` are green and Git status is `pushed-ci-green`.
- Enable PR auto-merge with squash when the PR is ready; protected `main` ensures merge occurs only after the merge gate passes.
- After merge: update backlog status/PR link/Git status in the next documentation-maintenance change; do not keep an implementation task alive to start another PR.
- No force-push on shared branches, branch reuse, or guessed semantic conflict resolution.

## Push And Merge Quality Gates

Required checks:

```text
lint
type
unit
integration
coverage
```

### Parallel execution

`lint`, `type`, `unit`, and offline `integration` start independently/in parallel. `unit` and `integration` produce separate raw coverage data.

### Coverage

`coverage` depends only on `unit` and `integration`, combines their raw data, and enforces production-code **line coverage >= 90.0%** for:

```text
application/
ingestion/
api/
scripts/
```

Tests, fixtures, generated artifacts, and `lake/` are excluded. `89.99%` fails; `90.00%` passes. Do not exclude production files merely to reach the threshold.

### Triggers

The same gate runs on:

```text
local pre-push
GitHub push
pull_request -> main
merge_group
```

### Target GitHub repository policy

`main` must be ruleset/protection controlled:

- pull request required;
- direct push, force push, and branch deletion blocked;
- required checks: `lint`, `type`, `unit`, `integration`, `coverage`;
- branch up-to-date / merge-queue compatible;
- squash merge only;
- repository auto-merge enabled;
- implementation PRs use auto-merge so GitHub completes them only after all required gates pass;
- head branch deleted after merge.

## Mandatory Design Patterns

Use patterns whenever they materially reduce coupling, clarify lifecycle/ownership, improve substitution in tests, or protect transaction boundaries. Do not introduce a pattern only to satisfy a label; prefer the simplest implementation that satisfies the contract. Prefer composition/`typing.Protocol` over inheritance.

Every PR declares `Design patterns:` explicitly. If no additional pattern is justified beyond the repository architecture, use `Architectural baseline only` rather than inventing one.

- **Ports and Adapters / Hexagonal Architecture** — `application` owns contracts/use cases; `ingestion` implements provider/filesystem adapters.
- **Adapter** — CBOE/STOXX/Yahoo/ECB/FRED and physical persistence implementations.
- **Strategy** — retry policy, update/reconcile planning policy, consumer resolution policy.
- **Registry/Factory** — canonical series/provider adapter routing; orchestration must not use provider `if/elif` ladders.
- **Repository** — Bronze, Silver, state, run manifest, inventory, Gold build and Gold catalog persistence.
- **Unit of Work** — one-series Bronze durability boundary and Gold catalog promotion boundary.
- **State Machine** — Gold publication `building -> complete|failed`; catalog alone owns publication status.
- **Materialized View** — root `manifest.json` and `feature_profile.png` are rebuildable views of authoritative `manifest.parquet`.
- **Mark-and-Sweep** — retention tombstones a build in the catalog before physical deletion.
- **Command** — CLI adapters parse/call/render; no provider/persistence business logic.
- **Dependency Injection** — clock, sleeper, HTTP client, repositories, provider registry, source-control identity, and policies are injected.
- **Specification/Policy Object** — governance validators encode repository/backlog invariants as executable rules rather than prose-only conventions.

## Initial Series Catalog

| Canonical ID | Provider | Source | Shape | Capability | Bootstrap |
|---|---|---|---|---|---|
| `vix` | CBOE | `VIX_History.csv` | `ohlc` | `full_file` | maximum exposed history |
| `vix9d` | CBOE | `VIX9D_History.csv` | `ohlc` | `full_file` | maximum exposed history |
| `vix3m` | CBOE | `VIX3M_History.csv` | `ohlc` | `full_file` | maximum exposed history when available |
| `vix6m` | CBOE | `VIX6M_History.csv` | `ohlc` | `full_file` | maximum exposed history when available |
| `vix1y` | CBOE | `VIX1Y_History.csv` | `ohlc` | `full_file` | maximum exposed history when available |
| `vstoxx` | STOXX | `V2TX` | `scalar` | `full_file` | maximum exposed history |
| `move` | Yahoo Finance | `^MOVE` | `ohlc` | `date_range` | maximum available history |
| `ciss` | ECB | `CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX` | `scalar` | `date_range` | maximum exposed history |
| `estr` | ECB | `EST.B.EU000A2X2A25.WT` | `scalar` | `date_range` | maximum exposed history |
| `euro_hy_oas` | FRED | `BAMLHE00EHYIOAS` | `scalar` | `date_range` | maximum currently exposed history; preserve older local history |
| `us_2y` | FRED | `DGS2` | `scalar` | `date_range` | maximum exposed history |
| `us_10y` | FRED | `DGS10` | `scalar` | `date_range` | maximum exposed history |
| `usd_broad` | FRED | `DTWEXBGS` | `scalar` | `date_range` | maximum exposed history |

No additional MVP series or implicit provider fallback is allowed without a separate PR.

## Medallion Storage Contract

```text
lake/
  bronze/
    provider=<provider>/series=<series_id>/year=<YYYY>/month=<MM>/data.parquet
  silver/
    series=<series_id>/year=<YYYY>/month=<MM>/data.parquet
  gold/
    dataset=regime_features_daily/
      versions/build_id=<YYYYMMDDTHHMMSSZ>/
        data.parquet
        manifest.json
        feature_profile.png
      manifest.parquet
      manifest.json
      feature_profile.png
  state/
    ingestion_state.parquet
  manifests/
    ingestion_runs.parquet
    dataset_inventory.parquet
```

### Bronze common schema

```text
series_id: String
provider: String
observation_date: Date
fetched_at_utc: Datetime(time_zone="UTC")
source_id: String
source_url: String
```

Payload is exactly OHLC (`open/high/low/close`) or scalar (`value`). Natural key `(provider, series_id, observation_date)`.

### Silver schema

```text
observation_date: Date
series_id: String
value: Float64
open: Float64 nullable
high: Float64 nullable
low: Float64 nullable
close: Float64 nullable
unit: String
provider: String
source_id: String
fetched_at_utc: Datetime(time_zone="UTC")
```

Natural key `(series_id, observation_date)`. OHLC uses `value=close`; scalar uses `value` and null OHLC.

### Gold timestamp

```text
timestamp_m1: Datetime(time_unit="us", time_zone="UTC")
```

First column, unique, strictly increasing, UTC midnight. It is observation-day identity, **not** provider publication/availability time. Gold contains no `observation_date`.

### Gold feature math

```text
delta_Nobs(t) = x(t) - x(previous Nth valid observation)
```

`zscore_60obs` uses last 60 valid observations including current and `ddof=0`; null before 60 observations or at zero variance. Cross-series features require same timestamp. No forward/back fill, interpolation, centered window, future data, or implicit as-of carry. Final Gold normalizes NaN to null and rejects infinity.

### Gold semantic versions

```text
schema_version  = 1
feature_version = 1
```

Schema version changes for column name/order/type changes; feature version changes for formula/parameter semantics without schema change. Runtime never auto-increments.

### Gold catalog schema

Authoritative `lake/gold/dataset=regime_features_daily/manifest.parquet` fields:

```text
dataset_id
build_id
status                  # building | complete | failed
current
started_at_utc
completed_at_utc
schema_version
feature_version
min_timestamp
max_timestamp
row_count
data_path
build_manifest_path
plot_path
pruned_at_utc
```

Root JSON/PNG are materialized views, not authority.

## Strict Delta Update Contract

The ingestion modes are explicit:

```text
bootstrap
update
reconcile
```

### Bootstrap

If authoritative Bronze contains no observation for the selected series, request maximum public history exposed by the configured provider.

### Normal `update` / `run-daily`

If Bronze exists, determine the delta window from **the newest durable Bronze observation**, never from the oldest retained observation:

```text
latest_stored_date = max(Bronze.observation_date)
request_start      = latest_stored_date - overlap_days
request_end        = injected_today
```

Default `overlap_days = 7` calendar days. The overlap exists only to catch recent equal-key revisions.

Mandatory invariants:

1. `latest_stored_date` is derived from authoritative Bronze (state may cache it but must not override Bronze truth).
2. Normal `update` and `run-daily` never choose `min(Bronze.observation_date)` as request start.
3. Normal `update` and `run-daily` never automatically switch to full-history `reconcile`.
4. If `request_end < latest_stored_date`, fail rather than fabricate a reverse/empty history state.
5. For `date_range` providers, send the exact bounded interval `[request_start, request_end]`; do not silently broaden it.
6. For `full_file` providers, a complete remote object may have to be downloaded because the upstream source has no bounded-history capability, but before logical diff/persistence filter accepted observations to `[request_start, request_end]` during normal update.
7. A normal update must rewrite only monthly partitions containing inserted/revised rows inside the logical delta window.
8. Provider rows outside the requested delta scope must not expand normal update semantics: bounded-provider out-of-window data is a contract error; full-file out-of-window data is ignored for the normal diff.
9. Source omission/shortening never deletes older retained history.

Canonical proof case required in planner/orchestration/provider integration tests:

```text
Bronze min date       = 2000-01-03
Bronze latest date    = 2026-08-18
injected today        = 2026-08-19
overlap_days          = 7
expected request      = 2026-08-11 .. 2026-08-19
forbidden request     = 2000-01-03 .. 2026-08-19
```

### Explicit `reconcile`

`reconcile` is a separate operator-requested command. It may request maximum currently exposed history to detect revisions older than the overlap window. It is **never invoked automatically by `run-daily`**. Operators may schedule it separately if desired.

A shorter/omitted response still never implies deletion. Explicit deletion semantics require a future source-mutation contract.

## PR Graph

Each PR's `Depends on:` field is authoritative; this diagram is informational only.

```text
PR-01 foundation + quality/Git policy
  |\
  | +--> PR-03 Parquet repositories
  +----> PR-02 registry/path contracts
             |\
             | +--> PR-04 HTTP/provider ports
             +----> PR-05 planner/state
  PR-02 + PR-03 --> PR-11 manifests/inventory

PR-02 + PR-04 + PR-05
       |      |      |      |      |
     PR-06  PR-07  PR-08  PR-09  PR-10
       \      |      |      |      /
        +-----+------+------+-+----+
                         + PR-03 + PR-11
                                  |
                                PR-12
                               /     \
                            PR-13   PR-14
                           /    \
                        PR-15  PR-16
                           \    /
                            PR-17
                           /     \
                        PR-18   PR-19
                          |
                        PR-20
                           \     /
                            PR-21
                              |
                            PR-22
                              |
                    PR-14 + PR-21 + PR-22
                              |
                            PR-23

PR-24 governance contract is an orthogonal repository-policy sidecar. It may run in parallel with already-active PR-04/PR-05/PR-11 and must merge before starting any new not-yet-active implementation PR.
```

---

## PR-01: Bootstrap Repository, Quality Gates, And Git Policy

Status: Merged

Updated: 2026-08-19

PR: #2

Git branch: `pr-01/repository-bootstrap-quality-gates`

Git status: `merged`

Agent lane: Foundation; one agent only

Depends on: none

Commit: `chore(pr-01): bootstrap repository and quality gates`

Design patterns: Command, Dependency Injection, Ports and Adapters.

Description:
- R1: Create Python >=3.13 `uv` project; runtime dependencies `polars`, `pyarrow`, `httpx`, `pydantic`, `PyYAML`, `matplotlib`; dev dependencies `pytest`, `pytest-cov`, `coverage`, `ruff`, `mypy`; no production pandas.
- R2: Create importable `application/`, `application/ports/`, `ingestion/`, `api/`, `scripts/`, `tests/unit/`, `tests/integration/`, `tests/fixtures/`; register `integration` and `network` pytest markers; preserve `AGENTS.md` rules.
- R3: Add Make targets `lint`, `type`, `unit`, `integration`, `coverage`, `quality-gate`; first four execute independently in parallel, test targets write separate coverage data, `coverage` combines and `--fail-under=90`.
- R4: Add repository-managed idempotent pre-push hook installer; dirty worktree, any execution failure, or combined coverage `<90.0%` blocks push.
- R5: Add `.github/workflows/quality-gates.yml` for `push`, `pull_request` to `main`, and `merge_group`: four parallel jobs exactly `lint|type|unit|integration`, plus `coverage` with `needs: [unit, integration]` and raw coverage artifact combination.
- R6: Add Conventional Commit/branch validator requiring `type(pr-XX): ...` scope to match `pr-XX/...`; exempt generated merge-group commits only.
- R7: Add idempotent GitHub repository setup script/documentation that configures protected `main`, five required checks, PR-only/no force/no delete, squash-only merging, auto-delete head branches, repository `allow_auto_merge`, and merge-queue compatibility; fail visibly when admin permissions are insufficient.
- R8: Add a documented/helper command for `gh pr merge <PR> --auto --squash` (or equivalent API) so implementation PRs are queued for auto-completion and cannot merge until branch protection requirements are green.
- R9: Add `.gitignore` for virtualenv/caches/coverage/temp outputs and `lake/`; keep README/ARCHITECTURE/AGENTS synchronized with implemented tooling.

Acceptance:
- A1 (verifies R1): dependency resolution succeeds and production dependency inspection finds no pandas.
- A2 (verifies R2): all roots exist/import, markers register, required tests exclude `network`, and AGENTS remains present.
- A3 (verifies R3): four children start independently; separate coverage data combine correctly; `89.99%` fails and `90.00%` passes.
- A4 (verifies R4): hook install is idempotent and each dirty/failure/coverage-negative case blocks push.
- A5 (verifies R5): workflow contract tests prove triggers, no inter-job `needs` among first four, exact coverage dependency/artifacts, and five stable check names.
- A6 (verifies R6): matching examples pass; malformed type/scope/PR mismatch fail deterministically.
- A7 (verifies R7): setup contract contains every required repository setting, is idempotent, and returns non-zero/actionable output when configuration cannot be applied.
- A8 (verifies R8): helper requests auto-squash merge but an integration/mock check proves required checks remain the gating condition.
- A9 (verifies R9): ignored artifacts are untracked and documentation does not contradict implemented tooling.

## PR-02: Define Registry, Paths, And Adapter Registry Contracts

Status: Merged

Updated: 2026-08-19

PR: #3

Git branch: `pr-02/series-registry-lake-contracts`

Git status: `merged`

Agent lane: Agent A

Depends on: PR-01

Commit: `feat(pr-02): define series and lake contracts`

Design patterns: Registry/Factory, Value Object, Dependency Injection.

Description:
- R1: Define immutable typed registry with exactly 13 canonical series and provider/source/unit/shape/frequency/bootstrap/capability metadata.
- R2: Restrict shape to `ohlc|scalar` and capability to `date_range|full_file`; VSTOXX is `scalar/full_file`; validate duplicate/unknown/empty/invalid metadata.
- R3: Define typed path service for every Bronze/Silver monthly file, Gold build/root artifact, state, run manifest, inventory path; no provider-local path literals.
- R4: Define canonical provider enum/identity contracts and an adapter-registry interface used later as Registry/Factory; lookup unknown provider fails explicitly.
- R5: Add exact fixed-path/registry tests for `2026-08-18` and build `20260818T020000Z` plus all invalid registry cases.

Acceptance:
- A1 (verifies R1): exactly 13 complete registry entries exist.
- A2 (verifies R2): only declared enum values pass and VSTOXX is unambiguous.
- A3 (verifies R3): one path service returns every documented exact path.
- A4 (verifies R4): fake adapters can register/resolve by provider without application `if/elif`, unknown provider fails.
- A5 (verifies R5): all fixed and invalid cases pass offline.

## PR-03: Implement Polars Lake Repositories And Atomic IO

Status: Merged

Updated: 2026-08-19

PR: #4

Git branch: `pr-03/polars-parquet-lake-io`

Git status: `merged`

Agent lane: Agent B

Depends on: PR-01

Commit: `feat(pr-03): add polars parquet repositories`

Design patterns: Repository, Adapter, Dependency Injection.

Description:
- R1: Define narrow repository/IO ports and Polars filesystem adapters for deterministic zero/one/multi monthly reads with caller-key ordering and efficient `min/max observation_date` discovery from authoritative Bronze.
- R2: Implement same-directory temp write, flush/fsync where supported, and `os.replace`; stale temp never becomes authoritative.
- R3: Implement pure diff by supplied natural key returning inserts/unchanged/revisions; reject duplicate incoming keys; equal-key new row replaces once.
- R4: Rewrite only months containing inserts/revisions; logical no-op preserves file hashes/mtime and unrelated months.
- R5: Add tests for read modes, authoritative min/max discovery, atomic interruption, stale temp, duplicate keys, revision, no-op, deterministic ordering, and unaffected partitions.

Acceptance:
- A1 (verifies R1): repository test doubles and filesystem adapters satisfy the same contracts, min/max are exact from fixtures, and production code contains no pandas.
- A2 (verifies R2): injected failures preserve prior destination; no stale temp is authoritative.
- A3 (verifies R3): classifications and duplicate rejection are exact/idempotent.
- A4 (verifies R4): no-op and unrelated partitions remain byte/mtime unchanged.
- A5 (verifies R5): all IO/repository cases pass offline.

## PR-04: Add Shared HTTP Port, Retry Strategy, And Provider Protocol

Status: Merged

Updated: 2026-08-19

PR: #5

Git branch: `pr-04/shared-http-provider-port`

Git status: `merged`

Agent lane: Agent B

Depends on: PR-01, PR-02

Commit: `feat(pr-04): add shared http provider port`

Design patterns: Ports and Adapters, Adapter, Strategy, Dependency Injection.

Description:
- R1: Define application-facing HTTP request/response port and `MarketDataProvider` protocol; application imports no `httpx`.
- R2: Provider request contract receives an explicit operation mode and logical date window; `date_range` adapters must honor exact bounds for normal update while `full_file` adapters receive the same logical window for post-fetch filtering.
- R3: Implement one `httpx` adapter with explicit timeouts and injected `RetryPolicy` Strategy: bounded retries for transient transport errors, `429`, `5xx`; no generic retry for other `4xx`.
- R4: Implement bounded exponential backoff with injected sleeper, deterministic tests, and numeric `Retry-After` capped by configured maximum.
- R5: Define typed sanitized provider error with provider/series/source/request category but no API key/auth/full-secret URL; add success/retry/bound-contract/protocol/redaction tests.

Acceptance:
- A1 (verifies R1): fake provider/HTTP implementations substitute without `httpx` in application.
- A2 (verifies R2): mocks prove normal update propagates exact logical bounds to every provider adapter contract.
- A3 (verifies R3): exact retry categories/attempts/timeouts are proven.
- A4 (verifies R4): deterministic delay/cap sequence passes without real sleep.
- A5 (verifies R5): safe context remains, configured secrets never appear, and all stated cases pass offline.

## PR-05: Implement Bootstrap, Strict Delta Update, Explicit Reconcile Planner And State

Status: Merged

Updated: 2026-08-19

PR: #8

Git branch: `pr-05/planner-delta-reconcile-state`

Git status: `merged`

Agent lane: Foundation; first free agent

Depends on: PR-02, PR-03

Commit: `feat(pr-05): add strict delta ingestion planner`

Design patterns: Strategy, Repository, Dependency Injection.

Description:
- R1: Implement pure planner modes `bootstrap|update|reconcile`; `bootstrap` only when no authoritative Bronze observation exists, `update` for normal existing-history execution, and `reconcile` only when explicitly requested by caller/operator.
- R2: For `update`, derive `latest_stored_date=max(authoritative Bronze observation_date)` and compute `request_start=latest_stored_date-overlap_days`, `request_end=injected_today`; default overlap 7; never use Bronze minimum date as update start; reject today earlier than latest stored date and invalid overlap/config.
- R3: Map `update` to exact bounded request for `date_range`; map `update` to `full_file` fetch plus mandatory logical filter window metadata for `full_file`; map explicit `reconcile` to maximum exposed history request. No clock/state condition may auto-promote `update` into `reconcile`.
- R4: Define `ingestion_state.parquet` key `(provider,series_id)` with last success, authoritative last observation cache, last requested start/end, operation mode, fetched/accepted/changed counts, and optional `last_reconcile_utc`; state cache disagreement with authoritative Bronze must fail or be repaired from Bronze, never broaden fetch scope.
- R5: State advances only after caller confirms durable Bronze plus success run manifest; no-op may advance success timestamp but not fabricate observation coverage; reconcile timestamp advances only after explicit successful reconcile.
- R6: All dates/times are injected; add canonical proof fixture `min=2000-01-03,max=2026-08-18,today=2026-08-19,overlap=7 -> 2026-08-11..2026-08-19`, plus an assertion that `2000-01-03..2026-08-19` is never emitted by normal update.

Acceptance:
- A1 (verifies R1): empty Bronze -> bootstrap; existing Bronze -> update; reconcile appears only from explicit reconcile request.
- A2 (verifies R2): exact latest-derived overlap bounds pass; minimum-date start, reverse date, and invalid configuration fail deterministically.
- A3 (verifies R3): `date_range` receives exact delta bounds; `full_file` receives exact logical filter window; normal update never emits maximum-history/reconcile instruction.
- A4 (verifies R4): typed state round-trip/upsert leaves one row/key and stale cache cannot override Bronze max date.
- A5 (verifies R5): failure preserves prior state; no-op/reconcile timestamp semantics are exact.
- A6 (verifies R6): canonical fixture emits only `2026-08-11..2026-08-19`; test spy proves no production planner wall-clock call and no normal full-history plan.

## PR-06: Add CBOE Volatility Provider

Status: Merged

Updated: 2026-08-19

PR: #10

Git branch: `pr-06/cboe-volatility-provider`

Git status: `merged`

Agent lane: Agent A

Depends on: PR-02, PR-04, PR-05, PR-24

Commit: `feat(pr-06): ingest cboe volatility indices`

Design patterns: Adapter, Ports and Adapters, Dependency Injection.

Description:
- R1: Implement one CBOE adapter for registered `vix|vix9d|vix3m|vix6m|vix1y` only through shared provider/HTTP contracts.
- R2: Treat sources as registry-driven `full_file`; unavailable registered source returns typed error, never fallback/synthetic data.
- R3: Parse exact Bronze common+OHLC with Polars; reject invalid/duplicate dates, missing/non-finite close, invalid natural key.
- R4: On normal update, although full file may be downloaded, filter parsed rows to caller's exact logical delta window before returning/diffing; out-of-window rows must not be persisted or cause old-month rewrites. On bootstrap/explicit reconcile, maximum history may be accepted. Shorter response never deletes retained rows.
- R5: Add representative fixtures/tests for all routes, exact delta-window filtering, parsing, invalid/duplicate, revision, shortened response, unavailable source, HTTP propagation.

Acceptance:
- A1 (verifies R1): only five registered canonical IDs resolve to CBOE.
- A2 (verifies R2): requests use registry source/full-file and no fallback path.
- A3 (verifies R3): exact schema and invalid cases pass.
- A4 (verifies R4): normal update accepts only in-window rows and touches only matching affected months; bootstrap/reconcile may accept full range; retained history survives shortening.
- A5 (verifies R5): all scenarios pass offline.

## PR-07: Add STOXX VSTOXX Provider

Status: Merged

Updated: 2026-08-19

PR: #11

Git branch: `pr-07/stoxx-vstoxx-provider`

Git status: `merged`

Agent lane: Agent A

Depends on: PR-02, PR-04, PR-05, PR-24

Commit: `feat(pr-07): ingest vstoxx history`

Design patterns: Adapter, Ports and Adapters, Dependency Injection.

Description:
- R1: Implement only registered `vstoxx/V2TX` through shared ports.
- R2: Parse registry-declared scalar Bronze `value: Float64`; no runtime provider-shape guessing.
- R3: `full_file` source may download complete history, but normal update filters to exact caller delta window before logical diff/persistence; bootstrap/explicit reconcile may accept maximum history; shorter response never deletes older retained data.
- R4: Reject invalid/duplicate dates and missing/non-finite values; propagate safe HTTP/provider errors; revisions replace equal keys once.
- R5: Add representative fixture/tests for bootstrap, explicit reconcile, strict normal delta filtering, stable source identity, invalid/duplicate, revision, shortening, error propagation.

Acceptance:
- A1 (verifies R1): only registered VSTOXX mapping is accepted.
- A2 (verifies R2): exact scalar Bronze schema is produced.
- A3 (verifies R3): normal update accepts only caller-window rows while reconcile/bootstrap can use full history and shortening never truncates retained history.
- A4 (verifies R4): invalid/error/revision cases are deterministic/sanitized.
- A5 (verifies R5): all scenarios pass offline.

## PR-08: Add Yahoo MOVE Provider

Status: Merged

Updated: 2026-08-19

PR: #12

Git branch: `pr-08/yahoo-move-provider`

Git status: `merged`

Agent lane: Agent A

Depends on: PR-02, PR-04, PR-05, PR-24

Commit: `feat(pr-08): ingest move index history`

Design patterns: Adapter, Ports and Adapters, Dependency Injection.

Description:
- R1: Implement isolated Yahoo adapter only for registered `move -> ^MOVE`; no Yahoo-specific behavior outside adapter.
- R2: Bootstrap/explicit reconcile request maximum available history; normal update passes the planner's exact `request_start/request_end` and must not broaden it.
- R3: Normalize only daily OHLC to exact Bronze; reject invalid/duplicate dates and missing/non-finite close; exclude volume/actions; bounded normal response outside requested dates is a contract failure.
- R4: Empty bounded response is valid no-op; shorter explicit reconcile response never truncates retained history; revisions replace once.
- R5: Add canonical delta request test (`2026-08-11..2026-08-19`), maximum-history separation, empty, out-of-window, invalid/duplicate, revision, shortening, schema, and HTTP errors.

Acceptance:
- A1 (verifies R1): only canonical MOVE mapping is accepted and application remains provider-agnostic.
- A2 (verifies R2): normal update makes exactly one bounded request with exact dates and never maximum-history fallback.
- A3 (verifies R3): schema/exclusion/invalid/out-of-window cases pass.
- A4 (verifies R4): no-op/shortening/revision semantics are exact.
- A5 (verifies R5): canonical delta fixture proves no request from historical minimum; all scenarios pass offline.

## PR-09: Add ECB CISS And ESTR Provider

Status: Merged

Updated: 2026-08-19

PR: #13

Git branch: `pr-09/ecb-ciss-estr-provider`

Git status: `merged`

Agent lane: Agent B

Depends on: PR-02, PR-04, PR-05, PR-24

Commit: `feat(pr-09): ingest ecb regime series`

Design patterns: Adapter, Ports and Adapters, Dependency Injection.

Description:
- R1: Implement only registered CISS and ESTR API mappings through shared ports.
- R2: Bootstrap/explicit reconcile request maximum exposed history; normal update maps exact planner bounds to provider `startPeriod/endPeriod` and never omits/broadens them; MVP does not use deletion events until explicit deletion semantics exist.
- R3: Parse exact scalar Bronze; missing/non-numeric/non-finite observations are absent, duplicates invalid; calendar gaps remain gaps; out-of-window bounded rows fail contract.
- R4: Treat provider no-results response for a valid bounded query as empty/no-op where source semantics indicate no matching observations; other typed HTTP errors propagate; revisions replace once.
- R5: Add canonical delta request test, both series, max-history explicit modes, missing/duplicate/gap/out-of-window, empty/no-result mapping, revision, shortening and HTTP errors.

Acceptance:
- A1 (verifies R1): exactly two mappings resolve.
- A2 (verifies R2): normal mode emits exact start/end and no maximum-history request; only bootstrap/reconcile do so.
- A3 (verifies R3): schema/missing/duplicate/gap/out-of-window behavior passes.
- A4 (verifies R4): valid no-result becomes no-op while real failures remain typed; revision is exact.
- A5 (verifies R5): canonical delta fixture proves no historical-minimum fetch and all scenarios pass offline.

## PR-10: Add FRED Rates, Credit, And Dollar Provider

Status: Merged

Updated: 2026-08-19

PR: #14

Git branch: `pr-10/fred-rates-credit-dollar-provider`

Git status: `merged`

Agent lane: Agent B

Depends on: PR-02, PR-04, PR-05, PR-24

Commit: `feat(pr-10): ingest fred regime series`

Design patterns: Adapter, Ports and Adapters, Dependency Injection.

Description:
- R1: Implement exactly registered `DGS2|DGS10|DTWEXBGS|BAMLHE00EHYIOAS` mappings through shared ports.
- R2: Bootstrap/explicit reconcile request maximum currently exposed history; normal update sends exact planner observation bounds and never omits/broadens them or silently falls back to full history.
- R3: Parse exact scalar Bronze; `.`, blank, missing/non-finite values are absent; duplicate dates invalid; out-of-window bounded rows fail contract.
- R4: Shorter reconcile/bootstrap responses never truncate retained history; explicit reconcile may revise historical equal keys, while normal update can revise only keys inside its delta window.
- R5: Inject required FRED API key/config; strip/redact it from persisted `source_url`, registry, logs, errors, repr, and fixtures.
- R6: Add canonical delta request fixture, four series, explicit max-history modes, missing/duplicate/out-of-window, historical revision, shortened HY history, API-key redaction, errors.

Acceptance:
- A1 (verifies R1): exactly four source/canonical mappings resolve.
- A2 (verifies R2): normal mode sends exact delta bounds and never full-history; bootstrap/reconcile remain explicitly separate.
- A3 (verifies R3): schema/missing/duplicate/out-of-window behavior passes.
- A4 (verifies R4): normal revision stays inside delta window and explicit reconcile may update older equal keys without truncation.
- A5 (verifies R5): configured secret is absent from every persisted/diagnostic artifact tested.
- A6 (verifies R6): canonical fixture proves no historical-minimum fetch and all scenarios pass offline.

## PR-11: Add Operational Manifest And Inventory Repositories

Status: Merged

Updated: 2026-08-19

PR: #7

Git branch: `pr-11/bronze-inventory-run-manifests`

Git status: `merged`

Agent lane: Agent B

Depends on: PR-02, PR-03

Commit: `feat(pr-11): add inventory and run manifests`

Design patterns: Repository, Adapter, Dependency Injection.

Description:
- R1: Define authoritative `dataset_inventory.parquet` snapshot fields `series_id,provider,min_observation_date,max_observation_date,row_count,duplicate_key_count,file_count`.
- R2: Define `ingestion_runs.parquet` unique `run_id` fields including provider/series/mode/requested bounds/fetched rows/accepted rows/inserted/revised/written partitions/status/timestamps/sanitized error.
- R3: Implement repository adapters: deterministic snapshot replacement for inventory and unique-run upsert for runs.
- R4: Inventory describes observed stored coverage only; no synthetic market-calendar missing metrics. `max_observation_date` is the authoritative planning reference for normal delta updates; `min_observation_date` must not be used as normal update request start.
- R5: Failed run never contains secrets or claims non-durable inserts/revisions/partitions; add empty/populated/success/failure/idempotency/request-bound tests.

Acceptance:
- A1 (verifies R1): exact inventory schema/values pass.
- A2 (verifies R2): exact run schema/status/request-bound values pass.
- A3 (verifies R3): deterministic round-trip and no duplicate run ID.
- A4 (verifies R4): no expected-calendar metric exists and tests distinguish min from max planning semantics.
- A5 (verifies R5): failure durability/redaction and all stated cases pass.

## PR-12: Add Registry-Driven Bronze Orchestration Unit Of Work

Status: Merged

Updated: 2026-08-19

PR: #15

Git branch: `pr-12/bronze-orchestration`

Git status: `merged`

Agent lane: Integration; one agent only

Depends on: PR-03, PR-05, PR-06, PR-07, PR-08, PR-09, PR-10, PR-11, PR-24

Commit: `feat(pr-12): orchestrate bronze updates`

Design patterns: Registry/Factory, Unit of Work, Repository, Dependency Injection.

Description:
- R1: Application service resolves series contract and provider via injected registries, planner, repositories; no provider implementation/HTTP imports or provider conditional ladder.
- R2: Expose `bootstrap|update|reconcile`; normal `update` reads authoritative Bronze max date and uses PR-05 strict delta plan. `reconcile` occurs only through explicit caller request; orchestration must not select it based on age/timer/state during normal update.
- R3: One selected series is a Unit of Work: fetch/parse -> enforce logical request window -> diff -> durable Bronze -> durable success run -> state advance; failure before commit preserves prior state/authoritative data.
- R4: Multi-series isolation: one failed series does not roll back separately committed series; failed run is recorded safely.
- R5: No-op writes success run with zero changes, rewrites no Bronze file, may advance success time but not observation coverage; normal update writes requested bounds exactly to run/state metadata.
- R6: Add fake-provider/repository tests for registry routing, all modes, canonical delta fixture, assertion that normal update never calls reconcile/max-history strategy, partial failure, barrier failures, restart, no-op, revision, explicit reconcile, multi-series isolation.

Acceptance:
- A1 (verifies R1): provider registry substitution works with no application provider-specific imports/conditionals.
- A2 (verifies R2): existing Bronze normal call always selects strict update; only explicit reconcile call selects reconcile.
- A3 (verifies R3): canonical fixture accepts only `2026-08-11..2026-08-19`; failure injection around each barrier proves commit semantics.
- A4 (verifies R4): one failure coexists with another durable success.
- A5 (verifies R5): file hash/mtime/state/run values prove no-op and exact-bound semantics.
- A6 (verifies R6): spy proves no normal full-history request and all stated scenarios pass offline.

## PR-13: Build Canonical Silver Daily Series

Status: Merged

Updated: 2026-08-19

PR: #16

Git branch: `pr-13/silver-canonical-series`

Git status: `merged`

Agent lane: Agent A

Depends on: PR-12, PR-24

Commit: `feat(pr-13): build canonical silver series`

Design patterns: Repository, Adapter, Dependency Injection.

Description:
- R1: Registry-driven Silver builder/repository produces exact canonical schema from selected retained Bronze.
- R2: OHLC maps `value=close` and preserves OHLC; scalar preserves value and null Float64 OHLC; identity/unit consistent.
- R3: Require unique key, strict per-series date order, finite non-null value, consistent provider/source; never create missing dates.
- R4: Diff and rewrite only affected monthly Silver partitions; identical rebuild physical no-op; unselected series untouched.
- R5: Add OHLC/scalar/schema/dtype/duplicate/non-finite/identity/gap/revision/no-op tests.

Acceptance:
- A1 (verifies R1): exact schema/order/types.
- A2 (verifies R2): both source shapes map exactly.
- A3 (verifies R3): all invalid/gap rules pass.
- A4 (verifies R4): revision touches one month; no-op/unselected unchanged physically.
- A5 (verifies R5): all cases pass offline.

## PR-14: Add Lake Inventory CLI

Status: Merged

Updated: 2026-08-19

PR: #17

Git branch: `pr-14/lake-inventory-cli`

Git status: `merged`

Agent lane: Agent B

Depends on: PR-11, PR-12, PR-24

Commit: `feat(pr-14): add lake inventory cli`

Design patterns: Command, Dependency Injection.

Description:
- R1: Command adapter `inventory` reads inventory repository and renders exactly seven stable fields.
- R2: Repeatable `--series`/`--provider` filters validate registry/provider values; unknown fails; no lake mutation.
- R3: Deterministic `--json` has same logical fields/order/values.
- R4: Empty valid result exits zero; config/read/schema/unknown-filter exits non-zero without mutation.
- R5: Add parser/render/filter/json/empty/corrupt-missing tests.

Acceptance:
- A1 (verifies R1): exact text fields/order.
- A2 (verifies R2): filter/unknown/no-mutation behavior passes.
- A3 (verifies R3): JSON/text logical equivalence passes.
- A4 (verifies R4): exact exit semantics pass.
- A5 (verifies R5): all cases pass offline.

## PR-15: Build Volatility Gold Feature Family

Status: Merged

Updated: 2026-08-19

PR: #18

Git branch: `pr-15/volatility-gold-features`

Git status: `merged`

Agent lane: Agent A

Depends on: PR-13, PR-24

Commit: `feat(pr-15): add volatility regime features`

Design patterns: Strategy for feature policy, Dependency Injection; otherwise functional core.

Description:
- R1: Convert Silver dates to unique/sorted first-column UTC-midnight `timestamp_m1: Datetime(us,UTC)`; remove `observation_date`; union family source dates only.
- R2: For `vix,vix9d,vix3m,vix6m,vix1y,vstoxx,move` output exact `<series>_level`, `_delta_5obs`, `_delta_20obs`, `_zscore_60obs` using global math.
- R3: Output exact `vix9d_vix_ratio,vix_vix3m_ratio,vix3m_minus_vix,vix6m_minus_vix,vix1y_minus_vix`; same timestamp only, null on missing/zero denominator.
- R4: No fill; observation lags count valid observations; 60-value `ddof=0`; insufficient/zero variance null.
- R5: Deterministic schema order: timestamp, each series in registry order with four features, then term features; nullable Float64.
- R6: Add hand-calculable timestamp/delta-gap/zscore/zero-variance/ratio/missing/no-future tests.

Acceptance:
- A1 (verifies R1): exact timestamp contract/no observation_date.
- A2 (verifies R2): all 28 series features/formulas exact.
- A3 (verifies R3): five term features/null rules exact.
- A4 (verifies R4): gap/59th/60th/zero-variance tests exact.
- A5 (verifies R5): full expected schema/order/types exact.
- A6 (verifies R6): all cases pass offline.

## PR-16: Build Macro/Credit/Rates/USD Gold Feature Family

Status: Merged

Updated: 2026-08-19

PR: #19

Git branch: `pr-16/macro-gold-features`

Git status: `merged`

Agent lane: Agent B

Depends on: PR-13, PR-24

Commit: `feat(pr-16): add macro regime features`

Design patterns: Strategy for feature policy, Dependency Injection; otherwise functional core.

Description:
- R1: Convert Silver to same canonical family timestamp contract; union source dates without fill.
- R2: Output CISS and Euro HY `level,delta_5obs,delta_20obs,zscore_60obs`.
- R3: Output `us_2y_level,us_2y_delta_20obs,us_10y_level,us_10y_delta_20obs,us_10y_minus_us_2y`; spread same timestamp only.
- R4: Output `estr_level,estr_delta_20obs,usd_broad_level,usd_broad_delta_20obs`; no carry.
- R5: Deterministic schema order R2->R3->R4 after timestamp; nullable Float64; same history/null rules.
- R6: Add hand-calculable timestamp/delta-gap/zscore/yield-pair/no-fill/zero-variance/no-future tests.

Acceptance:
- A1 (verifies R1): exact timestamp/union/no-fill contract.
- A2 (verifies R2): CISS/HY features exact.
- A3 (verifies R3): rates/spread exact including missing pair.
- A4 (verifies R4): ESTR/USD exact/no carry.
- A5 (verifies R5): full schema/order/types exact.
- A6 (verifies R6): all cases pass offline.

## PR-17: Assemble And Validate Canonical Gold Frame

Status: Merged

Updated: 2026-08-19

PR: #20

Git branch: `pr-17/canonical-gold-frame`

Git status: `merged`

Agent lane: Integration; one agent only

Depends on: PR-15, PR-16, PR-24

Commit: `feat(pr-17): assemble canonical daily gold frame`

Design patterns: Facade, Dependency Injection; functional core for deterministic assembly.

Description:
- R1: Outer-join feature-family frames only on timestamp; one union row, null preservation, no imputation/as-of carry.
- R2: Enforce one source-controlled exact ordered Gold schema: timestamp, PR-15 exact columns, PR-16 exact columns; reject missing/extra/renamed/reordered/wrong type/observation_date.
- R3: Validate non-empty, UTC-midnight exact dtype, unique strictly increasing timestamps; normalize feature NaN to null; reject infinity/non-Float64 feature types.
- R4: Add truncation causality regression: rebuild from Silver cutoff `t` and assert every Gold value `<=t` equals full-build prefix; deliberately leaky transform must be detected.
- R5: Keep assembly pure/storage-neutral: no build ID/files/hash/JSON/plot/catalog/publication imports or side effects.
- R6: Add join/schema/timestamp/NaN/infinity/causality/storage-boundary tests.

Acceptance:
- A1 (verifies R1): exact union/null/no-carry result.
- A2 (verifies R2): expected schema passes and every drift case fails.
- A3 (verifies R3): final frame has no NaN/infinity and invalid timestamps fail.
- A4 (verifies R4): multiple cutoff tests pass and leaky control fails.
- A5 (verifies R5): static/import tests prove no physical dependency.
- A6 (verifies R6): all cases pass offline.

## PR-18: Add Immutable Gold Build Store Repository

Status: Merged

Updated: 2026-08-19

PR: #21

Git branch: `pr-18/versioned-gold-storage`

Git status: `merged`

Agent lane: Agent A

Depends on: PR-17, PR-24

Commit: `feat(pr-18): add immutable gold build store`

Design patterns: Repository, Adapter, Dependency Injection.

Description:
- R1: Build ID from injected UTC second `YYYYMMDDTHHMMSSZ`; reject non-UTC/malformed/reused build directory.
- R2: GoldBuildStore creates directory, writes same-dir temp Parquet, validates readback schema/rows/bounds, fsync where supported, atomically names final; creation-only.
- R3: Return deterministic final-byte SHA-256, row count, min/max timestamp, ordered columns and semantic versions.
- R4: Explicit build reader requires build ID; no latest/glob/mtime selection; validates final Parquet.
- R5: Partial directory/final absence is incomplete and rejected; restart never overwrites/promotes it.
- R6: Add ID/collision/path/readback/hash/interruption/overwrite/partial/coexistence tests.

Acceptance:
- A1 (verifies R1): exact ID/invalid/collision semantics.
- A2 (verifies R2): success/failure atomic creation semantics exact.
- A3 (verifies R3): metadata/hash independently match file.
- A4 (verifies R4): explicit A never resolves B.
- A5 (verifies R5): partial directory remains incomplete/unpromoted.
- A6 (verifies R6): all cases pass offline.

## PR-19: Add Gold Catalog Repository And Resolution Strategies

Status: Merged

Updated: 2026-08-19

PR: #22

Git branch: `pr-19/gold-catalog-contract`

Git status: `merged`

Agent lane: Agent B

Depends on: PR-17, PR-24

Commit: `feat(pr-19): add gold catalog and resolution strategies`

Design patterns: Repository, Strategy, Dependency Injection.

Description:
- R1: Define exact 15-field catalog schema including `pruned_at_utc`; source-controlled semantic versions start `1/1` and never auto-bump.
- R2: GoldCatalogRepository atomically persists deterministic `(started_at_utc,build_id)` ordering; unique ID; statuses only `building|complete|failed`.
- R3: Validate pure logical invariants only: current only complete; at most one current; complete metadata required; pruned row has non-null `pruned_at_utc` and all three artifact paths null; selectable complete is non-pruned with all paths non-null and syntactically inside matching build ID. Do not inspect filesystem existence.
- R4: Define `ResolutionPolicy` Strategy `strict_current` (default) and `latest_compatible`; both filter exact caller-supported schema/feature sets and never select building/failed/pruned.
- R5: `strict_current` fails on incompatible/unselectable current; `latest_compatible` prefers compatible current else newest compatible selectable complete by completed time/build ID; no filesystem query.
- R6: Add schema/version/state/path-shape/pruned/current/policy/fallback/order/no-filesystem tests.

Acceptance:
- A1 (verifies R1): exact schema has 15 fields/1-1 constants and no legacy date fields.
- A2 (verifies R2): deterministic atomic round-trip and invalid duplicate/status fail.
- A3 (verifies R3): every logical/pruned/path-shape invalid case fails while repository performs no existence checks.
- A4 (verifies R4): both strategies substitute through one interface with exact version filtering.
- A5 (verifies R5): strict/fallback/order semantics are exact and filesystem spy is untouched.
- A6 (verifies R6): all cases pass offline.

## PR-20: Generate Immutable Build Manifest And Feature Profile

Status: Merged

Updated: 2026-08-19

PR: #23

Git branch: `pr-20/gold-build-sidecars`

Git status: `merged`

Agent lane: Agent A

Depends on: PR-18, PR-24

Commit: `feat(pr-20): add gold build sidecars`

Design patterns: Builder, Adapter, Dependency Injection.

Description:
- R1: Generate deterministic creation-only build `manifest.json` with exact artifact concepts: dataset/build identity, `artifact_state="built"`, schema/feature versions, build start/completion timestamps, rows, ordered columns, bounds, data path/SHA, feature-set hash, Git commit hash, plot path. Do not use catalog `building|complete|failed` status.
- R2: Compute deterministic feature-set SHA-256 from semantic versions + ordered names/dtypes + documented formula parameters; Git identity is injected and required except explicit deterministic test fallback.
- R3: Generate creation-only deterministic `feature_profile.png` from exact frame; numeric features in canonical order; timestamp excluded; no random sampling/wall-clock labels.
- R4: Validate Parquet/JSON/PNG bundle identity/hash/bounds/path/PNG before publication-ready; existing sidecar target never overwritten.
- R5: JSON/plot failure leaves incomplete immutable attempt and never mutates root catalog/materialized views.
- R6: Add exact JSON bytes/schema/hash/Git/PNG/order/exclusion/overwrite/mismatch/failure tests.

Acceptance:
- A1 (verifies R1): exact required artifact key set/values and no publication-status ambiguity.
- A2 (verifies R2): stable hash; formula/schema/version change changes hash; injected Git preserved.
- A3 (verifies R3): valid deterministic PNG/input order/exclusion.
- A4 (verifies R4): bundle validation and creation-only behavior exact.
- A5 (verifies R5): injected failures leave root state untouched.
- A6 (verifies R6): all cases pass offline.

## PR-21: Publish Gold With State Machine, Unit Of Work, And Materialized Views

Status: Merged

Updated: 2026-08-19

PR: #24

Git branch: `pr-21/gold-publication-state-machine`

Git status: `merged`

Agent lane: Integration; one agent only

Depends on: PR-19, PR-20, PR-24

Commit: `feat(pr-21): publish gold with catalog state machine`

Design patterns: State Machine, Unit of Work, Materialized View, Repository, Dependency Injection.

Description:
- R1: Implement publication State Machine: register `building,current=false`; create/validate immutable bundle; on build failure atomically finalize attempted row `failed,current=false`; previous current never changes.
- R2: Before promotion physically validate candidate via GoldBuildStore: exact schema/version/build ID, Parquet SHA/rows/bounds, build JSON identity/hash/path, valid PNG, containment in expected build directory.
- R3: Promotion Unit of Work atomically replaces authoritative catalog with new `complete,current=true` and old current demoted; this catalog replace is the only publication commit point.
- R4: Implement root `manifest.json` and `feature_profile.png` as `GoldMaterializedViewWriter`: after each successful catalog mutation regenerate from catalog/current bundle; never use them as commit authority or selection input.
- R5: If materialized-view refresh fails after catalog commit, return hard operational error but preserve catalog truth; startup/publication entry reconciles stale/missing root views from catalog/current bundle.
- R6: Recover stale non-current `building` rows to `failed`; never infer completion from filesystem presence. A current `building` row is invalid and stops publication for manual/invariant recovery.
- R7: Add failure-injection/state/promotion/physical-integrity/materialized-view/reconciliation/stale-building/first/subsequent publication tests.

Acceptance:
- A1 (verifies R1): only fully built candidate can continue; build failure leaves old current and failed attempt.
- A2 (verifies R2): every physical metadata/hash/path mismatch prevents promotion.
- A3 (verifies R3): exactly one current after success and event trace proves catalog replacement is commit point.
- A4 (verifies R4): root views exactly derive from catalog/current and are never read by resolver.
- A5 (verifies R5): post-commit view failure reports error, catalog remains new authority, next reconciliation repairs views.
- A6 (verifies R6): stale building is never promoted; invalid current-building stops safely.
- A7 (verifies R7): all stated scenarios pass offline.

## PR-22: Add Safe Gold Retention With Mark-And-Sweep

Status: Merged

Updated: 2026-08-19

PR: #25

Git branch: `pr-22/gold-build-retention`

Git status: `merged`

Agent lane: Foundation; first free agent

Depends on: PR-21, PR-24

Commit: `feat(pr-22): add mark and sweep gold retention`

Design patterns: Mark-and-Sweep, Strategy, Repository, Dependency Injection.

Description:
- R1: Default retain five physical complete builds including current per `(schema_version,feature_version)` pair; deterministic oldest non-current ordering by completed time/build ID.
- R2: Mark phase: validate eligible non-current complete/non-pruned row then atomically set all artifact paths null and `pruned_at_utc=injected_now`; current/building/failed/other-version never marked.
- R3: Sweep phase: only after successful mark delete `data.parquet,manifest.json,feature_profile.png` and empty directory; missing already-marked files are idempotent success.
- R4: Sweep interruption/failure leaves catalog safely unselectable and physical orphan(s); return operational error and retry orphan cleanup later. Never restore selectability automatically.
- R5: Resolver never returns marked row; repeated retention/cleanup idempotent; root materialized views are refreshed if catalog audit representation changes.
- R6: Add default/custom/order/current/version/mark-before-delete/partial-delete/orphan-retry/idempotency/resolver tests.

Acceptance:
- A1 (verifies R1): retained count/order exact per semantic pair.
- A2 (verifies R2): catalog tombstone occurs before any delete and protected rows untouched.
- A3 (verifies R3): valid sweep removes whole bundle; already-marked missing file is safe retry.
- A4 (verifies R4): injected partial delete leaves unselectable catalog plus retryable orphan, never dangling selectable path.
- A5 (verifies R5): policy/resolution/repeated runs/view refresh are exact.
- A6 (verifies R6): all cases pass offline.

## PR-23: Add Delta-Only Daily Medallion Pipeline And Operational CLI

Status: Merged

Updated: 2026-08-19

PR: #26

Git branch: `pr-23/daily-medallion-pipeline`

Git status: `merged`

Agent lane: Integration; one agent only

Depends on: PR-14, PR-21, PR-22, PR-24

Commit: `feat(pr-23): add delta-only daily medallion pipeline`

Design patterns: Command, Facade/Orchestrator, Unit of Work, Dependency Injection.

Description:
- R1: Command adapters `bootstrap,update,reconcile,silver-build,gold-build,inventory,run-daily`; parsing/config/render in `api`, use cases in `application`; Gold publish only PR-21 service.
- R2: On Gold-capable command entry, recover stale building rows and reconcile root materialized views before creating another build; this Gold-view reconciliation is unrelated to market-source full-history `reconcile`.
- R3: `run-daily` behavior is strict: empty selected series -> bootstrap; existing selected series -> **update only** using newest durable Bronze date and overlap through injected today. `run-daily` must never invoke source `reconcile`, maximum-history strategy, or historical-minimum request for an existing series. Then Bronze -> Silver -> full canonical Gold -> immutable bundle -> candidate validation -> atomic catalog promotion -> root view refresh -> retention -> inventory.
- R4: `reconcile` is an explicit separate source command and may request maximum source history; if operators want it periodically, documentation gives a separate scheduler example. It is never hidden inside `run-daily`.
- R5: `--series` restricts Bronze/Silver execution only; Gold always rebuilds full canonical schema from all currently available Silver; unknown series fail.
- R6: Failure semantics: provider failure makes run non-zero but independent committed Bronze series remain; any pre-promotion Silver/Gold failure leaves old current; post-promotion materialized-view/retention failure returns non-zero but catalog truth is not falsely rolled back.
- R7: Add structured stdlib logging context including `run_id`, command, series/provider where applicable, `request_start`, `request_end`, build ID after creation, stage, status; sanitize secrets; stable exit codes for validation/provider/persistence/publication errors.
- R8: End-to-end offline regression covers empty bootstrap followed by normal next-day delta request, recent revision inside overlap, assertion that historical minimum is never requested by `run-daily`, full-file provider logical-window filtering, date-range exact bounds, explicit separate old-history reconcile, shortened source, affected Silver months, Gold causality/schema, publication/materialized views, strict/fallback resolution, retention, no-op rerun, inventory.
- R9: Document daily cron/systemd examples for `run-daily` as delta-only, persistent lake path, FRED secret injection, overlap configuration, and a **separate optional explicit `reconcile` schedule**; no scheduled GitHub Actions ingestion.

Acceptance:
- A1 (verifies R1): all commands parse and layer boundaries/Gold publication path are exact.
- A2 (verifies R2): Gold materialized-view recovery repairs or stops safely and cannot trigger source full-history fetching.
- A3 (verifies R3): canonical existing-history fixture `min=2000-01-03,max=2026-08-18,today=2026-08-19,overlap=7` makes `run-daily` request only `2026-08-11..2026-08-19`; spies prove no source reconcile/max-history/min-date path is invoked.
- A4 (verifies R4): only explicit `reconcile` command can request maximum history; scheduler docs keep it separate from daily update.
- A5 (verifies R5): default 13/filter/unknown/full-Gold behavior exact.
- A6 (verifies R6): failure injection proves exact pre/post commit durable truth.
- A7 (verifies R7): log/exit fixtures contain exact request window context and no secret.
- A8 (verifies R8): full bootstrap/delta/revision/window-filter/explicit-reconcile/publication/resolution/retention/no-op scenario passes offline.
- A9 (verifies R9): README documents delta-only daily execution and optional separate reconciliation; no scheduled CI ingestion exists.

## PR-24: Enforce Backlog Git Metadata And Design-Pattern Governance

Status: Merged

Updated: 2026-08-19

PR: #9

Git branch: `pr-24/backlog-governance-contracts`

Git status: `merged`

Agent lane: Governance; one agent only

Depends on: PR-01

Commit: `chore(pr-24): enforce backlog governance contracts`

Design patterns: Specification/Policy Object, Command validation, Dependency Injection where validation inputs are externalized.

Description:
- R1: Synchronize all existing PR entries with actual GitHub delivery state and preserve exact `Git branch`, `Git status`, and PR-scoped Conventional Commit metadata.
- R2: Require every PR entry to declare non-empty `Design patterns`; pattern use is mandatory when it materially improves coupling/testability/lifecycle clarity, but cargo-cult patterns are forbidden.
- R3: Extend Git status vocabulary with `pushed-ci-failing` so pushed branches with failing required checks are represented truthfully instead of mislabeled active or green.
- R4: Add an offline executable backlog contract test that parses every `PR-XX` section and rejects missing/duplicate PR IDs, missing Git metadata, branch/commit PR-ID mismatch, invalid Conventional Commit syntax/type, invalid Git status, or missing Design-pattern metadata.
- R5: Keep `AGENTS.md` synchronized with the executable contract and require exact backlog branch plus PR-scoped Conventional Commits before commit/push.

Acceptance:
- A1 (verifies R1): every PR-01..PR-24 entry records its verified merged GitHub PR number, exact head branch, and `Merged/merged` delivery state.
- A2 (verifies R2): every PR-01..PR-24 section contains one non-empty `Design patterns` field and the docs explicitly forbid pattern-for-pattern's-sake implementation.
- A3 (verifies R3): allowed status tests accept `pushed-ci-failing`, reject unknown states, and reserve `pushed-ci-green` for all-green required remote gates.
- A4 (verifies R4): parser proves exactly 24 PR sections and all metadata contracts; negative fixtures for missing branch/status/pattern and PR-ID/commit mismatch fail deterministically.
- A5 (verifies R5): AGENTS and BACKLOG state the same branch/commit/status/pattern rules without contradiction.

## PR-25: Add Saturday Daily Update Cron Template

Status: Merged

Updated: 2026-08-21

PR: #27

Git branch: `pr-25/saturday-daily-update-cron`

Git status: `merged`

Agent lane: Operations; one agent only

Depends on: PR-23, PR-24

Commit: `feat(pr-25): add saturday daily update cron template`

Design patterns: Command; Architectural baseline only for declarative host scheduling.

Description:
- R1: Provide a versioned, installable host-crontab template that invokes only the existing `run-daily` command on Saturday at 10:00 local deployment-host time; it must use a persistent lake path and append logs.
- R2: Document installation, the exact schedule, host-local-time assumption, and the fact that the scheduled command remains delta-only and does not schedule source reconciliation.
- R3: Add offline regression coverage that guards the cron expression, command, persistent lake path, and absence of a scheduled GitHub Actions ingestion workflow.

Acceptance:
- A1 (verifies R1): the template has exactly `0 10 * * 6`, invokes `run-daily`, uses `/srv/market-regime/lake`, and redirects stdout/stderr to the operational log.
- A2 (verifies R2): README gives the install command and confirms Saturday 10:00 host-local execution with no implicit source reconciliation.
- A3 (verifies R3): offline tests validate the template and retain the no-scheduled-CI policy.

## PR-26: Repair Live Provider Compatibility

Status: Merged

Updated: 2026-08-22

PR: #28

Git branch: `pr-26/repair-live-provider-compatibility`

Git status: `merged`

Agent lane: Provider maintenance; one agent only

Depends on: PR-07, PR-08, PR-24

Commit: `fix(pr-26): repair live provider compatibility`

Design patterns: Adapter, Strategy, Dependency Injection.

Description:
- R1: Adapt the STOXX VSTOXX parser to accept the provider's current `Indexvalue` value-column spelling without weakening schema, numeric, or duplicate validation.
- R2: Supply Yahoo's chart endpoint with a stable client User-Agent and ignore only all-null OHLC bars, so the registered MOVE adapter can access its live source without fabricating observations while preserving bounded-request and retry behavior.
- R3: Add offline regression coverage for both live-source compatibility cases and retain the backlog metadata contract.

Acceptance:
- A1 (verifies R1): a semicolon-delimited `Date;Symbol;Indexvalue` V2TX fixture parses to the expected scalar observations.
- A2 (verifies R2): the Yahoo request has the stable User-Agent, all-null OHLC bars are ignored, partially missing bars fail, and exact update bounds remain unchanged.
- A3 (verifies R3): provider tests and the complete offline quality gate pass.

## PR-27: Parallelize Silver And Gold Polars Work

Status: Merged

Updated: 2026-08-22

PR: #29

Git branch: `pr-27/parallelize-polars-silver-gold`

Git status: `merged`

Agent lane: Performance; one agent only

Depends on: PR-23, PR-24

Commit: `perf(pr-27): parallelize silver and gold polars work`

Design patterns: Strategy, Dependency Injection, Facade/Orchestrator.

Description:
- R1: Use an explicit injected execution policy sized from Polars' thread pool so independent Silver builds and reads run concurrently without exceeding Polars' available-core bound.
- R2: Execute independent volatility and macro Gold feature families concurrently before deterministic canonical assembly.
- R3: Test the all-core policy and retain deterministic result ordering and existing pipeline behavior.

Acceptance:
- A1 (verifies R1): default policy worker count equals `polars.thread_pool_size()` and its map preserves input order.
- A2 (verifies R2): Gold feature-family calls use the execution policy before assembly.
- A3 (verifies R3): offline quality gate passes.

## PR-28: Add Post-Publication Gold Mirror

Status: Merged

Updated: 2026-08-22

PR: #30

Git branch: `pr-28/gold-publication-mirror`

Git status: `merged`

Agent lane: Operations; one agent only

Depends on: PR-21, PR-24

Commit: `feat(pr-28): add post-publication gold mirror`

Design patterns: Adapter, Repository, State Machine, Dependency Injection.

Description:
- R1: After the authoritative catalog promotion and root-view refresh succeed, optionally mirror the complete Gold directory through an injected adapter.
- R2: Configure the rsync destination through `MARKET_REGIME_GOLD_MIRROR_ROOT`; mirroring failure must not roll back an already-complete catalog row.
- R3: Document the configured host mirror and test the exact rsync semantics.

Acceptance:
- A1 (verifies R1): mirror runs only after catalog promotion and materialized-view refresh.
- A2 (verifies R2): rsync mirrors directory contents with archive, partial-transfer, and delayed-delete semantics.
- A3 (verifies R3): configuration and offline tests are exact.

## PR-29: Add Protected Operational YAML Configuration

Status: Merged

Updated: 2026-08-22

PR: #31

Git branch: `pr-29/operational-yaml-config`

Git status: `merged`

Agent lane: Operations; one agent only

Depends on: PR-25, PR-28

Commit: `feat(pr-29): add protected operational yaml config`

Design patterns: Adapter, Command, Dependency Injection.

Description:
- R1: Export protected operational YAML settings as shell-safe cron environment assignments.
- R2: Keep local `config.yaml` out of version control while validating required runtime settings.

Acceptance:
- A1 (verifies R1): exporter emits shell-safe required values.
- A2 (verifies R2): missing settings fail safely and config is ignored by Git.

## PR-30: Add Diagnostic Gold Feature Profile

Status: Merged

Updated: 2026-08-22

PR: #32

Git branch: `pr-30/diagnostic-gold-feature-profile`

Git status: `merged`

Agent lane: Gold presentation; one agent only

Depends on: PR-20, PR-24

Commit: `feat(pr-30): add diagnostic gold feature profile`

Design patterns: Adapter, Materialized View.

Description:
- R1: Replace the coverage-only Gold PNG with a dark diagnostic sheet containing a time series, distribution, coverage, and summary statistics for every canonical feature.
- R2: Preserve immutable sidecar semantics and render missing features explicitly without fabricating values.
- R3: Test deterministic valid diagnostic PNG generation.

Acceptance:
- A1 (verifies R1): every feature has a time-series and histogram panel with summary context.
- A2 (verifies R2): null-only features render as no-data panels.
- A3 (verifies R3): PNG contract tests pass offline.

## Definition Of MVP Complete

MVP is complete only when PR-01 through PR-24 are merged and:

- `main` is actually protected with the five required checks, squash-only PR merging, auto-merge enabled, and no direct/force/delete path;
- implementation branches/commits follow the PR Git contract;
- every backlog PR has explicit Git branch, truthful Git status, PR-scoped Conventional Commit, and Design-pattern metadata;
- established patterns are applied where they reduce coupling/testability/lifecycle ambiguity, without unnecessary pattern layering;
- local/remote gates run four execution checks in parallel plus combined production coverage >=90%;
- application follows documented ports/adapters and no provider conditional ladder leaks into orchestration;
- first execution bootstraps maximum history only when Bronze is empty;
- every normal `update`/`run-daily` for existing history derives its request start from **max stored observation date minus overlap**, never from historical minimum, and ends at injected today;
- `date_range` providers perform exact bounded network requests for normal update; `full_file` providers may download full remote files only because of provider capability but must filter to the logical delta window before normal diff/persistence;
- source full-history `reconcile` is explicit only and never auto-triggered by `run-daily`;
- Bronze/Silver update only affected monthly partitions and normal source omission never deletes retained history;
- Gold uses only canonical UTC `timestamp_m1`, explicit causal feature math, no NaN/infinity, and explicit semantic versions;
- every build is immutable Parquet + artifact manifest + deterministic plot with reproducibility hashes;
- build JSON never conflicts with catalog publication state because publication lifecycle exists only in `manifest.parquet`;
- catalog resolution is pure/policy-driven, strict-current by default, filesystem-recency independent;
- atomic catalog replacement is the Gold publication commit point; root JSON/PNG are recoverable materialized views;
- retention tombstones catalog rows before deleting bundles and cannot create a selectable partial bundle;
- daily pipeline logs exact delta request bounds, handles pre/post commit failures correctly, and remains network-free in required integration tests;
- README, ARCHITECTURE, AGENTS, and BACKLOG remain synchronized.

## PostgreSQL Gold Serving Plane

This section consolidates the former `BACKLOG_POSTGRES.md`. Only the canonical
Gold dataset is replicated to PostgreSQL; Parquet Gold remains authoritative.
The target is `10.10.1.3:54321`, the application role is exactly
`regime-loader`, and credentials are runtime-only and never committed,
logged, or persisted. PostgreSQL temporal storage follows the shared
`pg-temporal-v1` contract used by `xetra-loader` and `crypto-loader`: every
persisted instant is exactly `TIMESTAMPTZ(6)`, every PostgreSQL session is UTC,
the persistence boundary accepts only aware UTC zero-offset microsecond values,
and true date-only values remain `DATE`. PostgreSQL stores typed instants rather
than a string format; sanitized acceptance artifacts serialize instants as
`YYYY-MM-DDTHH:MM:SS.ffffffZ` for diagnostics only. The first synchronization
loads the complete current Gold history; later synchronizations reconcile the
complete row-digest state, including missed runs and historical corrections.
Sync logs use the existing `${PROJECT_ROOT}/.logs/regime-loader.log` path.

Dependency graph:

```text
PR-31 -> PR-32 -> PR-33
   |       |\
   |       +-> PR-34 -> PR-37 -> PR-38 -> PR-39
   +-------> PR-35 -----^   \
   +-------> PR-36 -----------+

PR-39 -> PR-40 -> PR-41 -> PR-42
```

## PR-31: Backlog PostgreSQL Sync Plan

PR name: `backlog-postgres-sync-plan`
Status: Merged
Updated: 2026-08-22
PR: #33
Git branch: `pr-31/backlog-postgres-sync-plan`
Git status: `merged`
Agent lane: Governance; one agent only
Depends on: none
Commit: `docs(pr-31): backlog-postgres-sync-plan add postgres sync backlog`
Design patterns: Specification/Policy Object; Architectural baseline only.

Description:
- R1: Define PR-31 through PR-39 with exact dependencies, Git metadata, and one-to-one requirements and acceptance criteria.
- R2: Define Gold-only serving to PostgreSQL at `10.10.1.3:54321`; Parquet Gold remains authoritative.
- R3: Define the dedicated `regime-loader` runtime role and protected credential handling.
- R4: Define UTC `timestamp_m1` storage as `TIMESTAMPTZ(6)` and observation-day identity.
- R5: Define complete bootstrap and complete accumulated-delta reconciliation semantics.
- R6: Define the shared project log path and an executable offline governance contract.

Acceptance:
- A1 (verifies R1): PR-31 through PR-39 metadata is complete and unique.
- A2 (verifies R2): the contract names only canonical Gold as the PostgreSQL source.
- A3 (verifies R3): role and credential rules contain no operational secret.
- A4 (verifies R4): the timestamp and UTC contracts are explicit.
- A5 (verifies R5): bootstrap, missed-run, and historical-revision behavior is explicit.
- A6 (verifies R6): the single project log path is explicit and tested.

## PR-32: PostgreSQL Gold Sync Contracts

PR name: `postgres-gold-sync-contracts`
Status: Merged
Updated: 2026-08-22
PR: #34
Git branch: `pr-32/postgres-gold-sync-contracts`
Git status: `merged`
Agent lane: Foundation; one weak agent
Depends on: PR-31
Commit: `feat(pr-32): postgres-gold-sync-contracts define gold sync boundary`
Design patterns: Ports and Adapters, Repository, Value Object, Dependency Injection.

Description:
- R1: Define the Gold-to-PostgreSQL dataset and internal sync-table contracts.
- R2: Define immutable sync state, row digest, delta plan, and result value objects.
- R3: Define a narrow application `GoldSyncRepository` protocol with no `psycopg` import.
- R4: Require exact schema/feature compatibility, catalog-current complete-build selection, UTC sessions, and redaction.

Acceptance:
- A1 (verifies R1): only canonical Gold is publishable.
- A2 (verifies R2): all value objects and result counts are typed and immutable.
- A3 (verifies R3): application contracts remain adapter-independent.
- A4 (verifies R4): incompatible or non-current sources fail deterministically.

## PR-33: Deterministic Gold Row Delta Planner

PR name: `gold-row-delta-planner`
Status: Merged
Updated: 2026-08-22
PR: #37
Git branch: `pr-33/gold-row-delta-planner`
Git status: `merged`
Agent lane: Application planning; one agent only
Depends on: PR-32
Commit: `feat(pr-33): gold-row-delta-planner compute complete gold delta`
Design patterns: Strategy, Value Object, Dependency Injection.

Description:
- R1: Plan deterministic insert/update/delete/unchanged sets from complete source and target digests.
- R2: Support empty-target bootstrap, stale keys, historical revisions, missed runs, and no-op checkpoints.
- R3: Preserve stable ordering, exact counts, and credential-free contracts.

Acceptance:
- A1 (verifies R1): mixed deltas contain exactly the expected keys and counts.
- A2 (verifies R2): bootstrap and accumulated missed-run reconciliation converge.
- A3 (verifies R3): repeated planning is deterministic and credential-free.

## PR-34: PostgreSQL Gold Sync Adapter

PR name: `postgres-gold-sync-adapter`
Status: Merged
Updated: 2026-08-22
PR: #38
Git branch: `pr-34/postgres-gold-sync-adapter`
Git status: `merged`
Agent lane: Agent B; PostgreSQL persistence
Depends on: PR-32
Commit: `feat(pr-34): postgres-gold-sync-adapter implement transactional repository`
Design patterns: Adapter, Repository, Unit of Work, Dependency Injection.

Description:
- R1: Add `psycopg` as the only PostgreSQL client and configure the exact endpoint, role, database, protected password, and UTC timezone.
- R2: Implement idempotent consumer and internal sync-table DDL with exact Gold types.
- R3: Read only state/digests for comparison and apply scoped insert/update/delete deltas under an advisory transaction lock.
- R4: Commit sync state last, roll back all mutations together, and redact credentials.

Acceptance:
- A1 (verifies R1): dependency and connection identity are exact.
- A2 (verifies R2): DDL, keys, columns, and internal tables are exact and idempotent.
- A3 (verifies R3): reads and mutation ordering avoid full-target reloads.
- A4 (verifies R4): failures roll back and diagnostics contain no credentials.

## PR-35: Provision Dedicated PostgreSQL Service Role

PR name: `postgres-service-role-provisioning`
Status: Merged
Updated: 2026-08-22
PR: #35
Git branch: `pr-35/postgres-service-role-provisioning`
Git status: `merged`
Agent lane: PostgreSQL operations; one weak agent
Depends on: PR-31
Commit: `feat(pr-35): postgres-service-role-provisioning add least privilege role setup`
Design patterns: Command, Least Privilege, Idempotent Provisioning.

Description:
- R1: Provision or validate exactly the `regime-loader` LOGIN role at the dedicated endpoint.
- R2: Enforce least-privilege attributes and only the `regime_loader` and `regime_loader_sync` schema rights.
- R3: Keep administrator and runtime credentials separate, protected, redacted, and idempotent; incompatible state fails safely.

Acceptance:
- A1 (verifies R1): endpoint and role identity are exact.
- A2 (verifies R2): all required role attributes and schema grants are exact.
- A3 (verifies R3): credential separation, idempotency, and failure behavior pass offline.

## PR-36: PostgreSQL Sync Operational Config

PR name: `postgres-sync-operational-config`
Status: Merged
Updated: 2026-08-22
PR: #36
Git branch: `pr-36/postgres-sync-operational-config`
Git status: `merged`
Agent lane: Operations; one weak agent
Depends on: PR-31
Commit: `feat(pr-36): postgres-sync-operational-config add repository postgres settings`
Design patterns: Adapter, Dependency Injection.

Description:
- R1: Extend protected ignored YAML config with exact PostgreSQL host, port, role, database, and password settings.
- R2: Export shell-safe `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, and `PGPASSWORD`; validate and redact failures.
- R3: Define `${PROJECT_ROOT}/.logs/regime-loader.log` as the canonical log path.

Acceptance:
- A1 (verifies R1): valid settings resolve exactly.
- A2 (verifies R2): export, validation, quoting, and redaction pass offline.
- A3 (verifies R3): config/log paths are ignored and canonical.

## PR-37: Gold To PostgreSQL Complete Delta Sync

PR name: `gold-postgres-delta-sync`
Status: Merged
Updated: 2026-08-22
PR: #39
Git branch: `pr-37/gold-postgres-delta-sync`
Git status: `merged`
Agent lane: Integration; one agent only
Depends on: PR-33, PR-34
Commit: `feat(pr-37): gold-postgres-delta-sync synchronize complete gold state`
Design patterns: Facade/Orchestrator, Unit of Work, Repository, Dependency Injection.

Description:
- R1: Resolve only the catalog-current compatible Gold build and reconcile its complete row-digest state.
- R2: Bootstrap every row, apply accumulated insert/update/delete deltas, propagate historical corrections, and perform no unrelated pipeline work.
- R3: Verify post-write bounds/counts, preserve prior state on failure, and return typed credential-free counts.

Acceptance:
- A1 (verifies R1): only current Gold and sync repositories are called.
- A2 (verifies R2): bootstrap, no-op, mixed delta, missed-run, revision, and delete cases are exact.
- A3 (verifies R3): verification failure and retry preserve consistency and result fields are exact.

## PR-38: PostgreSQL Gold Sync CLI

PR name: `postgres-gold-sync-cli`
Status: Merged
Updated: 2026-08-22
PR: #40
Git branch: `pr-38/postgres-gold-sync-cli`
Git status: `merged`
Agent lane: CLI/composition; one weak agent
Depends on: PR-35, PR-36, PR-37
Commit: `feat(pr-38): postgres-gold-sync-cli expose repository postgres synchronization`
Design patterns: Command, Dependency Injection.

Description:
- R1: Add exactly one `gold-sync-postgres` command composed from protected runtime variables and existing event logging.
- R2: Keep the command read-only toward local source/Gold production and report structured success/failure counts without secrets.

Acceptance:
- A1 (verifies R1): parser and dispatch expose exactly the command and exact connection settings.
- A2 (verifies R2): no-side-effect, structured-output, redaction, and stable error tests pass.

## PR-39: Sunday PostgreSQL Gold Sync Cron

PR name: `sunday-postgres-gold-sync-cron`
Status: Merged
Updated: 2026-08-22
PR: #41
Git branch: `pr-39/sunday-postgres-gold-sync-cron`
Git status: `merged`
Agent lane: Operations; one weak agent
Depends on: PR-38
Commit: `feat(pr-39): sunday-postgres-gold-sync-cron chain gold sync after daily run`
Design patterns: Command; Architectural baseline only for declarative scheduling.

Description:
- R1: Schedule the host-local chain at exactly `0 10 * * 0`.
- R2: Load protected config, create `.logs`, run `run-daily`, and run `gold-sync-postgres` only after success, using one log.
- R3: Preserve local Gold on PostgreSQL failure, provide a sync-only retry, and keep source reconciliation separate.

Acceptance:
- A1 (verifies R1): Sunday expression and no Saturday main schedule are exact.
- A2 (verifies R2): order, `&&` gating, log path, and full/delta semantics are tested.
- A3 (verifies R3): failure/retry, no-reconcile, no-secret, and no-scheduled-GitHub-ingestion cases pass.

## PR-40: Cross-Repository PostgreSQL Temporal Conformance Plan

PR name: `postgres-temporal-conformance-plan`
Status: In Progress
Updated: 2026-08-24
PR: TBD
Git branch: `pr-40/postgres-temporal-conformance-plan`
Git status: `active-clean`
Agent lane: Planning/governance; one agent only
Depends on: PR-39
Commit: `docs(pr-40): add postgres temporal conformance plan`
Design patterns: Specification/Policy Object; Architectural baseline only.

Description:
- R1: Freeze the shared `pg-temporal-v1` contract used by `xetra-loader` and `crypto-loader`: every PostgreSQL instant column is exactly `TIMESTAMPTZ(6)`, every session is UTC, the persistence boundary accepts only timezone-aware UTC zero-offset datetimes at microsecond precision, true calendar dates remain `DATE`, and serialized diagnostics use `YYYY-MM-DDTHH:MM:SS.ffffffZ` without treating PostgreSQL as text storage.
- R2: Record the audited implementation state precisely: current consumer/sync DDL already uses `TIMESTAMPTZ(6)` and the repository sets session timezone UTC, but `application.postgres_sync._utc` currently accepts any timezone-aware offset and silently normalizes it with `astimezone(UTC)`, while repository row decoding checks only `datetime` type; therefore the persistence/read boundary is weaker than the other two loaders and live target conformance is not independently proven by repository evidence.
- R3: Add PR-41 for strict boundary hardening plus live temporal verification and PR-42 for a separate controlled forced owned-schema/data rewrite; implementation and production operations must not be folded into this planning PR.
- R4: Extend the PostgreSQL completion gate so current PR-31..PR-39 fixture/code evidence is insufficient for final temporal conformance until PR-42 produces a real-target `PASS`.

Acceptance:
- A1 (verifies R1): the exact common type/precision/timezone/date/diagnostic rules appear without claiming that PostgreSQL stores a datetime string format.
- A2 (verifies R2): the finding matches the current `application/postgres_sync.py` and `ingestion/postgres_gold_repository.py` behavior and does not claim that live rows are already wrong.
- A3 (verifies R3): PR-41 and PR-42 are separately specified with exact dependencies, branches, commits, and non-overlapping concerns.
- A4 (verifies R4): final PostgreSQL temporal completion explicitly requires PR-42 real-target evidence.

## PR-41: PostgreSQL Temporal Boundary Hardening And Live Verifier

PR name: `postgres-temporal-boundary-hardening`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `pr-41/postgres-temporal-boundary-hardening`
Git status: `not-started (branch absent)`
Agent lane: Temporal contracts/verification; one weak agent
Depends on: PR-40
Commit: `fix(pr-41): enforce postgres temporal boundary`
Design patterns: Value Object, Specification/Policy Object, Adapter, Dependency Injection.

Description:
- R1: Change the application persistence contract so `_utc` rejects naive datetimes and rejects every aware datetime whose `utcoffset()` is not exactly zero; upstream code may normalize before the boundary, but the PostgreSQL application boundary must never silently reinterpret `+01:00`, `+02:00`, or another offset.
- R2: Harden PostgreSQL row decoding so every returned timestamp is timezone-aware and represents UTC/zero offset after the session is set to UTC; invalid driver/session values fail before constructing authoritative sync state or summaries.
- R3: Add one executable temporal specification requiring canonical Gold `timestamp_m1 = Datetime(us, UTC)`, all owned PostgreSQL instant columns in `regime_loader` and `regime_loader_sync` exactly `TIMESTAMPTZ(6)`, and any true date field exactly `DATE`; `TIMESTAMP WITHOUT TIME ZONE` and alternate precision are forbidden.
- R4: Add a live/read-mostly verifier that introspects `information_schema.columns`, requires `timestamp with time zone` plus `datetime_precision=6` for every owned timestamp column, rejects unexpected owned timestamp drift, verifies `SHOW TIME ZONE = UTC`, and compares current Gold dtype/semantics with target mapping.
- R5: Execute transaction-scoped microsecond round-trip probes around European DST transitions and require exact instant plus six-digit microsecond preservation; emit a deterministic sanitized report carrying `temporal_contract_version=pg-temporal-v1`, then roll back probes completely.

Acceptance:
- A1 (verifies R1): UTC values pass unchanged; naive, `+01:00`, `+02:00`, and another non-zero-offset fixture fail deterministically instead of being normalized at the boundary.
- A2 (verifies R2): aware UTC database values construct state successfully; naive/non-UTC fake driver values fail before state/summary construction.
- A3 (verifies R3): contract tests prove every expected owned instant uses exactly `TIMESTAMPTZ(6)`, `timestamp_m1` source is exactly `Datetime(us, UTC)`, and no date is coerced to timestamp.
- A4 (verifies R4): compatible schema/session passes; deliberate timestamp-without-time-zone, precision-3, missing/extra timestamp column, or non-UTC session fixture fails closed.
- A5 (verifies R5): `2026-03-29T00:59:59.123456Z` and `2026-10-25T01:30:00.654321Z` round-trip identically, probe leaves zero durable objects/rows, and the sanitized report cannot be `PASS` when any temporal check fails.

## PR-42: Forced PostgreSQL Temporal Schema And Data Rewrite

PR name: `postgres-temporal-authoritative-rewrite`
Status: Planned
Updated: 2026-08-24
PR: TBD
Git branch: `pr-42/postgres-temporal-authoritative-rewrite`
Git status: `not-started (branch absent)`
Agent lane: Production operations; one agent only
Depends on: PR-41
Commit: `chore(pr-42): rewrite postgres temporal serving state`
Design patterns: Command, Unit of Work, Adapter, Fail-Closed Verification.

Description:
- R1: Disable the Sunday `run-daily && gold-sync-postgres` chain for the maintenance window and acquire a dedicated rewrite lock so no concurrent normal writer can mutate the serving state.
- R2: Before destructive work, create an operator-controlled timestamped backup of only `regime_loader` and `regime_loader_sync`, record private restore/checksum evidence plus sanitized pre-rewrite schema/type/count summaries, and fail if backup validation is incomplete.
- R3: Force one-time recreation of only the owned `regime_loader` and `regime_loader_sync` schemas from current canonical DDL even when the old schema appears compatible; preserve unrelated schemas, roles, and every other repository's PostgreSQL state exactly.
- R4: Republish the complete catalog-current compatible Gold build from authoritative Parquet into the empty target through the validated normal sync path so every persisted instant is written under `pg-temporal-v1`.
- R5: Run PR-41 independently after rebuild and require exact Gold/PostgreSQL row count, logical keys, row digests, sync state, timestamp min/max, UTC session, and temporal-column equality.
- R6: Immediately run unchanged `gold-sync-postgres` again and require exactly zero inserts, updates, deletes, or timestamp/metadata rewrites; re-enable the Sunday schedule only after all checks are `PASS`.
- R7: Commit only a sanitized real-target acceptance report with `temporal_contract_version=pg-temporal-v1`; backup failure, schema mismatch, data mismatch, temporal mismatch, unrelated-schema change, or non-zero replay mutation blocks completion.

Acceptance:
- A1 (verifies R1): runbook/test evidence proves scheduled and concurrent writes are impossible during the rewrite window.
- A2 (verifies R2): validated backup precedes any drop/recreate and has a documented independently testable restore path.
- A3 (verifies R3): before/after catalog proves only `regime_loader` and `regime_loader_sync` were recreated and every owned instant column introspects exactly as `TIMESTAMPTZ(6)`.
- A4 (verifies R4): the complete current Gold dataset is present after bootstrap with exact canonical UTC microsecond timestamp semantics.
- A5 (verifies R5): PR-41 report is `PASS` and all row/key/digest/state/bounds comparisons are exact.
- A6 (verifies R6): immediate unchanged replay reports zero mutations and scheduling is restored only after PASS.
- A7 (verifies R7): report contains no credentials/raw market data, names `pg-temporal-v1`, and any injected mismatch prevents completion.

### PostgreSQL temporal completion extension

The PostgreSQL serving plane is not finally temporally certified merely because PR-31 through PR-39 are merged. It is temporally complete only when PR-40 is merged, PR-41 enforces and independently verifies `pg-temporal-v1`, and PR-42 has forced a one-time owned-schema/data rebuild from authoritative current Gold with a sanitized real-target acceptance report marked `PASS`.

## Abgeschlossene PRs – Kurzfassung

- PR-01–05: Repository-Grundlage, Qualitätsgates, Registry/Pfade, Parquet-I/O, HTTP-Port sowie Delta-/Reconcile-Planung.
- PR-06–10: Provider-Adapter für CBOE, STOXX, Yahoo, ECB und FRED.
- PR-11–14: Betriebsmanifeste, Bronze-Orchestrierung, kanonisches Silver und Inventory-CLI.
- PR-15–17: Volatilitäts- und Makro-Features sowie validierter kanonischer Gold-Frame.
- PR-18–22: Immutable Gold-Builds, Katalogauflösung, Sidecars, Publikations-State-Machine und sichere Retention.
- PR-23–25: Delta-only-Tagespipeline, Backlog-Governance und tägliches Cron-Template.
- PR-26–30: Live-Provider-Reparaturen, Polars-Parallelisierung, Gold-Mirror, geschützte YAML-Konfiguration und diagnostisches Gold-Profil.
- PR-31–39: PostgreSQL-Serving-Plan, Sync-Verträge/Planner/Adapter, Service-Rolle, Betriebskonfiguration, vollständige Delta-Synchronisierung, CLI und Sonntags-Cron.
