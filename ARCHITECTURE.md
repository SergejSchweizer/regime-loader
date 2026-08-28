# Architecture

This document is the durable engineering contract for `regime-loader`.

`BACKLOG.md` defines delivery order and acceptance criteria. `README.md` is the operator/consumer contract. `AGENTS.md` defines coding-agent behavior. None may intentionally contradict this document.

## System Purpose

`regime-loader` is a reusable daily market-state data product. It acquires open/public market and macro series, preserves historical observations, performs bounded incremental source updates during normal operation, normalizes data into a canonical daily representation, derives causal reusable features, and publishes immutable Gold snapshots.

It does not own regime classification, HMM states, targets, portfolio optimization, position sizing, or trading execution.

## Architectural Style

The repository uses **hexagonal architecture (Ports and Adapters)**.

```text
                         api / scripts
                              |
                              v
                         application
                  use cases + policies + ports
                    /                    \
                   v                      v
        persistence abstractions      provider abstractions
                   ^                      ^
                   |                      |
          Parquet/JSON/PNG          HTTP/provider adapters
          filesystem adapters
```

Dependency rule:

```text
api/scripts -> application/contracts <- ingestion adapters
```

`application/` must not import concrete HTTP clients, provider modules, matplotlib, filesystem-specific persistence implementations, or CLI parsing.

## Design Pattern Policy

Patterns are used to make boundaries explicit, not to maximize abstraction count.

### Adapter

Concrete CBOE/STOXX/Yahoo/ECB/FRED implementations adapt provider protocols into canonical application contracts. Filesystem/Parquet/JSON/PNG implementations adapt persistence ports.

### Strategy

Use explicit strategy/policy objects for behavior that may vary independently:

- HTTP retry policy;
- strict normal update versus explicit full reconciliation planning;
- consumer resolution (`strict_current` versus `latest_compatible`).

Do not scatter equivalent behavior across provider-specific conditionals.

### Registry / Factory

Canonical series metadata and provider adapter lookup are registry-driven. Application orchestration receives a provider registry/factory and must not contain provider-name `if/elif` ladders.

### Repository

Persistence is exposed through narrow repository-style ports, for example:

```text
BronzeRepository
SilverRepository
IngestionStateRepository
RunManifestRepository
InventoryRepository
GoldBuildStore
GoldCatalogRepository
GoldMaterializedViewWriter
```

Physical partition/file logic stays in adapters.

### Unit of Work

Two boundaries need explicit commit semantics:

1. **One-series Bronze execution** — source fetch/diff/write, success-run persistence, state advancement.
2. **Gold publication** — candidate bundle validation and atomic catalog promotion.

A use case must make the durability boundary obvious and test failure before/after it.

### State Machine

Gold catalog publication state is:

```text
building -> complete
        \-> failed
```

Only the root Gold catalog owns this lifecycle. Immutable build `manifest.json` describes artifact identity and must not independently claim publication state.

### Materialized View

Root Gold `manifest.json` and `feature_profile.png` are derived views of authoritative `manifest.parquet` plus its current build. They are rebuildable after interruption and never participate in current-build selection.

### Mark-and-Sweep

Retention first makes a non-current build unselectable in the catalog, then deletes its physical bundle. This avoids a crash window in which a catalog-selectable build is partially deleted.

### Command

CLI subcommands are adapters: parse arguments/configuration, call one application use case, render output, set exit code. They do not contain provider, feature, or persistence business rules.

### Dependency Injection

Inject clocks, sleepers, HTTP clients, repositories, provider registries, source-control identity, and policies. Tests must not depend on real wall clock, sleeping, network, or repository discovery where an injected boundary is appropriate.

Prefer `typing.Protocol`, immutable dataclasses/Pydantic models, and composition. Avoid inheritance hierarchies unless a true substitutable abstraction exists.

## Layer Ownership

| Layer | Owns | Must not own |
|---|---|---|
| `api/` | CLI parsing, validation, output/exit codes | provider HTTP/parsing, lake persistence, feature formulas |
| `application/` | use cases, contracts, policies, ports, state machine, validation | `httpx`, filesystem details, matplotlib, CLI parsing |
| `ingestion/` | provider adapters, HTTP adapter, Polars/Parquet IO, JSON/PNG materialization | CLI policy, regime decisions, portfolio logic |
| `scripts/` | operational wrappers, GitHub/quality tooling | hidden domain behavior |
| `tests/` | unit/contract/regression/offline integration | production behavior |

## Initial Series Registry

Exactly these canonical series are in MVP:

```text
vix
vix9d
vix3m
vix6m
vix1y
vstoxx
move
ciss
estr
euro_hy_oas
us_2y
us_10y
usd_broad
```

Provider ownership:

```text
CBOE   -> vix, vix9d, vix3m, vix6m, vix1y
STOXX  -> vstoxx
Yahoo  -> move
ECB    -> ciss, estr
FRED   -> euro_hy_oas, us_2y, us_10y, usd_broad
```

Every registry entry declares canonical ID, provider, source ID/file, unit, native shape (`ohlc|scalar`), frequency, bootstrap policy, and fetch capability (`date_range|full_file`).

Provider adapters must not invent unregistered symbols or silent fallbacks.

## Medallion Data Flow

```text
Bronze
 provider-shaped observations
 source audit metadata
 monthly Parquet
       |
       v
Silver
 canonical daily long form
 monthly Parquet
       |
       v
Gold feature families
 causal deterministic transforms
       |
       v
Canonical Gold frame
       |
       v
Immutable build bundle
 data.parquet + manifest.json + feature_profile.png
       |
       v
Gold catalog publication
 manifest.parquet <- authority
       |
       v
Root materialized views
 manifest.json + feature_profile.png
```

## Physical Lake Layout

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

Runtime `lake/` is ignored by Git.

## Bronze Contract

Common fields:

```text
series_id: String
provider: String
observation_date: Date
fetched_at_utc: Datetime(time_zone="UTC")
source_id: String
source_url: String
```

Payload is exactly one shape:

```text
ohlc   -> open, high, low, close
scalar -> value
```

Natural key:

```text
(provider, series_id, observation_date)
```

Rules:

- no synthetic observations;
- duplicate incoming natural keys are rejected;
- equal-key source revisions may replace retained rows once;
- upstream omission or a shorter response does not imply deletion;
- only affected monthly partitions are rewritten;
- logical no-op is a physical no-op.

## Silver Contract

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

Natural key:

```text
(series_id, observation_date)
```

Rules:

- OHLC source: `value == close`;
- scalar source: scalar `value`, null OHLC;
- no fill/interpolation;
- finite non-null `value` only;
- deterministic selected-series rebuild and monthly diff/write.

## Strict Source Update Contract

The planner exposes three explicit operation modes:

```text
bootstrap
update
reconcile
```

### Bootstrap

When authoritative Bronze has no observation for a selected series, request maximum public history exposed by the configured source.

### Normal Update

When Bronze exists, calculate the logical request window from the **newest durable Bronze observation**:

```text
latest_stored_date = max(Bronze.observation_date)
request_start      = latest_stored_date - overlap_days
request_end        = injected_today
```

Default overlap is seven calendar days. The overlap exists only to capture recent source revisions.

The historical minimum is explicitly **not** part of normal delta planning:

```text
min(Bronze.observation_date) != request_start
```

Canonical example:

```text
min stored       = 2000-01-03
latest stored    = 2026-08-18
today            = 2026-08-19
overlap          = 7 days
normal request   = 2026-08-11 .. 2026-08-19
```

A normal update requesting `2000-01-03 .. 2026-08-19` is a contract violation.

Additional invariants:

- `latest_stored_date` comes from authoritative Bronze; state is a cache/audit record and cannot broaden the request.
- if `request_end < latest_stored_date`, execution fails.
- `date_range` providers receive the exact bounded start/end and may not silently broaden to maximum history.
- bounded-provider rows outside the requested interval are contract errors.
- `full_file` providers may have to download their entire compact public object because the upstream source exposes no bounded endpoint; during normal update they must filter parsed observations to the exact logical window before diff/persistence.
- full-file out-of-window rows are ignored during normal delta diff and must not trigger old-month rewrites.
- source omission/shortening never deletes older retained history.

This distinction is deliberate: the project guarantees **logical/persistence delta-only normal execution** for all providers and **network-level delta retrieval where the provider supports date bounds**. It does not falsely claim network-level delta for public full-file-only sources.

### Explicit Reconcile

`reconcile` is an explicit operator command that may request maximum currently exposed source history to discover revisions older than the normal overlap window.

`run-daily` and normal `update` must **never automatically choose `reconcile`** based on elapsed time, state age, or any hidden policy. If periodic reconciliation is desired, it is scheduled as a separate explicit command.

The sole Sunday `run-daily -> gold-sync-postgres` chain is scheduled at `0 10 * * 0` using the explicit `Europe/Vienna` IANA timezone. It retains 10:00 local wall-clock execution across daylight-saving changes; reconciliation remains a separately scheduled explicit operator command.

Reconciliation still obeys the rule that source omission is not deletion. Explicit deletion handling requires a separate source-mutation contract.

State tracks at least last success, last observation cache, requested bounds, operation mode, fetched/accepted/changed row counts, and optional last successful explicit reconciliation time.

## One-Series Ingestion Unit Of Work

Application orchestration follows:

```text
plan
 -> provider fetch/parse
 -> enforce logical request window
 -> logical diff
 -> durable Bronze write
 -> durable success run manifest
 -> ingestion state advance       <- commit complete
```

If failure occurs before the commit boundary, previous state remains authoritative. Independently completed series remain durable.

A no-op may update `last_success_utc`, but must not change `last_observed_date` or rewrite Bronze files.

## Gold Timestamp Contract

Gold key:

```text
timestamp_m1: Datetime(time_unit="us", time_zone="UTC")
```

It is first column, unique, strictly increasing, and UTC midnight for the represented source date.

`timestamp_m1` is **observation-day identity only**. It is not provider publication time, availability time, or proof that a value was tradable at midnight. Same-day intraday backtests need an explicit downstream availability/lag policy.

Gold contains no `observation_date`.

## Gold Feature Math

For a single series:

```text
delta_Nobs(t) = x(t) - x(previous Nth valid observation)
```

Every source level has a causal `delta_1obs` feature. Existing 5- and 20-observation
deltas remain series-specific as declared by the feature family.

A 60-observation z-score uses the last 60 valid observations including `t` and population standard deviation (`ddof=0`). It is null before 60 observations or when standard deviation is zero.

Cross-series ratios/spreads require same `timestamp_m1` values.

Forbidden:

- forward fill;
- backward fill;
- interpolation;
- centered windows;
- future data;
- implicit as-of carry.

Before final Gold validation, feature NaN is normalized to null. Infinity is rejected. Final feature columns are nullable `Float64` with no NaN/infinity.

## Gold Semantic Versions

Initial constants:

```text
schema_version  = 2
feature_version = 1
```

`schema_version` changes for column name/order/type changes. `feature_version` changes when formulas/parameters change without a schema change. Runtime never auto-increments either.

## Gold Build Store

Build ID format:

```text
YYYYMMDDTHHMMSSZ
```

A build directory is creation-only. Existing build path reuse fails before overwrite.

Build store writes `data.parquet` via same-directory temporary file, validates readback, then atomically replaces the final file. It returns row count, timestamp bounds, and SHA-256 of final bytes.

Explicit build reads require a concrete build ID. No filesystem `latest` discovery exists.

## Immutable Build Manifest

Build `manifest.json` describes artifact identity, not publication status.

Required concepts include:

```text
dataset_id
build_id
artifact_state = built
schema_version
feature_version
build_started_at_utc
build_completed_at_utc
rows_out
columns
min_timestamp
max_timestamp
data_path
data_sha256
feature_set_hash
git_commit_hash
plot_path
```

`feature_set_hash` is deterministic from schema/feature versions plus ordered names/dtypes/formula parameters. Build JSON/PNG are creation-only siblings of Parquet.

## Gold Catalog Repository

Authoritative file:

```text
lake/gold/dataset=regime_features_daily/manifest.parquet
```

Exact catalog fields:

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

Catalog invariants:

- unique `build_id`;
- only `complete` may be current;
- zero current before first successful publication is valid;
- after publication at most one current row;
- `building`/`failed` are never selectable;
- pruned rows have `pruned_at_utc != null` and null artifact paths;
- selectable rows are complete, non-pruned, and have all artifact paths populated.

The catalog repository validates logical/path-shape invariants but does **not** inspect physical filesystem existence. Physical bundle integrity belongs to `GoldBuildStore`/publication validation.

## Consumer Resolution Strategy

Resolution is a pure catalog operation with explicit policy.

### `strict_current` — default

Require the current row to be complete, non-pruned, selectable, and compatible with the caller's supported schema/feature versions. Otherwise fail.

### `latest_compatible`

Prefer a compatible current row; if current is incompatible, choose newest compatible complete/non-pruned/selectable row ordered by:

```text
completed_at_utc DESC, build_id DESC
```

No resolution strategy inspects filesystem mtime/glob order.

After logical resolution, opening the selected build performs physical integrity validation.

## Gold Publication State Machine And Unit Of Work

Publication stages:

```text
register attempt -> building,current=false
        |
        v
create + validate immutable bundle
        |
      success ---------------- failure
        |                         |
        v                         v
atomic catalog promotion      catalog failed
new complete/current          current unchanged
old current demoted
        |
        v
refresh root materialized views
```

The **atomic catalog replacement is the publication commit point**.

Before promotion, validate Parquet hash/row/timestamps/schema/version, build JSON identity/hash/path values, valid plot, and expected build-directory containment.

Filesystem presence never promotes an interrupted build. Stale non-current `building` rows are recovered to `failed` unless a future explicit resume protocol is introduced.

## Root Materialized Views

Root:

```text
manifest.json
feature_profile.png
```

are derived from authoritative catalog/current build.

They are refreshed after each successful catalog mutation and reconciled on startup/publication entry. If refresh fails after a catalog commit, return an operational error but do not pretend the catalog commit rolled back. A later reconciliation regenerates the views.

This Gold-view reconciliation is unrelated to market-source full-history `reconcile`.

## Retention: Mark And Sweep

Default:

```text
gold_retention_successful_builds = 5
```

Evaluated per `(schema_version, feature_version)` pair including current.

Safe prune flow:

```text
select eligible non-current build
 -> validate catalog identity
 -> atomic catalog tombstone:
      data_path = null
      build_manifest_path = null
      plot_path = null
      pruned_at_utc = now
 -> physical bundle delete
 -> retry orphan cleanup if deletion failed/interrupted
```

The tombstone happens **before** deletion, so a crash cannot leave a selectable row pointing at a partially deleted bundle.

Current/building/failed/other-version rows are never pruned. Repeated retention is idempotent.

## Operational Manifests

`dataset_inventory.parquet` is a snapshot of observed Bronze coverage, not a synthetic market calendar. It records canonical identity, provider, min/max observed date, row count, duplicate-key count, and physical file count.

For normal source update planning, **max observed date is relevant; min observed date is not the update start**.

`ingestion_runs.parquet` records run ID, series/provider, operation mode, requested bounds, fetched/accepted/inserted/revised rows, written partitions, status, timestamps, and sanitized error metadata.

No secrets may appear in registry files, persisted URLs, logs, error strings, or fixtures.

## Daily Pipeline

`run-daily` performs:

```text
1. recover stale Gold building attempts
2. reconcile root Gold materialized views if needed
3. for each selected series:
      no Bronze -> bootstrap
      Bronze exists -> strict update from latest-overlap through today
4. Bronze selected/all series
5. Silver selected/all series
6. build full canonical Gold from current Silver
7. create immutable Parquet + JSON + PNG bundle
8. validate candidate
9. atomically promote catalog
10. refresh root materialized views
11. mark-and-sweep retention
12. refresh inventory
```

`run-daily` must never invoke market-source full-history `reconcile` for an existing series. The source `reconcile` command is separate and explicit.

Gold always uses the full canonical schema from all available Silver inputs even when Bronze/Silver source execution was filtered.

## Quality And Git Contract

Required checks:

```text
lint
type
unit
integration
coverage
```

`lint`, `type`, `unit`, and offline `integration` run in parallel. `coverage` combines unit + integration raw coverage and enforces production-code line coverage `>= 90.0%`.

`network` tests are excluded from required gates.

Target GitHub policy for `main`:

- protected/ruleset-controlled;
- pull request required;
- no direct push/force push/delete;
- required five checks;
- branch up to date / merge queue compatible;
- squash merge only;
- repository auto-merge enabled;
- implementation PRs use auto-merge so merge occurs only after all gate requirements are satisfied;
- merged head branch deleted automatically.

## Testing Contract

Tests are deterministic and offline by default.

Required classes include:

- exact schema/type/order tests;
- provider parsing fixtures;
- retry/error/secret-redaction tests;
- canonical delta request tests proving max-date-derived bounds and rejecting historical-minimum requests;
- `date_range` exact-bound tests;
- `full_file` post-fetch logical-window filtering tests;
- assertions that `run-daily` never auto-invokes source reconcile;
- no-op hash/mtime tests;
- failure injection around durability boundaries;
- restart/recovery tests;
- hand-calculable Gold formula tests;
- truncation-based causality regression;
- catalog resolution policy tests;
- materialized-view reconciliation tests;
- retention tombstone/orphan-cleanup tests;
- end-to-end bootstrap/delta/explicit-reconcile/publication regression.

## Relationship To `crypto-history-loader`

`crypto-history-loader` remains the design reference for deterministic medallion ownership, Polars/Parquet persistence, restart safety, `timestamp_m1`, Gold JSON manifests, and feature-profile plots.

`regime-loader` intentionally uses daily source semantics, monthly Bronze/Silver partitions, strict delta-only normal execution, optional explicit source reconciliation, and a catalog-driven immutable Gold publication model.
