# REGIME-LOADER

Reusable daily market-state loader for quantitative research and portfolio systems.

The repository acquires open/public market and macro series, preserves their historical observations in a deterministic Parquet lake, normalizes them into a canonical daily schema, derives causal market-state features, and publishes immutable Gold snapshots for downstream consumers. Canonical Gold can additionally be replicated into PostgreSQL as a rebuildable serving-plane copy; immutable Parquet Gold remains the source of truth.

The project is intentionally a **data product**, not a trading system. It does not own HMM states, `risk_on`/`risk_off` labels, prediction targets, portfolio weights, or execution decisions.

## Status

The reviewed medallion architecture is implemented through the atomic PR sequence in `BACKLOG.md`, including the PostgreSQL serving-plane sequence: only canonical Gold is replicated, while Bronze, Silver, immutable Gold bundles, and the authoritative Gold catalog remain local lake concerns.

Before implementing a backlog PR, coding agents must read `AGENTS.md`, `BACKLOG.md`, and `ARCHITECTURE.md`.

## Architecture

The implementation uses **hexagonal architecture (Ports and Adapters)** with dependency injection and composition.

```text
                         api / scripts
                              |
                              v
                         application
                    use cases + policies
                    /                 \
                   v                   v
          persistence ports       provider ports
                   ^                   ^
                   |                   |
        Parquet/PostgreSQL       HTTP/provider
        repository adapters       adapters
                   \                   /
                    +--------+---------+
                             |
                             v
                 Bronze -> Silver -> Gold
                                      |
                                      v
                              PostgreSQL serving
```

Core design patterns are deliberately explicit:

- **Adapter** for provider and persistence implementations.
- **Strategy** for retry, update/reconciliation, and consumer-resolution policies.
- **Registry/Factory** for canonical provider/series routing; orchestration must not use provider `if/elif` ladders.
- **Repository** for Bronze, Silver, ingestion state, run manifests, Gold build storage, the Gold catalog, and PostgreSQL serving state.
- **Unit of Work** for one-series ingestion durability, Gold publication commit boundaries, and transactional PostgreSQL deltas.
- **State Machine** for Gold publication: `building -> complete|failed`.
- **Materialized View** for root Gold JSON/PNG, derived from authoritative `manifest.parquet`.
- **Mark-and-Sweep** for safe Gold retention: make a build unselectable before deleting physical files.
- **Command** for CLI adapters that call application use cases without embedding business logic.

Prefer `typing.Protocol`, immutable contracts, pure transformations, and constructor injection over inheritance-heavy frameworks.

## Initial Series Catalog

| Canonical ID | Primary source | Purpose |
|---|---|---|
| `vix` | CBOE | S&P 500 implied volatility |
| `vix9d` | CBOE | short-horizon implied volatility |
| `vix3m` | CBOE | 3-month implied volatility |
| `vix6m` | CBOE | 6-month implied volatility |
| `vix1y` | CBOE | 1-year implied volatility |
| `vstoxx` | STOXX | Euro-area equity implied volatility |
| `move` | Yahoo Finance | US Treasury implied volatility |
| `ciss` | ECB | euro-area systemic stress |
| `estr` | ECB | euro short-term rate |
| `euro_hy_oas` | FRED | euro high-yield credit spread |
| `us_2y` | FRED | US 2-year Treasury yield |
| `us_10y` | FRED | US 10-year Treasury yield |
| `usd_broad` | FRED | broad US-dollar index |

No additional MVP series may be introduced without a separate backlog PR and matching contract updates.

## Source Update Policy

The loader distinguishes three explicit operation modes:

```text
bootstrap   -> first complete public history when no Bronze exists
update      -> normal delta-only execution for an existing series
reconcile   -> explicit operator-requested full-history reconciliation
```

### Normal update is delta-only

For an existing series, the authoritative Bronze data determines:

```text
latest_stored_date = max(Bronze.observation_date)
request_start      = latest_stored_date - overlap_days
request_end        = injected_today
```

Default `overlap_days = 7` calendar days so recent source corrections can still replace equal-key observations. The normal `update` command and `run-daily` **never automatically switch to full-history reconciliation**.

For `date_range` providers, the network request must use those exact bounds. For `full_file` providers such as catalogued CBOE/STOXX sources, the complete remote object may have to be downloaded, but the adapter restricts the logical update/diff to the requested delta window before persistence. Normal execution therefore rewrites only inserted/revised delta rows and affected monthly partitions.

A shorter upstream response is never interpreted as permission to delete older retained history. Equal-key observations may be revised. Explicit deletion semantics require an explicit provider contract and are not inferred from omission.

### Explicit reconciliation

`reconcile` is a separate explicit command. It may request maximum currently exposed history to detect older revisions outside the overlap window. It is **not invoked automatically by `run-daily`**. If operators want periodic reconciliation, they schedule the explicit `reconcile` command separately.

## Medallion Lake

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

Bronze and Silver use deterministic monthly partitions. Gold is small enough to publish complete immutable snapshots.

## Bronze

Bronze preserves provider-shaped observations plus safe ingestion metadata. Natural key:

```text
(provider, series_id, observation_date)
```

## Silver

Canonical daily long-form contract:

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

OHLC sources use `value == close`; scalar sources use `value` and null OHLC fields. Silver never fills missing dates.

## Gold

Gold uses the temporal key:

```text
timestamp_m1: Datetime(time_unit="us", time_zone="UTC")
```

Daily source dates map to UTC midnight. This is **observation-day identity**, not provider release time or tradability time. Gold contains no `observation_date` column.

Feature semantics are fixed and causal:

- `delta_Nobs(t) = x(t) - x(previous Nth valid observation)`;
- 60-observation z-scores use the last 60 valid observations including `t` and population standard deviation (`ddof=0`);
- no forward fill, backward fill, interpolation, centered windows, or implicit as-of carry;
- same-series rolling operations count valid observations, not calendar days;
- cross-series ratios/spreads require the same `timestamp_m1`;
- final Gold contains nulls but no NaN or infinity.

Initial semantic versions:

```text
schema_version  = 1
feature_version = 1
```

Schema version changes for column name/order/type changes. Feature version changes for formula/parameter changes that preserve schema.

## Immutable Gold Build Bundle

Each successful build directory is creation-only:

```text
versions/build_id=<YYYYMMDDTHHMMSSZ>/
  data.parquet
  manifest.json
  feature_profile.png
```

`data.parquet` is the canonical full-history Gold frame. Build `manifest.json` records dataset/build identity, semantic versions, ordered columns, row/timestamp bounds, `data_sha256`, `feature_set_hash`, source Git commit, and plot path.

## Authoritative Gold Catalog

The publication authority is:

```text
lake/gold/dataset=regime_features_daily/manifest.parquet
```

Only `manifest.parquet` chooses the current build. Consumers and PostgreSQL synchronization must never use directory order, mtime, or `max(build_id)`.

Consumer resolution is policy-driven:

- `strict_current` is the safe default: current must be compatible/selectable or resolution fails.
- `latest_compatible` is an explicit resilience policy for ordinary local consumers.

PostgreSQL synchronization deliberately uses the strict current compatible build and its explicit immutable `data_path`.

## Root JSON And Plot Are Materialized Views

The dataset root also exposes rebuildable `manifest.json` and `feature_profile.png`. These materialized views do not participate in consumer selection. The atomic catalog replacement is the Gold publication commit.

## Publication State Machine

```text
new attempt
    |
    v
building,current=false
    |
    +--> build/validate immutable bundle
    |          |
    |          +-- failure --> failed,current=false
    |
    v
atomic catalog promotion
new=complete,current=true
old=current=false
    |
    v
refresh root materialized views
```

A previous current build remains authoritative until the atomic catalog promotion. Filesystem presence never auto-promotes an interrupted build.

## Retention

Default retention keeps five physical successful builds per `(schema_version, feature_version)` pair, including current. Retention first marks a build unselectable in the catalog and only then deletes its immutable physical bundle.

## PostgreSQL Gold Serving Replica

PostgreSQL is a serving/research replica, not the canonical data store. The only synchronized dataset is `regime_features_daily`.

```text
canonical source: lake/gold/dataset=regime_features_daily/...
consumer table:  regime_data.regime_features_daily
sync state:      regime_data_sync.gold_sync_state
row digests:     regime_data_sync.gold_row_hashes
```

`timestamp_m1` is stored as `TIMESTAMPTZ(6)` and the database session is UTC. Feature columns are nullable `DOUBLE PRECISION`. Sync metadata never pollutes the consumer table.

The first successful `gold-sync-postgres` run is necessarily a complete bootstrap because PostgreSQL has no synchronized state. Every later run compares the complete current Gold state against the complete stored row-digest state. This is an **accumulated delta**: if one or more weekly runs were missed, the next run inserts all missing rows, updates historical revisions, deletes stale serving keys, leaves unchanged rows untouched, and advances the synchronized checkpoint atomically.

A semantic `schema_version` or `feature_version` mismatch fails closed; it never triggers a hidden full rewrite. PostgreSQL delete semantics affect only the rebuildable serving replica and do not alter Bronze, Silver, immutable Gold, or source-history retention rules.

## Operational CLI

Install/sync the project and use the console entry point:

```bash
uv sync
uv run regime-loader --help
```

The exact command surface is:

```text
bootstrap
update
reconcile
silver-build
gold-build
gold-sync-postgres
inventory
run-daily
```

Global options such as `--lake-root`, `--today`, and `--overlap-days` precede the subcommand. `--series` follows commands that accept a series restriction.

Examples:

```bash
# Normal bounded source update only.
uv run regime-loader \
  --lake-root /srv/market-regime/lake \
  update --series us_10y

# Explicit operator reconciliation; never invoked by run-daily.
uv run regime-loader \
  --lake-root /srv/market-regime/lake \
  reconcile --series us_10y

# Full local Gold publication path.
uv run regime-loader \
  --lake-root /srv/market-regime/lake \
  run-daily

# Synchronize the currently catalog-selected Gold build only.
uv run regime-loader \
  --lake-root /srv/market-regime/lake \
  gold-sync-postgres

# Rebuild and print the local inventory.
uv run regime-loader \
  --lake-root /srv/market-regime/lake \
  inventory --json
```

`gold-sync-postgres` is intentionally independent of `run-daily`: it does not construct provider clients and does not execute Bronze, Silver, Gold build/publication, mirror, retention, or source reconciliation. This makes a failed database synchronization safely retryable without rebuilding canonical Gold.

### Daily pipeline contract

`run-daily` is deliberately **delta-only** for sources:

```text
recover interrupted Gold publication/root views
        -> Bronze update (or bootstrap only when that series has no Bronze)
        -> selected Silver rebuild
        -> full canonical Gold from all available Silver series
        -> immutable bundle + physical validation
        -> authoritative catalog promotion
        -> root materialized-view refresh
        -> Gold retention
        -> inventory refresh
```

For existing Bronze the request window is always:

```text
max(Bronze.observation_date) - overlap_days .. injected today
```

With the default overlap this is seven calendar days. `run-daily` has no hidden call path to source `reconcile`, maximum-history loading, or the historical minimum.

### Runtime configuration

Use a **persistent** lake path. A container-local ephemeral path would lose incremental state and defeat delta planning.

FRED-backed source commands require `FRED_API_KEY`. Gold-capable commands (`gold-build`, `run-daily`) record the source Git commit and packaged/deployed environments should set `MARKET_REGIME_GIT_COMMIT` explicitly.

PostgreSQL synchronization requires the dedicated repository role and exact endpoint:

```text
PGHOST=10.10.1.3
PGPORT=54321
PGUSER=regime-loader
PGDATABASE=<serving database>
PGPASSWORD=<repository-specific secret>
```

Do not commit these values as a credential string. Deployment configuration lives in ignored `config.yaml`. `scripts/export_cron_config.py config.yaml` validates the exact host, port, and role and exports shell-safe `PG*`, lake, project, mirror, FRED, and logging variables. The repository password must be distinct from the PostgreSQL administrator password.

The canonical main log is enforced as:

```text
${PROJECT_ROOT}/.logs/regime-loader.log
```

The optional Gold mirror still runs only as part of local publication; a PostgreSQL sync failure does not roll back the authoritative Gold catalog.

### Scheduling

The data lake is intended to run on the deployment host/NAS, not as scheduled GitHub Actions ingestion. The checked-in crontab template runs every **Sunday at 10:00 in the deployment host's local time zone**. It loads protected configuration, creates the project log directory, publishes local Gold, and only after a successful `run-daily` synchronizes PostgreSQL:

```cron
0 10 * * 0 /srv/regime-loader/ops/run-regime-loader-sunday.sh
```

The runner script resolves its project root, exports the protected `config.yaml`, creates `.logs`, and appends both command streams to `regime-loader.log`. The PostgreSQL sync runs only after `run-daily` succeeds.

Install it for the service account after reviewing the absolute project path:

```bash
crontab ops/regime-loader.cron
```

Operational semantics are explicit:

- `run-daily` failure prevents PostgreSQL synchronization;
- `gold-sync-postgres` failure makes the cron job non-zero but does **not** roll back or invalidate the already published local Gold build;
- after a database-only failure, retry only `uv run regime-loader --lake-root "$LAKE_ROOT" gold-sync-postgres` rather than rerunning source ingestion;
- the first successful database synchronization is complete; subsequent synchronizations are accumulated deltas and catch up any missed weekly runs;
- source maximum-history reconciliation remains a separate explicit schedule/command and is never part of the Sunday main chain;
- both main commands append stdout/stderr to the same `${PROJECT_ROOT}/.logs/regime-loader.log` through `LOG_PATH`.

If periodic maximum-history source reconciliation is desired, schedule `reconcile` separately and less frequently. Keeping source reconciliation separate makes the normal bounded source-update contract observable and testable.

## Quality Gates

Required push and merge checks:

```text
lint
type
unit
integration
coverage
```

`lint`, `type`, `unit`, and offline `integration` run in parallel. Unit and integration suites produce independent coverage data. The `coverage` gate combines them and requires total production-code line coverage:

```text
>= 90.0%
```

Live provider tests are marked `network` and are excluded from required gates.

## Repository Structure

```text
api/                 CLI adapters only
application/         use cases, contracts, policies, ports
application/ports/   provider/persistence/clock/sleeper interfaces
ingestion/           provider + filesystem/PostgreSQL adapters
scripts/             operational wrappers and repo tooling
tests/unit/          deterministic unit tests
tests/integration/   offline component/E2E tests
tests/fixtures/      committed small fixtures
lake/                ignored runtime data
AGENTS.md             coding-agent rules
BACKLOG.md            core implementation backlog
BACKLOG_POSTGRES.md   PostgreSQL serving extension backlog
ARCHITECTURE.md       durable engineering contract
README.md             operator/consumer contract
```

## Documentation Contract

`BACKLOG.md`, `BACKLOG_POSTGRES.md`, `ARCHITECTURE.md`, `README.md`, and `AGENTS.md` must not intentionally contradict one another. A PR that changes a documented contract updates the relevant sidecars in the same PR.
