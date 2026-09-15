# DIVAREL — Data Ingest Service Guide

> **For AI agents working on the DIVAREL codebase.**
> DIVAREL is a small Rust binary that ingests two external data sources into the `bourse` PostgreSQL database: **Divar real-estate listing counts** (currently paused) and **USD/IRR exchange rate** from TGJU (active). It was originally a submodule of the `dock` monorepo.
>
> This document describes the code as of 2026-09-15.

---

## 1. Project Overview

**DIVAREL** is a **batch ingestion job** — it is not a long-running service. It runs once, does its fetches, writes to Postgres, and exits. In production it is scheduled by `cron` (inside its own container) to run daily.

- **Language:** Rust (edition 2021)
- **Runtime:** tokio async
- **DB driver:** `tokio-postgres` (no ORM, raw SQL)
- **HTTP client:** `reqwest`
- **Container:** built from `Dockerfile.divarel`, runs a cron daemon that fires the binary on a schedule
- **Repo:** `https://github.com/mahdavi110/divarel.git` (branch `main`)

### What it does

1. Creates (if missing) two tables in `bourse`: `divar_data` and `dollar`.
2. Fetches USD/IRR from `https://api.tgju.org/.../price_dollar_rl` and upserts every row into `dollar`.
3. ~~Fetches Divar real-estate counts and inserts into `divar_data`.~~ **Currently commented out — do not re-enable without discussion.**

### What it does NOT do

- It does **not** refresh materialized views. That is the responsibility of `haho`.
- It does **not** run as a persistent HTTP server.
- It does **not** expose any API.

---

## 2. Live State (as of 2026-09-15)

| Item | Value |
|------|-------|
| Source location on brx | `/root/dock/divarel/` (submodule) |
| Image on brx | **not built yet** |
| Container on brx | **not running** |
| Scheduled on brx | **not yet** (no crontab line, no compose entry) |
| Source location on eepa | removed 2026-09-15 (server migrated) |
| Remote git | `https://github.com/mahdavi110/divarel.git` |
| Branch | `main` |
| HEAD | `40277c1` |

### Tables it owns

On the brx `pgdk` (bourse DB):

| Table | Current rows | Last write |
|-------|--------------|-----------|
| `dollar` | 3890 | 2026-06-29 (stale) |
| `divar_data` | 0 | never populated |

---

## 3. Repository Layout

```
/root/dock/divarel/
├── AGENTS.md               this file
├── Cargo.toml              dependencies
├── Cargo.lock
├── deploy.bat              Windows dev helper (legacy)
├── src/
│   └── main.rs             everything lives here (283 lines)
└── .vscode/
    ├── launch.json
    └── settings.json
```

Everything is in a single file: `src/main.rs`.

Related files outside this repo (in the parent `dock`):

```
/root/dock/Dockerfile.divarel                 image definition
/root/dock/scripts/divarel/docker-entrypoint.sh   cron daemon launcher
/root/dock/scripts/divarel/run_divarel.sh         one-shot runner
```

---

## 4. Source — `src/main.rs`

### 4.1 `main()`

```
1. Connect to Postgres (tokio_postgres)
2. create_tables(&client)       → CREATE TABLE IF NOT EXISTS ...
3. for each (location, category, price, recent_ads) tuple:
       create_divar_url()        ← URL built, but fetch is COMMENTED OUT
4. insert_dollar()              → retry up to 5 times, 5 s apart
5. exit
```

### 4.2 `create_tables()`

Creates two tables if they don't exist:

```sql
CREATE TABLE IF NOT EXISTS divar_data (
    date        DATE DEFAULT CURRENT_DATE,
    lon1        DOUBLE PRECISION,
    lat1        DOUBLE PRECISION,
    lon2        DOUBLE PRECISION,
    lat2        DOUBLE PRECISION,
    category    TEXT,
    price       BIGINT,
    recent_ads  TEXT,
    count       INTEGER,
    PRIMARY KEY (date, lon1, lat1, lon2, lat2, category, price, recent_ads)
);

CREATE TABLE IF NOT EXISTS dollar (
    date          BIGINT PRIMARY KEY,
    dollar_price  INTEGER
);
```

> `date` in `dollar` is a **`BIGINT` in YYYYMMDD format**, not a real DATE. This is intentional and matches what `hws` reads.

### 4.3 `insert_dollar()`

```
1. HTTP GET https://api.tgju.org/v1/market/indicator/summary-table-data/price_dollar_rl?...
2. Parse JSON: data[i][6] → date (strip slashes → YYYYMMDD int)
                data[i][3] → dollar_price (strip commas → int)
3. Build one bulk INSERT with ON CONFLICT (date) DO NOTHING
4. Execute
```

**Retry:** the caller wraps this in `for attempt in 1..=5` with `sleep(5s)` between failures.

**Idempotent:** `ON CONFLICT DO NOTHING` means repeated runs on the same day are safe.

### 4.4 `create_divar_url()` + Divar path (⚠ DISABLED)

The function builds a URL to `https://api.divar.ir/v8/map-discovery/bbox/posts/count`, iterating over:

- 2 bounding boxes (Tehran + Isfahan)
- 2 categories: `apartment-sell`, `plot-old`
- 4 price buckets: 12B, 8B, 4B, -1 (=no filter)
- 3 recency filters: `1d`, `7d`, none

That's 48 URLs. **But `fetch_and_insert_data()` is commented out**, so nothing is fetched or inserted. The loop just builds URLs and drops them.

The materialized-view creation (`mv_divar_apartment`, `mv_divar_plotold`) is also commented out.

**Do not re-enable without an explicit decision** — the code exists but is intentionally paused.

---

## 5. Environment Variables

| Var | Default | Used by |
|-----|---------|---------|
| `PGHOST` | `pgdk` | `build_conn_str()` |
| `PGUSER` | `dev` | `build_conn_str()` |
| `PGPASSWORD` | `vatanampareyetanameyiran` | `build_conn_str()` |
| `PGDATABASE` | `bourse` | `build_conn_str()` |
| `TZ` | system | entrypoint |
| `CRON_SCHEDULE` | `0 2 * * *` | entrypoint |
| `RUN_ARGS` | (empty) | run_divarel.sh |

⚠ `PGPORT` is **not** read by the code — `build_conn_str()` omits it. Since `pgdk` is on the compose network (`pgdk:5432`), this works. If you ever change the port, fix `build_conn_str()` first.

---

## 6. Container / Deployment

### 6.1 Dockerfile (`/root/dock/Dockerfile.divarel`)

```
Stage 1 (builder): rust:1-bookworm
    → cargo build --release
Stage 2 (runtime): debian:bookworm-slim
    + ca-certificates, tzdata, cron, libssl3, libpq5
    → copies divarel binary + entrypoint + run script
    ENTRYPOINT ["/docker-entrypoint.sh"]
```

### 6.2 `docker-entrypoint.sh`

```sh
1. Set TZ
2. CRON_SCHEDULE (default "0 2 * * *")
3. Install crontab line:  $CRON_SCHEDULE /usr/local/bin/run_divarel.sh >> /var/log/divarel.log 2>&1
4. exec cron -f     ← cron runs in foreground (container stays alive)
```

### 6.3 `run_divarel.sh`

```sh
exec /usr/local/bin/divarel $RUN_ARGS
```

### 6.4 Running the container

There is no compose service yet. Manual run:

```bash
docker run -d \
  --name divarel \
  --network dock_net \
  --restart unless-stopped \
  -e PGHOST=pgdk \
  -e PGUSER=dev \
  -e PGPASSWORD=vatanampareyetanameyiran \
  -e PGDATABASE=bourse \
  -e TZ=Asia/Tehran \
  -e CRON_SCHEDULE="0 2 * * *" \
  divarel:latest
```

### 6.5 One-shot run (for testing / backfill)

```bash
docker run --rm --network dock_net \
  -e PGHOST=pgdk -e PGUSER=dev -e PGPASSWORD=vatanampareyetanameyiran -e PGDATABASE=bourse \
  divarel:latest /usr/local/bin/divarel
```

Or execute the binary directly inside a running container:

```bash
docker exec -it divarel /usr/local/bin/divarel
```

---

## 7. Operations

### 7.1 Build image from source

```bash
cd /root/dock
docker build -f Dockerfile.divarel -t divarel:latest .
```

### 7.2 Logs

```bash
docker logs divarel --tail 50 -f
docker exec divarel tail -50 /var/log/divarel.log
```

### 7.3 Verify dollar data after a run

```bash
docker exec pgdk psql -U dev -d bourse -c "
SELECT max(date) AS latest, count(*) FROM dollar;"
```

### 7.4 Force a fresh fetch

```bash
docker exec divarel /usr/local/bin/divarel
```

Since the INSERT is `ON CONFLICT DO NOTHING`, this is safe — no duplicates.

---

## 8. Known Quirks

### 8.1 `PGPORT` is not read
`build_conn_str()` omits `PGPORT` from the connection string. On `dock_net` the default (5432) works. This is a bug waiting to happen — if pgdk ever changes its internal port, divarel will fail with a confusing connection error.

### 8.2 `dollar` table is append-only via upsert-ignore
Once a date is inserted, the price for that date is never updated. If TGJU publishes a corrected price, divarel will **not** fix the existing row. The `ON CONFLICT (date) DO NOTHING` clause is the reason.

### 8.3 Divar path is dead code but kept alive
`fetch_and_insert_data`, the location/category/price/recency loops, and both matview creations are commented out. They read as if they run, but they don't. Do not assume `divar_data` is being populated — it isn't.

### 8.4 No error handling on `create_tables()` failure
If Postgres is unreachable, `main()` returns an error and the process exits with a non-zero status. Cron will log it and move on. There is no retry at the table-creation level (only `insert_dollar` retries).

### 8.5 Bulk INSERT is unparameterized
`insert_dollar()` builds the entire INSERT as one string with `format!("({}, {})", date, price)` and runs it via `client.execute(&insert_query, &[])`. This is safe here because the values are parsed integers from TGJU — but it is a fragile pattern that would break with any string column.

### 8.6 `date` column type mismatch
`dollar.date` is `BIGINT` (YYYYMMDD). `divar_data.date` is `DATE`. Two different conventions in the same service — do not "unify" them without checking `hws` first.

---

## 9. Server Context (brx)

- **Server:** `185.105.239.25` (`srv9201603445`)
- **Container network:** `dock_net`
- **DB:** `pgdk` (PostgreSQL 16, `bourse`)
- **Related services on same host:**
  - `appdk` (runs `haho` on a cron) — will feed the same DB
  - `brx`, `brx-staging` — UI services
  - `hws` — **to be deployed** (will read `dollar` via `mv_*`)
- **eepa (192.168.10.7):** source was removed 2026-09-15; do not expect it to be authoritative anymore.

### Dependencies on other services

- **Upstream data providers:** `api.tgju.org` (must be reachable from brx — check before deploying)
- **Downstream consumers:** `hws` reads `dollar` via `mv_stock_cumulative_*` and similar. If divarel stops, those matviews go stale.

---

## 10. Migration History

- 2026-09-15: source was extracted from `/root/dock/divarel/` on eepa (the old server). The image and container on eepa were **deleted** as part of a bourse-pipeline cleanup.
- Source still lives on brx as a `dock` submodule, but **no image has been built yet on brx**. The next deployment step is to build and run it there.

---

## 11. Future Improvements (Roadmap)

1. **Re-enable the Divar path** — code is written and complete, but paused. Re-enabling would populate `divar_data` and refresh `mv_divar_apartment` / `mv_divar_plotold`.
2. **Read `PGPORT`** — add it to `build_conn_str()` to make port overrides work.
3. **Parameterized bulk insert** — convert `insert_dollar()` to use `pgx`-style placeholders or `COPY` for safety + speed.
4. **Add a `/healthz`** — currently the container is only checkable by whether the last cron run wrote a fresh row.
5. **Alerts if dollar is stale** — no monitoring; a failed run silently leaves the table behind.

---

## 12. Quick Reference

| Task | Command |
|------|---------|
| Build image | `cd /root/dock && docker build -f Dockerfile.divarel -t divarel:latest .` |
| Run (one-shot) | `docker run --rm --network dock_net -e PGHOST=pgdk -e PGUSER=dev -e PGPASSWORD=... -e PGDATABASE=bourse divarel:latest` |
| Run as daemon | `docker run -d --name divarel --network dock_net --restart unless-stopped -e PGHOST=pgdk -e PGUSER=dev -e PGPASSWORD=... -e PGDATABASE=bourse -e TZ=Asia/Tehran divarel:latest` |
| Logs | `docker logs divarel --tail 50 -f` |
| Check dollar freshness | `docker exec pgdk psql -U dev -d bourse -c "SELECT max(date), count(*) FROM dollar;"` |
| Trigger manual fetch | `docker exec divarel /usr/local/bin/divarel` |
| Repo | `https://github.com/mahdavi110/divarel.git` |

---

**Last updated:** 2026-09-15 (brx migration)
