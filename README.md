# Scheduler

A database-backed job scheduler and executor in **Rust**. Jobs are stored in
**PostgreSQL**, which also does the coordination — there is no Redis, RabbitMQ,
Kafka, or any other broker. Multiple worker processes pull due jobs safely,
execute them, and record the result, with crash recovery and multi-tenant
fairness built in.

- **Exactly-once *pickup*** — two workers can never claim the same job
  (`FOR UPDATE SKIP LOCKED`).
- **Crash-safe** — a worker that dies mid-job has its job reclaimed by a reaper
  once its lease expires; nothing is lost.
- **Fenced completion** — a worker that lost its lease cannot overwrite the
  result of the worker that replaced it.
- **Fair** — a tenant with 10,000 jobs cannot starve a tenant with 5.
- **Scheduled** — jobs run at (or very close to) their `scheduled_at`.

---

## 1. Requirements

- **Rust** 1.94+ (edition 2024) — the highest minimum required by the dependency
  set (`sqlx` 0.9 needs 1.94)
- **PostgreSQL** 14+ (tested on 17)

---

## 2. Setup (3 commands)

```bash
# 1. Start PostgreSQL (Homebrew). Skip if you already have one running.
brew services start postgresql@17

# 2. Create the database
createdb hunar_jobs

# 3. Point the app at it and create the schema
export DATABASE_URL="postgres://$(whoami)@localhost:5432/hunar_jobs"
cargo run -p scheduler -- migrate
```

> `DATABASE_URL` must be set for every command. Copy `.env.example` for a list of
> all settings; the defaults work out of the box.

---

## 3. Use it

### Schedule a job (the producer side)

Anyone can enqueue with the CLI. A job has an **org** (tenant), a **kind**
(which handler runs it), an optional JSON **payload**, and a delay.

```bash
# Run the "sleep" handler right now for org 1
cargo run -p scheduler -- enqueue --org 1 --kind sleep

# Run it in 5 minutes
cargo run -p scheduler -- enqueue --org 1 --kind sleep --delay 5m

# Pass arguments to the handler
cargo run -p scheduler -- enqueue --org 2 --kind sleep --payload '{"min_ms":100,"max_ms":2000}'

# Try the retry path: "fail" retries until attempt 2, then succeeds
cargo run -p scheduler -- enqueue --org 3 --kind fail --max-attempts 4
```

Delay units: `ms`, `s` (default), `m`, `h`, `d`. See
`cargo run -p scheduler -- enqueue --help` for all options.

### Run workers (the consumer side)

```bash
cargo run -p scheduler -- run --workers 4
```

This starts one process with a pool of 4 concurrent jobs plus a reaper. Run it
in several terminals — or on several machines — to scale horizontally. Press
Ctrl-C for a graceful shutdown.

### Check the queue

```bash
cargo run -p scheduler -- status          # all orgs
cargo run -p scheduler -- status --org 1  # one org
```

```
pending   42
running   4
completed 1958
failed    1
```

---

## 4. Adding your own handler

This is how you add real work. A handler is a small struct implementing the
`Handler` trait; workers dispatch by matching a job's `kind` to a handler.

```rust
use async_trait::async_trait;
use job_core::JobOutcome;
use job_worker::{Handler, JobContext};
use serde_json::json;

struct SendEmail;

#[async_trait]
impl Handler for SendEmail {
    fn kind(&self) -> &'static str {
        "send_email" // must match the --kind you enqueue with
    }

    async fn run(&self, ctx: JobContext) -> JobOutcome {
        // context gives you job_id, org_id, attempt, payload, and a cancel token
        let to = ctx.payload["to"].as_str().unwrap_or_default();
        // ... do the work ...

        if to.is_empty() {
            // transient problem? retry. permanent? use Fatal and stop retrying.
            return JobOutcome::Retryable { error: "missing 'to'".into() };
        }
        JobOutcome::Success { result: json!({ "sent_to": to }) }
    }
}
```

Register it in `crates/scheduler/src/cli.rs` (see `build_registry`):

```rust
Registry::new()
    .add(SleepHandler::default())
    .add(SendEmail) // <-- add yours
```

Then schedule it with `--kind send_email`.

> **Handlers must be idempotent.** Delivery is *at-least-once*: a job may run
> again after a crash recovery. Key any side effects on `ctx.job_id` (see the
> `db_effect` handler for the pattern).

---

## 5. How it works (in one picture)

```
 producer (CLI)                PostgreSQL                    workers (N processes)
   enqueue  ──INSERT──▶  jobs(state=pending,         ◀──claim── FOR UPDATE SKIP LOCKED
                          org_id, kind, payload,               │
                          scheduled_at, priority)              ├─ heartbeat (renews lease)
                                                               ├─ run handler
                                                               └─ complete/fail (fenced by lease)
                          ▲                                    plus a reaper that resets
                          └──────────────────────────────────── expired leases back to pending
```

- **Claim.** One SQL statement selects due jobs and marks them running
  atomically. `FOR UPDATE SKIP LOCKED` makes concurrent claims disjoint, so no
  job is ever handed to two workers.
- **Lease + heartbeat.** A running job holds a lease (`lease_expires_at`). The
  worker renews it while running. A dead worker stops renewing; the lease
  lapses.
- **Reaper.** Periodically returns jobs whose lease expired to `pending` (or
  dead-letters them past `max_attempts`). This is what makes crashes safe.
- **Fence.** Every claim mints a fresh `lease_id`. Completion and failure writes
  are guarded by it, so a zombie worker that wakes up late updates 0 rows.
- **Fairness.** A per-claim cap (`SCHEDULER_PER_ORG_LIMIT`) limits how many jobs
  one organization contributes per round, and a per-organization **in-flight
  cap** (`SCHEDULER_PER_ORG_INFLIGHT`) limits how many it may have running at
  once — so a tenant with long jobs cannot occupy the whole pool.

See **[DESIGN.md](DESIGN.md)** for the design rationale (a short doc) and
**[DESIGN_DETAIL.md](DESIGN_DETAIL.md)** for the deep-dive (exact SQL, scaling
ceiling, failure analysis).

---

## 6. See the whole thing run (live demo)

One command runs the entire story — migrate, enqueue a large mixed workload,
start consumers, render a live dashboard, **kill a consumer mid-job to show
crash recovery**, then verify correctness:

```bash
DATABASE_URL="postgres://$(whoami)@localhost:5432/hunar_jobs" \
  python3 scripts/demo.py --jobs 100000
```

```
  SCHEDULER LIVE DEMO  100,064 jobs · 4 consumers · pool 16 · org-limit 5

  Progress  [██████████████████████████████████████░░░░]  87.3%   87,300 / 100,064
  Elapsed   01:12      Rate    1213 jobs/s      ETA   00:10

  pending      12,700     running        64     completed     87,300     failed     0

  Throughput  ▆▇███▇▆▅▅▆▇██▇▆▅▅▆▇█ 1213/s
  Retried        3     Reclaimed (crash recovery)      0     Duplicate effects 0 ✓

  Fairness by organization
    org 1   [███████████████████████████░░░]  87.1%   52,300 / 60,000
    org 2   [██████████████████████████████] 100.0%    9,000 /  9,000  ✓ drained
    ...
  ⚠ killed consumer pid 65255 while it held long jobs — their 5s lease expires, then the reaper reclaims them
```

At the end it prints a PASS/FAIL report covering all five required scenarios:

| Requirement | How the demo shows it |
|---|---|
| Exactly-once | `effects` counts: every `db_effect` job has exactly one committed effect; no duplicates/mismatches |
| Crash safety | a consumer is `SIGKILL`ed mid-job; `reclaimed` and probe recovery show the job reran and completed |
| Fairness | per-org bars: small tenants reach `✓ drained` while the large tenant still has a backlog |
| Scheduling | future-scheduled jobs are asserted *not* to have run early |
| Retries/dead-letter | `fail` jobs are generated and `retried > 0`; no unexpected failures |

Useful flags: `--jobs`, `--consumers`, `--pool`, `--per-org`, `--org-inflight`,
`--lease`, `--kill-after`, `--no-kill` (skip the crash injection), `--db <url>`
(>50 jobs across >3 orgs and 2–3 consumers happens by default).

The same script has two focused subcommands (see §7):

```bash
python3 scripts/demo.py loadtest --jobs 10000 --consumers 4   # throughput
python3 scripts/demo.py crash --lease 2                       # recovery only
```

> The tooling is a single plain-Python script using only the standard library —
> it shells out to `psql` and the `scheduler` binary. Nothing extra to install.

---

## 7. Testing & benchmarks

Unit tests run anywhere. Integration tests need a database; they create and drop
a throwaway database per test, so point `DATABASE_URL` at any database you can
create databases from.

```bash
# Fast, no database
cargo test --workspace --lib

# Everything, including integration (creates throwaway DBs)
DATABASE_URL="postgres://$(whoami)@localhost:5432/hunar_jobs" cargo test --workspace

# Lint + format gates (what CI runs)
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
```

What the integration suite proves:

| Test | Guarantee |
|---|---|
| `exactly_once` | 200 jobs, 4 concurrent workers → each effect applied exactly once |
| `crash_recovery` | an expired lease is reclaimed; a stale worker is fenced out |
| `heartbeat` | a renewed lease is never reaped; an expired one cannot be revived |
| `fairness` | a small tenant is served while a large tenant still has a backlog |
| `scheduling` | future jobs are not claimed early; due jobs run within tolerance |

**Micro-benchmark** (claim latency vs. batch size):

```bash
DATABASE_URL="postgres://$(whoami)@localhost:5432/hunar_jobs" \
  cargo bench -p job-store-postgres --bench claim
```

**Throughput harness** (seed jobs, run N consumers, measure jobs/s):

```bash
DATABASE_URL="postgres://$(whoami)@localhost:5432/hunar_jobs" \
  python3 scripts/demo.py loadtest --jobs 10000 --consumers 4 --pool 4
```

**Crash test** (kill a consumer mid-job, watch it recover):

```bash
DATABASE_URL="postgres://$(whoami)@localhost:5432/hunar_jobs" \
  python3 scripts/demo.py crash --lease 2
```

---

## 8. Project layout

A Cargo workspace split so the compiler enforces the architecture:

| Crate | Responsibility |
|---|---|
| `crates/job-core` | Domain types (`Job`, `NewJob`, `JobState`, `Lease`, …). No I/O. |
| `crates/job-store` | The `Store` trait — the storage abstraction. |
| `crates/job-store-postgres` | `PgStore` — the only crate that speaks SQL. |
| `crates/job-worker` | Worker loop, pool, heartbeat, reaper, `Handler` trait. |
| `crates/job-handlers` | Sample handlers (`sleep`, `fail`, `db_effect`). |
| `crates/scheduler` | The CLI binary; wires everything together. |

Plus:

| Path | Purpose |
|---|---|
| `migrations/` | SQL schema, applied by `scheduler migrate`. |
| `scripts/demo.py` | Live demo, load test and crash test (stdlib-only Python). |
| `DESIGN.md` | Short design rationale (the requested 1–2 pages). |
| `DESIGN_DETAIL.md` | Deep dive: exact SQL, failure analysis, scaling ceiling. |

---

## 9. Configuration

All optional except `DATABASE_URL`:

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — | Postgres connection string (required) |
| `SCHEDULER_QUEUE` | `default` | Queue the `run` command consumes |
| `SCHEDULER_POOL_SIZE` | `4` | Concurrent jobs per process |
| `SCHEDULER_LEASE_SECS` | `30` | Lease length (heartbeat renews at 1/3) |
| `SCHEDULER_PER_ORG_LIMIT` | `2` | Max jobs per org per claim round |
| `SCHEDULER_PER_ORG_INFLIGHT` | `2` | Max jobs an org may have running at once |
| `SCHEDULER_DRAIN_TIMEOUT_SECS` | `10` | Max shutdown wait before abandoning stuck jobs |
| `RUST_LOG` | `info` | Log level |

---

## 10. Stopping PostgreSQL

```bash
brew services stop postgresql@17
```

---

## 11. Troubleshooting

- **`DATABASE_URL must be set`** — export it (see §2). The app never defaults it.
- **`connection refused`** — is Postgres running? `brew services start postgresql@17`.
- **`role "…" does not exist`** — create it: `createuser -s "$(whoami)"`, or use an
  existing role in the URL.
- **Old schema after changing migrations** — the schema is created by
  `scheduler migrate`; drop and recreate the database to start clean.
