# Design

A database-backed job scheduler and executor. Jobs live in **PostgreSQL**, which
does the coordination — no external broker. This is the short version; the
[full detail is in DESIGN_DETAIL.md](DESIGN_DETAIL.md).

---

## 1. What it does

Producers enqueue jobs (`org_id`, `kind`, opaque `payload`, `scheduled_at`).
Multiple consumer processes pull due jobs, run the matching handler, and record
the result. It must run each job **at most once** per claim, **never lose** one
on a crash, stay **fair** across tenants, and respect the schedule.

## 2. Choices (and why)

| Choice | Why |
|---|---|
| **PostgreSQL** | `FOR UPDATE SKIP LOCKED` gives race-free concurrent dequeue; the DB is the coordination layer, so no broker is needed. |
| **Rust (edition 2024)** | Concurrency-heavy correctness; a single static binary that is trivial to `kill -9` in a crash test. |
| **`sqlx`** | Async Postgres with a migration runner. Runtime-checked queries (not the `query!` macros) so builds don't need a live DB. |
| **Tokio** | One process runs a pool of concurrent jobs; the worker threads are the CPU threads plus scheduled tasks. |

Rejected: SQLite (no `SKIP LOCKED`, single-writer — can't scale across nodes);
Redis/Kafka/etc. (excluded by the brief; the DB must back everything).

## 3. Schema

One `jobs` table holds state and ownership: `state`
(`pending`/`running`/`completed`/`failed`), `scheduled_at` (immutable intent) and
`next_retry_at` (effective due = `COALESCE(next_retry_at, scheduled_at)`),
`attempt`/`max_attempts`, and the ownership triple `lease_id`/`locked_by`/
`lease_expires_at`. Partial indexes on `state='pending'` keep the hot path small
as completed rows accumulate (the classic Postgres-queue MVCC pitfall). A small
`active_orgs` table (maintained by a trigger) lists tenants with pending work so
claims stay O(active tenants) instead of O(backlog). `effects` is a dedup table
keyed by `job_id`.

## 4. Concurrency: the claim

One statement (`UPDATE … FROM (… FOR UPDATE SKIP LOCKED) RETURNING …`) atomically
marks due jobs running for this worker. `SKIP LOCKED` guarantees two workers get
disjoint sets. The `LIMIT` is a small batch (default = pool size). The handler
then runs **outside** the claim transaction.

## 5. Failure handling

A `running` job holds a **lease** the worker keeps renewing (heartbeat). A
crashed worker stops renewing; the lease lapses and a **reaper** returns the job
to `pending` (or dead-letters it past `max_attempts`). Every claim mints a fresh
`lease_id`, and all terminal writes are fenced by it, so a revived "zombie"
worker updates 0 rows and cannot overwrite its replacement. `attempt` is bumped
at claim, so a job that reliably kills its worker still reaches `max_attempts`
and is dead-lettered. Handler panics are caught and turned into a failed attempt;
the heartbeat is tied to the execution's lifetime, so it can never outlive it.

**Honest guarantee.** Literal exactly-once *delivery* is impossible across an
arbitrary external side effect. What this system provides is: **at-least-once
attempts** + **at-most-one owner** (SKIP LOCKED) + **fenced completion** +
**exactly-once committed database effects** (a dedup table keyed on `job_id`).
Duplicate attempts are harmless; duplicate committed effects are impossible.

## 6. Fairness

Two mechanisms: a **per-claim cap** (how many jobs one tenant contributes per
round) and a **per-tenant in-flight cap** (how many it may have running at once,
across workers) — the latter is what stops a tenant with long jobs from occupying
the whole pool before others submit work. Queues give job-type isolation.

## 7. Scaling and the ceiling

- **Vertical:** more concurrent jobs per process; ceiling = the single primary's
  write/WAL throughput.
- **Horizontal (processes/nodes):** scales with `SKIP LOCKED` until the primary
  saturates.
- **The real wall:** MVCC dead-tuple bloat on the hot table under high churn.
  Defended by partial indexes and (planned) aggressive retention. Beyond that:
  PgBouncer, table partitioning, or sharding by `org_id`.

## 8. What I'd change with more time

- **Retention/archival job** for finished rows (bounds the table and autovacuum).
- **`LISTEN/NOTIFY`** wakeups to cut poll latency (currently bounded by a small
  poll interval, so drift is sub-second but not zero).
- **Metrics/observability** (per-queue depth, latency, reaper activity).
- **Compile-time-checked SQL** via the `sqlx` macros + a committed `SQLX_OFFLINE`
  cache.
- **Leader election** so exactly one node runs maintenance (today the reaper is
  idempotent and simply redundant across nodes).
- **External-effect idempotency helpers** (outbox pattern) for non-DB side
  effects.

## 9. Known limitations

- Poll-based scheduling, not push; worst-case drift is the poll interval.
- Fairness is per-round + in-flight caps, not weighted/hierarchical quotas.
- The `active_orgs` denormalization needs the advisory-lock dance (documented in
  the detail doc) to stay consistent under concurrent enqueue/prune.
