# Design

Why the system is built the way it is: the choices, the correctness argument,
the tradeoffs, and where it hits its ceiling.

---

## 1. Problem

Execute jobs on behalf of multiple tenants. Jobs are scheduled ahead of time and
must run when due. Multiple consumer processes run concurrently and must never
double-execute a job; a crashed worker must not lose one. A single tenant or job
type must not monopolize capacity. **No external broker is allowed** — the
database is both the task store and the coordination layer.

---

## 2. Technology choices

### PostgreSQL

The whole problem turns on one primitive: handing a set of due rows to one
worker while guaranteeing no other worker gets the same rows. Postgres provides
exactly that with **`SELECT … FOR UPDATE SKIP LOCKED`**, plus the transactional
integrity to make claim + state-change atomic. It is the coordination layer, so
we never need a broker.

- **vs. SQLite** — SQLite has no row-level locking and no `SKIP LOCKED`; writers
  serialize on a single database lock, so claims can't proceed in parallel and
  the system can't scale across nodes. A `SqliteStore` is possible behind our
  `Store` trait, but the concurrency story is fundamentally weaker.
- **vs. MySQL** — MySQL 8 has `SKIP LOCKED` too; Postgres wins on stronger
  transactional semantics, richer indexing, partial indexes, and `LISTEN/NOTIFY`
  for future low-latency wakeups.

### Rust (edition 2024)

The system is concurrency-heavy: many workers racing on shared rows, with subtle
invariants (fencing, lease ownership). Rust's type system turns several of those
invariants into compile-time guarantees (see §7), and it produces a single static
binary that is trivial to `kill -9` in a crash test and cheap to run many of.

### `sqlx`

Async Postgres driver. We use the **runtime-checked** query API
(`sqlx::query`/`query_as` with `&'static str` SQL), deliberately **not** the
`query!` macros: the macros validate SQL against a live database at *build*
time, which would make the project un-buildable without a running Postgres and
add a `.sqlx` cache to maintain. The tradeoff is that SQL is checked on first
use rather than at compile time; the integration tests hit every query, so
schema drift surfaces in CI regardless. `sqlx` also ships a first-class migration
runner, so no external `sqlx-cli` is required.

(If compile-time checking were wanted, the honest way is the macros plus a
committed `SQLX_OFFLINE` cache — noted here as a possible upgrade, not a claim
about the current code.)

### Async (Tokio) + a local task pool

One process runs many jobs concurrently via `tokio::spawn`. I/O-bound jobs (the
common case) overlap cleanly; the "worker threads" of the requirement are the
box's CPU threads plus Tokio's task scheduling.

---

## 3. Architecture

```
                 ┌───────────────────── PostgreSQL ─────────────────────┐
 producer (CLI)  │  jobs(state, org_id, kind, payload, scheduled_at, …)  │
   enqueue ─────▶│  effects(job_id PK)   migration bookkeeping           │
                 └───────────────┬───────────────────────┬───────────────┘
                                 │ claim (SKIP LOCKED)   │ reap (expired leases)
        ┌────────────────────────┴──────────┐   ┌────────┴─────────────────┐
        │ Worker process A                  │   │ Worker process B   … N   │
        │  claim loop → local pool          │   │  same loop               │
        │  per-job heartbeat                │   │                          │
        │  reaper task                      │   │                          │
        └───────────────────────────────────┘   └──────────────────────────┘
```

Any process can be a producer or a consumer; there is **no coordinator**. Workers
pull; Postgres arbitrates.

---

## 4. Data model

`jobs` is the heart. Key columns:

- `org_id` — tenant; drives fairness.
- `kind` — selects the handler; `payload` is opaque to the scheduler.
- `scheduled_at` — tenant intent, **never mutated**.
- `next_retry_at` — set after a retryable failure or a crash; the *effective due
  time* is `COALESCE(next_retry_at, scheduled_at)`, so retries don't overwrite
  the original schedule.
- `state ∈ {pending, running, completed, failed}`.
- `attempt` / `max_attempts` — retry budget; `attempt` is incremented **at claim**.
- `lease_id`, `locked_by`, `lease_expires_at` — ownership triple, set and cleared
  together.

Indexes:

```sql
-- Hot path: only pending rows, ordered by effective due time.
CREATE INDEX jobs_due_idx ON jobs (priority, COALESCE(next_retry_at, scheduled_at), id)
    WHERE state = 'pending';
-- Fairness: one due job per org, fast.
CREATE INDEX jobs_org_due_idx ON jobs (org_id, priority, COALESCE(next_retry_at, scheduled_at), id)
    WHERE state = 'pending';
-- Dedup key for at most one live job per key.
CREATE UNIQUE INDEX jobs_unique_key_idx ON jobs (unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('pending', 'running');
```

**Why partial indexes.** They cover only the live set, so the index stays small
even as completed rows accumulate. This is the single most important defense
against the classic Postgres-queue failure mode (§9).

---

## 5. The claim

The claim iterates **active tenants** rather than ranking the whole pending
backlog. `active_orgs` (maintained by a trigger on `jobs`) holds the set of
orgs with pending work per queue, so the claim is O(active tenants) instead of
O(pending jobs):

```sql
UPDATE jobs j
SET state = 'running', attempt = j.attempt + 1,
    locked_by = $4, lease_id = gen_random_uuid(), lease_expires_at = now() + make_interval(secs => $5)
FROM (
    SELECT id FROM (
        SELECT c.id, row_number() OVER (PARTITION BY a.org_id ORDER BY c.id) AS pos
        FROM active_orgs a
        CROSS JOIN LATERAL (
            SELECT j2.id
            FROM jobs j2
            WHERE j2.org_id = a.org_id AND j2.queue = a.queue AND j2.state = 'pending'
              AND COALESCE(j2.next_retry_at, j2.scheduled_at) <= now()
            ORDER BY j2.priority, COALESCE(j2.next_retry_at, j2.scheduled_at), j2.id
            LIMIT COALESCE($3, 4)          -- per-org cap
            FOR UPDATE SKIP LOCKED
        ) c
        WHERE a.queue = $1
    ) ranked
    ORDER BY ranked.pos, random()          -- interleave tenants; random breaks ties
    LIMIT $2
) picked
WHERE j.id = picked.id
RETURNING …;
```

Design notes, each earned the hard way:

1. **`FOR UPDATE SKIP LOCKED` sits on the base `jobs` scan (`j2`).** Locks must
   be taken on the rows being selected. Putting `SKIP LOCKED` on a materialized
   CTE or a window-function layer does *not* lock the base rows and silently
   reintroduces double-claims (caught by the concurrency test).
2. **Per-tenant via `LATERAL`, not a global window.** Ranking the entire pending
   set with `ROW_NUMBER() OVER (PARTITION BY org_id)` scans every pending row on
   every claim — at 100k pending that was ~34 ms per claim, capping throughput
   near 80 jobs/s. Pulling each tenant's jobs directly through the per-org index
   drops that to ~0.13 ms (**~270× faster**) and stays flat as the backlog grows.
3. **`ORDER BY pos, random()`, not `ORDER BY pos, id`.** When the batch is
   smaller than the number of active tenants (e.g. the worker pool is full, so
   `batch = 1`), a deterministic tiebreak always hands the slot to the lowest
   org id and starves everyone else. Randomising the tie rotates fairly across
   tenants. (Caught by observing a small tenant stall behind a large one.)
4. **Ordering within a tenant is `priority, effective_due, id`** — priority
   first, then first-come-first-served, stable.

The `active_orgs` set is maintained by an `AFTER INSERT OR UPDATE` trigger on
`jobs` (so any write path — CLI, bulk load, requeue — is covered) and pruned by
the reaper so it only ever lists tenants with live work.

**A subtle race, and its fix.** The trigger's upsert (`ON CONFLICT DO NOTHING`)
and the prune's `NOT EXISTS` test are both snapshot-based, so a pruner can delete
a membership it cannot yet see is live: it misses a *concurrent, uncommitted*
pending job whose trigger found the row already present and did nothing. The
result is a pending job with no `active_orgs` row — permanently unclaimable
until some later write happens to restore membership. The fix is a pair of
transactional advisory locks on a single key: every job write (the trigger)
takes it in **shared** mode and the prune takes it in **exclusive** mode. Shared
locks don't block each other, so claims/enqueues stay parallel; the prune simply
waits for in-flight producers and blocks new ones while it runs, making its
snapshot race-free. `tests/active_orgs.rs` reproduces the interleaving and
asserts the job stays claimable (the test fails without the locks).

Batch size (`LIMIT`) defaults to `pool_size`: one round trip fills the pool
without reserving a large batch a crash would strand. Claiming is one statement;
the handler then runs **outside** it.

---

## 6. Correctness: exactly-once *pickup*

Literal exactly-once *delivery* is impossible across an arbitrary side effect —
the honest engineering position, stated by every mature queue. We provide the
strongest composable guarantees:

| Layer | Guarantee | Mechanism |
|---|---|---|
| Claim | **at most one** worker owns a job | `FOR UPDATE SKIP LOCKED` + atomic `UPDATE` |
| Recovery | **no job lost** | lease expiry + reaper requeue |
| Completion | **no zombie overwrite** | terminal writes fenced by `lease_id` |
| Effect | **exactly-once committed DB effect** | dedup table keyed on `job_id` |
| Panic | **attempt is finalized** | handler panic caught; heartbeat tied to execution lifetime |

"Exactly-once committed effects" means: for a database-local effect, the handler
records it in a `job_id`-keyed dedup table (`ON CONFLICT DO NOTHING`), so a
re-run after a crash records nothing new. Attempts remain at-least-once, which is
the honest, defensible contract for arbitrary external side effects.

**Crash anywhere** is covered:

| When the worker dies | What happens |
|---|---|
| Before claiming | nothing claimed; others continue |
| After claim, before run | lease lapses → reaper requeues |
| Mid-run | same; retried after backoff |
| Just before marking done | new claim has a new `lease_id`; the zombie's write hits 0 rows |

The `attempt` counter is bumped **at claim**, so a job that reliably crashes its
worker still reaches `max_attempts` and is dead-lettered — it cannot loop forever.

---

## 7. SOLID, clean, KISS

- **Single responsibility** — one module/crate, one reason to change.
- **Open/closed** — new job types are *registered handlers*; the engine never
  changes.
- **Liskov / interface segregation** — small focused traits (`Store`, `Handler`),
  never one god-trait.
- **Dependency inversion** — the worker depends on `job-store` (the abstraction),
  never on `job-store-postgres`. The concrete is selected only in `scheduler`'s
  composition root.
- **Compiler-enforced boundaries** — the workspace layout makes an illegal
  dependency (e.g. the domain reaching for `sqlx`) a build error, not a code
  review comment.
- **Make ownership a type** — `ClaimedJob { job, lease }` can only be produced by
  claiming, so "complete a job you no longer own" is fenced by construction.
- **KISS** — no speculative abstraction. The `Store` trait earns its place
  because a second backend (SQLite) is plausible; nothing else is abstracted
  "just in case."

Comments explain **why**, not what, and only where the code is non-obvious (why
`SKIP LOCKED` is on the base scan, why `attempt` moves at claim, why the reap job
is unique).

---

## 8. Fairness

Two mechanisms:

1. **Per-claim per-org cap** (`SCHEDULER_PER_ORG_LIMIT`, default 2). The window
   function ranks a tenant's due jobs and keeps only the top `n`, so a tenant
   with 10,000 jobs contributes at most `n` to any single claim round. A tenant
   with 5 jobs is therefore served while the large tenant is still backlogged.
2. **Weighted/priority tiers** — `priority` orders across tenants when desired.

Measured by the `fairness` integration test: with the cap, a round draws from
both organizations, and the small tenant finishes long before the large one
drains.

Not claimed: strict per-tenant rate limits or quotas. Those are the natural next
step (a per-org in-flight counter), noted in §10.

---

## 9. Scheduling and drift

- Workers claim only `COALESCE(next_retry_at, scheduled_at) <= now()`.
- When idle, the worker sleeps until `MIN(effective_due)` (clamped), instead of
  busy-polling, so wakeups are timely and idle load is near zero.
- Drift is bounded by the poll/wakeup interval + claim latency (sub-second in
  the `scheduling` test).

The scalable latency improvement is `LISTEN/NOTIFY`: workers block on a Postgres
notification and wake instantly on insert. Deferred (§10) because it adds a
long-lived connection per worker for a latency win that polling already covers at
this scale.

---

## 10. Scaling and where it breaks

| Strategy | How | Ceiling |
|---|---|---|
| **Bigger machine** | raise `SCHEDULER_POOL_SIZE` | CPU; then the single Postgres primary's write/WAL throughput |
| **More processes / nodes** | run more `scheduler run` | scales near-linearly via `SKIP LOCKED` *until* the primary saturates on claim/reap/heartbeat writes |
| **The real wall** | high churn on `jobs` | MVCC dead-tuple bloat on the hot index (see below) |

**The classic failure (brandur, "Postgres Job Queues & Failure By MVCC").** High
job churn plus *any* long-running transaction causes the queue's index to fill
with dead tuples that VACUUM cannot remove while the snapshot lives; workers then
scan past thousands of dead entries per claim and the queue spirals. Our
defenses, in order of importance:

1. **Partial index on `state = 'pending'`** — the live set stays tiny; dead
   completed rows are not in the index at all.
2. **Aggressive retention** — completed rows should be archived/deleted on a
   schedule (a `retention` maintenance job; not in v1).
3. **Short transactions** — the handler runs outside the claim transaction;
   heartbeat and completion are single statements.

**When the primary is the bottleneck:**

- **PgBouncer** to multiplex connections.
- **Table partitioning** of `jobs` by time or `org_id` to keep indexes small and
  enable cheap archival.
- **Shard by `org_id`** across databases — the natural ceiling-breaker, since our
  `org_id` is already the fairness axis.

**Not built (and why):**

- **Leader election** — unnecessary here. The reaper's `UPDATE … WHERE
  lease_expires_at < now()` is idempotent, so concurrent reapers are correct, just
  slightly redundant. At scale, one could run the reaper as a **unique,
  self-rescheduling job** (`kind = 'reap'`, protected by the `unique_key` index)
  to guarantee a single maintenance owner without a leader-election table.
- **Checkpointing for multi-minute jobs** — out of scope; the lease already
  protects crash recovery, and idempotent handlers cover the rest.
- **Metrics/observability** — the `status` command covers the demo need.

---

## 11. Risks

| Risk | Mitigation |
|---|---|
| Lease expires under a genuinely long job (GC pause, DB blip) | heartbeat « lease (1/3); guarded renewal; abort handler on lost lease; fenced writes; idempotent handlers |
| Heartbeat failure storm hammering the DB | heartbeat is one indexed single-row `UPDATE`; `lease/3` interval |
| Poison job | `attempt` cap → dead-letter |
| Unknown `kind` | treated as `Fatal` (misconfig, not retryable) |
| Unbounded `jobs` table growth | retention job (next step); partial indexes bound the hot path meanwhile |
| Connection exhaustion under many workers | size the pool; PgBouncer at scale |
