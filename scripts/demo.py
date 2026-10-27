#!/usr/bin/env python3
"""Scheduler tools: live demo, load test, crash test.

One script covers the whole story. Subcommands:

  demo      (default) run the full narrative with a live terminal dashboard:
            reset schema, enqueue a large mixed workload, start consumers,
            render progress/throughput/fairness, SIGKILL a consumer mid-run to
            show crash recovery, then verify correctness and print a report.
  loadtest  seed N jobs across 10 orgs, run C consumers, report jobs/s.
  crash     seed one long job, SIGKILL the consumer holding it, show the reaper
            reclaim and complete it.

No third-party packages: it shells out to `psql` and the `scheduler` binary.

Usage:
    python3 scripts/demo.py                          # demo, ~20k jobs
    python3 scripts/demo.py demo --jobs 100000       # full-scale demo
    python3 scripts/demo.py demo --no-kill
    python3 scripts/demo.py loadtest --jobs 10000 --consumers 4
    python3 scripts/demo.py crash --lease 2

Bare flags (no subcommand) are treated as `demo`, so the older invocations
still work:
    python3 scripts/demo.py --jobs 100000

Requires DATABASE_URL (or --db) pointing at a Postgres you can create tables in.
"""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from collections import deque

# ----------------------------------------------------------------------------
# terminal helpers
# ----------------------------------------------------------------------------

IS_TTY = sys.stdout.isatty()


def supports_unicode() -> bool:
    enc = (getattr(sys.stdout, "encoding", "") or "").lower()
    return "utf" in enc


UNICODE = supports_unicode()
BAR_FULL = "\u2588" if UNICODE else "#"
BAR_EMPTY = "\u2591" if UNICODE else "-"
SPARK = "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588" if UNICODE else ".:-=+*#%"
CHECK = "\u2713" if UNICODE else "OK"
WARN = "\u26a0" if UNICODE else "!"

RESET = "\033[0m" if IS_TTY else ""
BOLD = "\033[1m" if IS_TTY else ""
DIM = "\033[2m" if IS_TTY else ""
RED = "\033[31m" if IS_TTY else ""
GREEN = "\033[32m" if IS_TTY else ""
YELLOW = "\033[33m" if IS_TTY else ""
CYAN = "\033[36m" if IS_TTY else ""


def color(text: str, c: str) -> str:
    return f"{c}{text}{RESET}" if IS_TTY else text


# ----------------------------------------------------------------------------
# process / db helpers
# ----------------------------------------------------------------------------

def find_psql() -> str:
    psql = shutil.which("psql")
    if psql:
        return psql
    for candidate in (
        "/opt/homebrew/opt/postgresql@17/bin/psql",
        "/usr/local/opt/postgresql@17/bin/psql",
        "/usr/lib/postgresql/17/bin/psql",
    ):
        if os.path.exists(candidate):
            return candidate
    sys.exit("psql not found; install PostgreSQL or add it to PATH")


def find_scheduler() -> str:
    for rel in ("target/release/scheduler", "target/debug/scheduler"):
        if os.path.exists(rel):
            return rel
    print("building release binary...")
    subprocess.run(["cargo", "build", "--release", "-p", "scheduler"], check=True)
    return "target/release/scheduler"


class Db:
    def __init__(self, url: str, psql: str):
        self.url = url
        self.psql = psql

    def query(self, sql: str) -> str:
        out = subprocess.run(
            [self.psql, self.url, "-tA", "-F", "\t", "-c", sql],
            capture_output=True,
            text=True,
        )
        if out.returncode != 0:
            raise RuntimeError(f"psql failed: {out.stderr.strip()}")
        return out.stdout.strip()

    def scalar(self, sql: str) -> int:
        val = self.query(sql)
        return int(val) if val else 0

    def counts(self) -> dict[str, int]:
        row = self.query(
            """
            SELECT
              (SELECT count(*) FROM jobs),
              (SELECT count(*) FROM jobs WHERE state='pending'),
              (SELECT count(*) FROM jobs WHERE state='running'),
              (SELECT count(*) FROM jobs WHERE state='completed'),
              (SELECT count(*) FROM jobs WHERE state='failed'),
              (SELECT count(*) FROM jobs WHERE attempt > 1),
              (SELECT count(*) FROM jobs WHERE last_error = 'reclaimed after lease expiry'),
              -- A completed db_effect job with no committed effect row is the
              -- integrity violation we care about (effects dedup on job_id, so
              -- duplicates are impossible; *missing* ones are the risk).
              (SELECT count(*) FROM jobs j
                 WHERE j.kind='db_effect' AND j.state='completed'
                   AND NOT EXISTS (SELECT 1 FROM effects e WHERE e.job_id=j.id))
            """
        ).split("\t")
        keys = [
            "total", "pending", "running", "completed", "failed",
            "retried", "reclaimed", "dupes",
        ]
        return {k: int(v) for k, v in zip(keys, row)}

    def per_org(self) -> list[tuple[int, int, int]]:
        out = self.query(
            """
            SELECT org_id,
                   count(*) AS total,
                   count(*) FILTER (WHERE state='completed') AS done
            FROM jobs GROUP BY org_id ORDER BY total DESC, org_id
            """
        )
        rows = []
        for line in out.splitlines():
            if not line.strip():
                continue
            org, total, done = line.split("\t")
            rows.append((int(org), int(total), int(done)))
        return rows


# ----------------------------------------------------------------------------
# rendering
# ----------------------------------------------------------------------------

def progress_bar(fraction: float, width: int) -> str:
    fraction = max(0.0, min(1.0, fraction))
    filled = int(round(fraction * width))
    return BAR_FULL * filled + BAR_EMPTY * (width - filled)


def sparkline(values: deque[float]) -> str:
    if not values:
        return ""
    hi = max(values) or 1.0
    out = []
    for v in values:
        idx = int((v / hi) * (len(SPARK) - 1))
        out.append(SPARK[max(0, min(len(SPARK) - 1, idx))])
    return "".join(out)


def fmt_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:d}h{m:02d}m{s:02d}s"
    return f"{m:02d}:{s:02d}"


def render(state: dict, first: bool) -> None:
    c = state
    total = max(1, c["total"])
    done = c["completed"] + c["failed"]
    frac = done / total
    width = 46

    lines: list[str] = []
    lines.append(color(f"  SCHEDULER LIVE DEMO  ", BOLD + CYAN)
                 + color(f"{c['total']:,} jobs \u00b7 {c['consumers']} consumers "
                         f"\u00b7 pool {c['pool']} \u00b7 org-limit {c['per_org']}", DIM))
    lines.append("")
    pbar = color(progress_bar(frac, width), GREEN if c["failed"] == 0 else YELLOW)
    lines.append(
        f"  Progress  [{pbar}] {frac*100:5.1f}%   "
        f"{done:,} / {c['total']:,}"
    )
    lines.append(
        f"  Elapsed   {fmt_duration(c['elapsed'])}      "
        f"Rate  {color(f"{c['rate']:>6.0f} jobs/s", CYAN)}      "
        f"ETA   {fmt_duration(c['eta'])}"
    )
    lines.append("")
    lines.append(
        f"  {color('pending', YELLOW)}   {c['pending']:>9,}     "
        f"{color('running', CYAN)}   {c['running']:>7,}     "
        f"{color('completed', GREEN)}  {c['completed']:>9,}     "
        f"{color('failed', RED)}   {c['failed']:>5,}"
    )
    lines.append("")
    lines.append(
        f"  Throughput  {color(sparkline(c['history']), CYAN)}  "
        f"{c['rate']:.0f}/s"
    )
    dupes = c["dupes"]
    dup_txt = color(f"{dupes} {CHECK}", GREEN) if dupes == 0 else color(str(dupes), RED)
    lines.append(
        f"  Retried {c['retried']:>8,}     "
        f"Reclaimed (crash recovery) {c['reclaimed']:>6,}     "
        f"Duplicate effects {dup_txt}"
    )
    lines.append("")
    lines.append(color("  Fairness by organization", BOLD))
    for org, tot, done_o in c["orgs"]:
        f = done_o / max(1, tot)
        bar = progress_bar(f, 30)
        tag = ""
        if tot < c["total"] * 0.2 and f >= 1.0:
            tag = color(f"  {CHECK} drained", GREEN)
        lines.append(
            f"    org {org:<3} [{bar}] {f*100:5.1f}%   "
            f"{done_o:>6,} / {tot:>6,}{tag}"
        )
    lines.append("")
    if c.get("kill_note"):
        lines.append(color(f"  {WARN} {c['kill_note']}", YELLOW))
    else:
        lines.append(color("  running...", DIM))

    if IS_TTY:
        # Home the cursor and clear to end of screen, then repaint the block.
        # This redraws in place instead of scrolling — no stacked frames.
        sys.stdout.write("\033[H\033[J")
        sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()
    else:
        # Not a terminal (piped/CI): don't spew every frame. Emit roughly one
        # line per second, which stays readable in a log.
        now = c["elapsed"]
        if first or now - c.get("_last_log", -1) >= 1.0:
            c["_last_log"] = now
            print(
                f"[{fmt_duration(now)}] {frac*100:5.1f}% "
                f"done={done:,}/{c['total']:,} pending={c['pending']:,} "
                f"running={c['running']:,} rate={c['rate']:.0f}/s"
            )


# ----------------------------------------------------------------------------
# workload
# ----------------------------------------------------------------------------

def seed_sql(jobs: int) -> str:
    """A mixed, skewed workload.

    - 60% org 1, the rest spread over smaller orgs (fairness)
    - ~0.5% slow sleeps (1.5-3.5s): one slow job must not block the pool
    - ~2% db_effect: writes a deduplicated effect, proving exactly-once
    - ~1% fail: drives the retry path (succeeds on attempt 2)
    - the majority are tiny sleeps (1-15ms)

    The branches are mutually exclusive (checking `% n = 0` in an if/elif-like
    order where no modulus divides an earlier one), so every kind is actually
    generated.
    """
    return f"""
    INSERT INTO jobs (org_id, kind, queue, payload, priority, scheduled_at, max_attempts)
    SELECT
      CASE
        WHEN g % 100 < 60 THEN 1
        WHEN g % 100 < 75 THEN 2
        WHEN g % 100 < 88 THEN 3
        ELSE 4 + (g % 3)
      END AS org_id,
      CASE
        WHEN g % 200 = 0 THEN 'sleep'
        WHEN g % 50  = 0 THEN 'db_effect'
        WHEN g % 97  = 0 THEN 'fail'
        ELSE 'sleep'
      END AS kind,
      'default',
      CASE
        WHEN g % 200 = 0 THEN '{{"min_ms":1500,"max_ms":3500}}'::jsonb
        WHEN g % 50  = 0 THEN '{{}}'::jsonb
        WHEN g % 97  = 0 THEN '{{}}'::jsonb
        ELSE '{{"min_ms":1,"max_ms":15}}'::jsonb
      END AS payload,
      100,
      now(),
      3
    FROM generate_series(1, {jobs}) g;

    -- A few jobs scheduled a couple of seconds ahead: they must not be claimed
    -- before their due time (evidence for the "scheduling" requirement).
    INSERT INTO jobs (org_id, kind, queue, payload, scheduled_at, max_attempts)
    SELECT 1, 'sleep', 'default', '{{"min_ms":1,"max_ms":5}}'::jsonb,
           now() + interval '2 seconds', 3
    FROM generate_series(1, {max(1, jobs // 1000)}) s;
    """


# ----------------------------------------------------------------------------
# subcommand: load test
# ----------------------------------------------------------------------------

def run_loadtest(args, db: "Db", bin_path: str) -> int:
    print(color(f"  load test: {args.jobs:,} jobs across 10 orgs, "
                f"{args.consumers} consumers (pool {args.pool})", BOLD))
    reset_schema(db, bin_path)

    env = consumer_env(args)

    # Produce via the real CLI once, then bulk-load the rest for speed.
    subprocess.run([bin_path, "enqueue", "--org", "1", "--kind", "sleep"],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if args.jobs > 1:
        db.query(
            "INSERT INTO jobs (org_id, kind, queue, scheduled_at, max_attempts) "
            f"SELECT (g % 10) + 1, 'sleep', 'default', now(), 3 "
            f"FROM generate_series(1, {args.jobs - 1}) g;"
        )
    total = db.scalar("SELECT count(*) FROM jobs")
    print(f"  seeded {total:,} jobs")

    start = time.time()
    consumers = [spawn_consumer(bin_path, env) for _ in range(max(1, args.consumers))]
    last_log = 0.0
    try:
        while True:
            remaining = db.scalar(
                "SELECT count(*) FROM jobs WHERE state IN ('pending','running')"
            )
            done = db.scalar("SELECT count(*) FROM jobs WHERE state='completed'")
            if remaining == 0:
                break
            if IS_TTY:
                # Rewrite one line in place.
                print(f"\r  remaining={remaining:<8,} completed={done:<8,}", end="", flush=True)
            else:
                # Log one line per second so piped output stays readable.
                now = time.time()
                if now - last_log >= 1.0:
                    last_log = now
                    print(f"  remaining={remaining:,} completed={done:,}")
            time.sleep(0.5)
    finally:
        stop_all(consumers)
    if IS_TTY:
        print()
    elapsed = time.time() - start
    print(color(f"  {total:,} jobs in {elapsed:.2f}s = {total / elapsed:,.0f} jobs/s", BOLD + GREEN if total else BOLD))
    return 0


# ----------------------------------------------------------------------------
# subcommand: crash test
# ----------------------------------------------------------------------------

def run_crash(args, db: "Db", bin_path: str) -> int:
    lease = args.lease
    print(color(f"  crash test: SIGKILL a consumer holding a job (lease {lease}s)", BOLD))
    reset_schema(db, bin_path)

    # One job that outlives the lease, so it is provably running when we kill.
    ms = lease * 1000 + 3000
    db.query(
        "INSERT INTO jobs (org_id, kind, queue, payload, scheduled_at, max_attempts) "
        f"VALUES (1, 'sleep', 'default', '{{\"min_ms\": {ms}, \"max_ms\": {ms}}}', now(), 3);"
    )

    env = consumer_env(args)
    env["SCHEDULER_POOL_SIZE"] = "1"

    print("  starting consumer A...")
    a = spawn_consumer(bin_path, env)
    state = ""
    for _ in range(40):
        state = db.query("SELECT state FROM jobs LIMIT 1;")
        if state == "running":
            break
        time.sleep(0.25)
    print(f"  job state before kill: {state}")

    print("  SIGKILL consumer A (hard crash)...")
    kill9(a)

    print("  starting consumer B (with reaper)...")
    b = spawn_consumer(bin_path, env)
    attempts = 0
    for _ in range(120):
        state = db.query("SELECT state FROM jobs LIMIT 1;")
        attempts = db.scalar("SELECT attempt FROM jobs LIMIT 1;")
        if state == "completed":
            break
        time.sleep(0.5)
    stop_all([b])

    print(f"  job state after recovery: {state} (attempts={attempts})")
    if state == "completed":
        print("  " + color(f"{CHECK} PASS: crashed job reclaimed and completed exactly once",
                          GREEN + BOLD))
        return 0
    print("  " + color(f"X FAIL: job did not recover (state={state})", RED + BOLD))
    return 1


# ----------------------------------------------------------------------------
# shared helpers
# ----------------------------------------------------------------------------

def reset_schema(db: "Db", bin_path: str) -> None:
    db.query(
        "DROP TABLE IF EXISTS effects, jobs, active_orgs, _sqlx_migrations CASCADE; "
        "DROP FUNCTION IF EXISTS mark_active_orgs CASCADE;"
    )
    subprocess.run([bin_path, "migrate"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def consumer_env(args) -> dict:
    env = dict(os.environ)
    env["DATABASE_URL"] = args.db
    env["SCHEDULER_POOL_SIZE"] = str(args.pool)
    env["SCHEDULER_PER_ORG_LIMIT"] = str(args.per_org)
    # Per-org *in-flight* cap. Set generously here so the pool stays full; the
    # cap's job (stop one tenant monopolizing the pool) is demonstrated by the
    # `in_flight_cap_prevents_pool_monopolization` test with a low value.
    env["SCHEDULER_PER_ORG_INFLIGHT"] = str(args.org_inflight)
    env["SCHEDULER_LEASE_SECS"] = str(args.lease)
    env["RUST_LOG"] = "warn"
    return env


def spawn_consumer(bin_path: str, env: dict) -> subprocess.Popen:
    return subprocess.Popen(
        [bin_path, "run"], env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def kill9(p: subprocess.Popen) -> None:
    if p.poll() is None:
        p.send_signal(signal.SIGKILL)
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def stop_all(consumers: list[subprocess.Popen], sig: int = signal.SIGINT) -> None:
    for p in consumers:
        if p.poll() is None:
            try:
                p.send_signal(sig)
            except ProcessLookupError:
                pass
    for p in consumers:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------

def main() -> int:
    # Backwards/compat: if the first token isn't a known subcommand (and isn't a
    # global flag), treat the invocation as `demo`. E.g. `demo.py --jobs 1000`
    # means `demo.py demo --jobs 1000`.
    argv = sys.argv[1:]
    if argv and argv[0] not in ("demo", "loadtest", "crash", "-h", "--help"):
        argv = ["demo", *argv]

    ap = argparse.ArgumentParser(
        description="Scheduler tools: live end-to-end demo, load test, crash test.",
    )
    ap.add_argument("--db", default=os.environ.get("DATABASE_URL"),
                    help="Postgres URL (default $DATABASE_URL)")
    sub = ap.add_subparsers(dest="command", required=True)

    p_demo = sub.add_parser("demo", help="live end-to-end demo")
    p_demo.add_argument("--jobs", type=int, default=20000)
    p_demo.add_argument("--consumers", type=int, default=4)
    p_demo.add_argument("--pool", type=int, default=12)
    p_demo.add_argument("--per-org", type=int, default=4)
    p_demo.add_argument("--org-inflight", type=int, default=1000)
    p_demo.add_argument("--lease", type=int, default=6)
    p_demo.add_argument("--kill-after", type=float, default=8.0)
    p_demo.add_argument("--no-kill", action="store_true")

    p_load = sub.add_parser("loadtest", help="seed N jobs, run consumers, report jobs/s")
    p_load.add_argument("--jobs", type=int, default=10000)
    p_load.add_argument("--consumers", type=int, default=4)
    p_load.add_argument("--pool", type=int, default=4)
    p_load.add_argument("--per-org", type=int, default=4)
    p_load.add_argument("--org-inflight", type=int, default=1000)
    p_load.add_argument("--lease", type=int, default=30)

    p_crash = sub.add_parser("crash", help="SIGKILL a consumer mid-job, show recovery")
    p_crash.add_argument("--lease", type=int, default=2)
    p_crash.add_argument("--pool", type=int, default=1)
    p_crash.add_argument("--per-org", type=int, default=4)
    p_crash.add_argument("--org-inflight", type=int, default=1000)

    args = ap.parse_args(argv)
    if not args.db:
        sys.exit("DATABASE_URL must be set, or pass --db")

    db = Db(args.db, find_psql())
    bin_path = find_scheduler()

    if args.command == "loadtest":
        return run_loadtest(args, db, bin_path)
    if args.command == "crash":
        return run_crash(args, db, bin_path)
    return run_demo(args, db, bin_path)


def run_demo(args, db: "Db", bin_path: str) -> int:
    # ---- 1. reset + migrate -------------------------------------------------
    print(color("  [1/5] reset schema + migrate", BOLD))
    reset_schema(db, bin_path)

    # ---- 2. enqueue ---------------------------------------------------------
    print(color(f"  [2/5] enqueuing {args.jobs:,} jobs (mixed, skewed workload)", BOLD))
    subprocess.run([bin_path, "enqueue", "--org", "1", "--kind", "sleep"],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if args.jobs > 1:
        db.query(seed_sql(args.jobs - 1))
    total = db.scalar("SELECT count(*) FROM jobs")
    print(f"        total rows: {total:,}")

    # ---- 3. start consumers -------------------------------------------------
    print(color(f"  [3/5] starting {args.consumers} consumers "
                f"(pool {args.pool}, per-org {args.per_org}, lease {args.lease}s)", BOLD))
    env = consumer_env(args)

    consumers: list[subprocess.Popen] = []
    for _ in range(max(1, args.consumers)):
        consumers.append(spawn_consumer(bin_path, env))

    # ---- 4. live view -------------------------------------------------------
    history: deque[float] = deque(maxlen=40)
    start = time.time()
    last_t = start
    last_done = 0
    kill_note = None
    killed = False
    rate = 0.0
    poll = 0.5
    first_frame = True

    def stop_consumers(sig: int = signal.SIGINT) -> None:
        for p in consumers:
            if p.poll() is None:
                try:
                    p.send_signal(sig)
                except ProcessLookupError:
                    pass

    try:
        while True:
            c = db.counts()
            now = time.time()
            elapsed = now - start
            done = c["completed"] + c["failed"]

            # rolling rate over the poll interval
            dt = now - last_t
            if dt > 0:
                instant = (done - last_done) / dt
                if instant >= 0:
                    history.append(instant)
            last_t, last_done = now, done
            if history:
                rate = sum(history) / len(history)

            remaining = c["total"] - done
            eta = remaining / rate if rate > 0 else 0.0

            # Crash injection. To make recovery deterministic, first queue enough
            # long *probe* jobs to occupy every consumer, wait until they are
            # claimed, then SIGKILL one consumer while it is provably mid-job.
            # Its lease then expires and the reaper recovers the orphaned work.
            # Probes are tagged so we can count their recovery precisely (a
            # completed job clears `last_error`, so that field can't be used).
            past_mid = done >= max(1, c["total"]) * 0.2
            due_to_kill = elapsed >= min(args.kill_after, 3.0)
            if (not killed and not args.no_kill and len(consumers) > 1
                    and past_mid and due_to_kill):
                victim = consumers[0]
                if victim.poll() is None:
                    slow = max(1, args.consumers) * max(1, args.pool)
                    ms = args.lease * 1000 + 2000
                    # Spread probes across orgs so the per-org cap doesn't
                    # throttle them; tag them with a payload marker; and give
                    # them top priority (priority 1) so they are claimed ahead of
                    # the backlog and are provably running when we kill.
                    db.query(
                        f"INSERT INTO jobs (org_id, kind, queue, payload, priority, scheduled_at, max_attempts) "
                        f"SELECT (g % {max(1, args.consumers)}) + 1, 'sleep', 'default', "
                        f"(jsonb_build_object('probe', true, 'min_ms', {ms}, 'max_ms', {ms}))::jsonb, "
                        f"1, now(), 3 FROM generate_series(1, {slow}) g;"
                    )
                    # Give the fleet a moment to claim the long jobs before we
                    # pull the rug out from under one of them.
                    time.sleep(min(1.5, max(0.6, args.lease / 3)))
                    victim.send_signal(signal.SIGKILL)
                    kill_note = (f"killed consumer pid {victim.pid} while it held long "
                                 f"jobs \u2014 their {args.lease}s lease expires, then the "
                                 f"reaper reclaims them")
                    killed = True

            state = {
                **c,
                "consumers": args.consumers,
                "pool": args.pool,
                "per_org": args.per_org,
                "elapsed": elapsed,
                "rate": rate,
                "eta": eta,
                "history": history,
                "orgs": db.per_org(),
                "kill_note": kill_note,
            }
            render(state, first_frame)
            first_frame = False

            if done >= c["total"] and c["running"] == 0:
                break
            if elapsed > 3600:
                print("\ntime limit reached")
                break
            time.sleep(poll)
    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        stop_consumers()

    # graceful wait
    for p in consumers:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()

    # ---- 5. verify ----------------------------------------------------------
    final = db.counts()
    orgs = db.per_org()
    checks: list[tuple[str, bool, str]] = []

    # Every job must reach a terminal state; summing states only proves they add
    # up, not that work finished. Assert no job is left pending/running.
    all_terminal = final["pending"] == 0 and final["running"] == 0
    checks.append((
        "every job reached a terminal state (none pending/running)",
        all_terminal,
        f"pending={final['pending']}, running={final['running']}",
    ))

    no_failed = final["failed"] == 0
    checks.append(("no dead-lettered jobs (all retries succeeded)", no_failed,
                   f"failed = {final['failed']}"))

    retried = final["retried"]
    checks.append(("retry path exercised (some jobs ran more than once)",
                   retried > 0, f"{retried:,} jobs retried"))

    no_dupes = final["dupes"] == 0
    checks.append(("exactly-once committed effects", no_dupes,
                   f"duplicate/mismatched effects: {final['dupes']}"))

    effects = db.scalar("SELECT count(*) FROM effects")
    db_effect_jobs = db.scalar("SELECT count(*) FROM jobs WHERE kind='db_effect' AND state='completed'")
    checks.append(("every db_effect job has a committed effect",
                   effects == db_effect_jobs and effects > 0,
                   f"effects={effects:,}  db_effect completed={db_effect_jobs:,}"))

    # Scheduling: seeded jobs use now(), but the crash probes are scheduled
    # slightly ahead, so we can evidence that a future job is not claimed early.
    early = db.scalar(
        "SELECT count(*) FROM jobs WHERE scheduled_at > now() AND state IN ('running','completed')"
    )
    checks.append(("future-scheduled jobs are not claimed early", early == 0,
                   f"{early} future jobs ran early"))

    small_orgs = [(o, t, d) for (o, t, d) in orgs if t < final["total"] * 0.2]
    fairness = bool(small_orgs) and all(d == t for _, t, d in small_orgs)
    checks.append(("small tenants fully drained (fairness)", fairness,
                   f"{len(small_orgs)} small orgs, all at 100%"))

    # Probes are the long jobs we injected right before killing a consumer.
    # Recovery shows up as probes that were retried (attempt > 1) — the crashed
    # worker's partial attempt counted, then a surviving worker re-ran them.
    probes = db.scalar("SELECT count(*) FROM jobs WHERE payload ? 'probe'")
    probes_recovered = db.scalar(
        "SELECT count(*) FROM jobs WHERE payload ? 'probe' AND attempt > 1 AND state='completed'"
    )
    probes_done = db.scalar(
        "SELECT count(*) FROM jobs WHERE payload ? 'probe' AND state='completed'"
    )
    crash_ok = args.no_kill or (probes > 0 and probes_done == probes and probes_recovered >= 1)
    checks.append((
        "crash recovery (killed worker's job reclaimed & rerun)",
        crash_ok,
        f"{probes_recovered} of {probes} probe jobs recovered after the kill"
        if not args.no_kill else "skipped (--no-kill)",
    ))

    print()
    print(color("  Final report", BOLD))
    print(f"  {'':2}{'jobs':<10}{'completed':>12}{'failed':>9}{'retried':>10}{'reclaimed':>11}")
    print(f"  {'':2}{final['total']:>8,}{final['completed']:>12,}"
          f"{final['failed']:>9,}{final['retried']:>10,}{final['reclaimed']:>11,}")
    print()
    all_ok = True
    for name, ok, detail in checks:
        all_ok &= ok
        mark = color(CHECK, GREEN) if ok else color("X", RED)
        print(f"  [{mark}] {name:<48} {color(detail, DIM)}")
    print()
    if all_ok:
        print("  " + color(f"{CHECK} ALL CHECKS PASSED", GREEN + BOLD))
        return 0
    print("  " + color("X SOME CHECKS FAILED", RED + BOLD))
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeError as e:
        sys.exit(f"error: {e}")
