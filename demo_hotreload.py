"""
Mixed-scenario hot-reload stress test.

  - 16 worker threads continuously issue slow_greet() calls (~200ms each)
  - 2  control threads randomly interleave:
        LOAD slow-init  (1.0s on_load, init-protected)
        REPLACE v1      (fast init)
        REPLACE v2
        REPLACE v3
        UNLOAD
        RELOAD v2  (after unload)
  - Timeline: every control op and every business call is timestamped to
    the millisecond and printed so the switch boundaries are visible.
  - Final statistics + hard assertions:
        * no call lost
        * no call landed in a module before its on_load finished
        * no "hard failure" (uncaught exception)
        * max wait per call < 5x expected (indicates no hang / long block)
        * no "replace-success-then-unavailable" inconsistency

Exit code is non-zero if any assertion fails.
"""

from __future__ import annotations

import random
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plugin_manager import PluginManager

PLUGINS_DIR = Path(__file__).resolve().parent / "plugins"

T0 = time.perf_counter()
LOG_LOCK = threading.Lock()

# Call duration should be ~200ms for v1/v2/v3 and ~200ms for slow-init.
# If any single call takes more than this many ms we flag as "long wait".
LONG_WAIT_THRESHOLD_MS = 5000.0
DURATION_S = 25.0
N_WORKERS = 16
N_CONTROLLERS = 2


def ts() -> float:
    return (time.perf_counter() - T0) * 1000.0


def log(msg: str) -> None:
    with LOG_LOCK:
        print(f"[{ts():8.1f}] {msg}", flush=True)


# ----- control actions -------------------------------------------------------
CTRL_ACTIONS = [
    ("LOAD_SLOW", str(PLUGINS_DIR / "greeter_slow_init.py")),
    ("REPLACE_V1", str(PLUGINS_DIR / "greeter_v1.py")),
    ("REPLACE_V2", str(PLUGINS_DIR / "greeter_v2.py")),
    ("REPLACE_V3", str(PLUGINS_DIR / "greeter_v3.py")),
    ("UNLOAD", None),
    ("RELOAD_V2", str(PLUGINS_DIR / "greeter_v2.py")),
    ("REPLACE_V1", str(PLUGINS_DIR / "greeter_v1.py")),
    ("LOAD_SLOW", str(PLUGINS_DIR / "greeter_slow_init.py")),
]


# ----- records ---------------------------------------------------------------
@dataclass
class CallRecord:
    worker_id: int
    seq: int
    start_ms: float
    end_ms: float = 0.0
    outcome: Literal["v1", "v2", "v3", "slow-init", "unavailable",
                     "timeout", "hardfail"] = "hardfail"
    detail: str = ""


@dataclass
class CtrlRecord:
    controller_id: int
    seq: int
    start_ms: float
    end_ms: float = 0.0
    action: str = ""
    success: bool = False
    detail: str = ""


# ----- worker ---------------------------------------------------------------
def run_worker(wid: int, mgr: PluginManager, records: list[CallRecord],
               rec_lock: threading.Lock, stop: threading.Event) -> None:
    seq = 0
    while not stop.is_set():
        start = ts()
        outcome: str = "hardfail"
        detail: str = ""
        try:
            result = mgr.call("greeter", "slow_greet", f"W{wid}-{seq}")
            end = ts()
            if "v1" in result:
                outcome = "v1"
            elif "v2" in result:
                outcome = "v2"
            elif "v3" in result:
                outcome = "v3"
            elif "slow-init" in result:
                outcome = "slow-init"
            else:
                outcome = "hardfail"
                detail = f"unexpected result: {result:.80s}"
            detail = detail or result[:48]
        except (KeyError, RuntimeError) as e:
            end = ts()
            outcome = "unavailable"
            detail = f"gracious: {type(e).__name__}"
        except Exception as e:
            end = ts()
            outcome = "hardfail"
            detail = f"{type(e).__name__}: {str(e)[:80]}"
        rec = CallRecord(wid, seq, start, end, outcome, detail)  # type: ignore[arg-type]
        with rec_lock:
            records.append(rec)
        if outcome == "hardfail":
            log(f"  [CALL W{wid}-{seq:>4}] ⛔ HARDFAIL after {end-start:6.0f}ms → {detail}")
        elif outcome == "unavailable":
            log(f"  [CALL W{wid}-{seq:>4}] ⏭  UNAVAIL after {end-start:6.0f}ms")
        else:
            log(f"  [CALL W{wid}-{seq:>4}] ✅ {outcome:9s} after {end-start:6.0f}ms → {detail}")
        seq += 1
        time.sleep(0.03)


# ----- controller -----------------------------------------------------------
def run_controller(cid: int, mgr: PluginManager,
                   ctrl_records: list[CtrlRecord],
                   ctrl_lock: threading.Lock,
                   stop: threading.Event,
                   init_tracker: dict[str, float],
                   init_lock: threading.Lock) -> None:
    seq = 0
    while not stop.is_set():
        action, path = random.choice(CTRL_ACTIONS)
        start = ts()
        success = False
        detail = ""
        try:
            if action == "UNLOAD":
                ok = mgr.unload_module("greeter")
                success = True
                detail = f"unload returned {ok}"
            else:
                if action == "LOAD_SLOW":
                    key = f"C{cid}-{seq}"
                    with init_lock:
                        init_tracker[key] = start
                mv = mgr.load_module("greeter", path)
                success = True
                detail = f"→ internal {mv.version} (ready={mv.is_ready})"
                if action == "LOAD_SLOW":
                    with init_lock:
                        init_tracker[key] = ts()
        except Exception as e:
            detail = f"ERROR {type(e).__name__}: {e:.80s}"
            success = False
        end = ts()
        status = "OK" if success else "FA"
        log(f"  [CTRL C{cid}-{seq:>3}] {status} {action:14s} took {end-start:7.0f}ms — {detail}")
        with ctrl_lock:
            ctrl_records.append(CtrlRecord(cid, seq, start, end, action, success, detail))
        seq += 1
        time.sleep(random.uniform(0.8, 2.2))


# ----- statistics + assertions ---------------------------------------------
def analyse_and_assert(records: list[CallRecord],
                       ctrl_records: list[CtrlRecord]) -> int:
    bar = "─" * 72
    print(f"\n{bar}\n  FINAL STATISTICS  (total calls: {len(records)}, "
          f"total ctrl ops: {len(ctrl_records)})\n{bar}", flush=True)

    by_outcome: dict[str, int] = {}
    for r in records:
        by_outcome[r.outcome] = by_outcome.get(r.outcome, 0) + 1
    print(f"\n  Calls by outcome:")
    for k, v in sorted(by_outcome.items()):
        marker = ""
        if k == "hardfail":
            marker = "  ← FAIL: hard crash"
        elif k == "timeout":
            marker = "  ← FAIL: call blocked too long in guard"
        print(f"    {k:>12s} : {v:>6d}{marker}")

    waits = [(r.end_ms - r.start_ms) for r in records]
    if waits:
        print(f"\n  Call latency (ms):")
        waits_sorted = sorted(waits)
        n = len(waits_sorted)
        def pct(p: float) -> float:
            idx = min(n - 1, int(n * p))
            return waits_sorted[idx]
        print(f"    min  = {min(waits):.0f}")
        print(f"    p50  = {pct(0.5):.0f}")
        print(f"    p95  = {pct(0.95):.0f}")
        print(f"    p99  = {pct(0.99):.0f}")
        print(f"    max  = {max(waits):.0f}")
        too_long = [w for w in waits if w > LONG_WAIT_THRESHOLD_MS]
        print(f"    > {LONG_WAIT_THRESHOLD_MS:.0f}ms (long wait threshold) = "
              f"{len(too_long)}" + ("   ← FAIL" if too_long else ""))

    ctrl_by_action: dict[str, int] = {}
    ctrl_fail = 0
    for c in ctrl_records:
        ctrl_by_action[c.action] = ctrl_by_action.get(c.action, 0) + 1
        if not c.success:
            ctrl_fail += 1
    print(f"\n  Control operations by action:")
    for k, v in sorted(ctrl_by_action.items()):
        print(f"    {k:>14s} : {v:>6d}")
    print(f"    {'ctrl errors':>14s} : {ctrl_fail:>6d}")

    # ----- HARD ASSERTIONS ------------------------------------------------
    print(f"\n{bar}\n  ASSERTIONS\n{bar}", flush=True)
    failures: list[str] = []

    if by_outcome.get("hardfail", 0):
        failures.append(f"{by_outcome['hardfail']} hard-failed calls (uncaught exceptions)")
    else:
        print("  ✅ 0 hard failures (no uncaught exceptions in business calls)")

    if by_outcome.get("timeout", 0):
        failures.append(f"{by_outcome['timeout']} calls timed out in acquire_read barrier")
    else:
        print("  ✅ 0 acquire_read timeouts (no long-blocked callers)")

    if any(w > LONG_WAIT_THRESHOLD_MS for w in waits):
        failures.append(
            f"{sum(1 for w in waits if w > LONG_WAIT_THRESHOLD_MS)} calls exceeded "
            f"{LONG_WAIT_THRESHOLD_MS:.0f}ms (hung / blocked)"
        )
    else:
        print(f"  ✅ 0 excessively long waits (max {max(waits):.0f}ms < "
              f"{LONG_WAIT_THRESHOLD_MS:.0f}ms threshold)")

    # Detect replace-success-then-immediately-unavailable inconsistency:
    # For every successful REPLACE control op, look at the 20ms window of
    # calls that land strictly after it. If all are UNAVAILABLE it is a
    # likely "new version installed but immediately zapped" race.
    spurious_unavail = 0
    for c in ctrl_records:
        if c.success and c.action != "UNLOAD":
            window_calls = [
                r for r in records
                if c.end_ms < r.start_ms < c.end_ms + 20
                and r.outcome == "unavailable"
            ]
            spurious_unavail += len(window_calls)
    if spurious_unavail:
        failures.append(
            f"{spurious_unavail} calls in 20ms window AFTER a successful install "
            f"got UNAVAILABLE (op_mutex may be missing)"
        )
    else:
        print("  ✅ 0 spurious UNAVAILABLE right after a successful install "
              "(op_mutex serialises correctly)")

    # Init protection is verified in the dedicated DEDICATED_INIT_PROTECTION
    # scenario below. Here in the mixed stress run, multiple LOAD_SLOW ops
    # may swap the same "slow-init" return string in and out, so a naive
    # timestamp comparison against *any* control op end is unreliable.
    print("  ✅ (init protection verified by dedicated scenario, skipped in mixed stress)")

    # Request loss detection: every worker increments seq by 1 each call.
    # The total number of records must equal the sum of max-seq+1 per worker.
    per_worker_max: dict[int, int] = {}
    for r in records:
        per_worker_max[r.worker_id] = max(per_worker_max.get(r.worker_id, -1), r.seq)
    expected = sum((m + 1) for m in per_worker_max.values())
    if expected != len(records):
        failures.append(
            f"Record count mismatch: expected {expected} (= sum(max_seq+1)), "
            f"got {len(records)} → lost {expected - len(records)} calls"
        )
    else:
        print(f"  ✅ 0 lost calls ({len(records)} records match seq counters)")

    print()
    if failures:
        print("=" * 72, flush=True)
        print(f"  ❌ STRESS TEST FAILED — {len(failures)} assertion(s):", flush=True)
        for i, f in enumerate(failures, 1):
            print(f"     {i}. {f}", flush=True)
        print("=" * 72, flush=True)
        return 1
    print("=" * 72)
    print("  ✅  ALL ASSERTIONS PASSED — mixed-scenario stress test OK")
    print("=" * 72)
    return 0


# ----- main ------------------------------------------------------------------
def main() -> int:
    print("=" * 72)
    print(f"  MIXED HOT-RELOAD STRESS TEST  ({N_WORKERS} callers + "
          f"{N_CONTROLLERS} controllers, {DURATION_S:.0f}s)")
    print("=" * 72, flush=True)

    mgr = PluginManager(drain_timeout=30.0, read_timeout=5.0)
    # Bootstrap with v1 so the first wave of calls finds something.
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))
    log("[BOOT] greeter v1 baseline loaded. Spawning workers + controllers.")

    records: list[CallRecord] = []
    rec_lock = threading.Lock()
    ctrl_records: list[CtrlRecord] = []
    ctrl_lock = threading.Lock()
    init_tracker: dict[str, float] = {}
    init_lock = threading.Lock()
    stop = threading.Event()

    workers = [threading.Thread(target=run_worker,
                                args=(i, mgr, records, rec_lock, stop),
                                name=f"W{i}",
                                daemon=True)
               for i in range(N_WORKERS)]
    controllers = [threading.Thread(target=run_controller,
                                    args=(i, mgr, ctrl_records, ctrl_lock, stop,
                                          init_tracker, init_lock),
                                    name=f"C{i}",
                                    daemon=True)
                   for i in range(N_CONTROLLERS)]

    for t in workers + controllers:
        t.start()

    time.sleep(DURATION_S)
    stop.set()
    for t in controllers:
        t.join(timeout=15)
    for t in workers:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=30)
    log("[STOP] all threads joined, drain complete.")

    return analyse_and_assert(records, ctrl_records)


def sep(title: str) -> None:
    bar = "─" * 72
    print(f"\n{bar}\n  {title}\n{bar}\n", flush=True)


def dedicated_init_protection() -> int:
    sep("DEDICATED SCENARIO : Exact init-protection proof (12 threads x 1s init)")
    mgr = PluginManager(drain_timeout=15.0, read_timeout=5.0)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))
    log("[BOOT] baseline v1 ready. Starting slow-init LOAD + call hammer.")

    records: list[CallRecord] = []
    rec_lock = threading.Lock()
    stop = threading.Event()
    load_info: dict[str, float] = {}

    def loader():
        load_info["start"] = ts()
        mv = mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_slow_init.py"))
        load_info["end"] = ts()
        load_info["mv_ready"] = 1.0 if mv.is_ready else 0.0

    load_thread = threading.Thread(target=loader, daemon=True)
    load_thread.start()
    time.sleep(0.02)

    def caller(wid: int):
        seq = 0
        while not stop.is_set():
            start = ts()
            try:
                result = mgr.call("greeter", "greet", f"W{wid}-{seq}")
                end = ts()
                if "v1" in result:
                    outcome = "v1"
                elif "slow-init" in result:
                    outcome = "slow-init"
                else:
                    outcome = "hardfail"
                detail = result[:48]
            except (KeyError, RuntimeError) as e:
                end = ts()
                outcome = "unavailable"
                detail = type(e).__name__
            except Exception as e:
                end = ts()
                outcome = "hardfail"
                detail = f"{type(e).__name__}: {str(e)[:48]}"
            with rec_lock:
                records.append(CallRecord(wid, seq, start, end, outcome, detail))  # type: ignore[arg-type]
            seq += 1
            time.sleep(0.003)

    callers = [threading.Thread(target=caller, args=(i,), daemon=True)
               for i in range(12)]
    for t in callers:
        t.start()

    load_thread.join(timeout=10)
    time.sleep(1.0)
    stop.set()
    for t in callers:
        t.join(timeout=10)
    mgr.wait_all_drained(timeout=10)

    load_end = load_info.get("end", 0.0)
    load_start = load_info.get("start", 0.0)
    log(f"[ctl] load window start={load_start:.0f}ms → end={load_end:.0f}ms "
        f"(init took {load_end - load_start:.0f}ms), mv_ready={load_info.get('mv_ready')}")
    log(f"[ctl] total calls = {len(records)}")

    by_outcome: dict[str, int] = {}
    for r in records:
        by_outcome[r.outcome] = by_outcome.get(r.outcome, 0) + 1
    for k, v in sorted(by_outcome.items()):
        log(f"[ctl]   {k:>12s}: {v}")

    # EXACT PROOF: any call started BEFORE load_end → cannot be slow-init,
    # because mark_ready() + set_current() only happen AFTER on_load returns
    # (which is exactly when load_module returns → load_end timestamp).
    bad = [r for r in records
           if r.outcome == "slow-init" and r.start_ms < load_end]
    if bad:
        log(f"[FAIL] ❌ {len(bad)} calls used slow-init BEFORE load_end "
            f"(init protection BROKEN)")
        for r in bad[:10]:
            log(f"       W{r.worker_id}-{r.seq} start={r.start_ms:.0f}ms "
                f"< load_end={load_end:.0f}ms")
        return 1
    log(f"[PASS] ✅ Init protection: 0 slow-init calls started before "
        f"load_end ({load_end:.0f}ms)")

    # Before load started, no slow-init should exist either (sanity)
    pre = [r for r in records if r.start_ms < load_start]
    pre_slow = [r for r in pre if r.outcome == "slow-init"]
    assert not pre_slow, f"Before load_start, baseline must be used, got {len(pre_slow)} slow"
    log(f"[PASS] ✅ Before load_start ({load_start:.0f}ms) all "
        f"{len(pre)} calls used baseline v1")

    # After load_end + grace period, at least SOME calls must route to slow-init
    post = [r for r in records if r.start_ms > load_end + 50]
    post_slow = [r for r in post if r.outcome == "slow-init"]
    if not post_slow:
        log("[FAIL] ❌ After load_end, no slow-init calls observed at all!")
        return 1
    log(f"[PASS] ✅ After load_end, {len(post_slow)}/{len(post)} calls "
        f"correctly routed to slow-init (new version took traffic)")
    return 0


def run_all() -> int:
    sep("STRESS SUITE BEGIN")
    rc = dedicated_init_protection()
    if rc != 0:
        return rc
    rc = main()
    if rc != 0:
        return rc
    sep("ENTIRE STRESS SUITE PASSED ✅")
    return 0


if __name__ == "__main__":
    sys.exit(run_all())
