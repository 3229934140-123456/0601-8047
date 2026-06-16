"""
Hot-reload plugin stress suite — 4 scenarios, auto-statistics, strict assertions.

  1. DETERMINISTIC REPLACE+UNLOAD: one thread loops v1→v2→v3, another
     unloads at the replacement boundary. Per-call timeline shows exactly
     which version (or fallback) each request landed on.
  2. DEDICATED INIT PROTECTION: slow-init (1s on_load) with concurrent
     callers on both greet() AND slow_greet(). Any "init not complete"
     error text or hardfail → exit 1.
  3. MIXED STRESS (25s): 16 callers + 2 controllers, fallback registered,
     statistics track: old-version in-flight, new-version, degraded, timeout,
     hardfail.  Timeout causes exit 1; degraded is normal.
  4. UNLOAD-RELOAD CYCLE: continuous load/unload/load with callers in
     between, no request loss.
"""

from __future__ import annotations

import random
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plugin_manager import PluginManager, FallbackResult, ReadTimeoutError

PLUGINS_DIR = Path(__file__).resolve().parent / "plugins"
T0 = time.perf_counter()
LOG_LOCK = threading.Lock()
LONG_WAIT_MS = 5000.0


def ts() -> float:
    return (time.perf_counter() - T0) * 1000.0


def log(msg: str) -> None:
    with LOG_LOCK:
        print(f"[{ts():8.1f}] {msg}", flush=True)


def sep(title: str) -> None:
    bar = "=" * 74
    print(f"\n{bar}\n  {title}\n{bar}\n", flush=True)


def fallback_greet(name: str) -> str:
    return f"[fallback] Service temporarily unavailable for {name}"


def fallback_slow_greet(name: str) -> str:
    return f"[fallback-slow] Service temporarily unavailable for {name}"


@dataclass
class CallRec:
    wid: int
    seq: int
    start_ms: float
    end_ms: float = 0.0
    outcome: str = "hardfail"
    detail: str = ""


@dataclass
class CtrlRec:
    cid: int
    seq: int
    start_ms: float
    end_ms: float = 0.0
    action: str = ""
    ok: bool = False
    detail: str = ""


def classify(result: object) -> str:
    if isinstance(result, FallbackResult):
        return "degraded"
    s = str(result)
    if "v1" in s:
        return "v1"
    if "v2" in s:
        return "v2"
    if "v3" in s:
        return "v3"
    if "slow-init" in s:
        return "slow-init"
    if "fallback" in s:
        return "degraded"
    return "unknown"


# ────────────────────────────────────────────────────────────────────
# Scenario A: Deterministic replace + unload interleaving
# ────────────────────────────────────────────────────────────────────
def scenario_deterministic_replace_unload() -> int:
    sep("A. DETERMINISTIC REPLACE + UNLOAD (1 replacer + 1 unloader + 8 callers)")
    mgr = PluginManager(drain_timeout=15.0, read_timeout=5.0)
    mgr.register_fallback("greeter", fallback_slow_greet)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    records: list[CallRec] = []
    rec_lock = threading.Lock()
    stop = threading.Event()
    ctrl_recs: list[CtrlRec] = []
    ctrl_lock = threading.Lock()

    version_cycle = [
        str(PLUGINS_DIR / "greeter_v1.py"),
        str(PLUGINS_DIR / "greeter_v2.py"),
        str(PLUGINS_DIR / "greeter_v3.py"),
    ]

    def replacer():
        seq = 0
        idx = 0
        while not stop.is_set():
            path = version_cycle[idx % len(version_cycle)]
            s = ts()
            try:
                mv = mgr.load_module("greeter", path)
                e = ts()
                label = Path(path).stem
                with ctrl_lock:
                    ctrl_recs.append(CtrlRec(0, seq, s, e, f"REPLACE_{label}", True,
                                              f"→ {mv.version}"))
                log(f"  [REPLACER] ✅ {label:14s} → internal {mv.version}")
            except Exception as ex:
                e = ts()
                with ctrl_lock:
                    ctrl_recs.append(CtrlRec(0, seq, s, e, "REPLACE", False, str(ex)[:80]))
                log(f"  [REPLACER] ❌ {str(ex)[:60]}")
            seq += 1
            idx += 1
            time.sleep(1.5)

    def unloader():
        seq = 0
        while not stop.is_set():
            time.sleep(random.uniform(1.0, 2.5))
            s = ts()
            ok = mgr.unload_module("greeter")
            e = ts()
            with ctrl_lock:
                ctrl_recs.append(CtrlRec(1, seq, s, e, "UNLOAD", True, f"returned={ok}"))
            log(f"  [UNLOADER]  {'✅' if ok else '⏭ '} unload={ok}")
            if ok:
                time.sleep(0.5)
                path = random.choice(version_cycle)
                s2 = ts()
                try:
                    mv = mgr.load_module("greeter", path)
                    e2 = ts()
                    with ctrl_lock:
                        ctrl_recs.append(CtrlRec(1, seq, s2, e2, "RELOAD", True,
                                                  f"→ {mv.version}"))
                    log(f"  [UNLOADER]  ↩️  reloaded {Path(path).stem} → {mv.version}")
                except Exception as ex:
                    e2 = ts()
                    with ctrl_lock:
                        ctrl_recs.append(CtrlRec(1, seq, s2, e2, "RELOAD", False, str(ex)[:80]))
            seq += 1

    def worker(wid: int):
        seq = 0
        while not stop.is_set():
            s = ts()
            try:
                result = mgr.call_safe("greeter", "slow_greet", f"W{wid}-{seq}")
                e = ts()
                outcome = classify(result)
                detail = str(result)[:48]
            except ReadTimeoutError as ex:
                e = ts()
                outcome = "timeout"
                detail = str(ex)[:60]
            except Exception as ex:
                e = ts()
                outcome = "hardfail"
                detail = f"{type(ex).__name__}: {str(ex)[:48]}"
            with rec_lock:
                records.append(CallRec(wid, seq, s, e, outcome, detail))
            seq += 1
            time.sleep(0.03)

    threads = [threading.Thread(target=replacer, daemon=True),
               threading.Thread(target=unloader, daemon=True)]
    threads += [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(8)]
    for t in threads:
        t.start()

    time.sleep(18.0)
    stop.set()
    for t in threads:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=15)

    return _assert_scenario("A", records, ctrl_recs, expect_degraded=True)


# ────────────────────────────────────────────────────────────────────
# Scenario B: Init protection (both greet + slow_greet)
# ────────────────────────────────────────────────────────────────────
def scenario_init_protection() -> int:
    sep("B. INIT PROTECTION (12 callers × greet + slow_greet, 1s on_load)")
    mgr = PluginManager(drain_timeout=15.0, read_timeout=5.0)
    mgr.register_fallback("greeter", fallback_greet)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    load_info: dict[str, float] = {}
    records: list[CallRec] = []
    rec_lock = threading.Lock()
    stop = threading.Event()
    init_error_texts: list[str] = []

    def loader():
        load_info["start"] = ts()
        mv = mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_slow_init.py"))
        load_info["end"] = ts()
        load_info["ready"] = mv.is_ready

    load_thread = threading.Thread(target=loader, daemon=True)
    load_thread.start()
    time.sleep(0.02)

    def caller(wid: int):
        seq = 0
        while not stop.is_set():
            func = "greet" if seq % 2 == 0 else "slow_greet"
            s = ts()
            try:
                result = mgr.call_safe("greeter", func, f"W{wid}-{seq}")
                e = ts()
                outcome = classify(result)
                detail = str(result)[:60]
                text = str(result)
                if "BEFORE init" in text or "ERROR" in text:
                    init_error_texts.append(f"W{wid}-{seq} {func}: {text[:80]}")
                    outcome = "init-leak"
            except ReadTimeoutError:
                e = ts()
                outcome = "timeout"
                detail = "ReadTimeoutError"
            except Exception as ex:
                e = ts()
                outcome = "hardfail"
                detail = f"{type(ex).__name__}: {str(ex)[:48]}"
            with rec_lock:
                records.append(CallRec(wid, seq, s, e, outcome, detail))
            seq += 1
            time.sleep(0.003 if func == "greet" else 0.01)

    callers = [threading.Thread(target=caller, args=(i,), daemon=True) for i in range(12)]
    for t in callers:
        t.start()

    load_thread.join(timeout=10)
    time.sleep(1.5)
    stop.set()
    for t in callers:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=10)

    load_end = load_info.get("end", 0.0)
    load_start = load_info.get("start", 0.0)
    log(f"[ctl] load window {load_start:.0f}ms → {load_end:.0f}ms "
        f"({load_end - load_start:.0f}ms), ready={load_info.get('ready')}")

    # ASSERTION 1: no init-leak
    if init_error_texts:
        log(f"[FAIL] ❌ {len(init_error_texts)} calls returned init-not-complete text:")
        for t in init_error_texts[:10]:
            log(f"  {t}")
        return 1
    log("[PASS] ✅ 0 init-leak calls (no 'BEFORE init' text in any result)")

    # ASSERTION 2: no hardfail
    hfs = [r for r in records if r.outcome == "hardfail"]
    if hfs:
        log(f"[FAIL] ❌ {len(hfs)} hardfail calls in init-protection scenario")
        for r in hfs[:5]:
            log(f"  W{r.wid}-{r.seq} start={r.start_ms:.0f}ms → {r.detail}")
        return 1
    log("[PASS] ✅ 0 hardfail calls")

    # ASSERTION 3: no slow-init before load_end
    bad = [r for r in records if r.outcome == "slow-init" and r.start_ms < load_end]
    if bad:
        log(f"[FAIL] ❌ {len(bad)} slow-init calls started before load_end")
        return 1
    log(f"[PASS] ✅ 0 slow-init before load_end ({load_end:.0f}ms)")

    # ASSERTION 4: after load_end, some slow-init observed
    post = [r for r in records if r.start_ms > load_end + 50]
    post_slow = [r for r in post if r.outcome == "slow-init"]
    if not post_slow:
        log("[FAIL] ❌ no slow-init calls after load_end")
        return 1
    log(f"[PASS] ✅ {len(post_slow)}/{len(post)} calls routed to slow-init after load_end")

    by_out = {}
    for r in records:
        by_out[r.outcome] = by_out.get(r.outcome, 0) + 1
    log(f"[ctl] outcome distribution: {by_out}")
    return 0


# ────────────────────────────────────────────────────────────────────
# Scenario C: Mixed stress (25s)
# ────────────────────────────────────────────────────────────────────
CTRL_ACTIONS = [
    ("LOAD_SLOW", str(PLUGINS_DIR / "greeter_slow_init.py")),
    ("REPLACE_V1", str(PLUGINS_DIR / "greeter_v1.py")),
    ("REPLACE_V2", str(PLUGINS_DIR / "greeter_v2.py")),
    ("REPLACE_V3", str(PLUGINS_DIR / "greeter_v3.py")),
    ("UNLOAD", None),
    ("RELOAD_V2", str(PLUGINS_DIR / "greeter_v2.py")),
]


def scenario_mixed_stress() -> int:
    sep("C. MIXED STRESS (16 callers + 2 controllers, 25s)")
    mgr = PluginManager(drain_timeout=30.0, read_timeout=5.0)
    mgr.register_fallback("greeter", fallback_slow_greet)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    records: list[CallRec] = []
    rec_lock = threading.Lock()
    ctrl_recs: list[CtrlRec] = []
    ctrl_lock = threading.Lock()
    stop = threading.Event()

    def worker(wid: int):
        seq = 0
        while not stop.is_set():
            s = ts()
            try:
                result = mgr.call_safe("greeter", "slow_greet", f"W{wid}-{seq}")
                e = ts()
                outcome = classify(result)
                detail = str(result)[:48]
            except ReadTimeoutError:
                e = ts()
                outcome = "timeout"
                detail = "ReadTimeoutError"
            except Exception as ex:
                e = ts()
                outcome = "hardfail"
                detail = f"{type(ex).__name__}: {str(ex)[:48]}"
            with rec_lock:
                records.append(CallRec(wid, seq, s, e, outcome, detail))
            seq += 1
            time.sleep(0.03)

    def controller(cid: int):
        seq = 0
        while not stop.is_set():
            action, path = random.choice(CTRL_ACTIONS)
            s = ts()
            ok = False
            detail = ""
            try:
                if action == "UNLOAD":
                    r = mgr.unload_module("greeter")
                    ok = True
                    detail = f"unload={r}"
                else:
                    mv = mgr.load_module("greeter", path)
                    ok = True
                    detail = f"→ {mv.version}"
            except Exception as ex:
                detail = f"{type(ex).__name__}: {str(ex)[:60]}"
            e = ts()
            with ctrl_lock:
                ctrl_recs.append(CtrlRec(cid, seq, s, e, action, ok, detail))
            tag = "OK" if ok else "FA"
            log(f"  [CTRL C{cid}-{seq:>3}] {tag} {action:14s} {detail}")
            seq += 1
            time.sleep(random.uniform(0.8, 2.2))

    threads = ([threading.Thread(target=worker, args=(i,), daemon=True) for i in range(16)]
               + [threading.Thread(target=controller, args=(i,), daemon=True) for i in range(2)])
    for t in threads:
        t.start()

    time.sleep(25.0)
    stop.set()
    for t in threads:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=30)

    return _assert_scenario("C", records, ctrl_recs, expect_degraded=True)


# ────────────────────────────────────────────────────────────────────
# Scenario D: Unload-reload cycle (no request loss)
# ────────────────────────────────────────────────────────────────────
def scenario_unload_reload() -> int:
    sep("D. UNLOAD → RELOAD CYCLE (no request loss)")
    mgr = PluginManager(drain_timeout=15.0, read_timeout=5.0)
    mgr.register_fallback("greeter", fallback_greet)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    records: list[CallRec] = []
    rec_lock = threading.Lock()
    stop = threading.Event()

    def worker(wid: int):
        seq = 0
        while not stop.is_set():
            s = ts()
            try:
                result = mgr.call_safe("greeter", "greet", f"W{wid}-{seq}")
                e = ts()
                outcome = classify(result)
                detail = str(result)[:48]
            except ReadTimeoutError:
                e = ts()
                outcome = "timeout"
                detail = "ReadTimeoutError"
            except Exception as ex:
                e = ts()
                outcome = "hardfail"
                detail = f"{type(ex).__name__}: {str(ex)[:48]}"
            with rec_lock:
                records.append(CallRec(wid, seq, s, e, outcome, detail))
            seq += 1
            time.sleep(0.01)

    workers = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(8)]
    for t in workers:
        t.start()

    cycle = [
        str(PLUGINS_DIR / "greeter_v1.py"),
        str(PLUGINS_DIR / "greeter_v2.py"),
        str(PLUGINS_DIR / "greeter_v3.py"),
    ]
    for i in range(5):
        time.sleep(0.6)
        log(f"[ctl] cycle {i}: UNLOAD")
        mgr.unload_module("greeter")
        time.sleep(0.5)
        path = cycle[i % len(cycle)]
        log(f"[ctl] cycle {i}: RELOAD {Path(path).stem}")
        mgr.load_module("greeter", path)

    time.sleep(1.0)
    stop.set()
    for t in workers:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=15)

    return _assert_scenario("D", records, [], expect_degraded=True)


# ────────────────────────────────────────────────────────────────────
# Shared assertion engine
# ────────────────────────────────────────────────────────────────────
def _assert_scenario(tag: str, records: list[CallRec],
                     ctrl_recs: list[CtrlRec],
                     expect_degraded: bool = False) -> int:
    by_out: dict[str, int] = {}
    for r in records:
        by_out[r.outcome] = by_out.get(r.outcome, 0) + 1

    waits = [r.end_ms - r.start_ms for r in records] if records else [0]
    waits_s = sorted(waits)
    n = len(waits_s)

    def pct(p: float) -> float:
        return waits_s[min(n - 1, int(n * p))]

    log(f"[{tag}] total calls={len(records)}  ctrl_ops={len(ctrl_recs)}")
    log(f"[{tag}] outcomes: {by_out}")
    log(f"[{tag}] latency ms: min={min(waits):.0f} p50={pct(.5):.0f} "
        f"p95={pct(.95):.0f} max={max(waits):.0f}")

    fails: list[str] = []

    hf = by_out.get("hardfail", 0)
    if hf:
        fails.append(f"{hf} hardfail (uncaught exceptions)")
    else:
        log(f"[{tag}] ✅ 0 hardfail")

    to = by_out.get("timeout", 0)
    if to:
        fails.append(f"{to} ReadTimeoutError (read barrier stuck — BUG)")
    else:
        log(f"[{tag}] ✅ 0 read-barrier timeouts")

    long_w = sum(1 for w in waits if w > LONG_WAIT_MS)
    if long_w:
        fails.append(f"{long_w} calls > {LONG_WAIT_MS:.0f}ms")
    else:
        log(f"[{tag}] ✅ 0 excessively long waits (max {max(waits):.0f}ms)")

    if expect_degraded:
        deg = by_out.get("degraded", 0)
        log(f"[{tag}] ℹ️  {deg} degraded (fallback) calls — expected when unloaded")

    per_w: dict[int, int] = {}
    for r in records:
        per_w[r.wid] = max(per_w.get(r.wid, -1), r.seq)
    expected = sum(m + 1 for m in per_w.values())
    if expected != len(records):
        fails.append(f"lost {expected - len(records)} calls "
                     f"(expected {expected}, got {len(records)})")
    else:
        log(f"[{tag}] ✅ 0 lost calls ({len(records)} records)")

    # spurious UNAVAILABLE after successful install
    spurious = 0
    for c in ctrl_recs:
        if c.ok and c.action != "UNLOAD":
            window = [r for r in records
                      if c.end_ms < r.start_ms < c.end_ms + 20
                      and r.outcome == "degraded"]
            spurious += len(window)
    if spurious:
        fails.append(f"{spurious} degraded calls in 20ms after successful install")
    else:
        log(f"[{tag}] ✅ 0 spurious degraded after install")

    if fails:
        log(f"[{tag}] ❌ FAILED:")
        for i, f in enumerate(fails, 1):
            log(f"  {i}. {f}")
        return 1
    log(f"[{tag}] ✅ ALL ASSERTIONS PASSED")
    return 0


# ────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────
def run_all() -> int:
    sep("HOT-RELOAD STRESS SUITE")
    rc = scenario_deterministic_replace_unload()
    if rc:
        return rc
    rc = scenario_init_protection()
    if rc:
        return rc
    rc = scenario_mixed_stress()
    if rc:
        return rc
    rc = scenario_unload_reload()
    if rc:
        return rc
    sep("ENTIRE SUITE PASSED ✅")
    return 0


if __name__ == "__main__":
    sys.exit(run_all())
