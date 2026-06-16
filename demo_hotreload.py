"""
Hot-reload plugin stress suite — CLI, 4+1 scenarios, timeline reports, stability mode.

  python demo_hotreload.py                      # 全部场景 + 导出 (默认)
  python demo_hotreload.py --only init          # 只跑初始化保护
  python demo_hotreload.py --only interleave    # 只跑确定性替换+卸载
  python demo_hotreload.py --only mixed --workers 24 --seconds 30
  python demo_hotreload.py --only cycle --no-export
  python demo_hotreload.py --stability 120      # 长稳模式 2 分钟
  python demo_hotreload.py --help               # 参数清单

输出文件:
  reports/{scenario}_timeline.json
  reports/{scenario}_timeline.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plugin_manager import PluginManager, FallbackResult, ReadTimeoutError

PLUGINS_DIR = Path(__file__).resolve().parent / "plugins"
REPORTS_DIR = Path(__file__).resolve().parent / "reports"
T0 = time.perf_counter()
LOG_LOCK = threading.Lock()
LONG_WAIT_MS = 5000.0

random.seed(42)  # 稳定全局随机种子


def ts() -> float:
    return (time.perf_counter() - T0) * 1000.0


def log(msg: str) -> None:
    with LOG_LOCK:
        print(f"[{ts():8.1f}] {msg}", flush=True)


def sep(title: str) -> None:
    bar = "=" * 74
    print(f"\n{bar}\n  {title}\n{bar}\n", flush=True)


# ────────────────────────────────────────────────────────────────────
# Fallback handlers
# ────────────────────────────────────────────────────────────────────
def fallback_greet(name: str) -> str:
    return f"[fallback] Service temporarily unavailable for {name}"


def fallback_slow_greet(name: str) -> str:
    return f"[fallback-slow] Service temporarily unavailable for {name}"


# ────────────────────────────────────────────────────────────────────
# Record types
# ────────────────────────────────────────────────────────────────────
@dataclass
class CallRec:
    kind: str = "CALL"
    wid: int = 0
    seq: int = 0
    start_ms: float = 0.0
    end_ms: float = 0.0
    outcome: str = "hardfail"
    detail: str = ""
    func: str = "slow_greet"


@dataclass
class CtrlRec:
    kind: str = "CTRL"
    cid: int = 0
    seq: int = 0
    start_ms: float = 0.0
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
# Timeline report: merge + sort + export
# ────────────────────────────────────────────────────────────────────
def build_timeline(calls: list[CallRec], ctrls: list[CtrlRec]) -> list[dict]:
    entries: list[dict] = []
    for c in calls:
        entries.append({
            "ts_ms": round(c.start_ms, 2),
            "sort_key": c.start_ms + 1e-9,
            "kind": "CALL",
            "id": f"W{c.wid}-{c.seq}",
            "sub": c.func,
            "outcome": c.outcome,
            "end_ms": round(c.end_ms, 2),
            "latency_ms": round(c.end_ms - c.start_ms, 2),
            "detail": c.detail,
        })
    for c in ctrls:
        entries.append({
            "ts_ms": round(c.start_ms, 2),
            "sort_key": c.start_ms,
            "kind": "CTRL",
            "id": f"C{c.cid}-{c.seq}",
            "sub": c.action,
            "outcome": "OK" if c.ok else "FAIL",
            "end_ms": round(c.end_ms, 2),
            "latency_ms": round(c.end_ms - c.start_ms, 2),
            "detail": c.detail,
        })
    entries.sort(key=lambda e: e["sort_key"])
    for e in entries:
        del e["sort_key"]
    return entries


def export_timeline(name: str, timeline: list[dict], do_export: bool) -> None:
    if not do_export or not timeline:
        return
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        json_path = REPORTS_DIR / f"{name}_timeline.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(timeline, f, ensure_ascii=False, indent=2)
        csv_path = REPORTS_DIR / f"{name}_timeline.csv"
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(timeline[0].keys()))
            writer.writeheader()
            writer.writerows(timeline)
        log(f"[report] timeline exported → {json_path}")
        log(f"[report] timeline exported → {csv_path}")
    except Exception as e:
        log(f"[report] export failed: {e}")


# ────────────────────────────────────────────────────────────────────
# Shared assertion engine
# ────────────────────────────────────────────────────────────────────
def assert_scenario(tag: str, calls: list[CallRec], ctrls: list[CtrlRec],
                    expect_degraded: bool = True) -> int:
    by_out: dict[str, int] = {}
    for r in calls:
        by_out[r.outcome] = by_out.get(r.outcome, 0) + 1

    waits = [r.end_ms - r.start_ms for r in calls] if calls else [0.0]
    waits_s = sorted(waits)
    n = len(waits_s)

    def pct(p: float) -> float:
        return waits_s[min(n - 1, int(n * p))]

    log(f"[{tag}] total calls={len(calls)}  ctrl_ops={len(ctrls)}")
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

    if "init-leak" in by_out and by_out["init-leak"] > 0:
        fails.append(f"{by_out['init-leak']} init-leak calls")

    long_w = sum(1 for w in waits if w > LONG_WAIT_MS)
    if long_w:
        fails.append(f"{long_w} calls > {LONG_WAIT_MS:.0f}ms")
    else:
        log(f"[{tag}] ✅ 0 excessively long waits (max {max(waits):.0f}ms)")

    if expect_degraded:
        deg = by_out.get("degraded", 0)
        log(f"[{tag}] ℹ️  {deg} degraded (fallback) calls")

    per_w: dict[int, int] = {}
    for r in calls:
        per_w[r.wid] = max(per_w.get(r.wid, -1), r.seq)
    expected = sum(m + 1 for m in per_w.values())
    if expected != len(calls):
        fails.append(f"lost {expected - len(calls)} calls "
                     f"(expected {expected}, got {len(calls)})")
    else:
        log(f"[{tag}] ✅ 0 lost calls ({len(calls)} records)")

    spurious = 0
    for c in ctrls:
        if c.ok and c.action != "UNLOAD":
            window = [r for r in calls
                      if c.end_ms < r.start_ms < c.end_ms + 20
                      and r.outcome == "degraded"]
            spurious += len(window)
    if spurious:
        fails.append(f"{spurious} degraded in 20ms after successful install")
    elif ctrls:
        log(f"[{tag}] ✅ 0 spurious degraded after install")

    if fails:
        log(f"[{tag}] ❌ FAILED:")
        for i, f in enumerate(fails, 1):
            log(f"  {i}. {f}")
        return 1
    log(f"[{tag}] ✅ ALL ASSERTIONS PASSED")
    return 0


# ────────────────────────────────────────────────────────────────────
# Scenario A: DETERMINISTIC replace + unload (zero random)
# ────────────────────────────────────────────────────────────────────
def scenario_interleave(workers: int = 8, seconds: float = 16.0) -> tuple[int, list[CallRec], list[CtrlRec]]:
    sep("A. DETERMINISTIC REPLACE + UNLOAD (no random, 1 replacer + 1 unloader)")
    mgr = PluginManager(drain_timeout=15.0, read_timeout=5.0)
    mgr.register_fallback("greeter", fallback_slow_greet)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    calls: list[CallRec] = []
    ctrls: list[CtrlRec] = []
    cl = threading.Lock()
    ctl = threading.Lock()
    stop = threading.Event()
    replace_complete = threading.Event()
    replace_count = {"n": 0}

    REPLACE_EVERY_S = 2.0       # 每 2s 替换一次
    UNLOAD_OFFSET_S = 0.25      # 替换完成后 250ms 卸载 (固定偏移，无随机)
    CYCLE = [
        ("REPLACE_V1", str(PLUGINS_DIR / "greeter_v1.py")),
        ("REPLACE_V2", str(PLUGINS_DIR / "greeter_v2.py")),
        ("REPLACE_V3", str(PLUGINS_DIR / "greeter_v3.py")),
    ]

    def replacer():
        seq = 0
        idx = 0
        while not stop.is_set():
            action, path = CYCLE[idx % len(CYCLE)]
            s = ts()
            replace_complete.clear()
            try:
                mv = mgr.load_module("greeter", path)
                e = ts()
                with ctl:
                    ctrls.append(CtrlRec("CTRL", 0, seq, s, e, action, True, f"→ {mv.version}"))
                log(f"  [REPLACER #{seq}] ✅ {action:12s} → {mv.version}")
            except Exception as ex:
                e = ts()
                with ctl:
                    ctrls.append(CtrlRec("CTRL", 0, seq, s, e, action, False, str(ex)[:80]))
                log(f"  [REPLACER #{seq}] ❌ {str(ex)[:60]}")
            finally:
                replace_count["n"] += 1
                replace_complete.set()
            seq += 1
            idx += 1
            replace_complete.set()
            t_end = (time.perf_counter() - T0) + REPLACE_EVERY_S
            while (time.perf_counter() - T0) < t_end and not stop.is_set():
                time.sleep(0.01)

    def unloader():
        seq = 0
        while not stop.is_set():
            # 等替换完成
            if not replace_complete.wait(timeout=3.0):
                if stop.is_set():
                    return
                continue
            time.sleep(UNLOAD_OFFSET_S)      # 固定偏移，100% 可复现
            s = ts()
            ok = mgr.unload_module("greeter")
            e = ts()
            with ctl:
                ctrls.append(CtrlRec("CTRL", 1, seq, s, e, "UNLOAD", True, f"unload={ok}"))
            log(f"  [UNLOADER  #{seq}] {'✅' if ok else '⏭ '} unload={ok}")
            if ok:
                time.sleep(0.5)               # 固定 500ms 后 reload
                _, path = CYCLE[(seq + 1) % len(CYCLE)]
                action_rl = f"RELOAD_{Path(path).stem}"
                s2 = ts()
                try:
                    mv = mgr.load_module("greeter", path)
                    e2 = ts()
                    with ctl:
                        ctrls.append(CtrlRec("CTRL", 1, seq, s2, e2, action_rl, True, f"→ {mv.version}"))
                    log(f"  [UNLOADER  #{seq}] ↩️  {action_rl} → {mv.version}")
                except Exception as ex:
                    e2 = ts()
                    with ctl:
                        ctrls.append(CtrlRec("CTRL", 1, seq, s2, e2, action_rl, False, str(ex)[:80]))
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
            with cl:
                calls.append(CallRec("CALL", wid, seq, s, e, outcome, detail, "slow_greet"))
            seq += 1
            t_next = (time.perf_counter() - T0) + 0.03
            while (time.perf_counter() - T0) < t_next and not stop.is_set():
                time.sleep(0.001)

    # 先启动 replacer + unloader 线程，确保它们开始于同一个 100ms 对齐点
    start_barrier = threading.Barrier(2, timeout=30)

    def replacer_boot():
        start_barrier.wait(timeout=10)
        replacer()

    def unloader_boot():
        start_barrier.wait(timeout=10)
        unloader()

    threads = [
        threading.Thread(target=replacer_boot, daemon=True),
        threading.Thread(target=unloader_boot, daemon=True),
    ] + [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(workers)]

    for t in threads:
        t.start()
    time.sleep(seconds)
    stop.set()
    for t in threads:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=15)

    rc = assert_scenario("A", calls, ctrls, expect_degraded=True)
    return rc, calls, ctrls


# ────────────────────────────────────────────────────────────────────
# Scenario B: INIT PROTECTION (greet + slow_greet both)
# ────────────────────────────────────────────────────────────────────
def scenario_init(workers: int = 12) -> tuple[int, list[CallRec], list[CtrlRec]]:
    sep("B. INIT PROTECTION (greet + slow_greet both, 1s on_load)")
    mgr = PluginManager(drain_timeout=15.0, read_timeout=5.0)
    mgr.register_fallback("greeter", fallback_greet)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    load_info: dict[str, float] = {}
    calls: list[CallRec] = []
    ctrls: list[CtrlRec] = []
    cl = threading.Lock()
    ctl = threading.Lock()
    stop = threading.Event()
    init_leak_texts: list[str] = []

    load_start_ts = ts()

    def loader():
        mv = mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_slow_init.py"))
        load_info["end"] = ts()
        load_info["ready"] = mv.is_ready
        with ctl:
            ctrls.append(CtrlRec("CTRL", 0, 0, load_start_ts, load_info["end"],
                                  "LOAD_SLOW_INIT", True,
                                  f"→ {mv.version} ready={mv.is_ready}"))

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
                if "BEFORE init" in text or "ERROR" in text or "not initialized" in text:
                    init_leak_texts.append(f"W{wid}-{seq} {func}: {text[:80]}")
                    outcome = "init-leak"
            except ReadTimeoutError:
                e = ts()
                outcome = "timeout"
                detail = "ReadTimeoutError"
            except Exception as ex:
                e = ts()
                outcome = "hardfail"
                detail = f"{type(ex).__name__}: {str(ex)[:48]}"
            with cl:
                calls.append(CallRec("CALL", wid, seq, s, e, outcome, detail, func))
            seq += 1
            step_s = 0.003 if func == "greet" else 0.01
            t_next = (time.perf_counter() - T0) + step_s
            while (time.perf_counter() - T0) < t_next and not stop.is_set():
                time.sleep(0.0005)

    callers = [threading.Thread(target=caller, args=(i,), daemon=True) for i in range(workers)]
    for t in callers:
        t.start()

    load_thread.join(timeout=10)
    time.sleep(1.5)
    stop.set()
    for t in callers:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=10)

    load_end = load_info.get("end", 0.0)
    log(f"[ctl] load start={load_start_ts:.0f}ms end={load_end:.0f}ms "
        f"({load_end - load_start_ts:.0f}ms), ready={load_info.get('ready')}")

    # 额外严格断言 (在 assert_scenario 之上)
    if init_leak_texts:
        log(f"[FAIL B] ❌ {len(init_leak_texts)} init-leak (error text in result):")
        for t in init_leak_texts[:10]:
            log(f"  {t}")
        return 1, calls, ctrls
    log("[PASS B] ✅ 0 init-leak text checks")

    hfs = [r for r in calls if r.outcome == "hardfail"]
    if hfs:
        log(f"[FAIL B] ❌ {len(hfs)} hardfail")
        return 1, calls, ctrls

    bad = [r for r in calls if r.outcome == "slow-init" and r.start_ms < load_end]
    if bad:
        log(f"[FAIL B] ❌ {len(bad)} slow-init before load_end")
        return 1
    log("[PASS B] ✅ 0 slow-init before load_end")

    post = [r for r in calls if r.start_ms > load_end + 50]
    post_slow = [r for r in post if r.outcome == "slow-init"]
    if not post_slow:
        log("[FAIL B] ❌ no slow-init after load_end")
        return 1, calls, ctrls
    log(f"[PASS B] ✅ {len(post_slow)}/{len(post)} routed to slow-init after load_end")

    by_out: dict[str, int] = {}
    for r in calls:
        by_out[r.outcome] = by_out.get(r.outcome, 0) + 1
    log(f"[ctl] outcomes: {by_out}")

    return assert_scenario("B", calls, ctrls, expect_degraded=False), calls, ctrls


# ────────────────────────────────────────────────────────────────────
# Scenario C: MIXED STRESS
# ────────────────────────────────────────────────────────────────────
CTRL_ACTIONS = [
    ("LOAD_SLOW", str(PLUGINS_DIR / "greeter_slow_init.py")),
    ("REPLACE_V1", str(PLUGINS_DIR / "greeter_v1.py")),
    ("REPLACE_V2", str(PLUGINS_DIR / "greeter_v2.py")),
    ("REPLACE_V3", str(PLUGINS_DIR / "greeter_v3.py")),
    ("UNLOAD", None),
    ("RELOAD_V2", str(PLUGINS_DIR / "greeter_v2.py")),
]


def scenario_mixed(workers: int = 16, seconds: float = 25.0) -> tuple[int, list[CallRec], list[CtrlRec]]:
    sep(f"C. MIXED STRESS ({workers} callers + 2 controllers, {seconds:.0f}s)")
    mgr = PluginManager(drain_timeout=30.0, read_timeout=5.0)
    mgr.register_fallback("greeter", fallback_slow_greet)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    calls: list[CallRec] = []
    ctrls: list[CtrlRec] = []
    cl = threading.Lock()
    ctl = threading.Lock()
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
            with cl:
                calls.append(CallRec("CALL", wid, seq, s, e, outcome, detail, "slow_greet"))
            seq += 1
            t_next = (time.perf_counter() - T0) + 0.03
            while (time.perf_counter() - T0) < t_next and not stop.is_set():
                time.sleep(0.001)

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
            with ctl:
                ctrls.append(CtrlRec("CTRL", cid, seq, s, e, action, ok, detail))
            tag = "OK" if ok else "FA"
            log(f"  [CTRL C{cid}-{seq:>3}] {tag} {action:14s} {detail}")
            seq += 1
            t_next = (time.perf_counter() - T0) + random.uniform(0.8, 2.2)
            while (time.perf_counter() - T0) < t_next and not stop.is_set():
                time.sleep(0.02)

    threads = ([threading.Thread(target=worker, args=(i,), daemon=True) for i in range(workers)]
               + [threading.Thread(target=controller, args=(i,), daemon=True) for i in range(2)])
    for t in threads:
        t.start()
    time.sleep(seconds)
    stop.set()
    for t in threads:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=30)

    rc = assert_scenario("C", calls, ctrls, expect_degraded=True)
    return rc, calls, ctrls


# ────────────────────────────────────────────────────────────────────
# Scenario D: UNLOAD-RELOAD cycle
# ────────────────────────────────────────────────────────────────────
def scenario_cycle(workers: int = 8) -> tuple[int, list[CallRec], list[CtrlRec]]:
    sep("D. UNLOAD → RELOAD CYCLE (no request loss)")
    mgr = PluginManager(drain_timeout=15.0, read_timeout=5.0)
    mgr.register_fallback("greeter", fallback_greet)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    calls: list[CallRec] = []
    ctrls: list[CtrlRec] = []
    cl = threading.Lock()
    ctl = threading.Lock()
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
            with cl:
                calls.append(CallRec("CALL", wid, seq, s, e, outcome, detail, "greet"))
            seq += 1
            t_next = (time.perf_counter() - T0) + 0.01
            while (time.perf_counter() - T0) < t_next and not stop.is_set():
                time.sleep(0.001)

    workers_threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(workers)]
    for t in workers_threads:
        t.start()

    cycle_paths = [
        str(PLUGINS_DIR / "greeter_v1.py"),
        str(PLUGINS_DIR / "greeter_v2.py"),
        str(PLUGINS_DIR / "greeter_v3.py"),
    ]
    seq = 0
    for i in range(5):
        time.sleep(0.6)
        s = ts()
        ok = mgr.unload_module("greeter")
        e = ts()
        with ctl:
            ctrls.append(CtrlRec("CTRL", 0, seq, s, e, "UNLOAD", True, f"unload={ok}"))
        log(f"  [cycle {i}] UNLOAD ok={ok}")
        seq += 1
        time.sleep(0.5)
        path = cycle_paths[i % len(cycle_paths)]
        s2 = ts()
        mv = mgr.load_module("greeter", path)
        e2 = ts()
        with ctl:
            ctrls.append(CtrlRec("CTRL", 0, seq, s2, e2, f"RELOAD_{Path(path).stem}",
                                  True, f"→ {mv.version}"))
        log(f"  [cycle {i}] RELOAD {Path(path).stem} → {mv.version}")
        seq += 1

    time.sleep(1.0)
    stop.set()
    for t in workers_threads:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=15)

    rc = assert_scenario("D", calls, ctrls, expect_degraded=True)
    return rc, calls, ctrls


# ────────────────────────────────────────────────────────────────────
# Scenario S: LONG STABILITY mode
# ────────────────────────────────────────────────────────────────────
def scenario_stability(seconds: float = 300.0, workers: int = 16,
                       tick_s: float = 10.0) -> int:
    sep(f"S. LONG STABILITY RUN ({seconds:.0f}s, {workers} callers, rollup every {tick_s:.0f}s)")
    mgr = PluginManager(drain_timeout=60.0, read_timeout=5.0)
    mgr.register_fallback("greeter", fallback_slow_greet)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    cl = threading.Lock()
    ctl = threading.Lock()
    stop = threading.Event()
    stats_lock = threading.Lock()
    stats = {
        "calls": 0, "degraded": 0, "timeout": 0, "hardfail": 0,
        "tick_calls": 0, "tick_degraded": 0, "tick_timeout": 0, "tick_hardfail": 0,
    }

    def worker(wid: int):
        seq = 0
        while not stop.is_set():
            try:
                result = mgr.call_safe("greeter", "slow_greet", f"W{wid}-{seq}")
                outcome = classify(result)
            except ReadTimeoutError:
                outcome = "timeout"
            except Exception:
                outcome = "hardfail"
            with stats_lock:
                stats["calls"] += 1
                stats["tick_calls"] += 1
                if outcome == "degraded":
                    stats["degraded"] += 1
                    stats["tick_degraded"] += 1
                elif outcome == "timeout":
                    stats["timeout"] += 1
                    stats["tick_timeout"] += 1
                elif outcome == "hardfail":
                    stats["hardfail"] += 1
                    stats["tick_hardfail"] += 1
            seq += 1
            time.sleep(0.03)

    def controller(cid: int):
        seq = 0
        while not stop.is_set():
            action, path = random.choice(CTRL_ACTIONS)
            try:
                if action == "UNLOAD":
                    mgr.unload_module("greeter")
                else:
                    mgr.load_module("greeter", path)
            except Exception:
                pass
            seq += 1
            t_next = (time.perf_counter() - T0) + random.uniform(0.5, 2.0)
            while (time.perf_counter() - T0) < t_next and not stop.is_set():
                time.sleep(0.03)

    threads = ([threading.Thread(target=worker, args=(i,), daemon=True) for i in range(workers)]
               + [threading.Thread(target=controller, args=(i,), daemon=True) for i in range(2)])
    for t in threads:
        t.start()

    start_t = time.perf_counter() - T0
    elapsed = 0.0
    ticks = 0
    while not stop.is_set() and elapsed < seconds:
        time.sleep(min(tick_s, seconds - elapsed))
        ticks += 1
        now = time.perf_counter() - T0
        elapsed = now - start_t
        with stats_lock:
            cur_inflight = sum(
                r.guard.reader_count
                for slot in list(mgr._slots.values())
                for r in ([slot.current] if slot.current else []) + slot.retiring_versions
            )
            tc = stats["tick_calls"]
            td = stats["tick_degraded"]
            tto = stats["tick_timeout"]
            thf = stats["tick_hardfail"]
            tcps = tc / tick_s if tc else 0.0
            stats["tick_calls"] = stats["tick_degraded"] = 0
            stats["tick_timeout"] = stats["tick_hardfail"] = 0
            log(f"  [ROLLUP #{ticks:>3}] t={elapsed:6.0f}s  |  "
                f"this-window: calls={tc} degr={td} to={tto} hf={thf} "
                f"cps={tcps:5.1f}  |  in_flight={cur_inflight}  |  "
                f"TOTAL calls={stats['calls']} degr={stats['degraded']} "
                f"to={stats['timeout']} hf={stats['hardfail']}")

    stop.set()
    for t in threads:
        t.join(timeout=30)
    mgr.wait_all_drained(timeout=60)

    with stats_lock:
        total_to = stats["timeout"]
        total_hf = stats["hardfail"]
        total = stats["calls"]
        total_dg = stats["degraded"]

    # 最终断言
    log(f"\n[S] FINAL: calls={total} degraded={total_dg} timeout={total_to} hardfail={total_hf}")
    if total == 0:
        log("[FAIL S] ❌ zero calls handled")
        return 1
    if total_to:
        log(f"[FAIL S] ❌ {total_to} read-barrier timeouts")
        return 1
    if total_hf:
        log(f"[FAIL S] ❌ {total_hf} hardfail")
        return 1
    log("[S] ✅ STABILITY PASSED (0 timeout, 0 hardfail)")
    return 0


# ────────────────────────────────────────────────────────────────────
# CLI + main
# ────────────────────────────────────────────────────────────────────
SCENARIOS = {
    "interleave": ("A", lambda args: scenario_interleave(args.workers, args.seconds)),
    "init":       ("B", lambda args: scenario_init(args.workers)),
    "mixed":      ("C", lambda args: scenario_mixed(args.workers, args.seconds)),
    "cycle":      ("D", lambda args: scenario_cycle(args.workers)),
}


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="demo_hotreload.py",
        description="Hot-reload plugin stress suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--only", choices=list(SCENARIOS.keys()),
                   help="只跑一个场景 (默认全部)")
    p.add_argument("--workers", type=int, default=None,
                   help="业务并发线程数 (interleave/mixed 默认 8/16/12/8)")
    p.add_argument("--seconds", type=float, default=None,
                   help="压测时长 (interleave默认16s, mixed默认25s)")
    p.add_argument("--stability", type=float, default=None, metavar="SECONDS",
                   help="启用长稳模式，指定运行秒数 (e.g. 120, 300)")
    p.add_argument("--stability-tick", type=float, default=10.0, metavar="SECONDS",
                   help="长稳模式滚动汇总间隔 (默认10s)")
    p.add_argument("--no-export", action="store_true",
                   help="不导出 timeline JSON/CSV")
    return p.parse_args(argv)


def apply_defaults(args: argparse.Namespace, scenario: str) -> argparse.Namespace:
    """根据场景填充默认 workers/seconds."""
    if args.workers is None:
        defaults_w = {"interleave": 8, "init": 12, "mixed": 16, "cycle": 8}
        args.workers = defaults_w.get(scenario, 8)
    if args.seconds is None:
        defaults_s = {"interleave": 16.0, "init": 3.0, "mixed": 25.0, "cycle": 6.0}
        args.seconds = defaults_s.get(scenario, 10.0)
    return args


def run_all(args: argparse.Namespace) -> int:
    export = not args.no_export

    if args.stability:
        return scenario_stability(args.stability,
                                  args.workers if args.workers else 16,
                                  args.stability_tick)

    todo = list(SCENARIOS.keys()) if args.only is None else [args.only]

    overall_rc = 0
    for name in todo:
        a = apply_defaults(argparse.Namespace(**vars(args)), name)
        tag, fn = SCENARIOS[name]
        rc, calls, ctrls = fn(a)
        if rc and overall_rc == 0:
            overall_rc = rc
        if export:
            tl = build_timeline(calls, ctrls)
            export_timeline(name, tl, True)
            sample = tl[:8] + (["..."] if len(tl) > 16 else []) + tl[-8:]
            log(f"[{tag}] timeline sample ({len(tl)} entries total):")
            for e in sample:
                if isinstance(e, str):
                    log(f"         ... ({len(tl) - 16} more entries)")
                    continue
                icon = "⏺  CTRL" if e["kind"] == "CTRL" else "▶ CALL"
                log(f"         {e['ts_ms']:8.1f}ms | {icon:8s} | "
                    f"{e['id']:>8s} | {e['outcome']:8s} | {e['detail'][:40]}")

    if overall_rc == 0:
        sep("ENTIRE SUITE PASSED ✅")
    else:
        sep(f"SUITE FAILED with rc={overall_rc} ❌")
    return overall_rc


if __name__ == "__main__":
    sys.exit(run_all(parse_args(sys.argv[1:])))
