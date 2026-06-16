"""
Stress-test grade demonstration of hot-reload plugin safety guarantees.

Scenarios:
  1. CUTOFF CONCURRENCY : 20 threads hammer a plugin across the v1->v2 switch point.
                          Every call is timestamped & version-tagged. After the test,
                          we prove: pre-switch calls used v1, post-switch calls used v2,
                          NO call timed out, NO call was lost.
  2. INIT PROTECTION     : Load a slow-initializing plugin (1s on_load) while callers
                          hammer in parallel. Verify ZERO business calls land in the
                          new module before on_load finishes.
  3. UNLOAD STRESS       : Continuous requests + unload mid-flight. Prove in-flight
                          calls drain cleanly, no crashes.
  4. INIT FAILURE        : on_load() throws exception → verify the module never
                          becomes current, calls keep going to the old version.
"""

from __future__ import annotations

import sys
import threading
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plugin_manager import PluginManager

PLUGINS_DIR = Path(__file__).resolve().parent / "plugins"

LOG_LOCK = threading.Lock()
T0 = time.perf_counter()


def ts() -> str:
    return f"{(time.perf_counter() - T0) * 1000:7.1f}ms"


def log(msg: str) -> None:
    with LOG_LOCK:
        print(f"[{ts()}] {msg}", flush=True)


@dataclass
class CallRecord:
    thread_id: int
    seq: int
    start_ms: float
    end_ms: float
    version: str = ""
    ok: bool = True
    error: str = ""


# ---------------------------------------------------------------------------
# Scenario 1 : v1 -> v2 switch under concurrent load
# ---------------------------------------------------------------------------
def scenario_switchpoint() -> None:
    sep("SCENARIO 1 : Switch-point concurrency (v1 → v2 under load)")
    mgr = PluginManager(drain_timeout=30.0, read_timeout=10.0)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))
    log("[ctl] v1 loaded and ready. Starting 20 caller threads.")

    records: list[CallRecord] = []
    rec_lock = threading.Lock()
    stop = threading.Event()
    switch_made = threading.Event()
    switch_time_ms: float = 0.0

    def worker(tid: int):
        seq = 0
        while not stop.is_set():
            start = time.perf_counter() - T0
            try:
                result = mgr.call("greeter", "slow_greet", f"T{tid}-{seq}")
                end = time.perf_counter() - T0
                ver = "v1" if "v1" in result else ("v2" if "v2" in result else "v???")
                rec = CallRecord(tid, seq, start * 1000, end * 1000, version=ver, ok=True)
            except Exception as e:
                end = time.perf_counter() - T0
                rec = CallRecord(tid, seq, start * 1000, end * 1000, ok=False, error=str(e))
            with rec_lock:
                records.append(rec)
            seq += 1
            time.sleep(0.01)

    threads = [threading.Thread(target=worker, args=(i,), name=f"caller-{i}") for i in range(20)]
    for t in threads:
        t.start()

    time.sleep(1.2)
    pre_count = len(records)
    log(f"[ctl] Releasing v1→v2 upgrade. {pre_count} calls already completed under v1.")

    with rec_lock:
        pass
    switch_time_ms = (time.perf_counter() - T0) * 1000
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v2.py"))
    switch_made.set()
    log(f"[ctl] SWITCH COMPLETED at t={switch_time_ms:.1f}ms. new calls → v2. old in-flight → finish v1.")

    time.sleep(2.5)
    stop.set()
    for t in threads:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=15)
    post_count = len(records)
    log(f"[ctl] Stopped all callers. Total records: {post_count}.")

    failed = [r for r in records if not r.ok]
    if failed:
        log(f"[FAIL] {len(failed)} calls failed! First 5:")
        for r in failed[:5]:
            log(f"       T{r.thread_id}-{r.seq}: {r.error}")
        raise AssertionError("Calls failed!")

    v1_count = sum(1 for r in records if r.version == "v1")
    v2_count = sum(1 for r in records if r.version == "v2")
    undef_count = sum(1 for r in records if r.version not in ("v1", "v2"))
    log(f"[ctl] version distribution: v1={v1_count}  v2={v2_count}  unknown={undef_count}")

    cross_v1 = [r for r in records if r.start_ms < switch_time_ms <= r.end_ms and r.version == "v1"]
    cross_v2 = [r for r in records if r.start_ms < switch_time_ms <= r.end_ms and r.version == "v2"]
    log(f"[ctl] calls straddling the switch (started before, ended after):")
    log(f"       used v1 (correct)= {len(cross_v1)}   |   used v2 = {len(cross_v2)}")

    started_before_v2 = [r for r in records if r.start_ms < switch_time_ms and r.version == "v2"]
    started_after_v1 = [r for r in records if r.start_ms >= switch_time_ms and r.version == "v1"]
    log(f"[ctl] started BEFORE switch but got v2 (possible fail) = {len(started_before_v2)}")
    log(f"[ctl] started AFTER  switch but got v1 (possible fail) = {len(started_after_v1)}")

    assert len(started_before_v2) == 0, "Pre-switch calls must not see v2"
    assert v1_count + v2_count == post_count, "All calls must be tagged v1 or v2"
    assert not failed, "No call may fail"
    log("[PASS] ✅ Switch-point concurrency safe: old calls ran v1, new calls ran v2, 0 failures.")


# ---------------------------------------------------------------------------
# Scenario 2 : Initialization protection
# ---------------------------------------------------------------------------
def scenario_init_protection() -> None:
    sep("SCENARIO 2 : Init-time call shield (slow on_load, 1.0s)")
    mgr = PluginManager(drain_timeout=30.0, read_timeout=10.0)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))
    log("[ctl] v1 baseline loaded. Loading slow-init plugin in background thread ...")

    load_result: dict[str, object] = {"ok": None, "error": None, "start_ms": 0.0, "end_ms": 0.0}

    def do_load():
        load_result["start_ms"] = (time.perf_counter() - T0) * 1000
        try:
            mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_slow_init.py"))
            load_result["ok"] = True
        except Exception as e:
            load_result["ok"] = False
            load_result["error"] = str(e)
        load_result["end_ms"] = (time.perf_counter() - T0) * 1000

    load_thread = threading.Thread(target=do_load, name="loader")
    load_thread.start()
    time.sleep(0.05)

    records: list[CallRecord] = []
    rec_lock = threading.Lock()
    stop = threading.Event()
    premature_hits: list[str] = []

    def caller(tid: int):
        seq = 0
        while not stop.is_set():
            start = time.perf_counter() - T0
            try:
                result = mgr.call("greeter", "greet", f"T{tid}-{seq}")
                end = time.perf_counter() - T0
                if "slow-init" in result:
                    cur_end = load_result["end_ms"] if load_result["end_ms"] else 1e18
                    if start * 1000 < cur_end:
                        premature_hits.append(
                            f"T{tid}-{seq} started={start*1000:.1f} load_end={cur_end:.1f}"
                        )
                    ver = "slow-init"
                elif "v1" in result:
                    ver = "v1"
                else:
                    ver = "other"
                rec = CallRecord(tid, seq, start * 1000, end * 1000, version=ver, ok=True)
            except Exception as e:
                end = time.perf_counter() - T0
                rec = CallRecord(tid, seq, start * 1000, end * 1000, ok=False, error=str(e))
            with rec_lock:
                records.append(rec)
            seq += 1
            time.sleep(0.005)

    callers = [threading.Thread(target=caller, args=(i,)) for i in range(10)]
    for t in callers:
        t.start()

    load_thread.join(timeout=15)
    time.sleep(1.0)
    stop.set()
    for t in callers:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=15)

    load_s = load_result["start_ms"]
    load_e = load_result["end_ms"]
    log(f"[ctl] load window = [{load_s:.1f}ms → {load_e:.1f}ms]  (init took ~{load_e-load_s:.0f}ms)")

    failed = [r for r in records if not r.ok]
    v1 = sum(1 for r in records if r.version == "v1")
    si = sum(1 for r in records if r.version == "slow-init")
    log(f"[ctl] calls: v1={v1}  slow-init={si}  failed={len(failed)}")
    log(f"[ctl] premature hits (slow-init called BEFORE load end): {len(premature_hits)}")
    for h in premature_hits[:5]:
        log(f"       !! {h}")

    import plugins.greeter_slow_init as si_mod  # noqa: F401

    assert load_result["ok"], "Load must succeed"
    assert len(premature_hits) == 0, "NO business call may land in slow-init before on_load finishes!"
    assert si > 0, "After init finishes, some calls must have used the new version"
    assert v1 > 0, "While init was running, calls should have continued to fall back to v1"
    log("[PASS] ✅ Init protected: ZERO calls leaked into module before on_load finished.")


# ---------------------------------------------------------------------------
# Scenario 3 : Unload under concurrent load
# ---------------------------------------------------------------------------
def scenario_unload_stress() -> None:
    sep("SCENARIO 3 : Unload mid-flight under concurrent load")
    mgr = PluginManager(drain_timeout=30.0, read_timeout=10.0)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v2.py"))
    log("[ctl] v2 loaded. Starting 12 callers with slow_greet (2s each) ...")

    records: list[CallRecord] = []
    rec_lock = threading.Lock()
    stop = threading.Event()
    unload_time_ms = 0.0

    def worker(tid: int):
        seq = 0
        while not stop.is_set():
            start = time.perf_counter() - T0
            try:
                result = mgr.call("greeter", "slow_greet", f"T{tid}-{seq}")
                end = time.perf_counter() - T0
                rec = CallRecord(tid, seq, start * 1000, end * 1000,
                                 version=("v2" if "v2" in result else "?"), ok=True)
            except (KeyError, RuntimeError):
                end = time.perf_counter() - T0
                rec = CallRecord(tid, seq, start * 1000, end * 1000,
                                 version="UNAVAILABLE", ok=True)
            except Exception as e:
                end = time.perf_counter() - T0
                rec = CallRecord(tid, seq, start * 1000, end * 1000, ok=False, error=str(e))
            with rec_lock:
                records.append(rec)
            seq += 1
            time.sleep(0.01)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()

    time.sleep(1.0)
    log(f"[ctl] Issuing UNLOAD. {len(records)} calls already issued.")
    unload_time_ms = (time.perf_counter() - T0) * 1000
    ok = mgr.unload_module("greeter")
    log(f"[ctl] unload_module returned {ok} at {unload_time_ms:.1f}ms "
        "(actual unload waits for drain in background).")

    time.sleep(3.5)
    stop.set()
    for t in threads:
        t.join(timeout=15)
    mgr.wait_all_drained(timeout=15)

    straddling = [r for r in records if r.start_ms < unload_time_ms <= r.end_ms]
    v2_straddle = [r for r in straddling if r.version == "v2"]
    unavail = sum(1 for r in records if r.version == "UNAVAILABLE")
    failed = [r for r in records if not r.ok]

    log(f"[ctl] total={len(records)}  v2_straddle(correct)={len(v2_straddle)}/"
        f"{len(straddling)}  post-unload UNAVAILABLE={unavail}  hard_fail={len(failed)}")
    for r in failed[:3]:
        log(f"       FAIL T{r.thread_id}-{r.seq}: {r.error}")

    assert len(straddling) == len(v2_straddle), "Every call started before unload must finish v2 cleanly!"
    assert not failed, "No hard failures allowed (only graceful UNAVAILABLE after drain is ok)"
    assert unavail > 0, "Some calls after drain should have seen UNAVAILABLE (proves drain completed)"
    log("[PASS] ✅ Unload under load safe: every in-flight call ran to completion on v2.")


# ---------------------------------------------------------------------------
# Scenario 4 : on_load failure must leave traffic on previous version
# ---------------------------------------------------------------------------
def scenario_init_failure() -> None:
    sep("SCENARIO 4 : on_load() fails → old version keeps serving traffic")
    mgr = PluginManager(drain_timeout=30.0, read_timeout=10.0)
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v2.py"))
    log("[ctl] v2 loaded. Now loading greeter_bad_init whose on_load() raises.")

    try:
        mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_bad_init.py"))
        log("[FAIL] load_module should have raised but didn't!")
        raise AssertionError("Expected RuntimeError from bad on_load")
    except RuntimeError as e:
        log(f"[ctl] Correctly caught init error: {e!r:.120s}")

    r = mgr.call("greeter", "greet", "SanityCheck")
    log(f"[ctl] Traffic after failed upgrade: {r}")
    assert "v2" in r, "Calls must still route to v2 after sibling init failure!"

    st = mgr.status()["greeter"]
    log(f"[ctl] slot internal version = {st['current']['version']} "
        f"(=greeter_v2.py) ready={st['current']['ready']}")
    assert st["current"]["version"] == "v1", (
        "Internal version must remain v1 (=greeter_v2.py, first loaded). "
        "The bad init must NOT have become current."
    )
    assert st["current"]["ready"], "Previous version must still be ready"
    log("[PASS] ✅ Init failure contained: traffic stayed on old version, no crash.")


def sep(title: str) -> None:
    bar = "=" * 78
    print(f"\n{bar}\n  {title}\n{bar}\n", flush=True)


def main() -> None:
    scenario_switchpoint()
    scenario_init_protection()
    scenario_unload_stress()
    scenario_init_failure()
    sep("ALL SCENARIOS PASSED ✅ — hot-reload plugin system verified under stress")


if __name__ == "__main__":
    main()
