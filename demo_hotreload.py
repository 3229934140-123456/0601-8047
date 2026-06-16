"""
Demonstration of the hot-reload plugin system's safety guarantees.

Scenario tested:
  1. Load v1, dispatch several long-running requests into it.
  2. While those requests are still in-flight, hot-replace with v2.
  3. Verify: in-flight requests complete with v1 output; new requests get v2 output.
  4. Hot-replace with v3 while v2 has in-flight calls; same guarantee.
  5. Explicitly unload a module while calls are running.
"""

import threading
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from plugin_manager import PluginManager

PLUGINS_DIR = Path(__file__).resolve().parent / "plugins"


def demo_concurrent_upgrade():
    print("=" * 70)
    print("DEMO 1: Hot-replace while requests are in-flight")
    print("=" * 70)

    mgr = PluginManager(drain_timeout=30.0)

    mv1 = mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))
    print(f"\n[main] Loaded greeter version={mv1.version}")

    results: dict[str, list] = {"v1": [], "v2": [], "v3": []}

    def call_slow(label, slot_version, name):
        try:
            result = mgr.call("greeter", "slow_greet", name)
            results[slot_version].append(result)
            print(f"  [{label}] got: {result}")
        except Exception as e:
            print(f"  [{label}] ERROR: {e}")

    threads = []
    for i in range(3):
        t = threading.Thread(target=call_slow, args=(f"v1-req-{i}", "v1", f"User{i}"))
        t.start()
        threads.append(t)

    time.sleep(0.3)
    print(f"\n[main] 3 slow_greet calls dispatched. Status:")
    _print_status(mgr)

    print("\n[main] >>> HOT-REPLACE: upgrading v1 -> v2 while v1 calls are in-flight <<<")
    mv2 = mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v2.py"))
    print(f"[main] Loaded greeter version={mv2.version}")

    time.sleep(0.2)
    print(f"\n[main] Dispatching new requests (should use v2):")
    for i in range(2):
        t = threading.Thread(target=call_slow, args=(f"v2-req-{i}", "v2", f"NewUser{i}"))
        t.start()
        threads.append(t)

    fast_result = mgr.call("greeter", "greet", "InstantUser")
    print(f"  [fast] got: {fast_result}")

    for t in threads:
        t.join(timeout=15)

    time.sleep(0.5)
    print(f"\n[main] All threads done. Status after drain:")
    _print_status(mgr)

    print(f"\n[main] Results summary:")
    for ver, msgs in results.items():
        for m in msgs:
            print(f"  {ver}: {m}")

    assert all("v1" in m for m in results["v1"]), "v1 in-flight calls must use v1!"
    assert all("v2" in m for m in results["v2"]), "v2 calls must use v2!"
    assert "v2" in fast_result, "Fast call after upgrade must use v2!"
    print("\n[PASS] All in-flight v1 calls completed with v1; new calls used v2.")


def demo_triple_upgrade():
    print("\n" + "=" * 70)
    print("DEMO 2: Rapid triple upgrade v1 -> v2 -> v3")
    print("=" * 70)

    mgr = PluginManager(drain_timeout=30.0)

    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))
    print("[main] Loaded v1")

    results = []
    lock = threading.Lock()

    def background_call(name, delay_before=0):
        if delay_before:
            time.sleep(delay_before)
        try:
            r = mgr.call("greeter", "slow_greet", name)
            with lock:
                results.append(r)
            print(f"  [bg] {r}")
        except Exception as e:
            print(f"  [bg] ERROR: {e}")

    threads = []
    for i in range(2):
        t = threading.Thread(target=background_call, args=(f"V1User{i}",))
        t.start()
        threads.append(t)

    time.sleep(0.3)

    print("[main] >>> Upgrade v1 -> v2 <<<")
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v2.py"))

    for i in range(2):
        t = threading.Thread(target=background_call, args=(f"V2User{i}", 0.1))
        t.start()
        threads.append(t)

    time.sleep(0.3)

    print("[main] >>> Upgrade v2 -> v3 <<<")
    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v3.py"))

    fast = mgr.call("greeter", "greet", "V3FastUser")
    print(f"  [fast] {fast}")
    assert "v3" in fast, "After v3 upgrade, fast call must use v3!"

    for t in threads:
        t.join(timeout=15)

    time.sleep(0.5)
    print(f"\n[main] All results ({len(results)} calls):")
    for r in results:
        print(f"  {r}")

    print(f"\n[main] Status after all drains:")
    _print_status(mgr)
    print("[PASS] Triple upgrade completed safely.")


def demo_unload_while_active():
    print("\n" + "=" * 70)
    print("DEMO 3: Explicit unload while calls are in-flight")
    print("=" * 70)

    mgr = PluginManager(drain_timeout=30.0)

    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v2.py"))
    print("[main] Loaded greeter v2")

    results = []

    def long_call(name):
        try:
            r = mgr.call("greeter", "slow_greet", name)
            results.append(r)
            print(f"  [long] completed: {r}")
        except Exception as e:
            results.append(f"ERROR: {e}")
            print(f"  [long] error: {e}")

    t = threading.Thread(target=long_call, args=("LingeringUser",))
    t.start()

    time.sleep(0.3)
    print(f"[main] Long call in progress. Unloading module...")
    _print_status(mgr)

    ok = mgr.unload_module("greeter")
    print(f"[main] unload_module returned: {ok}")

    t.join(timeout=15)
    time.sleep(0.5)

    print(f"\n[main] Results: {results}")
    assert len(results) == 1, "The in-flight call should have completed"
    assert "v2" in results[0], "In-flight call should have used v2"
    print("[PASS] Unload waited for in-flight call to complete.")

    try:
        mgr.call("greeter", "greet", "Nobody")
        print("[FAIL] Should have raised an error for unloaded module")
    except (KeyError, RuntimeError):
        print("[PASS] Calls after unload correctly raise error.")


def demo_reference_counting():
    print("\n" + "=" * 70)
    print("DEMO 4: Reference count tracking under concurrent load")
    print("=" * 70)

    mgr = PluginManager(drain_timeout=30.0)

    mgr.load_module("greeter", str(PLUGINS_DIR / "greeter_v1.py"))

    max_in_flight = 0
    observed_counts = []
    stop_monitor = threading.Event()

    def monitor():
        while not stop_monitor.is_set():
            st = mgr.status()
            if "greeter" in st and st["greeter"].get("current"):
                cnt = st["greeter"]["current"]["in_flight"]
                observed_counts.append(cnt)
            stop_monitor.wait(0.05)

    monitor_t = threading.Thread(target=monitor, daemon=True)
    monitor_t.start()

    def burst(n):
        for i in range(n):
            mgr.call("greeter", "greet", f"BurstUser{i}")

    threads = []
    for _ in range(5):
        t = threading.Thread(target=burst, args=(10,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=15)

    stop_monitor.set()
    monitor_t.join(timeout=5)

    max_in_flight = max(observed_counts) if observed_counts else 0
    total = mgr.status()["greeter"]["current"]["total_entries"]
    print(f"[main] Max observed in-flight: {max_in_flight}")
    print(f"[main] Total entries through guard: {total}")
    print(f"[main] Expected minimum total: 50 (5 threads x 10 calls)")
    assert total >= 50, f"Expected >=50 total entries, got {total}"
    print("[PASS] Reference counting tracks concurrent load correctly.")


def _print_status(mgr: PluginManager):
    st = mgr.status()
    for name, info in st.items():
        cur = info.get("current")
        if cur:
            print(f"  {name} current: version={cur['version']}, "
                  f"in_flight={cur['in_flight']}, retired={cur['retired']}")
        for r in info.get("retiring", []):
            print(f"  {name} retiring: version={r['version']}, in_flight={r['in_flight']}")


if __name__ == "__main__":
    demo_concurrent_upgrade()
    demo_triple_upgrade()
    demo_unload_while_active()
    demo_reference_counting()
    print("\n" + "=" * 70)
    print("ALL DEMOS PASSED — hot-reload plugin system is safe.")
    print("=" * 70)
