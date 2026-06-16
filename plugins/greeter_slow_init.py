import time
import threading

VERSION = "slow-init"
INIT_CALLS = []


def on_load():
    INIT_CALLS.append(time.time())
    for i in range(5):
        INIT_CALLS.append(f"step{i}")
        time.sleep(0.2)
    INIT_CALLS.append("on_load_complete")


def on_unload():
    pass


def greet(name: str) -> str:
    if "on_load_complete" not in INIT_CALLS:
        return f"!!ERROR: greet called BEFORE init finished! INIT_CALLS={INIT_CALLS}"
    time.sleep(0.2)
    return f"Hello, {name}! (from slow-init plugin)"


def slow_greet(name: str) -> str:
    if "on_load_complete" not in INIT_CALLS:
        return f"!!ERROR: slow_greet called BEFORE init finished! INIT_CALLS={INIT_CALLS}"
    time.sleep(2.0)
    return f"Slow hello, {name}! (from slow-init plugin)"
