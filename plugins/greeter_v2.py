import time

VERSION = "2.0"

def on_load():
    print(f"[greeter v{VERSION}] loaded")

def on_unload():
    print(f"[greeter v{VERSION}] unloaded")

def greet(name: str) -> str:
    time.sleep(0.2)
    return f"Hey, {name}! Welcome back! (from greeter v{VERSION})"

def slow_greet(name: str) -> str:
    time.sleep(2.0)
    return f"Slow hey, {name}! Welcome back! (from greeter v{VERSION})"
