import time

VERSION = "1.0"

def on_load():
    print(f"[greeter v{VERSION}] loaded")

def on_unload():
    print(f"[greeter v{VERSION}] unloaded")

def greet(name: str) -> str:
    time.sleep(0.3)
    return f"Hello, {name}! (from greeter v{VERSION})"

def slow_greet(name: str) -> str:
    time.sleep(2.0)
    return f"Slow hello, {name}! (from greeter v{VERSION})"
