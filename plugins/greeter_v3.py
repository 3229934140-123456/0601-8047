import time

VERSION = "3.0"

def on_load():
    print(f"[greeter v{VERSION}] loaded")

def on_unload():
    print(f"[greeter v{VERSION}] unloaded")

def greet(name: str) -> str:
    return f"Hiya, {name}! Long time no see! (from greeter v{VERSION})"

def slow_greet(name: str) -> str:
    time.sleep(1.5)
    return f"Slow hiya, {name}! Long time no see! (from greeter v{VERSION})"
