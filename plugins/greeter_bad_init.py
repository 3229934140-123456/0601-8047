import time

VERSION = "bad-init"


def on_load():
    time.sleep(0.5)
    raise RuntimeError("Intentional on_load failure for testing")


def greet(name: str) -> str:
    return f"You should never see this, {name}!"
