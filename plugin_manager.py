"""
Hot-reload plugin system with safe concurrent module lifecycle management.

Key guarantees:
  1. A module is never unloaded while in-flight requests are still executing inside it.
  2. Reference counting + read-write barrier tracks in-flight call count per module version.
  3. On upgrade, new requests route to the new version immediately; the old version enters
     a "retiring" state and is only unloaded after its last in-flight call completes.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional


class ModuleGuard:
    """
    Per-module-version guard that implements a read-write barrier + reference count.

    - Multiple callers (readers) can enter concurrently via ``acquire_read()``.
    - An exclusive ``acquire_write()`` blocks until all readers have left, then
      holds the lock so no new readers can enter.
    - ``retired`` flag: once set, new readers are rejected (they should be
      routed to the newer version), but existing readers finish normally.
    - ``ready`` flag: NOT set during on_load initialization; while not ready,
      new readers are immediately rejected (module not available yet).
    """

    def __init__(self, module_name: str, version: str) -> None:
        self.module_name = module_name
        self.version = version
        self._lock = threading.Lock()
        self._readers_zero = threading.Condition(self._lock)
        self._reader_count: int = 0
        self._writer_active: bool = False
        self._retired: bool = False
        self._ready: bool = False
        self._total_entries: int = 0

    @property
    def reader_count(self) -> int:
        with self._lock:
            return self._reader_count

    @property
    def retired(self) -> bool:
        with self._lock:
            return self._retired

    @property
    def ready(self) -> bool:
        with self._lock:
            return self._ready

    @property
    def total_entries(self) -> int:
        with self._lock:
            return self._total_entries

    def acquire_read(self, timeout: float = 30.0) -> bool:
        with self._lock:
            if self._retired or not self._ready:
                return False
            deadline = time.monotonic() + timeout
            while self._writer_active:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                if not self._readers_zero.wait(remaining):
                    return False
                if self._retired or not self._ready:
                    return False
            self._reader_count += 1
            self._total_entries += 1
            return True

    def release_read(self) -> None:
        with self._lock:
            self._reader_count -= 1
            if self._reader_count == 0:
                self._readers_zero.notify_all()

    def acquire_write(self, timeout: float = 300.0) -> bool:
        with self._lock:
            deadline = time.monotonic() + timeout
            while self._writer_active or self._reader_count > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                if not self._readers_zero.wait(remaining):
                    return False
            self._writer_active = True
            return True

    def release_write(self) -> None:
        with self._lock:
            self._writer_active = False
            self._readers_zero.notify_all()

    def mark_retired(self) -> None:
        with self._lock:
            self._retired = True

    def mark_ready(self) -> None:
        with self._lock:
            self._ready = True
            self._readers_zero.notify_all()

    def wait_readers_drain(self, timeout: float = 300.0) -> bool:
        with self._lock:
            deadline = time.monotonic() + timeout
            while self._reader_count > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                if not self._readers_zero.wait(remaining):
                    return False
            return True

    @contextmanager
    def read_guard(self, timeout: float = 30.0) -> Generator[bool, None, None]:
        acquired = self.acquire_read(timeout)
        try:
            yield acquired
        finally:
            if acquired:
                self.release_read()

    @contextmanager
    def write_guard(self, timeout: float = 300.0) -> Generator[bool, None, None]:
        acquired = self.acquire_write(timeout)
        try:
            yield acquired
        finally:
            if acquired:
                self.release_write()


@dataclass
class ModuleVersion:
    version: str
    module: Any
    guard: ModuleGuard
    load_time: float
    source_path: Optional[str] = None

    @property
    def is_retired(self) -> bool:
        return self.guard.retired

    @property
    def is_ready(self) -> bool:
        return self.guard.ready

    @property
    def in_flight(self) -> int:
        return self.guard.reader_count


class PluginSlot:
    """
    A named plugin slot that holds the current active version and a list of
    retiring versions that are waiting for in-flight calls to drain.

    The ``_op_mutex`` serialises top-level operations (load/unload/replace)
    so that a concurrent ``unload`` cannot tear down the version that a
    running ``load_module`` has just installed.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._lock = threading.Lock()
        self._op_mutex = threading.RLock()
        self._current: Optional[ModuleVersion] = None
        self._retiring: List[ModuleVersion] = []

    @property
    def op_mutex(self) -> threading.RLock:
        return self._op_mutex

    @property
    def current(self) -> Optional[ModuleVersion]:
        with self._lock:
            return self._current

    @property
    def retiring_versions(self) -> List[ModuleVersion]:
        with self._lock:
            return list(self._retiring)

    def set_current(self, mv: ModuleVersion) -> Optional[ModuleVersion]:
        old: Optional[ModuleVersion] = None
        with self._lock:
            if self._current is not None:
                old = self._current
                self._retiring.append(old)
            self._current = mv
        return old

    def clear_current(self) -> Optional[ModuleVersion]:
        old: Optional[ModuleVersion] = None
        with self._lock:
            old = self._current
            self._current = None
            if old is not None:
                self._retiring.append(old)
        return old

    def remove_retired(self, mv: ModuleVersion) -> None:
        with self._lock:
            self._retiring = [r for r in self._retiring if r is not mv]

    def acquire_read(self, timeout: float = 30.0) -> Optional[ModuleVersion]:
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            with self._lock:
                target = self._current
            if target is None:
                return None
            if target.guard.acquire_read(max(0.0, deadline - time.monotonic())):
                with self._lock:
                    if target is self._current:
                        return target
                target.guard.release_read()
            attempt += 1
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            if attempt % 3 == 0:
                time.sleep(min(0.005, remaining))

    def release_read(self, mv: ModuleVersion) -> None:
        mv.guard.release_read()


class PluginManager:
    """
    Central manager for hot-reloadable plugins.

    Lifecycle for a safe upgrade:
      1. Load new module version → create ModuleVersion with fresh guard.
      2. Atomically swap the slot's current pointer to the new version.
      3. Mark the old version's guard as ``retired`` — new readers rejected.
      4. Wait for the old version's reader count to drain to zero.
      5. Acquire write lock on old guard (guaranteed — no readers left).
      6. Call the module's ``on_unload`` hook if defined.
      7. Remove from ``sys.modules``; release write lock; drop reference.
    """

    def __init__(self, drain_timeout: float = 300.0, read_timeout: float = 30.0) -> None:
        self._slots: Dict[str, PluginSlot] = {}
        self._lock = threading.Lock()
        self._drain_timeout = drain_timeout
        self._read_timeout = read_timeout
        self._version_counter: int = 0

    def _next_version(self) -> str:
        self._version_counter += 1
        return f"v{self._version_counter}"

    def _get_or_create_slot(self, name: str) -> PluginSlot:
        with self._lock:
            if name not in self._slots:
                self._slots[name] = PluginSlot(name)
            return self._slots[name]

    def load_module(self, name: str, source_path: str, version: Optional[str] = None) -> ModuleVersion:
        slot = self._get_or_create_slot(name)
        abs_path = str(Path(source_path).resolve())
        ver = version or self._next_version()
        unique_key = f"_hotplug_{name}_{ver}_{id(abs_path)}_{time.monotonic_ns()}"

        spec = importlib.util.spec_from_file_location(unique_key, abs_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot create module spec from {abs_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[unique_key] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(unique_key, None)
            raise

        guard = ModuleGuard(name, ver)
        mv = ModuleVersion(
            version=ver,
            module=module,
            guard=guard,
            load_time=time.time(),
            source_path=abs_path,
        )

        init_error = None
        if hasattr(module, "on_load"):
            try:
                module.on_load()
            except Exception as e:
                init_error = e

        if init_error is not None:
            keys_to_remove = [
                k for k in list(sys.modules)
                if k.startswith(f"_hotplug_{name}_{ver}_")
            ]
            for k in keys_to_remove:
                sys.modules.pop(k, None)
            raise RuntimeError(
                f"Plugin '{name}' version {ver} on_load failed: {init_error}"
            ) from init_error

        guard.mark_ready()

        with slot.op_mutex:
            old = slot.set_current(mv)
            if old is not None:
                old.guard.mark_retired()
                self._drain_and_unload(old, slot)

        return mv

    def unload_module(self, name: str) -> bool:
        with self._lock:
            slot = self._slots.get(name)
        if slot is None:
            return False
        with slot.op_mutex:
            current = slot.clear_current()
            if current is None:
                return False
            current.guard.mark_retired()
            self._drain_and_unload(current, slot)
            return True

    def _drain_and_unload(self, mv: ModuleVersion, slot: PluginSlot) -> None:
        def _worker() -> None:
            drained = mv.guard.wait_readers_drain(self._drain_timeout)
            if not drained:
                return
            with mv.guard.write_guard(timeout=10.0) as ok:
                if not ok:
                    return
                if hasattr(mv.module, "on_unload"):
                    try:
                        mv.module.on_unload()
                    except Exception:
                        pass
                keys_to_remove = [
                    k for k in sys.modules
                    if k.startswith(f"_hotplug_{mv.guard.module_name}_{mv.version}_")
                ]
                for k in keys_to_remove:
                    sys.modules.pop(k, None)
            slot.remove_retired(mv)

        t = threading.Thread(target=_worker, daemon=True, name=f"drain-{mv.guard.module_name}-{mv.version}")
        t.start()

    @contextmanager
    def use(self, name: str) -> Generator[Any, None, None]:
        with self._lock:
            slot = self._slots.get(name)
        if slot is None:
            raise KeyError(f"Plugin '{name}' not found")
        mv = slot.acquire_read(self._read_timeout)
        if mv is None:
            raise RuntimeError(f"Plugin '{name}' is not available (retired or unavailable)")
        try:
            yield mv.module
        finally:
            slot.release_read(mv)

    def call(self, name: str, func_name: str, *args: Any, **kwargs: Any) -> Any:
        with self.use(name) as module:
            fn = getattr(module, func_name, None)
            if fn is None:
                raise AttributeError(f"Module '{name}' has no function '{func_name}'")
            return fn(*args, **kwargs)

    def status(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        with self._lock:
            slot_names = list(self._slots.keys())
        for name in slot_names:
            with self._lock:
                slot = self._slots.get(name)
            if slot is None:
                continue
            cur = slot.current
            entry: Dict[str, Any] = {}
            if cur:
                entry["current"] = {
                    "version": cur.version,
                    "in_flight": cur.in_flight,
                    "total_entries": cur.guard.total_entries,
                    "retired": cur.is_retired,
                    "ready": cur.is_ready,
                    "source": cur.source_path,
                }
            retiring = []
            for r in slot.retiring_versions:
                retiring.append({
                    "version": r.version,
                    "in_flight": r.in_flight,
                    "total_entries": r.guard.total_entries,
                    "ready": r.is_ready,
                    "source": r.source_path,
                })
            entry["retiring"] = retiring
            result[name] = entry
        return result

    def wait_all_drained(self, timeout: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout
        with self._lock:
            slots = list(self._slots.values())
        for slot in slots:
            for r in slot.retiring_versions:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                if not r.guard.wait_readers_drain(remaining):
                    return False
        return True
