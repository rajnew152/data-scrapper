"""Saved user data: every user's checkpoint (records, stats, custom
categories, dedup registry, credit ledger, ...) and Excel files are copied
to the database, so they survive a restart that wipes the server's disk
(Render's free plan does on every deploy, restart and spin-down).
Enabled with the accounts, when SUPABASE_DB_URL is set.

The local folder (config.client_dir) stays the working copy - the
collector reads and writes it exactly as before. This module mirrors it:

  * restore(client): before a user's data is first loaded in a process,
    every saved file missing locally is downloaded. After a wipe that is
    all of them; while the disk survives, none.
  * sync(client): uploads the files that changed (size / mtime) and drops
    the saved copies of files the user deleted (a deleted custom
    category, a reset checkpoint). The worker's syncer thread runs it for
    every user each PERSIST_SYNC_INTERVAL; category delete, reset, unload
    and shutdown run it at once.

A folder is only synced after it was restored in this process, so a wiped
folder can never delete the saved copy. The checkpoint is stored
zlib-compressed; Excel files are already compressed.

The backend is pluggable like auth's store: tests install MemoryFiles.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import zlib

from . import config, db

log = logging.getLogger("persist")

# what is mirrored: the checkpoint and the Excel files (no temp files)
NAME_RE = re.compile(r"^(state\.json|[\w\-. ]+\.xlsx)$")


class Unavailable(Exception):
    """A user's saved data could not be loaded (database unreachable)."""


# ------------------------------------------------------------------ backends
class PgFiles:
    """Files in the bdc.user_files table (db.py)."""

    def manifest(self, owner: str) -> set[str]:
        with db.connection() as conn:
            rows = conn.execute(f"SELECT name FROM {db.SCHEMA}.user_files"
                                " WHERE owner = %s", (owner,)).fetchall()
        return {r[0] for r in rows}

    def get(self, owner: str, name: str) -> bytes | None:
        with db.connection() as conn:
            row = conn.execute(f"SELECT data FROM {db.SCHEMA}.user_files"
                               " WHERE owner = %s AND name = %s",
                               (owner, name)).fetchone()
        return bytes(row[0]) if row else None

    def put(self, owner: str, name: str, data: bytes, size: int):
        with db.connection() as conn:
            conn.execute(
                f"INSERT INTO {db.SCHEMA}.user_files (owner, name, data, size)"
                " VALUES (%s, %s, %s, %s) ON CONFLICT (owner, name) DO UPDATE"
                " SET data = EXCLUDED.data, size = EXCLUDED.size,"
                " updated_at = now()", (owner, name, data, size))

    def delete(self, owner: str, name: str):
        with db.connection() as conn:
            conn.execute(f"DELETE FROM {db.SCHEMA}.user_files"
                         " WHERE owner = %s AND name = %s", (owner, name))


class MemoryFiles:
    """Same interface, in memory (tests)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.files: dict[tuple[str, str], bytes] = {}

    def manifest(self, owner: str) -> set[str]:
        with self.lock:
            return {n for o, n in self.files if o == owner}

    def get(self, owner: str, name: str) -> bytes | None:
        with self.lock:
            return self.files.get((owner, name))

    def put(self, owner: str, name: str, data: bytes, size: int):
        with self.lock:
            self.files[(owner, name)] = data

    def delete(self, owner: str, name: str):
        with self.lock:
            self.files.pop((owner, name), None)


_backend = None
_backend_lock = threading.Lock()


def enabled() -> bool:
    return _backend is not None or db.enabled()


def use_backend(backend):
    """Install a backend (tests); None = back to the database."""
    global _backend
    with _backend_lock:
        _backend = backend
        _mirrors.clear()


def _get_backend():
    global _backend
    if _backend is None:
        with _backend_lock:
            if _backend is None:
                _backend = PgFiles()
    return _backend


# ------------------------------------------------------------------- mirror
class _Mirror:
    """What the database holds for one user, as of this process's last
    restore / sync."""

    def __init__(self, saved: set[str], sigs: dict[str, tuple[int, int]]):
        self.lock = threading.Lock()
        self.saved = saved                 # names stored in the database
        self.sigs = sigs                   # name -> (size, mtime_ns) uploaded


_mirrors: dict[str, _Mirror] = {}
_restore_locks: dict[str, threading.Lock] = {}
_guard = threading.Lock()


def _sig(path: str) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
    except OSError:
        return None
    return st.st_size, st.st_mtime_ns


def _scan(folder: str) -> dict[str, tuple[int, int] | None]:
    """name -> (size, mtime_ns) of the mirrored files in `folder`; None for
    a file present but not readable right now (being replaced): it is
    neither uploaded nor treated as deleted. Raises if the folder cannot
    be listed - never mistake that for "every file was deleted"."""
    return {name: _sig(os.path.join(folder, name)) for name in os.listdir(folder)
            if NAME_RE.match(name) and ".tmp." not in name}


def _encode(name: str, data: bytes) -> bytes:
    return zlib.compress(data, 6) if name.endswith(".json") else data


def _decode(name: str, data: bytes) -> bytes:
    return zlib.decompress(data) if name.endswith(".json") else data


def _write_atomic(path: str, data: bytes):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.restore.tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, path)


def restore(client: str):
    """Bring back the user's saved files missing on local disk (once per
    process). Raises Unavailable when the database cannot be read - the
    caller must not go on with an empty folder, or new work would later
    replace the saved data."""
    if not enabled() or client in _mirrors:
        return
    with _guard:
        lock = _restore_locks.setdefault(client, threading.Lock())
    with lock:
        if client in _mirrors:
            return
        folder = config.client_dir(client)
        backend = _get_backend()
        sigs: dict[str, tuple[int, int]] = {}
        try:
            saved = {n for n in backend.manifest(client) if NAME_RE.match(n)}
            restored = 0
            for name in saved:
                path = os.path.join(folder, name)
                if os.path.exists(path):
                    continue     # local copy survived: it is the newer one
                data = backend.get(client, name)
                if data is None:
                    continue
                _write_atomic(path, _decode(name, data))
                sigs[name] = _sig(path)
                restored += 1
        except Exception as exc:
            log.exception("restoring saved data failed")
            raise Unavailable(str(exc)) from None
        if restored:
            log.info("restored %d saved file(s) of user %s", restored,
                     os.path.basename(folder))
        _mirrors[client] = _Mirror(saved, sigs)
        with _guard:
            _restore_locks.pop(client, None)


def sync(client: str) -> bool:
    """Upload the user's changed files, drop saved copies of deleted ones.
    False if the database could not be written (retried next time)."""
    m = _mirrors.get(client)
    if m is None:
        return True
    folder = config.client_dir(client)
    backend = _get_backend()
    with m.lock:
        try:
            try:
                local = _scan(folder)
            except FileNotFoundError:
                if m.saved:          # saved files but no folder: not a deletion
                    return True
                local = {}
            for name, sig in local.items():
                if sig is None or (name in m.saved and m.sigs.get(name) == sig):
                    continue
                try:
                    with open(os.path.join(folder, name), "rb") as fh:
                        data = fh.read()
                except FileNotFoundError:     # deleted since the scan
                    continue
                backend.put(client, name, _encode(name, data), len(data))
                m.saved.add(name)
                m.sigs[name] = sig
            for name in m.saved - local.keys():
                backend.delete(client, name)
                m.saved.discard(name)
                m.sigs.pop(name, None)
            return True
        except Exception as exc:
            log.warning("saving user data to the database failed (retried "
                        "in %.0f s): %s", config.PERSIST_SYNC_INTERVAL, exc)
            return False


def sync_all() -> bool:
    ok = True
    for client in list(_mirrors):
        ok = sync(client) and ok
    return ok


def start_syncer(stop: threading.Event) -> threading.Thread | None:
    """Background thread: sync_all() every PERSIST_SYNC_INTERVAL until
    `stop` is set (the caller runs a final sync_all() at shutdown)."""
    if not enabled():
        return None

    def loop():
        while not stop.wait(config.PERSIST_SYNC_INTERVAL):
            try:
                sync_all()
            except Exception:
                log.exception("user data sync failed")

    t = threading.Thread(target=loop, daemon=True, name="persist-sync")
    t.start()
    return t
