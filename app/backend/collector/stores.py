"""Per-user data: every user (browser client id) has their own StateStore -
records, stats, custom categories, dedup registry, crawl memory, credit
ledger, checkpoint file and Excel files - in config.client_dir(client).
One user never sees, counts against or deduplicates against another
user's records.

Shared across users: only the Serper response cache and in-flight request
dedup (SharedSearch), so the same search is never paid for twice.

Stores are loaded on first use and unloaded (checkpoint written first)
after config.STORE_IDLE_S without a request or a running job.

With a database configured, each user's folder is first restored from it
and kept mirrored to it (backend/persist.py), so a wiped disk loses
nothing.
"""

from __future__ import annotations

import itertools
import logging
import os
import threading
import time

from .. import config, persist
from .engine import SharedSearch, StateStore

log = logging.getLogger("stores")

STATE_FILE = "state.json"


class StoreRegistry:
    def __init__(self):
        self.lock = threading.Lock()
        self.stores: dict[str, StateStore] = {}
        self.last_used: dict[str, float] = {}
        self._gens = itertools.count(1)
        self.shared = SharedSearch()
        self.shared.load(self.cache_path)

    @property
    def cache_path(self) -> str:
        return os.path.join(config.OUTPUT_DIR, "search_cache.json")

    def get(self, client: str) -> StateStore:
        with self.lock:
            st = self.stores.get(client)
            if st is not None:
                self.last_used[client] = time.time()
                return st
        # outside the lock: may wait for the database (raises
        # persist.Unavailable rather than load an empty folder)
        persist.restore(client)
        with self.lock:
            self.last_used[client] = time.time()
            st = self.stores.get(client)
            if st is None:
                d = config.client_dir(client)
                path = os.path.join(d, STATE_FILE)
                st = StateStore.load(path)
                st._path, st._out_dir = path, d
                st.share(self.shared)
                # generation: a reloaded / reset store restarts its revisions
                st.gen = next(self._gens)
                self.stores[client] = st
            return st

    def loaded(self) -> list[tuple[str, StateStore]]:
        with self.lock:
            return list(self.stores.items())

    def replace(self, client: str) -> StateStore:
        """Reset: drop the user's store and start an empty one."""
        with self.lock:
            old = self.stores.pop(client, None)
        if old is not None:          # discard it: no pending save may resurrect it
            with old.lock:
                old._closed = True
                old._dirty = False
                old._dirty_exports = set()
            old._saver_wake.set()
        try:
            os.remove(os.path.join(config.client_dir(client), STATE_FILE))
        except OSError:
            pass
        return self.get(client)

    def evict_idle(self, busy: set[str]):
        """Unload stores idle for STORE_IDLE_S (not `busy` = running jobs)."""
        now = time.time()
        unloaded = []
        with self.lock:
            for c in [c for c, t in self.last_used.items()
                      if c not in busy and now - t > config.STORE_IDLE_S]:
                st = self.stores.pop(c, None)
                del self.last_used[c]
                if st is not None:
                    try:
                        st.close()
                    except Exception:
                        log.exception("saving unloaded user store")
                    unloaded.append(c)
        for c in unloaded:               # the database, outside the lock
            persist.sync(c)
        self.save_cache()

    def save_cache(self):
        try:
            self.shared.save(self.cache_path)
        except OSError as exc:
            log.warning("search cache not saved: %s", exc)

    def flush_all(self):
        for _, st in self.loaded():
            try:
                st.flush()
            except Exception:
                log.exception("final checkpoint of a user store")
        persist.sync_all()
        self.save_cache()
