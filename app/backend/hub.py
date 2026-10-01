"""API-side connection to the collection worker process.

  * starts the worker (multiprocessing, spawn) and supervises it: if it
    dies it is restarted with backoff and the jobs that were running are
    re-submitted (they resume from the checkpoint)
  * RPC: `await hub.call("collect", ...)` - requests are multiplexed over one
    pipe; a reader thread resolves the matching future
  * status bundles pushed by the worker (every STATUS_PUBLISH_INTERVAL) are
    kept in memory, so every status poll is answered without touching the
    worker: per-category stats, job snapshots, live-log ring buffers
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import multiprocessing as mp
import threading
import time
from collections import deque
from concurrent.futures import Future

from . import config

log = logging.getLogger("hub")

LOG_LINES = 300
CLIENT_CACHE_MAX = 5000      # users whose category view is kept in memory


class WorkerUnavailable(Exception):
    pass


class RpcError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status
        self.detail = detail


class JobLog:
    """Ring buffer of one job's live log with a global sequence number."""

    __slots__ = ("lines", "seq")

    def __init__(self):
        self.lines: deque[str] = deque(maxlen=LOG_LINES)
        self.seq = 0

    def apply(self, seq: int, lines: list[str], reset: bool):
        if reset:
            self.lines.clear()
        self.lines.extend(lines)
        self.seq = seq

    def since(self, after: int) -> tuple[list[str], bool]:
        n_new = self.seq - after
        if after <= 0 or n_new < 0 or n_new > len(self.lines):
            return list(self.lines), True
        return (list(self.lines)[-n_new:] if n_new else []), False


class WorkerHub:
    def __init__(self, worker_init=None):
        self.worker_init = worker_init
        self.proc: mp.Process | None = None
        self.conn = None
        self._send_lock = threading.Lock()
        self._ids = itertools.count(1)
        self._pending: dict[int, Future] = {}
        self._pending_lock = threading.Lock()
        self._stopping = False
        self._restarts: list[float] = []
        self.up = threading.Event()
        self.ready = threading.Event()        # first bundle received
        self.epoch = ""
        # --- cached worker state (written by the reader thread only) ---------
        # Per user (client id): categories + counts and record revisions.
        # Every user has their own data; nothing here is served to another.
        self.client_cats: dict[str, dict] = {}
        self.client_rev: dict[str, dict] = {}  # client -> {"ver", "gen", "cat"}
        self.jobs: dict[str, dict] = {}       # job id -> snapshot (incl. owner)
        self.logs: dict[str, JobLog] = {}
        self.by_client: dict[str, str] = {}   # client -> latest job id
        self.queue: dict = {"running": 0, "queued": 0,
                            "max_active": config.MAX_ACTIVE_JOBS}
        self.active: list = []
        self.worker_info: dict = {}
        self.last_bundle = 0.0
        self.job_params: dict[str, dict] = {}  # job id -> collect args (resume)
        self._learned: dict[str, float] = {}   # job id -> when first seen

    # -- lifecycle --------------------------------------------------------------
    def start(self, resume: list[dict] | None = None):
        from .worker import worker_main
        ctx = mp.get_context("spawn")
        parent, child = ctx.Pipe(duplex=True)
        self.proc = ctx.Process(target=worker_main, args=(child, resume, self.worker_init),
                                name="collector-worker", daemon=False)
        self.proc.start()
        child.close()
        self.conn = parent
        self.up.set()
        threading.Thread(target=self._reader, args=(parent, self.proc), daemon=True,
                         name="hub-reader").start()
        if not getattr(self, "_supervising", False):
            self._supervising = True
            threading.Thread(target=self._supervise, daemon=True,
                             name="hub-supervisor").start()
        log.info("collection worker process started (pid %s)", self.proc.pid)

    def stop(self, timeout: float = 30.0):
        self._stopping = True
        if self.proc is None:
            return
        try:
            fut = self._send("shutdown", {})
            fut.result(timeout=5)
        except Exception:
            pass
        self.proc.join(timeout)
        if self.proc.is_alive():
            log.warning("worker did not exit in %.0fs; terminating", timeout)
            self.proc.kill()        # it ignores SIGTERM (see worker_main)

    def _supervise(self):
        while not self._stopping:
            proc = self.proc
            if proc is not None:
                proc.join(1.0)
                if proc.is_alive() or self._stopping:
                    continue
                self.up.clear()
                self.ready.clear()
                self._fail_pending(WorkerUnavailable("collection worker restarted"))
                code = proc.exitcode
                now = time.time()
                self._restarts = [t for t in self._restarts if now - t < 600]
                if len(self._restarts) >= config.WORKER_RESTARTS_MAX:
                    log.error("collection worker died %d times in 10 min (last exit "
                              "code %s); not restarting", len(self._restarts), code)
                    self.proc = None
                    return
                self._restarts.append(now)
                resume = self._resume_list()
                log.error("collection worker exited unexpectedly (code %s); "
                          "restarting it and resuming %d job(s)", code, len(resume))
                time.sleep(min(10.0, 1.0 * 2 ** (len(self._restarts) - 1)))
                self.start(resume)
            else:
                time.sleep(1.0)

    def _resume_list(self) -> list[dict]:
        out = []
        for jid, snap in list(self.jobs.items()):
            if snap.get("status") in ("queued", "pending", "running", "recovering",
                                      "waiting") and jid in self.job_params:
                out.append(dict(self.job_params[jid], job_id=jid))
        return out

    # -- transport --------------------------------------------------------------
    def _send(self, cmd: str, args: dict) -> Future:
        fut: Future = Future()
        if not self.up.is_set() or self.conn is None:
            fut.set_exception(WorkerUnavailable("collection worker is starting"))
            return fut
        mid = next(self._ids)
        with self._pending_lock:
            self._pending[mid] = fut
        try:
            with self._send_lock:
                self.conn.send({"id": mid, "cmd": cmd, "args": args})
        except (OSError, EOFError, BrokenPipeError) as exc:
            with self._pending_lock:
                self._pending.pop(mid, None)
            fut.set_exception(WorkerUnavailable(str(exc)))
        return fut

    async def call(self, cmd: str, timeout: float = 30.0, **args):
        fut = self._send(cmd, args)
        try:
            return await asyncio.wait_for(asyncio.wrap_future(fut), timeout)
        except asyncio.TimeoutError:
            raise WorkerUnavailable(f"collection worker did not answer '{cmd}' "
                                    f"within {timeout:.0f}s") from None

    def _fail_pending(self, exc: Exception):
        with self._pending_lock:
            pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(exc)

    def _reader(self, conn, proc):
        while True:
            try:
                msg = conn.recv()
            except (EOFError, OSError):
                break
            except Exception:
                log.exception("bad message from worker")
                continue
            if msg.get("type") == "bundle":
                try:
                    self._apply_bundle(msg)
                except Exception:
                    log.exception("could not apply status bundle")
                continue
            with self._pending_lock:
                fut = self._pending.pop(msg.get("id"), None)
            if fut is None or fut.done():
                continue
            if msg.get("ok"):
                fut.set_result(msg.get("data"))
            else:
                fut.set_exception(RpcError(int(msg.get("status") or 500),
                                           str(msg.get("detail") or "error")))
        if proc is self.proc:
            self.up.clear()

    # -- cached state -------------------------------------------------------------
    def _apply_bundle(self, b: dict):
        if b.get("epoch") != self.epoch:        # a new worker instance
            self.epoch = b.get("epoch", "")
            # its stores restart their versions: re-fetch each user's view
            self.client_cats.clear()
            self.client_rev.clear()
        for client, view in b.get("clients", {}).items():
            self.set_client(client, view)
        for jid, snap in b.get("jobs", {}).items():
            self.remember_job(snap)
        for jid, lg in b.get("logs", {}).items():
            self.logs.setdefault(jid, JobLog()).apply(lg["seq"], lg["lines"],
                                                      lg["reset"])
        known = set(b.get("known", ()))
        if known:
            # a bundle built just before a submit can arrive after its reply:
            # never drop a job the API learned about moments ago
            now = time.time()
            for jid in [j for j in list(self.jobs) if j not in known
                        and now - self._learned.get(j, 0) > 5]:
                self._learned.pop(jid, None)
                snap = self.jobs.pop(jid)
                self.logs.pop(jid, None)
                self.job_params.pop(jid, None)
                if self.by_client.get(snap.get("owner")) == jid:
                    del self.by_client[snap["owner"]]
        self.queue = b.get("queue", self.queue)
        self.active = b.get("active", self.active)
        self.worker_info = b.get("worker", {})
        self.last_bundle = time.time()
        self.ready.set()

    def set_client(self, client: str, view: dict):
        """Store one user's category view unless a newer one is cached."""
        ver = tuple(view.get("ver") or (0, 0))
        cur = self.client_rev.get(client)
        if cur is not None and cur["ver"] > ver:
            return
        if client not in self.client_cats and len(self.client_cats) >= CLIENT_CACHE_MAX:
            try:                    # oldest entry; re-fetched on its next use
                old = next(iter(self.client_cats))
            except (StopIteration, RuntimeError):   # changed by the other thread
                old = None
            self.client_cats.pop(old, None)
            self.client_rev.pop(old, None)
        self.client_cats[client] = view["per_category"]
        self.client_rev[client] = {"ver": ver, "gen": ver[0],
                                   "cat": view.get("cat_rev", {})}

    def forget_client(self, client: str):
        self.client_cats.pop(client, None)
        self.client_rev.pop(client, None)

    def remember_job(self, snap: dict, log_lines: list[str] | None = None):
        """Store a snapshot (from a bundle or an RPC reply)."""
        jid = snap["id"]
        if jid not in self.jobs:
            self._learned[jid] = time.time()
        self.jobs[jid] = snap
        owner = snap.get("owner")
        if owner:
            cur = self.by_client.get(owner)
            if cur is None or cur == jid or (self.jobs.get(cur, {}).get("created_at", "")
                                             <= snap.get("created_at", "")):
                self.by_client[owner] = jid
        if log_lines is not None:
            self.logs.setdefault(jid, JobLog()).apply(snap.get("log_seq", 0),
                                                      log_lines, True)

    def client_job(self, client: str) -> tuple[dict | None, JobLog | None]:
        jid = self.by_client.get(client)
        if not jid:
            return None, None
        return self.jobs.get(jid), self.logs.get(jid)
