"""Multi-user job scheduling: every user's collection is its own job.

    submit ──> QUEUED ──(free slot, category not busy)──> running ... ──> done
                  │                                           │
                  └── stop: removed from the queue            └── on_finish ──> next queued job starts

Isolation rules
  * up to MAX_ACTIVE_JOBS jobs run at once; more wait in a FIFO queue
  * every user has their own data (state_for(client), see stores.py): their
    records are never visible to, or counted for, anyone else
  * one job per CATEGORY of one user's data at a time (the target counts
    that data, so two jobs filling it would race each other); a second job
    for a busy category waits, and other queued jobs go ahead of it
  * each client (browser) may have MAX_JOBS_PER_CLIENT queued/running jobs
  * shared resources are split fairly: every running job gets an equal
    slice of the crawl threads (config.job_crawl_workers) and of the Serper
    account's concurrency (job.search_share), and all jobs pass through the
    same account rate limiter (search.account_limits)

A job never waits on another job's work: its searches, crawl pool,
planner, watchdog and failure handling are its own. Only the Serper
response cache is shared between users (internally locked).
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime

from .. import config
from .engine import ACTIVE_STATUSES, CollectionJob, StateStore

log = logging.getLogger("jobs")


def _slot(job) -> tuple:
    """What two jobs must not fill at once: one category of one user's data."""
    return id(getattr(job, "state", None)), job.category


class JobRejected(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status = status
        self.detail = detail


class JobManager:
    def __init__(self, state: StateStore | None = None, max_active: int | None = None,
                 job_factory=CollectionJob, state_for=None):
        # state_for(client) -> that user's StateStore (per-user data); without
        # it every job uses the one `state`.
        self.state = state
        self.state_for = state_for or (lambda client: state)
        self.max_active = max_active or config.MAX_ACTIVE_JOBS
        self.job_factory = job_factory
        self.lock = threading.RLock()
        self.jobs: dict[str, CollectionJob] = {}       # id -> job (live + history)
        self.queue: deque[str] = deque()                # queued ids, FIFO
        self.active: dict[str, CollectionJob] = {}      # started, not finished
        self.history: deque[str] = deque()              # finished ids, oldest first
        self.by_client: dict[str, str] = {}             # client -> latest job id
        self.changed: set[str] = set()                  # ids changed since last drain
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._reaper, daemon=True,
                                        name="jobs-reaper")
        self._thread.start()

    # -- queries ----------------------------------------------------------------
    def active_count(self) -> int:
        return max(1, len(self.active))

    def client_job(self, client: str) -> CollectionJob | None:
        with self.lock:
            jid = self.by_client.get(client)
            return self.jobs.get(jid) if jid else None

    def queue_position(self, job_id: str) -> int:
        with self.lock:
            try:
                return self.queue.index(job_id) + 1
            except ValueError:
                return 0

    def summary(self) -> dict:
        with self.lock:
            return {"running": len(self.active), "queued": len(self.queue),
                    "max_active": self.max_active,
                    "busy_categories": sorted({j.category for j in self.active.values()})}

    def drain_changed(self) -> set[str]:
        with self.lock:
            out, self.changed = self.changed, set()
            return out

    # -- commands ---------------------------------------------------------------
    def submit(self, client: str, category: str, keywords: list[str], location: str,
               target: int, provider: str, max_queries: int, geo: dict | None = None,
               job_id: str | None = None) -> CollectionJob:
        with self.lock:
            mine = [j for j in self.jobs.values()
                    if j.owner == client and j.status in ACTIVE_STATUSES]
            if len(mine) >= config.MAX_JOBS_PER_CLIENT:
                raise JobRejected(409, "You already have a collection running or "
                                       "queued. Stop it first.")
            if len(self.queue) >= config.MAX_QUEUED_JOBS:
                raise JobRejected(503, "The server is at capacity (too many "
                                       "collections waiting). Try again in a few "
                                       "minutes.")
            job = self.job_factory(self.state_for(client), category, keywords, location,
                                   target, provider, max_queries, geo=geo)
            if job_id:
                job.id = job_id
            job.owner = client
            job.status = "queued"
            job.crawl_workers = config.job_crawl_workers()
            job.search_share = self.active_count
            job.on_finish = self._on_finish
            self.jobs[job.id] = job
            self.queue.append(job.id)
            self.by_client[client] = job.id
            self.changed.add(job.id)
            self._schedule()
            if job.status == "queued":
                busy = any(_slot(j) == _slot(job) for j in self.active.values())
                job.say(f"Queued: position {self.queue_position(job.id)} - "
                        + ("another collection is filling this category right now; "
                           "this one starts when it finishes"
                           if busy else f"all {self.max_active} collection slots are "
                                        f"busy; starts automatically when one frees up"))
            return job

    def stop(self, client: str, job_id: str | None = None) -> CollectionJob | None:
        with self.lock:
            jid = job_id or self.by_client.get(client)
            job = self.jobs.get(jid) if jid else None
            if job is None or job.owner != client:
                return None
            job.stop()
            if jid in self.queue:           # never started: finish it right here
                self.queue.remove(jid)
                job.status = "stopped"
                job.stop_reason = "stopped before it started"
                job.finished_at = datetime.now().isoformat(timespec="seconds")
                job.say("Stopped before it started.")
                self._retire(job)
                self._schedule()
            self.changed.add(jid)
            return job

    def stop_all(self, timeout: float = 20.0):
        """Shutdown: stop every job (progress is saved by each job)."""
        with self.lock:
            for jid in list(self.queue):
                self.jobs[jid].stop()
            self.queue.clear()
            running = list(self.active.values())
        for j in running:
            j.stop()
        t_end = time.time() + timeout
        for j in running:
            if j.thread is not None:
                j.thread.join(max(0.1, t_end - time.time()))
        self._stop.set()

    def category_busy(self, category: str, state: StateStore | None = None) -> bool:
        """A job of `state`'s data (default: any) is filling `category`."""
        with self.lock:
            return any(j.category == category and j.status in ACTIVE_STATUSES
                       and (state is None or j.state is state)
                       for j in self.jobs.values())

    def any_active(self, state: StateStore | None = None) -> bool:
        with self.lock:
            return any(j.status in ACTIVE_STATUSES and (state is None or j.state is state)
                       for j in self.jobs.values())

    def owners_active(self) -> set[str]:
        with self.lock:
            return {j.owner for j in self.jobs.values() if j.status in ACTIVE_STATUSES}

    # -- scheduling -------------------------------------------------------------
    def _schedule(self):
        """Start queued jobs while slots are free (caller holds self.lock)."""
        busy = {_slot(j) for j in self.active.values()}
        for jid in list(self.queue):
            if len(self.active) >= self.max_active:
                break
            job = self.jobs[jid]
            if _slot(job) in busy:
                continue                 # wait for the job filling this category
            self.queue.remove(jid)
            self.active[jid] = job
            busy.add(_slot(job))
            job.status = "pending"
            self.changed.add(jid)
            try:
                job.start()
            except Exception as exc:     # e.g. cannot start a thread
                log.exception("could not start job %s", jid)
                self.active.pop(jid, None)
                job.status, job.error = "failed", f"could not start: {exc}"
                job.stop_reason = job.error
                self._retire(job)

    def _on_finish(self, job: CollectionJob):
        with self.lock:
            if self.active.pop(job.id, None) is not None:
                self._retire(job)
            self.changed.add(job.id)
            self._schedule()

    def _retire(self, job: CollectionJob):
        """Move a finished job to the bounded history (caller holds lock)."""
        self.history.append(job.id)
        while len(self.history) > config.JOB_HISTORY:
            old = self.history.popleft()
            gone = self.jobs.pop(old, None)
            if gone is not None and self.by_client.get(gone.owner) == old:
                del self.by_client[gone.owner]

    def _reaper(self):
        """Safety net: a job thread that died without its finish callback
        (should not happen) must not hold a slot forever."""
        while not self._stop.wait(1.0):
            with self.lock:
                dead = [j for j in self.active.values()
                        if j.thread is not None and not j.thread.is_alive()]
                for j in dead:
                    log.error("job %s thread ended without finishing; releasing "
                              "its slot", j.id)
                    self.active.pop(j.id, None)
                    if j.status in ACTIVE_STATUSES:
                        j.status = "failed"
                        j.error = j.stop_reason = "collection thread ended unexpectedly"
                    self._retire(j)
                    self.changed.add(j.id)
                if dead:
                    self._schedule()
