"""Collection engine: runs one collection job in a background thread, with
live progress, checkpointing (JSON state + regenerated Excel files) and
resume. Orchestrates search -> dedup -> crawl -> extract -> validate -> store.

Pipeline (all stages overlap; nothing waits for a "query" to finish):

    DiscoveryPlanner --> search pool (SERPER_CONCURRENCY in flight)
        each response is processed the moment it arrives, in its own thread:
        result -> identity (domain / Places cid) -> already seen / already a
        record?  -> skip (no crawl spent)  else -> crawl pool immediately
    crawl pool (CRAWL_WORKERS) -> analysis process pool (parse/extract)
        -> validate -> dedup registry -> record store

Credit control: a new search is launched only while the records expected
from work already in flight (pending crawls x measured valid rate, plus
in-flight searches x measured candidates per search) are short of the target,
and never beyond the run's credit budget. The run stops the moment the
target is reached.

Fault tolerance - ONE ERROR NEVER STOPS THE JOB. Failures are isolated at the
smallest unit: an HTTP request (bounded retries with backoff in the provider),
a search (re-queued with backoff by the planner, then recorded as failed), a
URL / parser / worker task (logged with worker, operation, query and URL;
the pool keeps running), and the controller loop itself (RECOVERING, backoff,
retry). A watchdog tracks progress per stage; when nothing new arrives it
diagnoses the stalled stage, retries failed searches, abandons stuck crawl
tasks, replaces a jammed worker pool and escalates the search strategy.

Terminal conditions (the only ones):
    completed  valid unique records >= target
    stopped    the user pressed Stop
    exhausted  every search tier is used up, or the credit safety cap is hit
               (exact reason logged; never reported as success)
    failed     genuinely unrecoverable: provider auth/credits gone, or the
               controller loop failing MAX_CONTROLLER_ERRORS times in a row"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from urllib.parse import urlparse

from .. import config, fastjson
from . import analysis
from .categories import (CATEGORIES, cities_for, custom_terms, expansion_phrases,
                         search_phrases, slugify_custom)
from .crawler import Fetcher
from .credits import CreditLedger, ledger_key
from .deduplicator import DedupRegistry
from .discovery import TIER_HELP, DiscoveryPlanner, SearchRequest
from .exporter import write_category_file, write_master_summary
from .extractor import Extracted, split_address
from .geo import expansion_geos, nearby_geos
from .prio_pool import PriorityThreadPool
from .normalize import clean_text, norm_domain, norm_name
from .search import ProviderDisabled, make_provider
from .validator import confidence, is_aggregator, is_skip_domain, validate

log = logging.getLogger("engine")

LIST_PAGE_MIN_EXTERNAL_LINKS = 12
CRAWLED_MEMORY_MAX = 60_000   # persisted crawl-attempt keys
SLOW_START = 4                # searches in flight until yield is measured
SEARCH_FAIL_STREAK = 3        # consecutive failed searches -> RECOVERING + pause
SEARCH_PAUSE_BASE_S = 2.0     # circuit breaker: 2 s, 4 s, 8 s ... between probes
SEARCH_PAUSE_MAX_S = 30.0
CREDIT_POLL_S = 60            # balance check interval while out of credits
# waiting = paused because the Serper account is out of credits; the job stays
# alive and resumes by itself once the balance is > 0 (or the user stops it).
# queued = accepted, waiting for a free job slot (or for another job that is
# collecting the same category) - see jobs.JobManager.
ACTIVE_STATUSES = ("queued", "pending", "running", "recovering", "waiting")
_PLACE_FIELDS = ("title", "website", "cid", "placeId", "phoneNumber", "address",
                 "category")
ORGANIC_PAIR_MIN = 0.8        # pair organic with a Places search only while its
                              # valid-records-per-credit is >= 80% of Places'
ORGANIC_SAMPLE_EVERY = 4      # ... otherwise sample 1 cell in 4 to keep measuring
KIND_WARMUP_CREDITS = 4       # both kinds are measured this long before judging
EXPLORE_MIN_REMAINING = 250   # records still needed to justify exploration
TERMINAL_STATUSES = ("completed", "stopped", "exhausted", "failed")
# Crawl queue order (lower first). Measured yield per crawl task (Advisory /
# USA, 100 records): Places listings 70%, search results + mined directory
# links together 12%. The pool is saturated on a small host, so the
# high-yield work goes first and the target is reached with fewer crawls.
PRIO_PLACE, PRIO_RESULT, PRIO_MINED = 0, 1, 2


class JobFailed(Exception):
    """Genuinely unrecoverable condition; the message is the exact reason."""


class Timings:
    """Thread-safe per-stage wall-time accounting (perf instrumentation)."""

    def __init__(self):
        self._t: dict[str, float] = defaultdict(float)
        self._n: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def add(self, stage: str, dt: float):
        with self._lock:
            self._t[stage] += dt
            self._n[stage] += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {s: {"total_s": round(self._t[s], 2), "count": self._n[s],
                        "avg_ms": round(1000 * self._t[s] / self._n[s], 1)}
                    for s in self._t if self._n[s]}


# --------------------------------------------------------------------------- #
# Persistent state (checkpoint / resume)
# --------------------------------------------------------------------------- #

class SharedSearch:
    """Serper response cache + in-flight request dedup shared by every
    user's store: two users running the same search pay for it once. Only
    raw search results are shared - records, dedup registry and crawl
    memory stay per user."""

    def __init__(self):
        self.cache: dict[str, dict] = {}
        self.lock = threading.Lock()
        self.sf_lock = threading.Lock()
        self.inflight: dict[str, threading.Event] = {}
        self._saved_sig = (0, 0.0)

    def merge(self, cache: dict):
        now = time.time()
        with self.lock:
            for k, v in cache.items():
                if isinstance(v, dict) and now - v.get("ts", 0) < config.SEARCH_CACHE_TTL:
                    self.cache.setdefault(k, v)

    def load(self, path: str):
        try:
            with open(path, "rb") as fh:
                self.merge(fastjson.loads(fh.read()))
        except (OSError, ValueError):
            pass
        with self.lock:
            self._saved_sig = self._sig()

    def _sig(self):
        return (len(self.cache), max((v.get("ts", 0) for v in self.cache.values()),
                                     default=0.0))

    def save(self, path: str):
        """Write the cache if it changed since the last save (atomic)."""
        with self.lock:
            sig = self._sig()
            if sig == self._saved_sig:
                return
            data = dict(self.cache)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.{os.getpid()}.tmp"
        with open(tmp, "wb") as fh:
            fh.write(fastjson.dumps(data))
        os.replace(tmp, path)
        with self.lock:
            self._saved_sig = sig


class StateStore:
    def __init__(self, path: str | None = None, out_dir: str | None = None):
        # Checkpoint file + Excel folder. Default (None): the app-wide ones
        # in config (read when used, so tests can repoint them); per-user
        # stores get their own folder (see collector/stores.py).
        self._path = path
        self._out_dir = out_dir
        self.records: dict[str, list[dict]] = {c: [] for c in CATEGORIES}
        self.stats: dict[str, dict] = {
            c: {"discovered": 0, "duplicates": 0, "failed_urls": 0} for c in CATEGORIES}
        self.custom: "OrderedDict[str, dict]" = OrderedDict()  # slug -> {display, file}
        self.registry = DedupRegistry()
        self.done_queries: set[str] = set()
        # Request-level Serper dedup cache: normalized request -> result.
        # Persisted, so a re-run / resume / re-collect after delete NEVER
        # re-pays for a search it already made (within the TTL).
        self.search_cache: dict[str, dict] = {}
        self.cache_lock = threading.Lock()
        self.shares_cache = False        # True: cache persisted by its SharedSearch
        # Discovery planner memory per category: used query cells (canonical
        # keys) + measured phrase/location novelty, so a resumed run continues
        # with NEW searches instead of repeating or rephrasing old ones.
        self.discovery: dict[str, dict] = {}
        # Crawl targets already attempted (domain, or URL for directory
        # pages) -> category. A resumed run never re-crawls them.
        self.crawled: dict[str, str] = {}
        # Last job per category (target, status, progress, stop reason) so a
        # restart knows where a run was and why it ended.
        self.jobs: dict[str, dict] = {}
        # Credit ledger = per-query cost/yield log + the global executed-query
        # registry (a paid normalized request is never sent again).
        self.ledger = CreditLedger()
        self.lock = threading.Lock()
        self._save_lock = threading.Lock()
        # Revisions: bumped whenever a category's records change. The API
        # caches serialized record lists / category stats keyed by them.
        self.rev = 0
        self.cat_rev: dict[str, int] = defaultdict(int)
        self._stats_cache: tuple[int, dict] | None = None
        self._cat_stats: dict[str, tuple[int, dict]] = {}   # slug -> (cat_rev, stats)
        # In-flight Serper request dedup shared by ALL jobs: two jobs issuing
        # the same normalized request at once pay for it only once.
        self.sf_lock = threading.Lock()
        self.search_inflight: dict[str, threading.Event] = {}
        # Coalesced background checkpointing: jobs mark the state dirty and
        # ONE saver thread writes it at most every STATE_SAVE_INTERVAL, no
        # matter how many jobs run (each job used to rewrite the whole
        # multi-MB checkpoint every 5 s on its own).
        self._dirty = False
        self._dirty_exports: set[str] = set()
        self._saver: threading.Thread | None = None
        self._saver_wake = threading.Event()
        self._saver_path = ""
        self.saves = 0
        self.last_save_s = 0.0
        self._closed = False

    @property
    def path(self) -> str:
        return self._path or config.STATE_PATH

    @property
    def out_dir(self) -> str:
        return self._out_dir or config.OUTPUT_DIR

    def share(self, shared: SharedSearch):
        """Use the cross-user search cache instead of a private one."""
        if self.search_cache:
            shared.merge(self.search_cache)
        self.search_cache = shared.cache
        self.cache_lock = shared.lock
        self.sf_lock = shared.sf_lock
        self.search_inflight = shared.inflight
        self.shares_cache = True

    def close(self):
        """Unload: write a pending checkpoint and let the saver thread end."""
        self._closed = True
        self._saver_wake.set()
        self.flush()

    # -- revisions / stats ------------------------------------------------------
    def touch(self, category: str):
        """Records of `category` changed (caller may hold self.lock)."""
        self.rev += 1
        self.cat_rev[category] += 1

    def append_record(self, category: str, record: dict, limit: int) -> bool:
        """Store one validated, deduplicated record unless the category is
        already at `limit`. Atomic with respect to other jobs."""
        with self.lock:
            recs = self.records[category]
            if len(recs) >= limit:
                return False
            recs.append(record)
            self.touch(category)
            return True

    def category_stats(self) -> dict[str, dict]:
        """slug -> {count, emails, websites}; recomputed only when records
        changed since the last call (the API used to rescan every record of
        every category on every status poll), and then only for the
        categories that changed: a running job adds records to ONE category,
        and rescanning all ~10,000 records of every category on each status
        bundle (every 0.5 s) cost ~2.5 ms each time."""
        with self.lock:
            cached = self._stats_cache
            if cached is not None and cached[0] == self.rev:
                return cached[1]
            out = {}
            for c, rs in self.records.items():
                rev = self.cat_rev.get(c, 0)
                hit = self._cat_stats.get(c)
                if hit is None or hit[0] != rev:
                    hit = (rev, {"count": len(rs),
                                 "emails": sum(1 for r in rs if r.get("Business Email")),
                                 "websites": sum(1 for r in rs
                                                 if r.get("Official Website"))})
                    self._cat_stats[c] = hit
                out[c] = hit[1]
            self._stats_cache = (self.rev, out)
            return out

    # -- coalesced background saving -------------------------------------------
    def request_save(self, path: str, export_category: str | None = None):
        """Mark the checkpoint dirty (and optionally a category's Excel file);
        the saver thread writes it within STATE_SAVE_INTERVAL."""
        with self.lock:
            self._dirty = True
            self._saver_path = path
            if export_category:
                self._dirty_exports.add(export_category)
            if self._saver is None or not self._saver.is_alive():
                self._saver = threading.Thread(target=self._saver_loop, daemon=True,
                                               name="state-saver")
                self._saver.start()

    def _saver_loop(self):
        last_export = 0.0
        while True:
            self._saver_wake.wait(config.STATE_SAVE_INTERVAL)
            self._saver_wake.clear()
            with self.lock:
                dirty, path = self._dirty, self._saver_path
                self._dirty = False
                if self._closed and not dirty and not self._dirty_exports:
                    self._saver = None
                    return
                exports = set()
                if time.time() - last_export >= config.EXPORT_MIN_INTERVAL:
                    exports, self._dirty_exports = self._dirty_exports, set()
            if dirty and path:
                try:
                    self.save(path)
                except Exception:
                    log.exception("background checkpoint failed")
                    with self.lock:
                        self._dirty = True
            if exports:
                last_export = time.time()
                try:
                    self.export_categories(exports)
                except Exception:
                    log.exception("background Excel export failed")

    def flush(self):
        """Write a pending checkpoint now (shutdown)."""
        with self.lock:
            dirty, path = self._dirty, self._saver_path
            self._dirty = False
        if dirty and path:
            self.save(path)

    def export_categories(self, categories, out_dir: str | None = None,
                          force: bool = False):
        """Regenerate the given categories' Excel files + the master summary
        (each skipped when its content is unchanged, unless force)."""
        out_dir = out_dir or self.out_dir
        cats = self.all_categories()
        with self.lock:
            records = {c: list(rs) for c, rs in self.records.items()}
            stats = {c: dict(s) for c, s in self.stats.items()}
        for c in categories:
            if c in cats:
                write_category_file(cats[c]["display"], cats[c]["file"],
                                    records.get(c, []), out_dir, force=force)
        write_master_summary(cats, records, stats, out_dir)

    def cache_get(self, key: str):
        with self.cache_lock:
            hit = self.search_cache.get(key)
            if hit and time.time() - hit.get("ts", 0) < config.SEARCH_CACHE_TTL:
                return hit["result"]
            return None

    @staticmethod
    def _trim(row):
        """Keep only the fields the pipeline reads (Places rows carry
        ratings, hours, thumbnails ... that would bloat the checkpoint)."""
        if isinstance(row, dict):
            return {k: row[k] for k in _PLACE_FIELDS if k in row}
        return row

    def cache_put(self, key: str, result):
        result = [self._trim(r) for r in result]
        with self.cache_lock:
            if len(self.search_cache) >= config.SEARCH_CACHE_MAX:
                oldest = sorted(self.search_cache.items(),
                                key=lambda kv: kv[1].get("ts", 0))
                for k, _ in oldest[:len(oldest) // 4]:
                    del self.search_cache[k]
            self.search_cache[key] = {"result": result, "ts": time.time()}

    def all_categories(self) -> "OrderedDict[str, dict]":
        """Ordered slug -> {"display", "file"} for the fixed ten plus customs."""
        out: "OrderedDict[str, dict]" = OrderedDict(
            (c, {"display": CATEGORIES[c]["display"], "file": CATEGORIES[c]["file"]})
            for c in CATEGORIES)
        with self.lock:
            out.update({s: dict(d) for s, d in self.custom.items()})
        return out

    def ensure_custom(self, name: str) -> str:
        """Find or create a custom category for this display name; returns slug."""
        name = " ".join(name.split())
        with self.lock:
            for slug, d in self.custom.items():
                if d["display"].lower() == name.lower():
                    return slug
            slug = slugify_custom(name)
            n = 2
            while slug in self.custom or slug in CATEGORIES:
                slug = f"{slugify_custom(name)}_{n}"
                n += 1
            fname = f"{10 + len(self.custom) + 1:02d}_{slug}.xlsx"
            self.custom[slug] = {"display": name, "file": fname}
            self.records.setdefault(slug, [])
            self.stats.setdefault(slug, {"discovered": 0, "duplicates": 0, "failed_urls": 0})
            self.touch(slug)
            return slug

    def delete_category(self, slug: str) -> tuple[int, bool, str]:
        """Delete all records of a category. Custom categories are removed
        entirely; fixed ones stay with zero records. Dedup-registry entries,
        executed-query markers, planner memory and crawl markers owned by the
        category are purged so the same companies can be re-collected later.
        Returns (records removed, was_custom, excel filename)."""
        # registry first (its own lock; never nested inside self.lock)
        prefix = f"{slug}|"
        with self.registry.lock:
            for d in (self.registry.domains, self.registry.names,
                      self.registry.emails, self.registry.phones):
                for k in [k for k, owner in d.items() if owner.startswith(prefix)]:
                    del d[k]
        self.ledger.purge_category(slug)   # its queries may be re-run later
        with self.lock:
            removed = len(self.records.get(slug, []))
            qprefix = f"{slug}||"
            self.done_queries = {q for q in self.done_queries
                                 if not q.startswith(qprefix)}
            self.discovery.pop(slug, None)
            self.crawled = {k: v for k, v in self.crawled.items() if v != slug}
            is_custom = slug in self.custom
            if is_custom:
                fname = self.custom[slug]["file"]
                del self.custom[slug]
                self.records.pop(slug, None)
                self.stats.pop(slug, None)
            else:
                fname = CATEGORIES[slug]["file"]
                self.records[slug] = []
                self.stats[slug] = {"discovered": 0, "duplicates": 0,
                                    "failed_urls": 0}
            self.touch(slug)
        return removed, is_custom, fname

    def mark_crawled(self, key: str, slug: str) -> bool:
        """Atomically claim a crawl target. False if it was attempted before
        (this run or any earlier run)."""
        with self.lock:
            if key in self.crawled:
                return False
            if len(self.crawled) >= CRAWLED_MEMORY_MAX:
                for k in list(self.crawled)[:CRAWLED_MEMORY_MAX // 4]:
                    del self.crawled[k]
            self.crawled[key] = slug
            return True

    def save(self, path: str, discovery_extra: dict | None = None):
        registry = self.registry.to_dict()
        ledger = self.ledger.to_dict()
        with self.cache_lock:
            search_cache = {} if self.shares_cache else dict(self.search_cache)
        with self.lock:   # shallow copies: json.dump runs outside the lock
            payload = {
                "saved_at": datetime.now().isoformat(timespec="seconds"),
                "records": {c: list(rs) for c, rs in self.records.items()},
                "stats": {c: dict(s) for c, s in self.stats.items()},
                "custom_categories": dict(self.custom),
                "registry": registry,
                "done_queries": sorted(self.done_queries),
                "search_cache": search_cache,
                "discovery": dict(self.discovery) | (discovery_extra or {}),
                "crawled": dict(self.crawled),
                "jobs": {k: dict(v) for k, v in self.jobs.items()},
                "credit_ledger": ledger,
            }
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Atomic: write + fsync a temp file, then rename over the checkpoint.
        # A crash mid-write leaves the previous checkpoint intact.
        t0 = time.perf_counter()
        with self._save_lock:
            data = fastjson.dumps(payload)   # orjson: ~20 ms for 11 MB
            tmp = f"{path}.{os.getpid()}.tmp"
            with open(tmp, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            for attempt in range(5):
                try:
                    os.replace(tmp, path)
                    break
                except PermissionError:   # Windows: file briefly held by AV/indexer
                    if attempt == 4:
                        raise
                    time.sleep(0.2 * (attempt + 1))
            self.saves += 1
            self.last_save_s = time.perf_counter() - t0

    @classmethod
    def load(cls, path: str) -> "StateStore":
        st = cls()
        if not os.path.exists(path):
            return st
        try:
            with open(path, "rb") as fh:
                payload = fastjson.loads(fh.read())
        except (OSError, ValueError) as exc:
            log.warning("could not load checkpoint %s: %s", path, exc)
            return st
        st.custom = OrderedDict(payload.get("custom_categories", {}))
        for cat in list(CATEGORIES) + list(st.custom):
            st.records[cat] = payload.get("records", {}).get(cat, [])
            st.stats.setdefault(cat, {"discovered": 0, "duplicates": 0, "failed_urls": 0})
            if cat in payload.get("stats", {}):
                st.stats[cat].update(payload["stats"][cat])
        st.registry = DedupRegistry.from_dict(payload.get("registry", {}))
        # The registry is snapshotted just before the records, so records
        # stored in between have no dedup keys in the file: re-register them,
        # or a resumed run could collect the same business twice.
        repaired = sum(st.registry.ensure(r, cat)
                       for cat, recs in st.records.items() for r in recs)
        if repaired:
            log.info("checkpoint load: re-registered %d missing dedup keys", repaired)
        st.done_queries = set(payload.get("done_queries", []))
        now = time.time()
        st.search_cache = {
            k: v for k, v in payload.get("search_cache", {}).items()
            if isinstance(v, dict) and now - v.get("ts", 0) < config.SEARCH_CACHE_TTL}
        st.discovery = dict(payload.get("discovery", {}))
        st.crawled = dict(payload.get("crawled", {}))
        st.jobs = dict(payload.get("jobs", {}))
        st.ledger = CreditLedger(payload.get("credit_ledger"))
        return st


# --------------------------------------------------------------------------- #
# Collection job
# --------------------------------------------------------------------------- #

class CollectionJob:
    def __init__(self, state: StateStore, category: str, keywords: list[str],
                 location: str, target: int, provider_name: str, max_queries: int,
                 geo: dict | None = None):
        self.state = state
        self.category = category
        self.keywords = [k.strip() for k in keywords if k.strip()]
        self.location = location
        self.geo = {k: (geo or {}).get(k, "").strip() for k in ("city", "state", "country")}
        if category in CATEGORIES:
            self.display = CATEGORIES[category]["display"]
            self.terms = CATEGORIES[category]["terms"]
        else:  # user-defined custom category (registered in the state store)
            self.display = state.custom[category]["display"]
            self.terms = custom_terms(self.display, self.keywords)
        self.target = max(1, min(target, config.MAX_TARGET))
        self.provider_name = provider_name
        # max_queries = size of the INITIAL plan the user sees; when those
        # searches are used up the planner keeps generating new ones (new
        # locations / sub-niches) within the credit budget.
        self.max_queries = max(1, min(max_queries, config.MAX_MAX_QUERIES))
        # Spend safety valve only (0 = off). It scales with the target - the
        # old fixed ceiling of 2 x MAX_MAX_QUERIES (800 credits) made a
        # 2000-record target unreachable by construction.
        self.credit_budget = (
            max(2 * self.max_queries,
                math.ceil(self.target * config.MAX_CREDITS_PER_RECORD))
            if config.MAX_CREDITS_PER_RECORD > 0 else 0)
        self.phrases = search_phrases(category, self.display, self.keywords)
        self.extra_phrases = expansion_phrases(category, self.display, self.keywords)
        self.geos = expansion_geos(
            self.geo["city"], self.geo["state"],
            self.geo["country"] or self.location, self.location,
            cities_for(self.geo["country"] or self.location))
        self.extra_geos = nearby_geos(self.geo["city"], self.geo["state"],
                                      self.geo["country"])

        # pending|running|recovering|completed|stopped|exhausted|failed
        self.status = "pending"
        self.error = ""
        self.recovery = ""               # what is being recovered (RECOVERING)
        self.stop_reason = ""            # exact reason for a terminal state
        self.started_at = ""
        self.finished_at = ""
        self.counters = {
            "queries_executed": 0, "discovered": 0, "valid": 0,
            "duplicates": 0, "failed": 0, "emails": 0, "phones": 0,
            "websites": 0, "search_requests": 0, "serper_credits": 0,
            "serper_429": 0, "serper_timeouts": 0, "cache_hits": 0,
            "serper_ok": 0, "serper_failed": 0, "places_followups": 0,
            "candidate_urls": 0, "unique_domains": 0, "tasks_done": 0,
            "search_results": 0, "new_results": 0, "skipped_known": 0,
            "searches_done": 0,
            # fault tolerance
            "search_failures": 0, "search_retries": 0, "searches_failed": 0,
            "worker_errors": 0, "abandoned_tasks": 0, "pool_restarts": 0,
            "controller_errors": 0, "recoveries": 0, "strategy_escalations": 0,
            "watchdog_actions": 0,
            # credit accounting (this run)
            "places_credits": 0, "organic_credits": 0, "places_requests": 0,
            "organic_requests": 0, "valid_places": 0, "valid_organic": 0,
            "skipped_executed": 0, "skipped_target": 0,
            "credits_after_target": 0, "timeouts_maybe_charged": 0,
            "duplicate_results": 0,
        }
        self._seen_ids: set[str] = set()
        self._recent_new: deque[int] = deque(maxlen=config.YIELD_EXHAUSTED_STREAK)
        self._stop_search = ""            # "", "budget"
        self._closed = False
        # watchdog / progress clocks (wall time)
        self._last_new_record = 0.0
        self._last_search_ok = 0.0
        self._last_task_done = 0.0
        self._last_stall_action = 0.0
        self._search_fail_streak = 0
        self._search_pause_until = 0.0
        self._recovery_kind = ""          # "search" | "crawl" | "controller"
        self._credit_wait_since = 0.0     # > 0 while paused for Serper credits
        self._next_credit_check = 0.0
        self._credit_ckpt_done = False
        self._running: dict[int, dict] = {}   # task id -> info (running now)
        self._tasks: dict[int, dict] = {}     # task id -> info (submitted, unfinished)
        self._run_lock = threading.Lock()
        self._latencies: list[tuple[float, str, str]] = []  # (s, kind, query)
        self.run_started = 0.0
        # in-flight request dedup is shared with every other job (StateStore)
        self._sf_lock = state.sf_lock
        self._search_inflight = state.search_inflight
        self.log_lines: deque[str] = deque(maxlen=300)
        self.log_seq = 0                 # lines ever logged (incremental log API)
        self._log_lock = threading.Lock()
        # -- multi-job scheduling hooks (set by jobs.JobManager) --------------
        self.id = uuid.uuid4().hex[:16]
        self.owner = ""                  # client id of the user who started it
        self.created_at = datetime.now().isoformat(timespec="seconds")
        self.crawl_workers = config.CRAWL_WORKERS   # this job's crawl threads
        # number of jobs currently sharing the Serper account; this job keeps
        # at most its fair share of SERPER_CONCURRENCY searches in flight
        self.search_share = lambda: 1
        self.on_finish = None            # callable(job), from the job thread
        self._last_export = 0.0
        self._last_ckpt = 0.0
        self.timings = Timings()
        self._inflight: set = set()
        self._inflight_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.fetcher = Fetcher()
        self.fetcher.timings = self.timings
        self.provider = None
        self.planner: DiscoveryPlanner | None = None
        self._pool: PriorityThreadPool | None = None
        self.attempted: set[str] = set()
        self.lock = threading.Lock()

    # -- live log -------------------------------------------------------------
    def say(self, msg: str):
        line = f"{datetime.now().strftime('%H:%M:%S')}  {msg}"
        with self._log_lock:
            self.log_lines.append(line)
            self.log_seq += 1
        log.info("[job %s] %s", self.id[:6], msg)

    def log_since(self, seq: int) -> tuple[int, list[str], bool]:
        """(current seq, lines logged after `seq`, reset). reset=True when
        the caller's cursor is older than the buffer (it gets everything)."""
        with self._log_lock:
            cur, n_new = self.log_seq, self.log_seq - seq
            if seq <= 0 or n_new < 0 or n_new > len(self.log_lines):
                return cur, list(self.log_lines), True
            return cur, (list(self.log_lines)[-n_new:] if n_new else []), False

    # -- control --------------------------------------------------------------
    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True,
                                       name=f"job-{self.id[:6]}")
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    # -- state machine --------------------------------------------------------
    def _recovering(self, kind: str, why: str):
        """RUNNING -> RECOVERING (a component failed; the job keeps going).
        Logged once per distinct reason, never as a job error."""
        with self.lock:
            changed = self.recovery != why
            entering = self.status == "running"
            if entering:
                self.status = "recovering"
                self.counters["recoveries"] += 1
            if self.status == "recovering":
                self.recovery, self._recovery_kind = why, kind
        if changed and (entering or self.status == "recovering"):
            self.say(f"Recovering: {why}")

    def _recovered(self, kind: str, what: str):
        """RECOVERING -> RUNNING once the failing component works again."""
        with self.lock:
            if self.status != "recovering" or (kind and self._recovery_kind
                                                and kind != self._recovery_kind):
                return
            self.status = "running"
            self.recovery, self._recovery_kind = "", ""
        self.say(f"Recovered: {what} - collection continues")

    def _enter_credit_wait(self, why: str):
        """Serper account out of credits: WAITING, not FAILED. Searches stop,
        in-flight crawls finish, progress is saved, and the controller checks
        the balance every CREDIT_POLL_S to resume automatically."""
        n = self.category_count()
        with self.lock:
            if self._credit_wait_since or self.status not in ("running", "recovering"):
                return
            self._credit_wait_since = time.time()
            self._next_credit_check = time.time() + CREDIT_POLL_S
            self._credit_ckpt_done = False
            self.status = "waiting"
            self.recovery = (f"Serper credits used up at {n}/{self.target}. Everything "
                             f"found is saved. Add credits at serper.dev and the "
                             f"collection continues by itself (balance checked every "
                             f"{CREDIT_POLL_S} s) - or press Stop.")
            self._recovery_kind = "credits"
        log.warning("paused for Serper credits: %s", why)
        self.say(f"Paused: {self.recovery}")

    def _check_credits(self):
        """Controller thread, while WAITING: save once, then poll the balance."""
        if not self._credit_ckpt_done:
            self._credit_ckpt_done = True
            self.checkpoint(force=True)
        now = time.time()
        if now < self._next_credit_check:
            return
        self._next_credit_check = now + CREDIT_POLL_S
        bal = self.provider.balance()
        if bal is None or bal <= 0:   # unknown, empty or overdrawn: keep waiting
            self.say(f"Still waiting for Serper credits (balance "
                     f"{'unknown' if bal is None else bal}) - progress "
                     f"{self.category_count()}/{self.target} is saved")
            return
        self.provider.reenable()
        with self.lock:
            self._credit_wait_since = 0.0
            self.status = "running"
            self.recovery, self._recovery_kind = "", ""
        self._last_new_record = self._last_stall_action = time.time()
        self.say(f"Resumed: Serper balance {bal} credits - collection continues "
                 f"from {self.category_count()}/{self.target}")

    def _preflight_credits(self):
        """Log the balance against what the rest of the target needs; start
        paused if the account is already empty."""
        bal = self.provider.balance()
        if bal is None:
            return
        need = max(0, self.target - self.category_count())
        est = math.ceil(need / 3)   # measured live: ~3-5 valid records per credit
        self.say(f"Serper balance: {bal} credits; the remaining {need} records "
                 f"need roughly {est} (at ~3 valid records per credit)")
        if bal <= 0:
            self._enter_credit_wait("balance is 0 at start")

    def _worker_error(self, info: dict, exc: BaseException):
        """A task crashed. Log everything needed to reproduce it; the pool,
        the other workers and the job carry on."""
        self._bump("worker_errors")
        log.error("worker error: worker=%s op=%s query=%r url=%s type=%s msg=%s at=%s",
                  info.get("worker") or threading.current_thread().name,
                  info.get("op", "?"), (info.get("query") or "")[:80],
                  (info.get("url") or "")[:120], type(exc).__name__,
                  str(exc)[:200], datetime.now().isoformat(timespec="seconds"),
                  exc_info=(type(exc), exc, exc.__traceback__))
        self.say(f"Worker error: {info.get('op', 'task')} "
                 f"{(info.get('url') or info.get('query') or '')[:70]} -> "
                 f"{type(exc).__name__}: {str(exc)[:80]} (isolated; job continues)")

    def done(self) -> bool:
        return (self.stop_event.is_set()
                or self.category_count() >= self.target)

    def category_count(self) -> int:
        with self.state.lock:
            return len(self.state.records[self.category])

    # -- record plumbing ------------------------------------------------------
    def _bump(self, key: str, n: int = 1):
        with self.lock:
            self.counters[key] += n

    def _note_failure(self, url: str, reason: str):
        if self._closed:   # straggler fetch after the job ended: not reported
            return
        self._bump("failed")
        with self.state.lock:
            self.state.stats[self.category]["failed_urls"] += 1
        self.say(f"Failed: {url[:80]} ({reason})")

    def _note_duplicate(self, name: str, why: str):
        self._bump("discovered")
        self._bump("duplicates")
        with self.state.lock:
            self.state.stats[self.category]["discovered"] += 1
            self.state.stats[self.category]["duplicates"] += 1
        self.say(f"Duplicate: {name[:50]} ({why})")

    def _try_add(self, record: dict) -> bool:
        """Validate + dedup + store one candidate record."""
        if self._closed:
            return False
        self._bump("discovered")
        with self.state.lock:
            self.state.stats[self.category]["discovered"] += 1
        t0 = time.perf_counter()
        ok, reason = validate(record, from_places=bool(record.get("_cid")))
        if ok:
            record["Confidence Score"] = confidence(record, self.terms)
        self.timings.add("validate", time.perf_counter() - t0)
        if not ok:
            self.say(f"Rejected: {record.get('Company Name', '?')[:50]} ({reason})")
            return False
        t0 = time.perf_counter()
        added, dup_of = self.state.registry.check_and_add(record, self.category)
        self.timings.add("dedup", time.perf_counter() - t0)
        if not added:
            self._bump("duplicates")
            with self.state.lock:
                self.state.stats[self.category]["duplicates"] += 1
            self.say(f"Duplicate: {record['Company Name'][:50]} (already have {dup_of})")
            return False
        kind = "places" if "_cid" in record else "organic"
        record.pop("_cid", None)
        lk = record.pop("_lk", "")
        if not self.state.append_record(self.category, record, self.target):
            return False
        self._last_new_record = time.time()
        self._bump("valid")
        self._bump(f"valid_{kind}")
        if lk:   # credit ledger: this search produced one more valid record
            self.state.ledger.attribute_valid(lk)
        if self.planner is not None:
            self.planner.on_valid(record.get("Search Query", ""), kind)
        if record.get("Business Email"):
            self._bump("emails")
        if record.get("Business Phone"):
            self._bump("phones")
        if record.get("Official Website"):
            self._bump("websites")
        self.say(f"Found: {record['Company Name'][:60]}  "
                 f"email={'yes' if record.get('Business Email') else 'no'} "
                 f"phone={'yes' if record.get('Business Phone') else 'no'}")
        if self.status == "recovering" and self._recovery_kind != "search":
            self._recovered("", "new records are being stored again")
        return True

    def _fetch_analyze(self, url: str, *, extract: bool = True,
                       contact_links: bool = True, mine: bool = False):
        """Fetch in this thread (pure network I/O), then parse + extract in
        the analysis process pool (CPU, off the GIL).
        Returns (analysis dict or None, final URL, failure reason)."""
        raw, final_url, reason = self.fetcher.fetch_raw(url)
        if raw is None:
            return None, final_url, reason
        t0 = time.perf_counter()
        res = analysis.analyze(raw, final_url, extract, contact_links, mine,
                               config.DIRECTORY_CANDIDATES_CAP)
        self.timings.add("analyze_wait", time.perf_counter() - t0)
        if "error" in res:
            return None, final_url, res["error"]
        self.timings.add("parse", res.get("parse_s", 0.0))
        if res.get("extract_s"):
            self.timings.add("extract", res["extract_s"])
        return res, final_url, ""

    @staticmethod
    def _to_extracted(res: dict) -> Extracted:
        ex = Extracted()
        ex.name = res.get("name", "") or ""
        ex.email = list(res.get("email") or [])
        ex.phone = list(res.get("phone") or [])
        ex.address = res.get("address", "") or ""
        ex.services = res.get("services", "") or ""
        return ex

    def _split_geo(self, address: str) -> tuple[str, str, str]:
        """Parse city/state/country from the address; blanks fall back to the
        user's structured location fields (the query was scoped to them)."""
        hint = "" if any(self.geo.values()) else self.location
        city, state_, country = split_address(address, hint)
        return (city or self.geo["city"], state_ or self.geo["state"],
                country or self.geo["country"])

    def _base_record(self, query: str) -> dict:
        return {
            "Category": self.display,
            "Subcategory": "",
            "Search Query": query,
            "Collected At": datetime.now().isoformat(timespec="seconds"),
        }

    # -- crawling -------------------------------------------------------------
    @staticmethod
    def _crawl_key(url: str) -> str:
        """Directory/list pages are keyed by URL (clutch.co/austin and
        clutch.co/dallas list different firms); company sites by domain."""
        if is_skip_domain(url):
            p = urlparse(url)
            return f"{norm_domain(url)}{p.path.rstrip('/')}".lower()
        return norm_domain(url)

    def _mark_attempted(self, url: str) -> bool:
        """True if this crawl target was already attempted (this run or a
        previous one) or already produced a record."""
        key = self._crawl_key(url)
        if not key:
            return True
        with self.lock:
            if key in self.attempted:
                return True
            self.attempted.add(key)
        with self.state.registry.lock:
            if key in self.state.registry.domains:
                return True
        return not self.state.mark_crawled(key, self.category)

    def crawl_site(self, url: str, query: str, allow_mining: bool,
                   lk: str = "") -> list[str]:
        """Crawl one discovered site; may add a record. Returns directory-mined
        candidate URLs."""
        if self.done() or self._mark_attempted(url):
            return []
        skip = is_skip_domain(url)
        if skip and not allow_mining:
            return []
        self.say(f"Crawling: {url[:90]}")
        # directories/list pages are only mined for links, never extracted
        res, final_url, reason = self._fetch_analyze(
            url, extract=not skip, contact_links=not skip, mine=allow_mining)
        if res is None:
            self._note_failure(url, reason)
            return []

        candidates: list[str] = []
        if allow_mining:
            links = res.get("external_links") or []
            if is_aggregator(final_url) or len(links) >= LIST_PAGE_MIN_EXTERNAL_LINKS:
                candidates = links
                self.say(f"Directory source: {norm_domain(final_url)} "
                         f"-> {len(links)} company links mined")
        if skip or is_skip_domain(final_url):
            return candidates

        if res.get("blocked"):
            self._note_failure(final_url, f"blocked/error page ({res.get('title', '')[:40]})")
            return candidates

        data = self._to_extracted(res)
        source_page = final_url
        # Homepage already has email + phone (+ address, unless
        # CONTACT_PAGES_FOR_ADDRESS is off): stop, no more pages. Otherwise
        # follow at most 2 contact/about links, stopping early.
        if not (data.email and data.phone
                and (data.address or not config.CONTACT_PAGES_FOR_ADDRESS)):
            for cp in (res.get("contact_links") or [])[:2]:
                if self.done():
                    break
                r2, fu2, _ = self._fetch_analyze(cp, contact_links=False)
                if r2 is None or r2.get("blocked"):
                    continue
                before = (bool(data.email), bool(data.phone), bool(data.address))
                data.merge_missing(self._to_extracted(r2))
                if (bool(data.email), bool(data.phone), bool(data.address)) != before:
                    source_page = fu2
                if data.email and data.phone and data.address:
                    break

        if not (data.phone or data.email):
            return candidates  # quality gate: a firm site has a contact channel

        p = urlparse(final_url)
        website = f"{p.scheme}://{p.netloc.lower()}"
        city, state_, country = self._split_geo(data.address)
        record = self._base_record(query) | {
            "Company Name": data.name or norm_domain(final_url),
            "Official Website": website,
            "Business Email": "; ".join(data.email),
            "Business Phone": "; ".join(data.phone),
            "Full Business Address": data.address,
            "City": city, "State": state_, "Country": country,
            "Industry": self.display,
            "Services": data.services,
            "Source URL": url,
            "Source Page": source_page,
            "_lk": lk,
        }
        self._try_add(record)
        return candidates

    def enrich_place(self, place: dict, query: str, lk: str = ""):
        """Turn one Google Places row into a record; crawl its site for email."""
        if self.done():
            return
        title = clean_text(place.get("title"))
        if not title:
            return
        website = clean_text(place.get("website"))
        cid = str(place.get("cid") or place.get("placeId") or "")
        email: list[str] = []
        industry = clean_text(place.get("category"))
        services = industry
        site_url = ""
        source_page = ""
        if website.lower().startswith("http") and not is_skip_domain(website):
            p = urlparse(website)
            site_url = f"{p.scheme}://{p.netloc.lower()}"
            if not self._mark_attempted(website):
                self.say(f"Crawling: {site_url[:90]}")
                res, final_url, reason = self._fetch_analyze(site_url)
                if res is None:
                    self._note_failure(site_url, reason)
                else:
                    if not res.get("blocked"):
                        data = self._to_extracted(res)
                        source_page = final_url
                        # Places already supplied phone + address; only chase
                        # the email, via at most one contact page.
                        if not data.email:
                            for cp in (res.get("contact_links") or [])[:1]:
                                r2, fu2, _ = self._fetch_analyze(cp, contact_links=False)
                                if r2 is not None and not r2.get("blocked"):
                                    data.merge_missing(self._to_extracted(r2))
                                    if data.email:
                                        source_page = fu2
                        email = data.email
                        if data.services:
                            services = f"{services} | {data.services}" if services else data.services
                        fp = urlparse(final_url)
                        if fp.netloc:
                            site_url = f"{fp.scheme}://{fp.netloc.lower()}"
        address = clean_text(place.get("address"))
        city, state_, country = self._split_geo(address)
        record = self._base_record(query) | {
            "Company Name": title[:150],
            "Official Website": site_url,
            "Business Email": "; ".join(email),
            "Business Phone": clean_text(place.get("phoneNumber")),
            "Full Business Address": address,
            "City": city, "State": state_, "Country": country,
            "Industry": industry or self.display,
            "Services": services[:500],
            "Source URL": f"https://www.google.com/maps?cid={cid}" if cid else "google places",
            "Source Page": source_page,
            "_cid": cid,
            "_lk": lk,
        }
        self._try_add(record)

    # -- provider counters ----------------------------------------------------
    def _sync_provider_counters(self):
        if self.provider is not None:
            with self.lock:
                self.counters["search_requests"] = self.provider.requests_made
                self.counters["serper_credits"] = getattr(self.provider, "credits_used", 0)
                self.counters["serper_ok"] = getattr(self.provider, "ok_count", 0)
                self.counters["serper_failed"] = getattr(self.provider, "fail_count", 0)

    # -- crawl task tracking --------------------------------------------------
    def _guarded(self, info: dict):
        """Runs one pool task. Any exception is isolated to this task: logged
        with worker / operation / query / URL, counted, and swallowed so the
        worker thread, the pool and the job all keep running."""
        info["t"] = time.time()
        info["worker"] = threading.current_thread().name
        with self._run_lock:
            self._running[id(info)] = info
        try:
            return info["fn"](*info["args"])
        except Exception as exc:
            self._worker_error(info, exc)
            return None
        finally:
            with self._run_lock:
                self._running.pop(id(info), None)

    def _submit_task(self, op: str, url: str, query: str, fn, *args,
                     on_result=None, info: dict | None = None, prio: int = 0):
        """Submit fn(*args) to the crawl pool, tracked for backlog accounting,
        the stuck-task watchdog and pool replacement."""
        if self.done() or self._pool is None:
            return None
        if info is None:
            info = {"op": op, "url": url, "query": query, "fn": fn,
                    "args": args, "on_result": on_result, "prio": prio}
        info.update(fut=None, t=None, worker="", abandoned=False)
        with self._run_lock:
            self._tasks[id(info)] = info
        try:
            fut = self._pool.submit(self._guarded, info, prio=info.get("prio", 0))
        except RuntimeError:   # pool shutting down (job ending / being replaced)
            with self._run_lock:
                self._tasks.pop(id(info), None)
            return None
        info["fut"] = fut
        with self._inflight_lock:
            self._inflight.add(fut)

        def _done(f, info=info):
            with self._inflight_lock:
                self._inflight.discard(f)
            with self._run_lock:
                if self._tasks.get(id(info)) is info and info.get("fut") is f:
                    self._tasks.pop(id(info), None)
            if f.cancelled():
                return
            self._last_task_done = time.time()
            self._bump("tasks_done")
            cb = info.get("on_result")
            if cb is not None and not self.done():
                try:
                    cb(f.result())
                except Exception as exc:
                    self._worker_error(dict(info, op=f"{info['op']}:callback"), exc)
            if self.status == "recovering" and self._recovery_kind == "crawl":
                self._recovered("crawl", "crawl workers completing tasks again")
        fut.add_done_callback(_done)
        return fut

    def _crawl_backlog(self) -> int:
        with self._inflight_lock:
            return len(self._inflight)

    def _submit_crawl(self, url: str, query: str, allow_mining: bool, lk: str = ""):
        def mine(candidates):
            for u in (candidates or []):
                if self.done():
                    return
                if self._claim_result(norm_domain(u)):
                    self._submit_crawl(u, query, False, lk)
        if self._submit_task("crawl", url, query, self.crawl_site, url, query,
                             allow_mining, lk,
                             on_result=mine if allow_mining else None,
                             prio=PRIO_RESULT if allow_mining else PRIO_MINED) is not None:
            self._bump("candidate_urls")

    def _submit_place(self, place: dict, query: str, lk: str = ""):
        site = clean_text(place.get("website") or "") or clean_text(place.get("title") or "")
        if self._submit_task("enrich_place", site, query, self.enrich_place,
                             place, query, lk, prio=PRIO_PLACE) is not None:
            self._bump("candidate_urls")

    # -- search ---------------------------------------------------------------
    def _timed_search(self, kind: str, query: str, page: int = 1):
        """One credit-gated search request. Returns (results, source, credits)
        with source "api" (paid), "cache" (free re-use of a stored response),
        "executed" (already paid for earlier and not cached: skipped - its
        businesses are in the dedup registry / crawl memory already) or
        "target" (the target was reached while this search waited).
        Checks run cheapest-first; only the last step spends a credit."""
        key = ledger_key(kind, query, page)     # normalized request identity
        if self.done():
            self._bump("skipped_target")
            return [], "target", 0
        cached = self.state.cache_get(key)
        if cached is not None:
            self._bump("cache_hits")
            self.say(f"Reused cached results for \"{query}\" ({kind}) - "
                     f"no Serper credits spent")
            return list(cached), "cache", 0
        # In-flight dedup: if this exact request is already on the wire,
        # wait for THAT one instead of paying for a second copy.
        with self._sf_lock:
            ev = self._search_inflight.get(key)
            owner = ev is None
            if owner:
                self._search_inflight[key] = ev = threading.Event()
        if not owner:
            ev.wait(timeout=60)
            cached = self.state.cache_get(key)
            if cached is not None:
                self._bump("cache_hits")
                return list(cached), "cache", 0
            return [], "executed", 0   # the original attempt failed; don't re-pay
        t0 = time.perf_counter()
        paid = False
        try:
            prev = self.state.ledger.executed(key)
            if prev is not None:   # persistent executed-query registry
                self._bump("skipped_executed")
                self.state.ledger.note_skip()
                self.say(f"Skipped \"{query}\" ({kind}): the same search was "
                         f"already paid for ({prev.get('category', '?')}, "
                         f"{prev.get('new', 0)} new results then) - no credit spent")
                return [], "executed", 0
            if self.done():        # last check right before paying
                self._bump("skipped_target")
                return [], "target", 0
            paid = True
            if kind == "organic":
                result = (self.provider.organic(query, config.ORGANIC_RESULTS_PER_QUERY)
                          if page == 1 else self.provider.organic_page(query, page))
            else:
                result = self.provider.places_page(query, page)
            self.state.cache_put(key, list(result))
            # Serper's own per-response credit count (1 per page today),
            # thread-local in the provider so parallel searches don't mix
            credits = int(getattr(self.provider, "last_credits", 1) or 1)
            return result, "api", credits
        finally:
            if paid:
                self._bump(f"{kind}_requests")
                dt = time.perf_counter() - t0
                self.timings.add(f"search_{kind}", dt)
                with self.lock:
                    if len(self._latencies) < 400:
                        self._latencies.append((dt, kind, query))
            with self._sf_lock:
                self._search_inflight.pop(key, None)
            ev.set()

    def _claim_result(self, ident: str, check_crawled: bool = True) -> bool:
        """True if this search-result identity is NEW (not seen this run, not
        already a record, and - for crawl-only candidates - not crawled in an
        earlier run). Claims it."""
        if not ident:
            return False
        with self.lock:
            if ident in self._seen_ids:
                return False
            self._seen_ids.add(ident)
        with self.state.registry.lock:
            if ident in self.state.registry.domains:
                return False
        if not check_crawled:
            return True
        with self.state.lock:
            return ident not in self.state.crawled

    def _process_organic(self, urls: list[str], req: SearchRequest, lk: str = "") -> int:
        new = 0
        for u in urls:
            if is_skip_domain(u) and not is_aggregator(u):
                continue   # social / news / gov: never a company, never mined
            if not self._claim_result(self._crawl_key(u)):
                self._bump("skipped_known")
                continue
            new += 1
            self._submit_crawl(u, req.query, True, lk)
        return new

    def _process_places(self, places: list[dict], req: SearchRequest, lk: str = "") -> int:
        new = 0
        for p in places:
            title = clean_text(p.get("title") or "")
            website = clean_text(p.get("website") or "")
            cid = str(p.get("cid") or p.get("placeId") or "")
            dom = norm_domain(website) if website.lower().startswith("http") else ""
            ident = dom or (f"cid:{cid}" if cid else f"name:{title.lower()}")
            # A listing whose site was crawled in an earlier run is still a
            # valid Places record (enrich_place just skips the re-crawl).
            if not self._claim_result(ident, check_crawled=False):
                self._bump("skipped_known")
                if title:
                    self._note_duplicate(title, "already found - no crawl spent")
                continue
            # Pre-crawl dedup against stored records (cid / name / phone /
            # domain): a business we already have costs no crawl at all.
            owner = self.state.registry.peek({
                "Company Name": title, "Official Website": website,
                "Business Phone": clean_text(p.get("phoneNumber") or ""),
                "_cid": cid})
            if owner:
                self._note_duplicate(title, f"already have {owner}")
                continue
            new += 1
            self._submit_place(p, req.query, lk)
        return new

    def _search_task(self, req: SearchRequest):
        """One Serper request, processed the moment it returns (dedup ->
        crawl submission happen in this thread, overlapping other searches).
        Failure isolation: a failed request is re-queued by the planner with
        backoff (bounded), a failing result-processing step is logged and
        skipped. Only ProviderDisabled (auth / credits gone) propagates."""
        if self.done():
            self.planner.on_skipped(req)
            return
        attempt = int(req.meta.get("attempt", 0))
        label = (f"{req.kind}" if req.page == 1 else f"{req.kind} page {req.page}")
        if attempt:
            self._bump("search_retries")
            label += f", retry {attempt}"
        elif req.page > 1:
            self._bump("places_followups")
        self.say(f"Searching: \"{req.query}\" ({label})")
        try:
            results, source, credits = self._timed_search(req.kind, req.query, req.page)
            results = results or []
        except ProviderDisabled as exc:
            if exc.kind == "credits":   # pause, keep this search for later
                self.planner.requeue(req)
                self._enter_credit_wait(str(exc))
                return
            self.planner.on_skipped(req)
            raise
        except Exception as exc:
            err = f"{type(exc).__name__}: {str(exc)[:120]}"
            # a search that timed out may already have been billed: re-queue
            # it at most once instead of paying for it up to 3 more times
            timed_out = getattr(exc, "kind", "") == "timeout"
            if timed_out:
                self._bump("timeouts_maybe_charged",
                           int(getattr(self.provider, "last_timeouts", 1) or 1))
            outcome = self.planner.on_failed(
                req, err, min(1, config.SEARCH_RETRIES) if timed_out
                else config.SEARCH_RETRIES)
            self._bump("search_failures")
            if outcome == "failed":
                self._bump("searches_failed")
            with self.lock:
                self._search_fail_streak += 1
                streak = self._search_fail_streak
                if streak >= SEARCH_FAIL_STREAK:
                    # parallel requests that failed together count once each,
                    # so the exponent grows gently and the pause is capped
                    pause = min(SEARCH_PAUSE_MAX_S, SEARCH_PAUSE_BASE_S
                                * 2 ** min(8, (streak - SEARCH_FAIL_STREAK) // 2))
                    self._search_pause_until = max(self._search_pause_until,
                                                   time.time() + pause)
            self.say(f"Search failed: \"{req.query[:60]}\" ({label}) - {err} -> "
                     + ("re-queued with backoff" if outcome == "retry"
                        else "recorded as failed; moving on"))
            if streak >= SEARCH_FAIL_STREAK:
                self._recovering("search", f"search worker: {streak} Serper requests "
                                 f"failed in a row ({err[:60]}); retrying with backoff")
            return
        if source in ("target", "executed"):
            # nothing was sent: release the request (no yield to learn from)
            self.planner.on_result(req, 0, 0, credits=0)
            return
        with self.lock:
            self._search_fail_streak = 0
            self._search_pause_until = 0.0
        self._last_search_ok = time.time()
        if self.status == "recovering" and self._recovery_kind == "search":
            self._recovered("search", "Serper is answering again")
        self._sync_provider_counters()
        lk = ledger_key(req.kind, req.query, req.page)
        late = source == "api" and self.done()   # paid, but target already met
        timeouts = int(getattr(self.provider, "last_timeouts", 0) or 0) \
            if source == "api" else 0
        # ledger entry first: records produced while the results are being
        # processed are attributed to it (valid-per-query accounting)
        self.state.ledger.record(
            lk, kind=req.kind, query=req.query, page=req.page, category=self.category,
            credits=credits, results=len(results), new=0, source=source,
            timeouts=timeouts)
        new = 0
        try:
            if req.kind == "places":
                new = self._process_places(results, req, lk)
            else:
                new = self._process_organic(results, req, lk)
        except Exception as exc:   # one bad result set never kills the job
            self._worker_error({"op": f"process_{req.kind}_results",
                                "query": req.query, "url": ""}, exc)
        if source == "api":
            self.state.ledger.add_new(lk, new)
        if source == "api":
            self._bump(f"{req.kind}_credits", credits)
            self._bump("timeouts_maybe_charged", timeouts)
            if late:
                self._bump("credits_after_target", credits)
        self.planner.on_result(req, len(results), new, credits=credits)
        self._bump("searches_done")
        self._bump("search_results", len(results))
        self._bump("new_results", new)
        self._bump("duplicate_results", len(results) - new)
        with self.lock:
            self._recent_new.append(new)   # watchdog: duplicate-heavy detection
        with self.state.lock:
            self.state.done_queries.add(f"{self.category}||{req.query.lower()}")
        dup = len(results) - new
        cost = f"{credits} credit" if source == "api" else "cached, 0 credits"
        self.say(f"{req.kind.title()} results for \"{req.query[:60]}\": "
                 f"{len(results)} ({new} new, {dup} already known; {cost})")

    # -- credit-aware scheduling ---------------------------------------------
    def _want_organic(self) -> bool:
        """Should the next cell ALSO be searched organically (a second credit)?
        Decided on measured valid-records-per-credit per search type, which
        the planner keeps per category across runs:
          * PLACES_FIRST (small hosts): no, while Places yields at least
            PLACES_FIRST_MIN_YIELD records per credit - an organic record
            costs ~3x the crawling of a Places one
          * until both types have KIND_WARMUP_CREDITS credits: yes (measure)
          * organic >= ORGANIC_PAIR_MIN x Places: yes
          * otherwise 1 cell in ORGANIC_SAMPLE_EVERY (keeps measuring it);
            the planner still searches organically wherever Places is thin
            and offers organic for every Places-only cell in the deep tier.
          * endgame (the remaining gap fits in ~2 searches): only the better
            type is paid for.
        The old rule kept pairing organic down to 50% of Places' efficiency,
        i.e. accepted paying one credit for half the records of another."""
        if self.provider is None or not self.provider.supports_places:
            return True
        if config.PLACES_FIRST and not self._places_weak():
            return False
        eff = self._kind_eff()
        if eff is None:
            return True
        eff_o, eff_p = eff
        need = self.target - self.category_count()
        if need <= 2 * max(eff_o, eff_p, 0.5):
            return eff_o > eff_p
        if eff_o >= ORGANIC_PAIR_MIN * eff_p:
            return True
        return self.planner.cells_issued % ORGANIC_SAMPLE_EVERY == 0

    def _want_places(self, want_organic: bool) -> bool:
        """The mirror image of _want_organic. Live A/B (Recruitment / USA):
        with list pages mined up to 60 links, organic earned 7.6 valid
        records per credit vs 6.0 for Places (old script: 14.6 vs 5.1), so
        Places is not automatically the better credit either. Whichever type
        measures clearly weaker is only sampled; the deep tier still offers
        Places for every organic-only cell."""
        if self.provider is None or not self.provider.supports_places:
            return False
        if not want_organic:
            return True
        eff = self._kind_eff()
        if eff is None:
            return True
        eff_o, eff_p = eff
        need = self.target - self.category_count()
        if need <= 2 * max(eff_o, eff_p, 0.5):
            return eff_p >= eff_o
        if eff_p >= ORGANIC_PAIR_MIN * eff_o:
            return True
        return self.planner.cells_issued % ORGANIC_SAMPLE_EVERY == 0

    def _places_weak(self) -> bool:
        """PLACES_FIRST gives way once Places has had a fair trial for this
        category and yields under PLACES_FIRST_MIN_YIELD records per credit."""
        credits, records = self.planner.kind_efficiency()["places"]
        return (credits >= 2 * KIND_WARMUP_CREDITS
                and records < config.PLACES_FIRST_MIN_YIELD * credits)

    def _kind_eff(self) -> tuple[float, float] | None:
        """(organic, places) valid records per credit, or None while warming
        up. Warm-up counts requests SENT (results of parallel searches arrive
        late; counting returned credits stretched the warm-up to ~9 pairs)."""
        if (self.planner.issued("organic") < KIND_WARMUP_CREDITS
                or self.planner.issued("places") < KIND_WARMUP_CREDITS):
            return None
        eff = self.planner.kind_efficiency()
        (oc, orr), (pc, prr) = eff["organic"], eff["places"]
        # shrunk toward a common prior: a few early records can't flip it
        prior = (orr + prr + 3.0) / (oc + pc + 1.0)
        return ((orr + 2 * prior) / (oc + 2), (prr + 2 * prior) / (pc + 2))

    def _valid_per_credit(self) -> float:
        """Measured valid records per paid credit this run (prior ~3, the
        live average over earlier runs)."""
        with self.lock:
            c = self.counters
            paid = c["places_credits"] + c["organic_credits"]
            return (c["valid"] + 6.0) / (paid + 2.0)

    def _expected_pending(self, searches_inflight: int, backlog: int) -> float:
        with self.lock:
            done, valid = self.counters["tasks_done"], self.counters["valid"]
            sd, cand = self.counters["searches_done"], self.counters["candidate_urls"]
        vpc = (valid + 2.0) / (done + 5.0)                  # valid per candidate
        cps = (cand + 12.0) / (sd + 2.0)                    # candidates per search
        return backlog * vpc + searches_inflight * cps * vpc

    def _may_search(self, searches_inflight: int) -> bool:
        if self.done() or self._stop_search or self._credit_wait_since:
            return False
        if time.time() < self._search_pause_until:   # failure streak backoff
            return False
        self._sync_provider_counters()
        if (self.credit_budget
                and self.counters["serper_credits"] + searches_inflight >= self.credit_budget):
            self._stop_search = "budget"
            self.say(f"Credit safety cap reached ({self.credit_budget} credits = "
                     f"{config.MAX_CREDITS_PER_RECORD} x target) - no further "
                     f"searches; finishing in-flight work. Raise or disable "
                     f"MAX_CREDITS_PER_RECORD in app/.env to go further.")
            return False
        # Slow start: measure real yield before scaling out. (Measured: with
        # 16 searches fired blind at t=0, 29 credits were committed before
        # the first crawl finished and ~40% of their candidates were never
        # needed.)
        if self.counters["searches_done"] < SLOW_START and searches_inflight >= SLOW_START:
            return False
        backlog = self._crawl_backlog()
        if backlog >= config.SEARCH_BACKLOG_LIMIT * self.crawl_workers:
            return False
        need = self.target - self.category_count()
        if need <= 0:
            return False
        # Dynamic search budget: never have more searches in flight than the
        # remaining gap needs (2,000 left -> full concurrency; 3 left -> 1).
        # Every search still on the wire when the target is hit is a wasted
        # credit, so the budget shrinks as the target gets close.
        max_inflight = max(1, min(self._search_cap(),
                                  math.ceil(need / self._valid_per_credit())))
        if searches_inflight >= max_inflight:
            return False
        return self._expected_pending(searches_inflight, backlog) < need

    def _search_cap(self) -> int:
        """This job's fair share of the account-wide search concurrency."""
        try:
            share = max(1, int(self.search_share()))
        except Exception:
            share = 1
        return max(1, math.ceil(config.SERPER_CONCURRENCY / share))

    # -- checkpoint / export ---------------------------------------------------
    def checkpoint(self, force: bool = False):
        """Job meta + planner memory go into the shared state every
        STATE_SAVE_INTERVAL. Normally the state's saver thread then writes
        the checkpoint (coalesced across all running jobs; a crash loses at
        most a few seconds of work) and this category's Excel file (at most
        every EXPORT_MIN_INTERVAL). force=True (job end, credit pause,
        failure) writes both before returning."""
        if not force and time.time() - self._last_ckpt < config.STATE_SAVE_INTERVAL:
            return
        self._last_ckpt = time.time()
        try:
            t0 = time.perf_counter()
            meta = self._job_meta()
            memory = self.planner.export() if self.planner else None
            with self.state.lock:
                self.state.jobs[self.category] = meta
                if memory is not None:
                    self.state.discovery[self.category] = memory
            if not force:
                self.state.request_save(self.state.path, export_category=self.category)
                return
            self.state.save(self.state.path)
            self.timings.add("export_state", time.perf_counter() - t0)
            t0 = time.perf_counter()
            self.state.export_categories([self.category])
            self._last_export = time.time()
            self.timings.add("export_excel", time.perf_counter() - t0)
        except Exception as exc:
            self.say(f"Checkpoint failed: {str(exc)[:120]}")

    def export_all(self):
        self.state.export_categories(list(self.state.all_categories()))

    # -- main loop ------------------------------------------------------------
    def _make_planner(self) -> DiscoveryPlanner:
        prefix = f"{self.category}||"
        with self.state.lock:
            legacy = {q[len(prefix):] for q in self.state.done_queries
                      if q.startswith(prefix)}
            saved = self.state.discovery.get(self.category)
        return DiscoveryPlanner(
            self.phrases, self.geos, saved=saved, legacy_done=legacy,
            places=self.provider.supports_places, max_page=config.PLACES_MAX_PAGE,
            extra_phrases=self.extra_phrases, extra_geos=self.extra_geos)

    def _run(self):
        self.status = "running"
        self.started_at = datetime.now().isoformat(timespec="seconds")
        t0 = self.run_started = time.time()
        self._last_new_record = self._last_task_done = self._last_stall_action = t0
        try:
            self.provider = make_provider(self.provider_name, config.serper_api_key())
            if hasattr(self.provider, "on_event"):
                def _ev(kind: str):
                    if kind == "429":
                        self._bump("serper_429")
                    elif kind == "timeout":
                        self._bump("serper_timeouts")
                self.provider.on_event = _ev
            self.planner = self._make_planner()
            self._preflight_credits()
            self.say(f"Job started: {self.display} | target {self.target} | "
                     f"provider {self.provider_name} | "
                     f"{len(self.planner.phrases)} search phrases x "
                     f"{len(self.planner.geos)} locations "
                     f"({self.planner.candidate_cells()} unused searches, "
                     f"{self.planner.dropped_rephrasings} trivial rephrasings "
                     f"dropped) | credit budget {self.credit_budget}")
            if self.category_count() > 0:
                self.say(f"Resuming: {self.category_count()}/{self.target} records "
                         f"already collected for this category; "
                         f"{self.planner.pending_retries()} previously failed "
                         f"search(es) will be retried")
            self._pool = self._new_crawl_pool()
            spool = ThreadPoolExecutor(max_workers=config.SERPER_CONCURRENCY,
                                       thread_name_prefix=f"search-{self.id[:6]}")
            try:
                reason = self._control_loop(spool)
            finally:
                # Stop immediately: queued work is cancelled; running fetches
                # finish in the background but can no longer add records.
                self._closed = True
                spool.shutdown(wait=False, cancel_futures=True)
                if self._pool is not None:
                    self._pool.shutdown(wait=False, cancel_futures=True)
            self._finish(reason, t0)
        except ProviderDisabled as exc:
            self._fail(f"Search provider disabled: {exc}", str(exc))
        except JobFailed as exc:
            self._fail(f"Job failed: {exc}", str(exc))
        except Exception as exc:   # setup / finalisation bug: still logged exactly
            log.exception("job crashed")
            self._fail(f"Job failed: {type(exc).__name__}: {exc}",
                       f"{type(exc).__name__}: {exc}")
        finally:
            self.finished_at = datetime.now().isoformat(timespec="seconds")
            cb = self.on_finish
            if cb is not None:
                try:
                    cb(self)
                except Exception:
                    log.exception("job finish callback failed")

    def _new_crawl_pool(self) -> PriorityThreadPool:
        return PriorityThreadPool(
            max_workers=self.crawl_workers,
            thread_name_prefix=f"crawl{self.counters['pool_restarts'] or ''}")

    # -- controller -----------------------------------------------------------
    def _control_loop(self, spool: ThreadPoolExecutor) -> str:
        """Drive the pipeline until the target is reached, the user stops,
        or the search space / credit cap is exhausted. A failing iteration
        puts the job in RECOVERING and is retried with backoff; only
        MAX_CONTROLLER_ERRORS consecutive failures make it FAILED.
        Returns "" (target reached / stopped), "exhausted" or "budget"."""
        searches: dict = {}   # future -> SearchRequest
        errors = 0
        while not self.done():
            try:
                reason = self._control_step(spool, searches)
                if errors:
                    errors = 0
                    self._recovered("controller", "controller loop healthy again")
                if reason:
                    return reason
            except (ProviderDisabled, JobFailed):
                raise
            except Exception as exc:
                errors += 1
                self._bump("controller_errors")
                log.exception("controller loop error %d/%d", errors,
                              config.MAX_CONTROLLER_ERRORS)
                if errors >= config.MAX_CONTROLLER_ERRORS:
                    raise JobFailed(
                        f"collection controller failed {errors} times in a row; "
                        f"last error {type(exc).__name__}: {exc}") from exc
                self._recovering("controller", f"controller error {type(exc).__name__}: "
                                 f"{str(exc)[:80]} (retry {errors}/"
                                 f"{config.MAX_CONTROLLER_ERRORS})")
                time.sleep(min(10.0, 0.25 * 2 ** errors))
        return ""

    def _control_step(self, spool: ThreadPoolExecutor, searches: dict) -> str:
        # 1. harvest finished searches (_search_task isolates its own errors;
        #    anything escaping it is logged here and the search re-queued)
        for f in [f for f in searches if f.done()]:
            req = searches.pop(f)
            exc = None if f.cancelled() else f.exception()
            if isinstance(exc, ProviderDisabled):
                raise exc
            if exc is not None:
                self._worker_error({"op": "search", "query": req.query, "url": ""}, exc)
                self.planner.on_failed(req, f"{type(exc).__name__}: {exc}",
                                       config.SEARCH_RETRIES)
        if self._credit_wait_since:   # paused for credits: poll, don't search
            self._check_credits()
        # 2. keep up to SERPER_CONCURRENCY searches in flight while the work
        #    already in the pipeline can't reach the target; when the current
        #    strategy tier has nothing left, escalate instead of stopping
        planner_empty = False
        # measuring page-2 novelty costs a few credits once per category;
        # worth it only when enough of the target remains to use what it learns
        self.planner.allow_explore = (self.target - self.category_count()
                                      >= EXPLORE_MIN_REMAINING)
        while (len(searches) < self._search_cap()
               and self._may_search(len(searches))):
            want_organic = self._want_organic()
            reqs = self.planner.next_requests(want_organic,
                                              self._want_places(want_organic))
            if not reqs:
                if searches or self.planner.pending_retries():
                    break              # wait for results / retry backoff
                if not self._escalate("the current searches are used up"):
                    planner_empty = True
                    break
                continue
            first = reqs[0]
            if first.page == 1 and not first.meta.get("attempt"):
                self._bump("queries_executed")
            for r in reqs:
                try:
                    searches[spool.submit(self._search_task, r)] = r
                except RuntimeError:   # search pool shutting down
                    self.planner.on_skipped(r)
        # 3. watchdog: stuck tasks, stalled stages, strategy refresh
        self._watchdog(searches)
        # 4. terminal only when nothing is in flight and nothing is left
        if not searches and self._crawl_backlog() == 0:
            if self._stop_search == "budget":
                return "budget"
            if planner_empty:
                return "exhausted"
        # 5. wait for the next event
        if searches:
            wait(list(searches), timeout=0.25, return_when=FIRST_COMPLETED)
        else:
            time.sleep(0.25)
        self.checkpoint()
        return ""

    def _escalate(self, why: str) -> bool:
        prev = self.planner.tier_name
        tier = self.planner.escalate()
        if tier is None:
            return False
        self._bump("strategy_escalations")
        self.say(f"Search strategy: {why} at {self.category_count()}/{self.target} "
                 f"('{prev}' tier) - generating new searches: '{tier}' tier, "
                 f"{TIER_HELP.get(tier, '')} "
                 f"({self.planner.candidate_cells()} new phrase x location searches)")
        return True

    # -- watchdog -------------------------------------------------------------
    def _watchdog(self, searches: dict):
        now = time.time()
        # A) crawl tasks stuck far beyond every timeout: stop counting them
        #    as pipeline work, and replace the pool if they jam it
        with self._run_lock:
            running = list(self._running.values())
        stuck = [i for i in running if not i["abandoned"] and i["t"]
                 and now - i["t"] > config.STUCK_TASK_S]
        for info in stuck:
            info["abandoned"] = True
            fut = info.get("fut")
            if fut is not None:
                with self._inflight_lock:
                    self._inflight.discard(fut)
            self._bump("abandoned_tasks")
            log.warning("watchdog: abandoning stuck task worker=%s op=%s query=%r "
                        "url=%s running=%ds", info["worker"], info["op"],
                        (info["query"] or "")[:60], (info["url"] or "")[:100],
                        now - info["t"])
        if stuck:
            self._recovering("crawl", f"crawl workers: {len(stuck)} task(s) stuck "
                             f">{config.STUCK_TASK_S}s abandoned "
                             f"(e.g. {(stuck[0]['url'] or '')[:60]})")
        jammed = [i for i in running if i["abandoned"] and not i.get("pool_replaced")]
        if len(jammed) >= max(4, self.crawl_workers // 4):
            self._replace_crawl_pool(f"{len(jammed)} workers blocked by stuck tasks")

        # B) no new record for a while: diagnose, then act (never terminate).
        #    Not while paused for credits - that stall has a known cause.
        idle = now - self._last_new_record
        if (self._credit_wait_since or idle < config.WATCHDOG_STALL_S
                or now - self._last_stall_action < config.WATCHDOG_STALL_S):
            return
        self._last_stall_action = now
        self._bump("watchdog_actions")
        stage, detail = self._diagnose(searches, now)
        actions = []
        n = self.planner.expedite_retries()
        if n:
            actions.append(f"retrying {n} failed search(es) now")
        long_stall = idle >= config.WATCHDOG_BOTTLENECK_S
        if stage == "crawl stalled" and long_stall:
            self._replace_crawl_pool("crawl stage stalled")
            actions.append("replaced the crawl worker pool")
        if stage in ("duplicate-heavy results", "search idle") or (
                stage == "low yield" and long_stall):
            if self._escalate(f"{stage} for {int(idle)}s"):
                actions.append("switched to a wider search strategy")
        if stage == "search failing":
            actions.append("Serper backoff active; failed searches re-queued")
        if long_stall:
            log.warning("watchdog bottleneck after %ds without new records: %s | %s",
                        idle, stage, detail)
        self.say(f"Watchdog: no new records for {int(idle)}s at "
                 f"{self.category_count()}/{self.target}; bottleneck: {stage} "
                 f"[{detail}] -> " + ("; ".join(actions) or "waiting on in-flight work"))

    def _diagnose(self, searches: dict, now: float) -> tuple[str, str]:
        backlog = self._crawl_backlog()
        with self._run_lock:
            active = len(self._running)
        with self.lock:
            recent = list(self._recent_new)
            streak = self._search_fail_streak
            failed = self.counters["searches_failed"]

        def ago(t: float) -> str:
            return f"{int(now - t)}s ago" if t else "never"
        if streak >= SEARCH_FAIL_STREAK:
            stage = "search failing"
        elif backlog and now - self._last_task_done > 60:
            stage = "crawl stalled"
        elif len(recent) >= 5 and sum(recent) == 0:
            stage = "duplicate-heavy results"
        elif not searches and backlog == 0:
            stage = "search idle"
        else:
            stage = "low yield"
        detail = (f"searches in flight {len(searches)}, crawl queue {backlog}, "
                  f"active workers {active}, last search ok {ago(self._last_search_ok)}, "
                  f"last crawl done {ago(self._last_task_done)}, "
                  f"retries pending {self.planner.pending_retries()}, "
                  f"failed searches {failed}, strategy tier {self.planner.tier_name}")
        return stage, detail

    def _replace_crawl_pool(self, why: str):
        """Swap in a fresh crawl pool. Tasks still queued in the old one are
        cancelled there and resubmitted to the new workers; stuck tasks keep
        their old threads (they time out on their own) but no longer block."""
        old = self._pool
        with self._run_lock:
            queued = [i for i in self._tasks.values()
                      if i.get("fut") is not None and i["t"] is None]
            for i in self._running.values():
                if i["abandoned"]:
                    i["pool_replaced"] = True
        self._bump("pool_restarts")
        self._pool = self._new_crawl_pool()
        if old is not None:
            old.shutdown(wait=False, cancel_futures=True)
        moved = 0
        for info in queued:
            if info["fut"].cancelled() and self._submit_task(
                    info["op"], info["url"], info["query"], info["fn"],
                    info=info) is not None:
                moved += 1
        log.warning("crawl pool replaced (%s); %d queued task(s) resubmitted", why, moved)
        self._recovering("crawl", f"crawl worker pool replaced ({why}); "
                         f"{moved} queued task(s) moved to fresh workers")

    # -- finalisation -----------------------------------------------------------
    def _finish(self, reason: str, t0: float):
        """Target reached / stopped / exhausted: final dedup check, checkpoint,
        Excel export + verification, then the terminal state."""
        self._sync_provider_counters()
        with self.state.lock:
            self.state.discovery[self.category] = self.planner.export()
        self._log_slowest()
        self._log_credits()
        summary = self._finalize_outputs()
        n = self.category_count()
        credits = self.counters["serper_credits"]
        if n >= self.target:
            self.status = "completed"
            self.stop_reason = "target reached"
            self.say(f"Target reached: {n} of {self.target} valid unique records in "
                     f"{round(time.time() - t0)}s using {credits} Serper credits. "
                     f"{summary} Status: COMPLETED.")
        elif self.stop_event.is_set():
            self.status = "stopped"
            self.stop_reason = "stopped by user"
            self.say(f"Job stopped by user at {n}/{self.target}. Progress saved - "
                     f"start again to resume.")
        else:
            self.status = "exhausted"
            if reason == "budget":
                self.stop_reason = (
                    f"credit safety cap reached ({self.credit_budget} credits = "
                    f"{config.MAX_CREDITS_PER_RECORD} x target). Raise "
                    f"MAX_CREDITS_PER_RECORD in app/.env (0 = no cap) and start "
                    f"again to continue.")
            else:
                self.stop_reason = (
                    "every search strategy tier is used up (base, relaxed, "
                    "expanded phrases + nearby locations, deep result pages) - "
                    "the remaining search space returns only companies already "
                    "found. Add keywords or broaden the location to continue.")
            self.say(f"Could only find {n} of {self.target} genuine valid companies: "
                     f"{self.stop_reason} {summary}")
        self._save_job_meta()

    def _finalize_outputs(self) -> str:
        with self.state.lock:
            recs = list(self.state.records[self.category])
        doms: set[str] = set()
        names: set[str] = set()
        dup_dom = dup_name = 0
        for r in recs:
            d = norm_domain(r.get("Official Website") or "")
            nm = norm_name(r.get("Company Name") or "")
            if d:
                dup_dom += d in doms
                doms.add(d)
            if nm:
                dup_name += nm in names
                names.add(nm)
        if dup_dom or dup_name:
            log.warning("final dedup check: %d duplicate domain(s), %d duplicate "
                        "name(s) among %d records", dup_dom, dup_name, len(recs))
        self.checkpoint(force=True)
        export_note = ""
        for attempt in range(2):
            try:
                if attempt:   # checkpoint(force=True) above already exported;
                    # the file on disk disagrees, so rewrite it regardless
                    self.state.export_categories([self.category], force=True)
                rows = self._excel_rows()
                if rows == len(recs):
                    export_note = f"Excel verified: {rows} rows."
                    break
                export_note = (f"Excel row count {rows} != {len(recs)} records "
                               f"(re-exporting)")
            except Exception as exc:   # e.g. file open in Excel
                export_note = (f"Excel export failed ({type(exc).__name__}: "
                               f"{str(exc)[:60]}) - close the file and restart to "
                               f"regenerate; all records are saved in the checkpoint.")
                log.warning("final export attempt %d failed: %s", attempt + 1, exc)
        emails = sum(1 for r in recs if r.get("Business Email"))
        phones = sum(1 for r in recs if r.get("Business Phone"))
        sites = sum(1 for r in recs if r.get("Official Website"))
        return (f"Valid unique: {len(recs)}, duplicates removed: "
                f"{self.counters['duplicates']}, emails: {emails}, phones: {phones}, "
                f"websites: {sites}; final dedup check: {dup_dom} duplicate domains, "
                f"{dup_name} duplicate names. {export_note}")

    def _excel_rows(self) -> int:
        from openpyxl import load_workbook
        path = os.path.join(self.state.out_dir,
                            self.state.all_categories()[self.category]["file"])
        wb = load_workbook(path, read_only=True)
        try:
            return sum(1 for row in wb.active.iter_rows(min_row=2, values_only=True)
                       if any(v not in (None, "") for v in row))
        finally:
            wb.close()

    def _fail(self, message: str, error: str):
        """FAILED: genuinely unrecoverable; the exact reason is logged."""
        self._closed = True
        self.status = "failed"
        self.error = error
        self.stop_reason = error
        self.say(message[:300])
        try:
            self.checkpoint(force=True)
        except Exception:
            log.exception("checkpoint after failure")
        self._save_job_meta()

    def _job_meta(self) -> dict:
        return {"target": self.target, "status": self.status,
                "collected": self.category_count(), "keywords": self.keywords,
                "location": self.location, "geo": dict(self.geo),
                "started_at": self.started_at,
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "stop_reason": self.stop_reason,
                "serper_credits": self.counters["serper_credits"],
                "job_id": self.id, "max_queries": self.max_queries,
                "provider": self.provider_name}

    def _save_job_meta(self):
        try:
            meta = self._job_meta()   # takes state.lock itself: build first
            with self.state.lock:
                self.state.jobs[self.category] = meta
            self.state.save(self.state.path)
        except Exception:
            log.exception("could not save job metadata")

    # -- performance metrics ---------------------------------------------------
    def perf(self) -> dict:
        elapsed = max(0.001, (time.time() - self.run_started)
                      if self.run_started else 0.001)
        with self.lock:
            lats = sorted(d for d, _, _ in self._latencies)
            c = dict(self.counters)
        tsnap = self.timings.snapshot()
        fetches = tsnap.get("fetch", {}).get("count", 0)
        p95 = lats[int(0.95 * (len(lats) - 1))] if lats else 0.0
        disc = max(1, c["discovered"])
        credits = c["serper_credits"]
        attempts = c["serper_ok"] + c["serper_failed"]
        with self._run_lock:
            active = len(self._running)
        retries = c["search_retries"] + getattr(self.provider, "retry_count", 0)
        return {
            "search_success_rate": round(c["serper_ok"] / attempts, 3) if attempts else 1.0,
            "crawl_avg_ms": tsnap.get("fetch", {}).get("avg_ms", 0),
            "validation_avg_ms": tsnap.get("validate", {}).get("avg_ms", 0),
            "active_workers": active,
            "crawl_queue": self._crawl_backlog(),
            "failed_operations": (c["failed"] + c["search_failures"]
                                  + c["worker_errors"] + c["abandoned_tasks"]),
            "retries": retries,
            "secs_since_last_record": round(time.time() - self._last_new_record)
            if self._last_new_record else None,
            "strategy_tier": self.planner.tier_name if self.planner else "",
            "elapsed_s": round(elapsed, 1),
            "queries_per_min": round(60 * c["queries_executed"] / elapsed, 1),
            "serper_requests_per_min": round(60 * c["search_requests"] / elapsed, 1),
            "sites_per_min": round(60 * fetches / elapsed, 1),
            "valid_per_min": round(60 * c["valid"] / elapsed, 1),
            "valid_per_credit": round(c["valid"] / credits, 2) if credits else 0,
            "new_results_per_search": round(c["new_results"] / c["searches_done"], 2)
            if c["searches_done"] else 0,
            "result_duplicate_rate": round(1 - c["new_results"] / c["search_results"], 3)
            if c["search_results"] else 0,
            "serper_avg_ms": round(1000 * sum(lats) / len(lats)) if lats else 0,
            "serper_p95_ms": round(1000 * p95),
            "serper_requests": c["search_requests"],
            "serper_429": c["serper_429"],
            "serper_timeouts": c["serper_timeouts"],
            "cache_hits": c["cache_hits"],
            "duplicate_rate": round(c["duplicates"] / disc, 3),
        }

    def credit_metrics(self) -> dict:
        """Credit-efficiency dashboard for this run."""
        with self.lock:
            c = dict(self.counters)
        ps = self.planner.summary() if self.planner else {}
        credits = c["places_credits"] + c["organic_credits"]
        valid = c["valid"]
        # Only requests that were ready to be SENT and were answered without
        # a credit count as saved. Strategy decisions (organic not paired,
        # page-2 deferred) are reported separately: the old logic would have
        # reached the target sooner and not sent all of them, so counting
        # them as savings would overstate it (A/B: ~16 real vs ~100 counted).
        avoided = {
            "cache_hits": c["cache_hits"],
            "executed_query_skips": c["skipped_executed"],
            "skipped_target_reached": c["skipped_target"],
        }
        return {
            "total_credits": credits,
            "places_credits": c["places_credits"],
            "organic_credits": c["organic_credits"],
            "total_queries": c["places_requests"] + c["organic_requests"],
            "places_calls": c["places_requests"],
            "organic_calls": c["organic_requests"],
            "cached_queries": c["cache_hits"],
            "skipped_duplicate_queries": c["skipped_executed"],
            "search_results": c["search_results"],
            "new_domains": c["new_results"],
            "duplicate_results": c["duplicate_results"],
            "valid_businesses": valid,
            "valid_from_places": c["valid_places"],
            "valid_from_organic": c["valid_organic"],
            "valid_per_credit": round(valid / credits, 2) if credits else None,
            "credits_per_valid": round(credits / valid, 3) if valid else None,
            "places_valid_per_credit": round(c["valid_places"] / c["places_credits"], 2)
            if c["places_credits"] else None,
            "organic_valid_per_credit": round(c["valid_organic"] / c["organic_credits"], 2)
            if c["organic_credits"] else None,
            "credits_after_target": c["credits_after_target"],
            "timeouts_maybe_charged": c["timeouts_maybe_charged"],
            "followup_pages": ps.get("followups_issued", 0),
            "followup_novelty_ratio": ps.get("followup_novelty_ratio"),
            "estimated_credits_saved": sum(avoided.values()),
            "saved_breakdown": avoided,
            "organic_not_paired": ps.get("organic_not_paired", 0),
            "places_not_paired": ps.get("places_not_paired", 0),
            "followup_pages_deferred": ps.get("followups_not_needed", 0),
        }

    def _log_credits(self):
        m = self.credit_metrics()
        self.say(f"Credits: {m['total_credits']} paid ({m['places_credits']} Places, "
                 f"{m['organic_credits']} organic) -> {m['valid_businesses']} valid = "
                 f"{m['valid_per_credit']} valid/credit (Places "
                 f"{m['places_valid_per_credit']}, organic {m['organic_valid_per_credit']}); "
                 f"{m['new_domains']} new / {m['duplicate_results']} duplicate results; "
                 f"{m['cached_queries']} cached, {m['skipped_duplicate_queries']} "
                 f"already-executed searches skipped, {m['credits_after_target']} "
                 f"credits landed after the target; {m['estimated_credits_saved']} "
                 f"requests answered without a credit {m['saved_breakdown']}; "
                 f"{m['organic_not_paired']} cells searched on Places only, "
                 f"{m['followup_pages']} further result pages fetched, "
                 f"{m['followup_pages_deferred']} deferred")
        try:
            self.state.ledger.write_csv(os.path.join(self.state.out_dir,
                                                     "credit_ledger.csv"))
        except OSError as exc:   # e.g. open in Excel: the ledger is in the checkpoint
            log.warning("credit ledger CSV not written: %s", exc)

    def _log_slowest(self):
        with self.lock:
            worst = sorted(self._latencies, reverse=True)[:10]
        if worst:
            log.info("slowest Serper requests this run:")
            for dt, kind, q in worst:
                log.info("  %6.0f ms  %-7s  %s", dt * 1000, kind, q[:70])
        p = self.perf()
        self.say(f"Perf: {p['serper_requests_per_min']} Serper req/min, "
                 f"{p['sites_per_min']} sites/min, {p['valid_per_min']} valid/min, "
                 f"{p['valid_per_credit']} valid/credit, "
                 f"{p['new_results_per_search']} new results/search, "
                 f"dup rate {p['result_duplicate_rate']}, "
                 f"Serper avg {p['serper_avg_ms']}ms / p95 {p['serper_p95_ms']}ms, "
                 f"search success {p['search_success_rate']}, "
                 f"crawl avg {p['crawl_avg_ms']}ms, validate avg "
                 f"{p['validation_avg_ms']}ms, failed ops {p['failed_operations']}, "
                 f"retries {p['retries']}")

    # -- status for the API ----------------------------------------------------
    def snapshot(self, include_log: bool = True) -> dict:
        with self.lock:
            counters = dict(self.counters)
        planner = self.planner
        snap = {
            "id": self.id,
            "created_at": self.created_at,
            "status": self.status,
            "error": self.error,
            "recovery": self.recovery,
            "stop_reason": self.stop_reason,
            "category": self.category,
            "category_display": self.display,
            "keywords": self.keywords,
            "location": self.location,
            "target": self.target,
            "provider": self.provider_name,
            "queries_planned": planner.candidate_cells() if planner else 0,
            "estimated_credits": self.credit_budget,
            "collected": self.category_count(),
            "target_reached": self.category_count() >= self.target,
            "counters": counters,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "timings": self.timings.snapshot(),
            "perf": self.perf(),
            "credits": self.credit_metrics(),
            "discovery": planner.summary() if planner else {},
            "log_seq": self.log_seq,
        }
        if include_log:
            snap["log"] = list(self.log_lines)
        return snap
