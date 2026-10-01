"""App configuration. The Serper key comes ONLY from the environment or app/.env
(never hardcoded, never logged, never sent to the frontend)."""

from __future__ import annotations

import hashlib
import os

APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../app
FRONTEND_DIR = os.path.join(APP_DIR, "frontend")
# OUTPUT_DIR / STATE_PATH are set below, after app/.env is loaded
# (APP_OUTPUT_DIR may relocate the data folder).

# Demo cost-control defaults (each Serper search or places page = 1 credit).
DEFAULT_TARGET = 100
MAX_TARGET = 2000
TARGET_CHOICES = [20, 50, 100, 250, 500, 1000, 1500, 2000]
DEFAULT_MAX_QUERIES = 12
MAX_MAX_QUERIES = 400            # large targets need hundreds of queries
YIELD_EXHAUSTED_STREAK = 10      # stop after N consecutive zero-yield queries
ORGANIC_RESULTS_PER_QUERY = 10   # one Serper page
PLACES_PAGES_PER_QUERY = 1       # one Serper page (10 places)
# Company links mined per directory/list page. These candidates cost NO
# Serper credit (only a crawl). The old business_collector.py used 60; the
# app's 15 truncated 57 of 104 list pages in live runs (old script: 27.7 new
# companies per mined list), throwing away most of what an organic credit
# had already paid for.
DIRECTORY_CANDIDATES_CAP = 60


def _int_env(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.environ.get(name, "") or default)))
    except ValueError:
        return default


def _float_env(name: str, default: float, lo: float, hi: float) -> float:
    try:
        return max(lo, min(hi, float(os.environ.get(name, "") or default)))
    except ValueError:
        return default


def _read_first(*paths: str) -> str:
    for p in paths:
        try:
            with open(p, encoding="ascii") as fh:
                return fh.read().strip()
        except OSError:
            continue
    return ""


def _effective_cpus() -> float:
    """CPUs this process may really use. os.cpu_count() reports the whole
    host, so in a container with a CPU quota (Render free = 0.1 CPU) it can
    be 8-64x too high. Reads the cgroup v2 / v1 quota; falls back to the
    affinity mask / cpu_count when there is none (Windows, bare metal)."""
    try:
        n = float(len(os.sched_getaffinity(0)))
    except (AttributeError, OSError):
        n = float(os.cpu_count() or 4)
    quota, period = None, None
    v2 = _read_first("/sys/fs/cgroup/cpu.max").split()
    if len(v2) == 2 and v2[0] != "max":
        quota, period = v2
    else:
        q = _read_first("/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
                        "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us")
        p = _read_first("/sys/fs/cgroup/cpu/cpu.cfs_period_us",
                        "/sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us")
        if q and p and not q.startswith("-"):
            quota, period = q, p
    try:
        if quota and period and float(period) > 0:
            n = min(n, float(quota) / float(period))
    except ValueError:
        pass
    return max(0.05, n)


def _memory_limit_mb() -> float:
    """Container memory limit in MB, or 0 when unlimited / unknown."""
    raw = _read_first("/sys/fs/cgroup/memory.max",
                      "/sys/fs/cgroup/memory/memory.limit_in_bytes")
    try:
        v = int(raw)
    except ValueError:
        return 0.0
    return 0.0 if v >= 1 << 60 else v / (1024 * 1024)


EFFECTIVE_CPUS = _effective_cpus()
MEMORY_LIMIT_MB = _memory_limit_mb()
# A small container (e.g. Render free: 0.1 CPU / 512 MB) gets defaults that
# fit it; every value below can still be overridden by its env variable.
SMALL_HOST = EFFECTIVE_CPUS < 1.0 or 0 < MEMORY_LIMIT_MB <= 1024


_DOTENV_KEYS: set[str] = set()   # names whose value came from app/.env


def _read_dotenv() -> dict[str, str]:
    """Parse app/.env (KEY=VALUE lines) into a dict; {} if missing/unreadable."""
    path = os.path.join(APP_DIR, ".env")
    values: dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                # drop trailing "  # comment" (as used in .env.example)
                value = value.split(" #", 1)[0].split("\t#", 1)[0]
                key, value = key.strip(), value.strip().strip('"').strip("'")
                if key:
                    values[key] = value
    except OSError:
        pass
    return values


def _load_dotenv() -> None:
    """Tiny .env loader; does not override real env vars."""
    for key, value in _read_dotenv().items():
        if key not in os.environ:
            os.environ[key] = value
            _DOTENV_KEYS.add(key)


# Must run before the tuning constants below read os.environ.
_load_dotenv()

OUTPUT_DIR = os.path.abspath(os.environ.get("APP_OUTPUT_DIR", "").strip()
                             or os.path.join(APP_DIR, "output"))
STATE_PATH = os.path.join(OUTPUT_DIR, "demo_state.json")


def client_dir(client: str) -> str:
    """Private data folder of one user (browser): their checkpoint, Excel
    files and credit ledger. Every user sees only their own records. The
    folder name is a hash, so client ids never appear on disk."""
    h = hashlib.sha256(client.encode("utf-8")).hexdigest()[:32]
    return os.path.join(OUTPUT_DIR, "users", h)


# A user's data is unloaded from the worker's memory after this long without
# a request or a running job (it is reloaded from disk on the next request).
STORE_IDLE_S = 900


# Parallel site fetches. Tune without code changes via SCRAPER_CONCURRENCY
# (or the older APP_CRAWL_WORKERS) in .env, range 4..100. Crawling is
# I/O-bound; robots.txt and per-site page limits are always respected.
CRAWL_WORKERS = _int_env("SCRAPER_CONCURRENCY",
                         _int_env("APP_CRAWL_WORKERS", 16 if SMALL_HOST else 48,
                                  4, 100), 4, 100)
# (connect, read) seconds per request - dead hosts fail fast. Not lower by
# default on small hosts: a CPU-starved TLS handshake then times out and a
# live site is skipped as dead (measured at 0.1 CPU).
FETCH_TIMEOUT = (_float_env("FETCH_CONNECT_TIMEOUT", 5.0, 1.0, 60.0),
                 _float_env("FETCH_READ_TIMEOUT", 12.0, 2.0, 120.0))
# robots.txt probe (connect, read); a connect/timeout failure marks the host dead
ROBOTS_TIMEOUT = (_float_env("ROBOTS_CONNECT_TIMEOUT", 4.0, 1.0, 60.0),
                  _float_env("ROBOTS_READ_TIMEOUT", 8.0, 2.0, 120.0))
# Mid-run Excel regeneration, seconds (the final export always happens; the
# state JSON is the crash-safe checkpoint). A 2,000-row sheet costs ~0.5 s
# of CPU - ~5 s of wall time on a 0.1-CPU host - so small hosts refresh the
# mid-run files less often. Unchanged files are never rewritten.
EXPORT_MIN_INTERVAL = _float_env("EXPORT_MIN_INTERVAL", 60.0 if SMALL_HOST else 30.0,
                                 5.0, 3600.0)
STATE_SAVE_INTERVAL = 5.0

# HTML parsing/extraction runs in worker processes (off the GIL).
# 0 = parse inside crawl threads (old behaviour).
ANALYZE_PROCESSES = _int_env("ANALYZE_PROCESSES",
                             # each is a full Python process (~50 MB): none
                             # on a small host, else one per spare real CPU
                             0 if SMALL_HOST else
                             max(1, min(8, int(EFFECTIVE_CPUS) - 2)), 0, 16)
# Pages analysed at the same time when parsing runs in the crawl threads
# (ANALYZE_PROCESSES=0). Measured: 32 threads parsing at once cost 95 ms
# CPU per page (GIL / cache thrash) vs 41 ms one at a time (122 vs 74 ms
# under a 0.1-CPU cap), so on a small host page analysis takes turns;
# fetches stay fully parallel.
ANALYZE_THREADS = _int_env("ANALYZE_THREADS", max(1, round(EFFECTIVE_CPUS)), 1, 64)

# ---- Crawl work per record (small hosts are CPU-bound: every page fetched
# and analysed costs time) ----------------------------------------------------
# Page bytes read per fetch. Measured on 154 live pages (p50 182 KB, p90
# 541 KB, max 2 MB): 768 KB cut 5 pages and changed no email / phone /
# address; 512 KB lost phones on 7 of 19 cut pages.
PAGE_MAX_BYTES = _int_env("PAGE_MAX_BYTES", 768_000 if SMALL_HOST else 2_000_000,
                          100_000, 20_000_000)
# 1 = search Google Places only, organic results just where Places is thin
# (< 5 listings) and in the deep tier. A Places record needs ~1.3 page
# analyses (phone + address come with the listing), an organic one 3-4
# (directory pages, contact pages); live, 100 records took 263 analyses
# with 8 organic searches vs 160 with 6. Pairing resumes for a category
# whose Places yield is below PLACES_FIRST_MIN_YIELD records per credit.
PLACES_FIRST = _int_env("PLACES_FIRST", 1 if SMALL_HOST else 0, 0, 1)
PLACES_FIRST_MIN_YIELD = _float_env("PLACES_FIRST_MIN_YIELD", 2.0, 0.0, 50.0)
# 1 = a site whose homepage has email + phone still gets up to 2 contact
# pages crawled for a missing address (26 of 113 measured homepages).
CONTACT_PAGES_FOR_ADDRESS = _int_env("CONTACT_PAGES_FOR_ADDRESS",
                                     0 if SMALL_HOST else 1, 0, 1)

# ---- Serper client tuning (all overridable in .env) ------------------------
SERPER_CONCURRENCY = _int_env("SERPER_CONCURRENCY", 6, 1, 16)  # requests in flight
SERPER_RPS = _int_env("SERPER_RPS", 8, 1, 50)          # adaptive ceiling
SEARCH_PREFETCH = _int_env("SEARCH_PREFETCH", 3, 1, 8)  # queries searched ahead
                                 # of the one being crawled
SEARCH_BACKLOG_LIMIT = 2         # x CRAWL_WORKERS: queued crawl tasks above
                                 # which no new searches are launched
PLACES_MAX_PAGE = 3              # follow-up Places pages for productive queries


# Spend safety valve per run: credits <= max(2 x max_queries, target x this).
# The planner keeps generating NEW searches (wider tiers) until the target is
# reached; this cap is the only spend-based stop. 0 disables it. Measured
# yield is ~2-3 valid records per credit, so 1.5 leaves ample headroom.
MAX_CREDITS_PER_RECORD = _float_env("MAX_CREDITS_PER_RECORD", 1.5, 0.0, 20.0)
SERPER_MAX_ATTEMPTS = _int_env("SERPER_MAX_ATTEMPTS", 4, 1, 8)  # per request
SEARCH_RETRIES = _int_env("SEARCH_RETRIES", 2, 0, 5)  # re-queues of a failed search
# A TIMED-OUT request may already have been billed, so it is resent at most
# this many times (429 / 5xx / network errors are never billed and keep the
# full SERPER_MAX_ATTEMPTS backoff budget).
SERPER_TIMEOUT_RETRIES = _int_env("SERPER_TIMEOUT_RETRIES", 1, 0, 3)
# (connect, read). Measured: Places sometimes answers only after ~16 s. The
# old 15 s read timeout gave up just before the answer and then paid for the
# same request again (53 timeouts, 44 of them re-sent successfully). Searches
# run in their own pool, so waiting longer never stalls the crawl pipeline.
SERPER_TIMEOUT = (_float_env("SERPER_CONNECT_TIMEOUT", 5.0, 1.0, 60.0),
                  _float_env("SERPER_READ_TIMEOUT", 30.0, 5.0, 120.0))
SEARCH_CACHE_TTL = 7 * 24 * 3600  # re-use a search response for a week
SEARCH_CACHE_MAX = 1000           # entries (Places rows are trimmed first)
MAX_CONNECTIONS = _int_env("MAX_CONNECTIONS", 100, 16, 200)
KEEPALIVE_CONNECTIONS = _int_env("KEEPALIVE_CONNECTIONS", 100, 16, 200)
# robots.txt answers kept in memory (LRU, 1 h TTL). A parsed robots.txt is
# ~1-22 KB (measured), so 20,000 hosts can hold ~20-400 MB; each company
# site is crawled once anyway, so a small host keeps far fewer.
ROBOTS_CACHE_HOSTS = _int_env("ROBOTS_CACHE_HOSTS", 2000 if SMALL_HOST else 20_000,
                              100, 100_000)
PER_DOMAIN_CONCURRENCY = 1       # by design: one page at a time per website
# total seconds per page (slow-drip bodies); read/5xx retries: crawler.make_session
FETCH_DEADLINE = _float_env("FETCH_DEADLINE", 25.0, 5.0, 300.0)

# ---- Collection watchdog (fault tolerance) ---------------------------------
STUCK_TASK_S = _int_env("STUCK_TASK_S", 180, 5, 3600)       # abandon a crawl task
WATCHDOG_STALL_S = _int_env("WATCHDOG_STALL_S", 60, 5, 3600)   # no new record ->
                                 # diagnose, retry, refresh search strategy
WATCHDOG_BOTTLENECK_S = _int_env("WATCHDOG_BOTTLENECK_S", 300, 10, 7200)
MAX_CONTROLLER_ERRORS = 25       # consecutive controller-loop failures -> FAILED

# ---- Multi-user job scheduling (worker process) ----------------------------
# Collections run in a separate worker process, several at once. Jobs beyond
# MAX_ACTIVE_JOBS wait in a FIFO queue; a category is collected by one job at
# a time (a second job for it queues behind the first); each client (browser)
# may have MAX_JOBS_PER_CLIENT queued/running jobs.
# Small host: 3 at once, so a second / third user does not wait behind the
# first. Page parsing shares one CPU lane (ANALYZE_THREADS), so concurrent
# jobs split the CPU instead of thrashing it.
MAX_ACTIVE_JOBS = _int_env("MAX_ACTIVE_JOBS", 3 if SMALL_HOST else 4, 1, 32)
MAX_QUEUED_JOBS = _int_env("MAX_QUEUED_JOBS", 500, 1, 10_000)
MAX_JOBS_PER_CLIENT = _int_env("MAX_JOBS_PER_CLIENT", 1, 1, 10)
# Crawl threads across ALL running jobs (see job_crawl_workers): a job takes
# what is left (at most CRAWL_WORKERS) but never less than an equal slice
# (at least 8). Bounds threads / sockets / memory.
# Small host: 48 = a lone job's 32, then 16 for each job that joins it.
GLOBAL_CRAWL_WORKERS = _int_env("GLOBAL_CRAWL_WORKERS", 48 if SMALL_HOST else 192,
                                16, 800)
JOB_HISTORY = 300                # finished jobs kept for their owners' status
# worker -> API status bundle cadence (s). Browsers poll every 3 s, so a
# small host publishes once a second instead of twice.
STATUS_PUBLISH_INTERVAL = _float_env("STATUS_PUBLISH_INTERVAL",
                                     1.0 if SMALL_HOST else 0.5, 0.2, 10.0)
WORKER_RESTARTS_MAX = 5          # crash restarts per 10 min before giving up

# ---- API protection ---------------------------------------------------------
# Limits are per CLIENT (the browser's X-Client-Id), so many users behind one
# office NAT / reverse proxy are not throttled together; the per-IP ceiling
# only stops a single address from flooding the server. It is sized for
# 2,000 users behind ONE address polling every 3 s (40,000/min) - behind a
# reverse proxy also set TRUSTED_PROXY_HOPS so real client IPs are seen.
RATE_LIMIT_CLIENT_PER_MIN = _int_env("RATE_LIMIT_CLIENT_PER_MIN", 240, 10, 100_000)
RATE_LIMIT_COLLECT_PER_MIN = _int_env("RATE_LIMIT_COLLECT_PER_MIN", 10, 1, 10_000)
RATE_LIMIT_IP_PER_MIN = _int_env("RATE_LIMIT_IP_PER_MIN", 60_000, 100, 100_000_000)
# Serper credits are paid by the server owner, and a client id is just a
# random string any caller can change - so what costs credits is ALSO
# limited per network address (IP), which a caller cannot rotate freely:
# collection starts per minute, collections queued/running at once, and
# Serper credits spent per rolling 24 h (0 = no daily limit).
RATE_LIMIT_COLLECT_PER_IP_PER_MIN = _int_env("RATE_LIMIT_COLLECT_PER_IP_PER_MIN", 20,
                                             1, 100_000)
MAX_JOBS_PER_IP = _int_env("MAX_JOBS_PER_IP", 3, 1, 1000)
MAX_CREDITS_PER_IP_PER_DAY = _int_env("MAX_CREDITS_PER_IP_PER_DAY", 1500, 0, 10_000_000)
# Login / sign-up / login attempts per address per minute (password guessing).
RATE_LIMIT_AUTH_PER_IP_PER_MIN = _int_env("RATE_LIMIT_AUTH_PER_IP_PER_MIN", 10, 1, 10_000)

# ---- User accounts (backend/auth.py) ------------------------------------
# Set MONGODB_URI to require login: users + sessions are stored in MongoDB
# and each account gets its own records. Unset = anonymous per-browser data.
MONGODB_URI = os.environ.get("MONGODB_URI", "").strip()
MONGODB_DB = os.environ.get("MONGODB_DB", "").strip() or "business_data_collector"
SESSION_DAYS = _int_env("SESSION_DAYS", 30, 1, 365)
# Reverse proxies in front of the app that APPEND the caller's address to
# X-Forwarded-For (Render: 1). The client IP is the entry that many places
# from the right; everything left of it is caller-supplied and ignored.
# 0 = no proxy: use the TCP peer address and ignore X-Forwarded-For.
TRUSTED_PROXY_HOPS = _int_env("TRUSTED_PROXY_HOPS", 0, 0, 10)
# status poll cadence the server suggests to the browser (the page follows it)
POLL_MS_ACTIVE = _int_env("POLL_MS_ACTIVE", 3000, 1000, 60_000)
POLL_MS_IDLE = _int_env("POLL_MS_IDLE", 10_000, 2000, 300_000)  # no running job

APP_VERSION = "1.1.0"


_env_cache: dict = {"mtime": None, "values": {}}


def _dotenv_cached() -> dict[str, str]:
    """app/.env contents, re-parsed only when the file changes (this runs on
    every /api/config and job start, so it must not hit the disk each time)."""
    try:
        mtime = os.path.getmtime(os.path.join(APP_DIR, ".env"))
    except OSError:
        mtime = None
    if mtime != _env_cache["mtime"]:
        _env_cache["values"] = _read_dotenv() if mtime is not None else {}
        _env_cache["mtime"] = mtime
    return _env_cache["values"]


def serper_api_key() -> str:
    """The Serper key. When it came from app/.env the file is re-read, so a
    key swapped in there (e.g. a topped-up account) works without a restart.
    A real environment variable always wins and is never re-read."""
    if "SERPER_API_KEY" in _DOTENV_KEYS or "SERPER_API_KEY" not in os.environ:
        fresh = _dotenv_cached().get("SERPER_API_KEY", "").strip()
        if fresh:
            os.environ["SERPER_API_KEY"] = fresh
            _DOTENV_KEYS.add("SERPER_API_KEY")
    return os.environ.get("SERPER_API_KEY", "").strip()


def job_crawl_workers(in_use: int = 0) -> int:
    """Crawl threads for a job starting while running jobs hold in_use: what
    is left of the global budget (up to CRAWL_WORKERS), never less than an
    equal slice. A job running alone gets the full CRAWL_WORKERS - an equal
    slice reserved for jobs that are not there halved its threads (measured
    on Render Free: 100 records 45-62 s with 16 threads)."""
    fair = max(8, min(CRAWL_WORKERS, GLOBAL_CRAWL_WORKERS // max(1, MAX_ACTIVE_JOBS)))
    return max(fair, min(CRAWL_WORKERS, GLOBAL_CRAWL_WORKERS - in_use))


def auth_token() -> str:
    """Optional shared access code. When set (APP_AUTH_TOKEN), every /api
    request must carry it in the X-Auth-Token header. Recommended whenever the
    app is reachable beyond this machine (network launcher, reverse proxy)."""
    return os.environ.get("APP_AUTH_TOKEN", "").strip()


def allowed_origins() -> list[str]:
    """CORS origins. Local defaults; override with APP_ALLOWED_ORIGINS
    (comma-separated) behind a production domain. Never '*'."""
    env = os.environ.get("APP_ALLOWED_ORIGINS", "").strip()
    if env:
        return [o.strip() for o in env.split(",") if o.strip()]
    return ["null", "http://127.0.0.1:8100", "http://localhost:8100"]


def force_hsts() -> bool:
    """Send Strict-Transport-Security (set APP_FORCE_HTTPS=1 once the app is
    served over HTTPS via a reverse proxy)."""
    return os.environ.get("APP_FORCE_HTTPS", "").strip() in ("1", "true", "yes")
