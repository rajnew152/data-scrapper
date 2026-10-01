"""FastAPI backend for the Business Data Collector.

Run from the app/ directory (production settings, see serve.py):
    python -m backend.serve --port 8100

Architecture (built for ~2,000 concurrent browser sessions):

    browser ──HTTP──> API process (this module, one asyncio event loop)
                        │  answers every read from memory: status bundles,
                        │  per-category stats, serialized record lists,
                        │  index.html, Excel downloads (ETag / 304 / gzip)
                        └─pipe─> collection worker process (worker.py)
                                   jobs, crawl threads, analysis pool,
                                   checkpoints, Excel exports

Request path:  Entry ─(no Origin header: same-origin page)─> Guard
                     └─(Origin header: cross-origin)─> CORS ─> Guard
               Guard = security checks, then the hot GET routes (status,
               health, config, records, files, index.html) are answered by
               plain async functions; everything else (validated POSTs,
               downloads, credits) goes to FastAPI. Measured: FastAPI's
               routing / dependency machinery alone cost ~1.2 ms per request,
               which capped one process at ~650 status polls per second.

Each browser identifies itself with a random X-Client-Id (stored in the
browser); every user has their own job, jobs run concurrently (see
collector/jobs.py) and one user's collection never blocks another's.
"""

from __future__ import annotations

import asyncio
import gzip
import hmac
import logging
import logging.handlers
import os
import queue
import re
import sys
import time
from collections import OrderedDict, defaultdict
from urllib.parse import parse_qsl

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel, Field

from . import config, fastjson
from .collector.categories import SUMMARY_FILE
from .hub import RpcError, WorkerHub, WorkerUnavailable

ACTIVE = ("queued", "pending", "running", "recovering", "waiting")

# ---------------------------------------------------------------------------
# Logging: through a queue, so file / console writes never run on the event
# loop. Successful GET polls are not logged one by one (at 700 req/s that
# was most of the server's work); a per-minute access summary replaces them.
# ---------------------------------------------------------------------------

_log_queue: queue.Queue = queue.Queue(-1)
_log_listener: logging.handlers.QueueListener | None = None


def _setup_logging():
    global _log_listener
    if _log_listener is not None:
        return
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    handlers = []
    try:
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            os.path.join(config.OUTPUT_DIR, "app.log"),
            maxBytes=5_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        handlers.append(fh)
    except OSError:
        pass
    ch = logging.StreamHandler(sys.stderr)
    ch.setFormatter(fmt)
    ch.setLevel(logging.INFO)
    handlers.append(ch)
    _log_listener = logging.handlers.QueueListener(_log_queue, *handlers,
                                                   respect_handler_level=True)
    _log_listener.start()
    root = logging.getLogger()
    root.handlers[:] = [logging.handlers.QueueHandler(_log_queue)]
    root.setLevel(logging.INFO)


_setup_logging()
log = logging.getLogger("api")

api = FastAPI(title="Business Data Collector", docs_url=None, redoc_url=None,
              openapi_url=None)

# Test hook: a picklable callable run first inside the worker process
# (the load-test harness installs its simulators with it).
WORKER_INIT = None
hub = WorkerHub()

MAX_BODY_BYTES = 64 * 1024
CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{16,64}$")
SAFE_FILE_RE = re.compile(r"^[\w\-. ]+\.xlsx$")
XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
JSON_T = b"application/json"

SECURITY_HEADERS = [
    (b"content-security-policy",
     b"default-src 'self'; script-src 'self' 'unsafe-inline'; "
     b"style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
     b"font-src https://fonts.gstatic.com; img-src 'self' data:; "
     b"connect-src 'self'; object-src 'none'; base-uri 'self'; "
     b"frame-ancestors 'none'"),
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"no-referrer"),
    (b"permissions-policy", b"camera=(), microphone=(), geolocation=()"),
]
HSTS = (b"strict-transport-security", b"max-age=31536000; includeSubDomains")


class Reply:
    """A response produced by a fast-path handler."""

    __slots__ = ("status", "body", "ctype", "headers")

    def __init__(self, status: int, body: bytes = b"", ctype: bytes = JSON_T,
                 headers: list | None = None):
        self.status, self.body, self.ctype = status, body, ctype
        self.headers = headers or []


def jreply(obj, status: int = 200) -> Reply:
    return Reply(status, fastjson.dumps(obj))


def error_reply(status: int, detail: str) -> Reply:
    return Reply(status, fastjson.dumps({"detail": detail}))


# ---------------------------------------------------------------------------
# Rate limiting + access statistics
# ---------------------------------------------------------------------------

class RateLimiter:
    """Sliding-window counters (current + previous minute), O(1) memory per
    key, stale keys pruned. Runs on the event loop thread only (no lock).
    The old version kept a deque of every request timestamp per key and
    never dropped keys."""

    def __init__(self, window: float = 60.0):
        self.window = window
        self.b: dict[str, list] = {}       # key -> [window index, count, prev count]
        self._last_prune = time.monotonic()

    def hit(self, key: str, limit: int) -> bool:
        now = time.monotonic()
        w = int(now // self.window)
        e = self.b.get(key)
        if e is None:
            self.b[key] = [w, 1, 0]
            self._prune(now)
            return False
        if e[0] != w:
            e[2] = e[1] if e[0] == w - 1 else 0
            e[0], e[1] = w, 0
        frac = (now % self.window) / self.window
        if e[2] * (1.0 - frac) + e[1] >= limit:
            return True
        e[1] += 1
        return False

    def _prune(self, now: float):
        if now - self._last_prune < self.window:
            return
        self._last_prune = now
        w = int(now // self.window)
        for k in [k for k, e in self.b.items() if e[0] < w - 1]:
            del self.b[k]


class AccessStats:
    """Per-minute request summary (count, errors, p50/p95 by endpoint)."""

    def __init__(self):
        self.t0 = time.monotonic()
        self.lat: dict[str, list[float]] = defaultdict(list)
        self.errors = 0
        self.throttled = 0

    def add(self, path: str, status: int, dt: float):
        ep = path if path.startswith("/api/") else "/(static)"
        if ep.startswith("/api/download/"):
            ep = "/api/download"
        xs = self.lat[ep]
        if len(xs) < 20_000:
            xs.append(dt)
        if status >= 500:
            self.errors += 1
        elif status == 429:
            self.throttled += 1
        if time.monotonic() - self.t0 >= 60:
            self.flush()

    def flush(self):
        span = time.monotonic() - self.t0
        parts = []
        total = 0
        for ep, xs in sorted(self.lat.items()):
            xs.sort()
            total += len(xs)
            parts.append(f"{ep} {len(xs)} (p50 {1000 * xs[len(xs) // 2]:.1f}ms "
                         f"p95 {1000 * xs[int(len(xs) * 0.95)]:.1f}ms)")
        if total:
            log.info("access %.0fs: %d requests (%.1f/s), %d errors, %d throttled | %s",
                     span, total, total / span, self.errors, self.throttled,
                     "; ".join(parts))
        self.__init__()


def client_key(headers: dict) -> str:
    """The caller's client id, or "" when missing / malformed. There is no
    fallback to the IP address: callers sharing (or faking) an address
    would share one user's data."""
    cid = headers.get(b"x-client-id", b"").decode("latin-1")
    return cid if CLIENT_ID_RE.match(cid) else ""


def client_ip(scope, headers: dict) -> str:
    """The caller's network address. Behind TRUSTED_PROXY_HOPS proxies that
    append to X-Forwarded-For it is the entry that many places from the
    right: entries further left are written by the caller and can be
    faked (the old setup trusted them, so rotating a fake address got
    around every per-IP limit)."""
    peer = (scope.get("client") or ("?", 0))[0]
    hops = config.TRUSTED_PROXY_HOPS
    if not hops:
        return peer
    xff = [p.strip() for p in headers.get(b"x-forwarded-for", b"").decode("latin-1")
           .split(",") if p.strip()]
    return xff[-hops] if len(xff) >= hops else peer


# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------

class Blob:
    """A cacheable response body: raw + gzip bytes + ETag."""

    __slots__ = ("raw", "gz", "etag", "tag")

    def __init__(self, raw: bytes, etag: str, tag=None, compress: bool = True):
        self.raw = raw
        self.gz = gzip.compress(raw, 5) if compress and len(raw) > 1400 else None
        self.etag = etag
        self.tag = tag

    def reply(self, headers: dict, ctype: bytes, cache: bytes = b"no-cache",
              extra: list | None = None) -> Reply:
        hdrs = [(b"etag", self.etag.encode()), (b"cache-control", cache),
                (b"vary", b"Accept-Encoding"), *(extra or ())]
        if headers.get(b"if-none-match", b"").decode("latin-1") == self.etag:
            return Reply(304, b"", ctype, hdrs)
        if self.gz is not None and b"gzip" in headers.get(b"accept-encoding", b""):
            hdrs.append((b"content-encoding", b"gzip"))
            return Reply(200, self.gz, ctype, hdrs)
        return Reply(200, self.raw, ctype, hdrs)

    def response(self, request: Request, media_type: str, extra: dict | None = None,
                 cache: str = "no-cache") -> Response:
        r = self.reply({k: v for k, v in request.scope["headers"]},
                       media_type.encode(), cache.encode(),
                       [(k.lower().encode(), v.encode()) for k, v in (extra or {}).items()])
        resp = Response(r.body, status_code=r.status, media_type=media_type)
        for k, v in r.headers:
            resp.headers[k.decode()] = v.decode()
        return resp


class BlobCache:
    """Small LRU of Blobs with single-flight loading."""

    def __init__(self, size: int):
        self.size = size
        self.items: "OrderedDict[object, Blob]" = OrderedDict()
        self.loading: dict[object, asyncio.Future] = {}

    def get(self, key):
        b = self.items.get(key)
        if b is not None:
            self.items.move_to_end(key)
        return b

    def put(self, key, blob: Blob):
        self.items[key] = blob
        self.items.move_to_end(key)
        while len(self.items) > self.size:
            self.items.popitem(last=False)

    async def load(self, key, loader):
        """Run `loader()` once for concurrent requests of the same key."""
        fut = self.loading.get(key)
        if fut is not None:
            return await asyncio.shield(fut)
        fut = asyncio.get_running_loop().create_future()
        self.loading[key] = fut
        try:
            blob = await loader()
            self.put(key, blob)
            fut.set_result(blob)
            return blob
        except BaseException as exc:
            fut.set_exception(exc)
            fut.exception()          # mark retrieved
            raise
        finally:
            self.loading.pop(key, None)


_records_cache = BlobCache(256)
_files_cache = BlobCache(24)
_static: dict[str, tuple[float, Blob]] = {}
_files_listing: "OrderedDict[str, tuple[float, bytes]]" = OrderedDict()  # per user


class ServiceError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status, self.detail = status, detail


def worker_error(exc: Exception) -> ServiceError:
    if isinstance(exc, RpcError):
        return ServiceError(exc.status, exc.detail)
    return ServiceError(503, "The collection service is restarting - try again "
                             "in a few seconds.")


async def ensure_ready(timeout: float = 10.0):
    """Wait (briefly) for the first status bundle after startup. During a
    worker restart the last known state keeps being served."""
    if hub.ready.is_set() or hub.last_bundle:
        return
    t_end = time.monotonic() + timeout
    while not hub.ready.is_set() and time.monotonic() < t_end:
        await asyncio.sleep(0.05)
    if not hub.ready.is_set():
        raise ServiceError(503, "The collection service is starting - try again "
                                "in a few seconds.")


async def my_categories(client: str) -> dict:
    """The caller's OWN categories with their counts. Every user has their
    own data; one user's records are never counted or shown to another."""
    cats = hub.client_cats.get(client)
    if cats is None:
        try:
            view = await hub.call("categories", timeout=30, client=client)
        except (RpcError, WorkerUnavailable) as exc:
            raise worker_error(exc) from None
        hub.set_client(client, view)
        cats = hub.client_cats.get(client, view["per_category"])
    return cats


def public_job(snap: dict | None, jlog=None, log_after: int = 0) -> dict | None:
    if snap is None:
        return None
    job = {k: v for k, v in snap.items() if k != "owner"}
    if jlog is not None:
        lines, reset = jlog.since(log_after)
        job["log"], job["log_reset"], job["log_seq"] = lines, reset, jlog.seq
    else:
        job["log"], job["log_reset"] = [], True
    return job


def _query(scope) -> dict:
    qs = scope.get("query_string") or b""
    return dict(parse_qsl(qs.decode("latin-1"))) if qs else {}


def _int(v, default: int) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Fast-path handlers (GET): fn(scope, headers, client) -> Reply
# ---------------------------------------------------------------------------

async def h_health(scope, headers, client) -> Reply:
    """Unauthenticated liveness probe. `worker` reports the collection
    worker: up | starting | down. `client_ip` is the address the per-IP
    limits apply to (check it after deploying behind a proxy: it must be
    your own public address, not the proxy's)."""
    fresh = time.time() - hub.last_bundle < 5
    worker = "up" if hub.up.is_set() and fresh else (
        "starting" if hub.proc is not None else "down")
    return jreply({"status": "ok", "version": config.APP_VERSION,
                   "auth_required": bool(config.auth_token()), "worker": worker,
                   "client_ip": scope.get("bdc", ("", ""))[1]})


async def h_config(scope, headers, client) -> Reply:
    await ensure_ready()
    cats = await my_categories(client)
    return jreply({
        "categories": [{"slug": c, **d} for c, d in cats.items()],
        "providers": ["serper"],
        "serper_key_present": bool(config.serper_api_key()),
        "defaults": {"target": config.DEFAULT_TARGET,
                     "max_target": config.MAX_TARGET,
                     "target_choices": config.TARGET_CHOICES,
                     "max_queries": config.DEFAULT_MAX_QUERIES,
                     "credits_per_query": 1 + config.PLACES_PAGES_PER_QUERY},
        "limits": {"max_active_jobs": config.MAX_ACTIVE_JOBS,
                   "jobs_per_user": config.MAX_JOBS_PER_CLIENT},
    })


async def h_status(scope, headers, client) -> Reply:
    """The caller's own collection (with live-log lines after `log_after`
    of job `log_job`), the caller's own per-category counts and the shared
    queue. Served from memory. Without log_after the whole log is returned
    (old pages)."""
    await ensure_ready()
    cats = await my_categories(client)
    q = _query(scope)
    log_after = _int(q.get("log_after"), 0)
    log_job = q.get("log_job", "")
    snap, jlog = hub.client_job(client)
    if snap is not None and log_job and log_job != snap.get("id"):
        log_after = 0                      # a different job: send it all
    job = public_job(snap, jlog, log_after)
    active = job is not None and job.get("status") in ACTIVE
    return jreply({
        "job": job,
        "per_category": cats,
        "queue": hub.queue,
        "active_jobs": hub.active,
        "poll_ms": config.POLL_MS_ACTIVE if active else config.POLL_MS_IDLE,
    })


async def h_records(scope, headers, client) -> Reply:
    await ensure_ready()
    q = _query(scope)
    category = q.get("category")
    if not category:
        return error_reply(422, "category is required")
    if category not in await my_categories(client):
        return error_reply(400, "unknown category")
    limit = max(0, min(_int(q.get("limit"), 50), config.MAX_TARGET * 2))
    cr = hub.client_rev.get(client) or {}
    rev = (cr.get("gen", 0), cr.get("cat", {}).get(category, 0))
    # the caller's own records only: the cache is keyed by client
    key = (hub.epoch, client, category, limit)
    blob = _records_cache.get(key)
    if blob is None or blob.tag < rev:
        epoch = hub.epoch

        async def load():
            try:
                data = await hub.call("records", timeout=30, client=client,
                                      category=category, limit=limit)
            except (RpcError, WorkerUnavailable) as exc:
                raise worker_error(exc) from None
            r = tuple(data.pop("rev", rev))
            return Blob(fastjson.dumps(data),
                        f'"r-{epoch}-{category}-{r[0]}.{r[1]}-{limit}"', tag=r)
        blob = await _records_cache.load(key, load)
    return blob.reply(headers, JSON_T)


def _list_files(folder: str) -> bytes:
    out = []
    if os.path.isdir(folder):
        for name in sorted(os.listdir(folder)):
            if name.endswith(".xlsx") and SAFE_FILE_RE.match(name) \
                    and ".tmp." not in name:
                p = os.path.join(folder, name)
                try:
                    st = os.stat(p)
                except OSError:
                    continue
                out.append({"name": name, "size": st.st_size, "modified": st.st_mtime})
    return fastjson.dumps({"files": out})


async def h_files(scope, headers, client) -> Reply:
    """The caller's own Excel files."""
    t, body = _files_listing.get(client, (0.0, b""))
    if time.monotonic() - t > 2.0:
        body = await asyncio.to_thread(_list_files, config.client_dir(client))
        _files_listing[client] = (time.monotonic(), body)
        _files_listing.move_to_end(client)
        while len(_files_listing) > 1000:
            _files_listing.popitem(last=False)
    return Reply(200, body)


async def _static_file(headers, name: str, ctype: bytes) -> Reply:
    path = os.path.join(config.FRONTEND_DIR, name)
    cached = _static.get(name)
    now = time.monotonic()
    if cached is None or now - cached[0] > 2.0:
        try:
            st = await asyncio.to_thread(os.stat, path)
        except OSError:
            return error_reply(404, "not found")
        etag = f'"s-{st.st_mtime_ns}-{st.st_size}"'
        if cached is None or cached[1].etag != etag:
            def read():
                with open(path, "rb") as fh:
                    return fh.read()
            data = await asyncio.to_thread(read)
            cached = (now, Blob(data, etag))
        else:
            cached = (now, cached[1])
        _static[name] = cached
    return cached[1].reply(headers, ctype)


async def h_index(scope, headers, client) -> Reply:
    return await _static_file(headers, "index.html", b"text/html; charset=utf-8")


async def h_favicon(scope, headers, client) -> Reply:
    return await _static_file(headers, "favicon.svg", b"image/svg+xml")


FAST_ROUTES = {
    "/api/health": h_health, "/api/status": h_status, "/api/config": h_config,
    "/api/records": h_records, "/api/files": h_files,
    "/": h_index, "/favicon.ico": h_favicon,
    "/health": h_health,   # platform probes; outside /api, so no auth/rate limit
}


# ---------------------------------------------------------------------------
# Guard: security checks for every request, then fast path or FastAPI.
# Request size limit, rate limits, optional shared-token auth, safe 500s,
# security headers, slow/failed request logging, access summary.
# ---------------------------------------------------------------------------

class Guard:
    def __init__(self, app_):
        self.app = app_
        self.rl = RateLimiter()
        self.stats = AccessStats()

    def _reject(self, path: str, headers: dict, ip: str, who: str):
        """(status, detail, extra headers) or None."""
        try:
            if int(headers.get(b"content-length", b"0") or 0) > MAX_BODY_BYTES:
                return 413, "Request body too large", ()
        except ValueError:
            return 400, "Invalid Content-Length", ()
        retry = ((b"retry-after", b"30"),)
        if self.rl.hit(f"ip|{ip}", config.RATE_LIMIT_IP_PER_MIN):
            log.warning("rate limit (per IP) exceeded: ip=%s path=%s", ip, path)
            return 429, "Too many requests, slow down", retry
        if not who:
            if path != "/api/health":
                return 400, "Missing or invalid X-Client-Id - reload the page", ()
            who = f"ip:{ip}"                     # health probes: rate limit only
        if path == "/api/collect":
            # per address too: a caller can send a new client id every time
            if self.rl.hit(f"collect-ip|{ip}", config.RATE_LIMIT_COLLECT_PER_IP_PER_MIN) \
                    or self.rl.hit(f"collect|{who}", config.RATE_LIMIT_COLLECT_PER_MIN):
                log.warning("rate limit (collect) exceeded: client=%s ip=%s", who[:12], ip)
                return 429, "Too many requests, slow down", retry
        elif self.rl.hit(f"api|{who}", config.RATE_LIMIT_CLIENT_PER_MIN):
            log.warning("rate limit exceeded: client=%s ip=%s path=%s", who[:12], ip, path)
            return 429, "Too many requests, slow down", retry
        token = config.auth_token()
        if token and path != "/api/health":
            supplied = headers.get(b"x-auth-token", b"")
            if not hmac.compare_digest(supplied, token.encode()):
                log.warning("auth failure: ip=%s path=%s", ip, path)
                return 401, "Access code required", ()
        return None

    @staticmethod
    async def _send(send, r: Reply, is_api: bool, method: str):
        hdrs = [(b"content-type", r.ctype),
                (b"content-length", str(len(r.body)).encode()), *r.headers]
        if is_api and not any(k == b"cache-control" for k, _ in r.headers):
            hdrs.append((b"cache-control", b"no-store"))
        hdrs.extend(SECURITY_HEADERS)
        if config.force_hsts():
            hdrs.append(HSTS)
        await send({"type": "http.response.start", "status": r.status, "headers": hdrs})
        await send({"type": "http.response.body",
                    "body": b"" if method == "HEAD" else r.body})

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path, method = scope["path"], scope["method"]
        is_api = path.startswith("/api")
        t0 = time.perf_counter()
        headers = dict(scope["headers"])
        ip = client_ip(scope, headers)
        who = client_key(headers)
        scope["bdc"] = (who, ip)          # for the FastAPI endpoints
        if is_api and method != "OPTIONS":
            bad = self._reject(path, headers, ip, who)
            if bad is not None:
                await self._send(send, Reply(bad[0], fastjson.dumps({"detail": bad[1]}),
                                             headers=list(bad[2])), True, method)
                self.stats.add(path, bad[0], time.perf_counter() - t0)
                return
        status = 0
        fast = FAST_ROUTES.get(path) if method in ("GET", "HEAD") else None
        try:
            if fast is not None:
                try:
                    r = await fast(scope, headers, who)
                except ServiceError as exc:
                    r = error_reply(exc.status, exc.detail)
                status = r.status
                await self._send(send, r, is_api, method)
            else:
                started = [0]
                hsts = config.force_hsts()

                async def send_wrapper(message):
                    if message["type"] == "http.response.start":
                        started[0] = message["status"]
                        hdrs = list(message.get("headers", ()))
                        present = {k.lower() for k, _ in hdrs}
                        hdrs.extend(h for h in SECURITY_HEADERS if h[0] not in present)
                        if is_api and b"cache-control" not in present:
                            hdrs.append((b"cache-control", b"no-store"))
                        if hsts:
                            hdrs.append(HSTS)
                        message = dict(message, headers=hdrs)
                    await send(message)
                try:
                    await self.app(scope, receive, send_wrapper)
                finally:
                    status = started[0]
        except Exception:
            # Never leak stack traces or internal paths to clients.
            log.exception("unhandled error on %s %s", method, path)
            if not status:
                await self._send(send, error_reply(500, "Internal server error"),
                                 True, method)
                status = 500
        dt = time.perf_counter() - t0
        self.stats.add(path, status, dt)
        if (method not in ("GET", "HEAD", "OPTIONS") or dt > 1.0
                or (status >= 400 and status not in (404,))):
            log.info("%s %s -> %s (%dms) ip=%s", method, path, status, dt * 1000, ip)


class Entry:
    """Outermost ASGI app. Same-origin requests (the page served by this
    app sends no Origin header on GETs) skip the CORS layer; cross-origin
    ones (file:// page, another host) go through it. CORS still wraps the
    guard, so even 401/429 responses carry CORS headers and preflights pass.
    Origins are locked to local defaults unless APP_ALLOWED_ORIGINS
    overrides them - never "*"."""

    def __init__(self, guard: Guard, cors):
        self.guard, self.cors = guard, cors

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            for k, _ in scope["headers"]:
                if k == b"origin":
                    break
            else:
                await self.guard(scope, receive, send)
                return
        await self.cors(scope, receive, send)


guard = Guard(api)
app = Entry(guard, CORSMiddleware(
    guard,
    allow_origins=config.allowed_origins(),
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-Auth-Token", "X-Client-Id"],
    # Chrome's Private Network Access: file:// pages calling 127.0.0.1 send a
    # preflight that must be acknowledged or the request is blocked.
    allow_private_network=True,
))


# ---------------------------------------------------------------------------
# FastAPI endpoints (validated bodies, downloads, less frequent calls)
# ---------------------------------------------------------------------------

@api.exception_handler(ServiceError)
async def _service_error(request: Request, exc: ServiceError):
    return Response(fastjson.dumps({"detail": exc.detail}), status_code=exc.status,
                    media_type="application/json")


def jresponse(obj, status: int = 200) -> Response:
    return Response(fastjson.dumps(obj), status_code=status, media_type="application/json")


def client_of(request: Request) -> str:
    """Caller's client id (the guard has already rejected requests without
    a valid one)."""
    return request.scope["bdc"][0]


def ip_of(request: Request) -> str:
    return request.scope["bdc"][1]


class CollectRequest(BaseModel):
    category: str = Field(max_length=80)   # fixed/custom slug, or "__custom__"
    custom_category: str = Field(default="", max_length=60)
    keywords: str = Field(default="", max_length=1000)
    country: str = Field(default="", max_length=60)
    state: str = Field(default="", max_length=60)   # state / region
    city: str = Field(default="", max_length=60)
    location: str = Field(default="", max_length=120)  # legacy free-text location
    target: int = Field(default=config.DEFAULT_TARGET, ge=1, le=config.MAX_TARGET)
    provider: str = "serper"
    max_queries: int = Field(default=config.DEFAULT_MAX_QUERIES, ge=1,
                             le=config.MAX_MAX_QUERIES)


class StopRequest(BaseModel):
    job_id: str = Field(default="", max_length=64)


class DeleteCategoryRequest(BaseModel):
    category: str = Field(max_length=80)


@api.post("/api/collect")
async def start_collect(req: CollectRequest, request: Request):
    if req.provider not in ("serper",):
        raise HTTPException(400, "unknown search provider")
    if not config.serper_api_key():
        raise HTTPException(400, "SERPER_API_KEY is not configured. "
                                 "Copy .env.example to app/.env and set your key.")
    client = client_of(request)
    geo = {"city": req.city.strip(), "state": req.state.strip(),
           "country": req.country.strip()}
    # Query location: "City, State, Country" from whichever parts are given.
    location = ", ".join(p for p in (geo["city"], geo["state"], geo["country"]) if p) \
        or req.location.strip()
    args = {"client": client, "category": req.category,
            "custom_category": req.custom_category,
            "keywords": [k for k in req.keywords.split(",") if k.strip()],
            "location": location, "geo": geo, "target": req.target,
            "provider": req.provider, "max_queries": req.max_queries,
            "ip": ip_of(request)}
    try:
        view = await hub.call("collect", timeout=30, **args)
    except (RpcError, WorkerUnavailable) as exc:
        raise worker_error(exc) from None
    lines = view.pop("log", [])
    hub.remember_job(view, lines)
    hub.job_params[view["id"]] = args
    return jresponse(public_job(view, hub.logs.get(view["id"])))


@api.post("/api/stop")
async def stop_collect(request: Request, req: StopRequest | None = None):
    try:
        view = await hub.call("stop", timeout=45, client=client_of(request),
                              job_id=(req.job_id if req else "") or None)
    except (RpcError, WorkerUnavailable) as exc:
        raise worker_error(exc) from None
    hub.remember_job(view)
    return jresponse({"ok": True})


@api.get("/api/credits")
async def credits(request: Request):
    """Lifetime Serper credit accounting across all runs and categories
    (the executed-query registry) plus the caller's current/last run."""
    try:
        data = await hub.call("credits", timeout=15, client=client_of(request))
    except (RpcError, WorkerUnavailable) as exc:
        raise worker_error(exc) from None
    return jresponse(data)


@api.get("/api/download/{name}")
async def download(name: str, request: Request):
    """One of the caller's own Excel files."""
    client = client_of(request)
    known = {d["file"] for d in (await my_categories(client)).values()} | {SUMMARY_FILE}
    if name not in known:
        raise HTTPException(404, "unknown file")
    path = os.path.join(config.client_dir(client), name)
    try:
        st = await asyncio.to_thread(os.stat, path)
    except OSError:
        raise HTTPException(404, "file not generated yet") from None
    key = (path, st.st_mtime_ns, st.st_size)
    blob = _files_cache.get(key)
    if blob is None:
        # Served from memory: the file is open only for the moment it is read,
        # so a slow download never blocks the worker's atomic replace of it
        # (Windows refuses to replace a file that is open).
        async def load():
            def read():
                with open(path, "rb") as fh:
                    return fh.read()
            try:
                data = await asyncio.to_thread(read)
            except OSError:
                raise HTTPException(404, "file not generated yet") from None
            return Blob(data, f'"f-{st.st_mtime_ns}-{st.st_size}"', compress=False)
        blob = await _files_cache.load(key, load)
    return blob.response(request, XLSX, {
        "Content-Disposition": f'attachment; filename="{name}"'})


@api.post("/api/category/delete")
async def delete_category(req: DeleteCategoryRequest, request: Request):
    """Delete one of the caller's categories' collected data (and, for a
    custom category, the category itself and its Excel file)."""
    client = client_of(request)
    try:
        data = await hub.call("delete_category", timeout=60, client=client,
                              category=req.category)
    except (RpcError, WorkerUnavailable) as exc:
        raise worker_error(exc) from None
    hub.forget_client(client)        # next poll fetches the fresh category list
    _files_listing.pop(client, None)
    return jresponse(data)


@api.post("/api/reset")
async def reset(request: Request):
    """Wipe the caller's own checkpoint (records + dedup registry). Excel
    files stay."""
    client = client_of(request)
    try:
        data = await hub.call("reset", timeout=60, client=client)
    except (RpcError, WorkerUnavailable) as exc:
        raise worker_error(exc) from None
    hub.forget_client(client)
    return jresponse(data)


# ---------------------------------------------------------------------------
# Lifecycle + single-instance guard: two copies sharing one output folder
# would run two workers that overwrite each other's checkpoints (lost
# progress, not corruption - saves are atomic). Warn loudly instead of
# refusing, so dev --reload workflows keep working.
# ---------------------------------------------------------------------------

def _pid_alive(pid: int) -> bool:
    import ctypes
    if pid <= 0 or os.name != "nt":
        return False
    handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
    if not handle:
        return False
    try:
        code = ctypes.c_ulong()
        ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        return code.value == 259  # STILL_ACTIVE
    finally:
        ctypes.windll.kernel32.CloseHandle(handle)


@api.on_event("startup")
async def _startup():
    lock = os.path.join(config.OUTPUT_DIR, "app.lock")
    try:
        if os.path.exists(lock):
            with open(lock, "r", encoding="utf-8") as fh:
                other = int(fh.read().strip() or 0)
            if other and other != os.getpid() and _pid_alive(other):
                log.warning(
                    "ANOTHER INSTANCE (PID %d) is already running with this "
                    "data folder. Run only ONE copy of the app - a second one "
                    "can overwrite the first one's collected progress. Use "
                    "start_app.bat / http://127.0.0.1:8100 and close this one.",
                    other)
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        with open(lock, "w", encoding="utf-8") as fh:
            fh.write(str(os.getpid()))
    except (OSError, ValueError):
        pass
    hub.worker_init = WORKER_INIT
    hub.start()


@api.on_event("shutdown")
async def _shutdown():
    guard.stats.flush()
    await asyncio.to_thread(hub.stop)
    lock = os.path.join(config.OUTPUT_DIR, "app.lock")
    try:
        with open(lock, "r", encoding="utf-8") as fh:
            if int(fh.read().strip() or 0) == os.getpid():
                os.remove(lock)
    except (OSError, ValueError):
        pass
    if _log_listener is not None:
        _log_listener.stop()
