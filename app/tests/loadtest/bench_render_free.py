"""Render-Free emulation benchmark (Windows only): one 100-record collection
with this process confined to BENCH_CPUS (default 0.1) CPU and 512 MB by
Windows Job Object hard caps, the render.yaml env overrides, and per-stage
profiling. Measured on 2026-10-01: an unmodified run at BENCH_CPUS=1.0 took
35.1 s, the time users saw on Render Free, so 1.0 matches the host as
observed and 0.1 is the advertised worst case.

  * stage wall times (engine Timings + homepage vs contact page fetches)
  * crawl queue wait (submit -> worker start)
  * CPU by stage (per-thread CPU deltas attributed to the sampled stack)
  * records timeline, failure reasons, peak RAM

Serper responses are recorded to output/bench/serper_tape.json and replayed
(with their recorded latency) on later runs, so before/after runs see the
same search results and repeat runs cost no credits. BENCH_REPLAY=0 = live.
A guard stops the run if credits are spent while no crawl task completes.

Usage (from app/):
    python tests/loadtest/bench_render_free.py <app_dir> <label> [target] [category] [country] [keywords]
    set BENCH_CPUS=1.0 & python tests/loadtest/bench_render_free.py . after
Results: output/bench/result_<label>.json
"""
from __future__ import annotations

import collections
import ctypes
import ctypes.wintypes as wt
import json
import os
import sys
import threading
import time

APP = os.path.abspath(sys.argv[1])
LABEL = sys.argv[2]
TARGET = int(sys.argv[3]) if len(sys.argv) > 3 else 100
CATEGORY = sys.argv[4] if len(sys.argv) > 4 else "Advisory"
COUNTRY = sys.argv[5] if len(sys.argv) > 5 else "USA"
KEYWORDS = sys.argv[6] if len(sys.argv) > 6 else "management consulting"
CPU_LIMIT = float(os.environ.get("BENCH_CPUS", "0.1"))
MEM_MB = int(os.environ.get("BENCH_MEM_MB", "512"))

# render.yaml values (+ the small-host defaults config.py would pick on Render)
RENDER_ENV = {
    "MAX_ACTIVE_JOBS": "1", "GLOBAL_CRAWL_WORKERS": "32", "SCRAPER_CONCURRENCY": "32",
    "EXPORT_MIN_INTERVAL": "60", "STATUS_PUBLISH_INTERVAL": "1",
    "ROBOTS_CACHE_HOSTS": "2000", "ANALYZE_PROCESSES": "0",
    "ANALYZE_THREADS": "1",   # what config computes on Render (0.1 CPU quota)
}
for k, v in RENDER_ENV.items():
    os.environ.setdefault(k, v)

# ---------------------------------------------------------------- job object
k32 = ctypes.WinDLL("kernel32", use_last_error=True)


class IO_COUNTERS(ctypes.Structure):
    _fields_ = [(n, ctypes.c_ulonglong) for n in (
        "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
        "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]


class BASIC(ctypes.Structure):
    _fields_ = [("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wt.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wt.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wt.DWORD),
                ("SchedulingClass", wt.DWORD)]


class EXTENDED(ctypes.Structure):
    _fields_ = [("BasicLimitInformation", BASIC), ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t)]


class CPURATE(ctypes.Structure):
    _fields_ = [("ControlFlags", wt.DWORD), ("CpuRate", wt.DWORD)]


k32.CreateJobObjectW.restype = wt.HANDLE
k32.GetCurrentProcess.restype = wt.HANDLE
k32.AssignProcessToJobObject.argtypes = [wt.HANDLE, wt.HANDLE]
k32.SetInformationJobObject.argtypes = [wt.HANDLE, ctypes.c_int, ctypes.c_void_p, wt.DWORD]
k32.GetThreadTimes.argtypes = [wt.HANDLE] + [ctypes.c_void_p] * 4
job = k32.CreateJobObjectW(None, None)
ncpu = os.cpu_count() or 1
rate = CPURATE(0x1 | 0x4, max(1, round(10000 * CPU_LIMIT / ncpu)))  # ENABLE|HARD_CAP
assert k32.SetInformationJobObject(job, 15, ctypes.byref(rate), ctypes.sizeof(rate)), ctypes.get_last_error()
ext = EXTENDED()
ext.BasicLimitInformation.LimitFlags = 0x100        # PROCESS_MEMORY
ext.ProcessMemoryLimit = MEM_MB * 1024 * 1024
assert k32.SetInformationJobObject(job, 9, ctypes.byref(ext), ctypes.sizeof(ext)), ctypes.get_last_error()
k32.GetCurrentProcess.restype = wt.HANDLE
assert k32.AssignProcessToJobObject(job, k32.GetCurrentProcess()), ctypes.get_last_error()

# sanity: the cap really holds
_t0, _c0 = time.perf_counter(), time.process_time()
while time.perf_counter() - _t0 < 2.0:
    pass
CAP_CHECK = round((time.process_time() - _c0) / (time.perf_counter() - _t0), 3)

# ------------------------------------------------------------------ the app
sys.path.insert(0, APP)
import tempfile  # noqa: E402
import logging  # noqa: E402

from backend import config  # noqa: E402

tmp = tempfile.mkdtemp(prefix=f"bench_{LABEL}_")
config.OUTPUT_DIR = tmp
config.STATE_PATH = os.path.join(tmp, "state.json")
logging.basicConfig(level=logging.INFO, filename=os.path.join(tmp, "bench.log"),
                    format="%(asctime)s %(levelname)-7s %(threadName)s %(name)s: %(message)s")

from backend.collector import engine  # noqa: E402
from backend.collector.engine import CollectionJob, StateStore  # noqa: E402

T0 = [0.0]
stage = collections.defaultdict(lambda: [0.0, 0])
stage_lock = threading.Lock()


def add(name, dt):
    with stage_lock:
        s = stage[name]
        s[0] += dt
        s[1] += 1


# queue wait: submit -> worker start
from concurrent.futures import ThreadPoolExecutor  # noqa: E402
_orig_pool_submit = ThreadPoolExecutor.submit


def _pool_submit(self, fn, /, *args, **kw):
    if getattr(fn, "__name__", "") == "_guarded" and args and isinstance(args[0], dict):
        args[0]["_sub"] = time.perf_counter()   # stamped before it can start
    return _orig_pool_submit(self, fn, *args, **kw)


ThreadPoolExecutor.submit = _pool_submit

_orig_guarded = CollectionJob._guarded


def _guarded(self, info):
    if "_sub" in info:
        add("crawl_queue_wait", time.perf_counter() - info["_sub"])
    t = time.perf_counter()
    try:
        return _orig_guarded(self, info)
    finally:
        add(f"task_{info.get('op')}", time.perf_counter() - t)


CollectionJob._guarded = _guarded

# homepage vs contact page
_orig_fa = CollectionJob._fetch_analyze


def _fa(self, url, *, extract=True, contact_links=True, mine=False):
    t = time.perf_counter()
    r = _orig_fa(self, url, extract=extract, contact_links=contact_links, mine=mine)
    kind = "page_contact" if not contact_links and extract else (
        "page_directory" if not extract else "page_home")
    add(kind + ("_ok" if r[0] is not None else "_fail"), time.perf_counter() - t)
    return r


CollectionJob._fetch_analyze = _fa

# failures
fail_reasons = collections.Counter()
_orig_nf = CollectionJob._note_failure


def _nf(self, url, reason):
    r = reason.split("(")[0].strip()[:40]
    fail_reasons["HTTP 4xx/5xx" if r.startswith("HTTP") else r] += 1
    return _orig_nf(self, url, reason)


CollectionJob._note_failure = _nf

# Serper record/replay tape: identical search results (and their recorded
# latency) for every run of the same query; misses go to the real API.
from backend.collector import search as _search  # noqa: E402
BENCH_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "output", "bench")
os.makedirs(BENCH_DIR, exist_ok=True)
TAPE_PATH = os.path.join(BENCH_DIR, "serper_tape.json")
try:
    TAPE = json.load(open(TAPE_PATH, encoding="utf-8"))
except OSError:
    TAPE = {}
tape_lock = threading.Lock()
tape_stats = collections.Counter()
_orig_send = _search.SerperProvider._send


class _FakeResp:
    status_code = 200
    headers: dict = {}

    def __init__(self, data):
        self._d = data
        self.text = json.dumps(data)[:300]

    def json(self):
        return self._d


def _send(self, url, payload):
    key = url + "|" + json.dumps(payload, sort_keys=True)
    hit = TAPE.get(key) if os.environ.get("BENCH_REPLAY", "1") == "1" else None
    if hit is not None:
        if self._slots is not None:
            self._slots.acquire()
        try:
            self.rate.acquire()
            self._tl.t_sent = time.time()
            time.sleep(hit["lat"])
        finally:
            if self._slots is not None:
                self._slots.release()
        tape_stats["replayed"] += 1
        return _FakeResp(hit["data"])
    t = time.perf_counter()
    resp = _orig_send(self, url, payload)
    tape_stats["live"] += 1
    if resp.status_code == 200:
        try:
            with tape_lock:
                TAPE[key] = {"lat": round(time.perf_counter() - t, 3), "data": resp.json()}
        except ValueError:
            pass
    return resp


_search.SerperProvider._send = _send

# new TCP/TLS connections vs requests
import urllib3.connection as _uc  # noqa: E402
conn_stats = collections.Counter()
_orig_connect = _uc.HTTPConnection.connect


def _connect(self):
    conn_stats["https" if isinstance(self, _uc.HTTPSConnection) else "http"] += 1
    return _orig_connect(self)


_uc.HTTPConnection.connect = _connect
_uc.HTTPSConnection.connect = (lambda f: (lambda self: (conn_stats.__setitem__("https", conn_stats["https"] + 1), f(self))[1]))(_uc.HTTPSConnection.connect)

# records timeline
timeline = []
_orig_append = StateStore.append_record


def _append(self, category, record, limit):
    ok = _orig_append(self, category, record, limit)
    if ok:
        timeline.append(round(time.time() - T0[0], 2))
    return ok


StateStore.append_record = _append

# excel / state writes anywhere (saver thread included)
for _name in ("export_categories", "save"):
    _orig = getattr(StateStore, _name)

    def _wrap(self, *a, _o=_orig, _n=_name, **kw):
        t = time.perf_counter()
        try:
            return _o(self, *a, **kw)
        finally:
            add(f"write_{_n}", time.perf_counter() - t)
    setattr(StateStore, _name, _wrap)

# ---------------------------------------------- CPU-by-stage thread sampler
k32.OpenThread.restype = wt.HANDLE
FT = wt.FILETIME
handles = {}
last_cpu = {}
cpu_by = collections.Counter()
samples_by = collections.Counter()


def thread_cpu(tid):
    h = handles.get(tid)
    if h is None:
        h = handles[tid] = k32.OpenThread(0x0800, False, tid)  # QUERY_LIMITED_INFORMATION
    c, e, kt, ut = FT(), FT(), FT(), FT()
    if not h or not k32.GetThreadTimes(h, ctypes.byref(c), ctypes.byref(e),
                                       ctypes.byref(kt), ctypes.byref(ut)):
        return None
    f = lambda x: (x.dwHighDateTime << 32 | x.dwLowDateTime) / 1e7  # noqa: E731
    return f(kt) + f(ut)


def classify(frame):
    chain = []
    while frame is not None and len(chain) < 40:
        chain.append((frame.f_code.co_filename.replace("\\", "/"), frame.f_code.co_name))
        frame = frame.f_back
    for fn, name in chain:          # innermost first
        if name == "do_handshake":
            return "tls_handshake"
        if name in ("getaddrinfo",):
            return "dns"
        if fn.endswith("socket.py") and name in ("create_connection",):
            return "tcp_connect"
        if fn.endswith("ssl.py") and name in ("read", "recv_into", "recv"):
            return "net_read"
        if fn.endswith("socket.py") and name in ("readinto",):
            return "net_read"
        if fn.endswith("ssl.py") and name in ("sendall", "send", "write"):
            return "net_write"
        if "openpyxl" in fn or fn.endswith("exporter.py"):
            return "excel"
        if fn.endswith("fastjson.py") or "/json/" in fn:
            return "json_state"
        if fn.endswith("dom.py") or "lxml" in fn:
            return "html_parse"
        if fn.endswith("extractor.py"):
            return "extract"
        if fn.endswith("robotparser.py"):
            return "robots_parse"
        if fn.endswith("threading.py") and name in ("wait",):
            return "idle_wait"
        if fn.endswith("queue.py") or name == "_worker" and fn.endswith("thread.py"):
            return "idle_wait"
        if name == "sleep":
            return "idle_wait"
    fn, name = chain[0] if chain else ("?", "?")
    return f"other:{os.path.basename(fn)}:{name}"


stop_sampler = threading.Event()


def sampler():
    me = threading.get_native_id()
    while not stop_sampler.wait(0.1):
        frames = sys._current_frames()
        for th in threading.enumerate():
            tid = th.native_id
            if tid is None or tid == me:
                continue
            fr = frames.get(th.ident)
            if fr is None:
                continue
            cat = classify(fr)
            cpu = thread_cpu(tid)
            if cpu is None:
                continue
            prev = last_cpu.get(tid, cpu)
            last_cpu[tid] = cpu
            cpu_by[cat] += cpu - prev
            samples_by[cat] += 1


# -------------------------------------------------------------------- run
class PMC(ctypes.Structure):
    _fields_ = [("cb", wt.DWORD), ("PageFaultCount", wt.DWORD)] + [
        (n, ctypes.c_size_t) for n in (
            "PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
            "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
            "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]


if os.environ.get("BENCH_SWITCH"):
    sys.setswitchinterval(float(os.environ["BENCH_SWITCH"]))


def main():
    if not config.serper_api_key():
        sys.exit("SERPER_API_KEY missing")
    # mimic the worker process's start-up warm-ups (they run before any job)
    if "tls-warmup" in open(os.path.join(APP, "backend", "worker.py"), encoding="utf-8").read():
        from backend.collector import tls
        tls.shared_tls_context()
    st = StateStore()
    max_q = min(config.MAX_MAX_QUERIES, max(12, TARGET // 4))
    job = CollectionJob(st, CATEGORY, [k.strip() for k in KEYWORDS.split(",")],
                        COUNTRY, TARGET, "serper", max_q, geo={"country": COUNTRY})
    job.crawl_workers = config.job_crawl_workers()
    th = threading.Thread(target=sampler, daemon=True, name="sampler")
    th.start()

    def guard():   # never burn credits on a broken pipeline
        while job.status in ("pending", "running", "recovering"):
            time.sleep(1)
            if job.counters.get("serper_credits", 0) >= 12 and job.counters.get("tasks_done", 0) == 0:
                print("GUARD: credits spent but no crawl task finished - stopping", flush=True)
                job.stop()
                return
    threading.Thread(target=guard, daemon=True).start()

    def publisher():   # what the worker does every STATUS_PUBLISH_INTERVAL
        while job.status in ("pending", "running", "recovering"):
            time.sleep(config.STATUS_PUBLISH_INTERVAL)
            t, c = time.perf_counter(), time.thread_time()
            job.snapshot(include_log=False)
            st.category_stats()
            job.log_since(0)
            add("status_bundle_wall", time.perf_counter() - t)
            add("status_bundle_cpu", time.thread_time() - c)
    threading.Thread(target=publisher, daemon=True, name="publisher").start()
    cpu0 = time.process_time()
    T0[0] = time.time()
    t0 = time.perf_counter()
    job._run()
    wall = time.perf_counter() - t0
    cpu = time.process_time() - cpu0
    stop_sampler.set()
    th.join()
    pmc = PMC()
    pmc.cb = ctypes.sizeof(PMC)
    ps = ctypes.WinDLL("psapi"); ps.GetProcessMemoryInfo.argtypes = [wt.HANDLE, ctypes.c_void_p, wt.DWORD]
    ps.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb)
    c = job.counters
    valid = len(st.records[CATEGORY])
    lat = sorted(d for d, _, _ in job._latencies)
    tim = job.timings.snapshot()
    stages = {k: {"total_s": round(v[0], 2), "count": v[1],
                  "avg_ms": round(1000 * v[0] / v[1], 1)} for k, v in sorted(stage.items())}
    tot_cpu = sum(cpu_by.values()) or 1
    out = {
        "label": LABEL, "status": job.status, "stop_reason": job.stop_reason,
        "cap_check_cpus": CAP_CHECK, "workers": job.crawl_workers,
        "wall_s": round(wall, 1), "valid_records": valid,
        "records_per_min": round(60 * valid / wall, 1),
        "first_record_s": timeline[0] if timeline else None,
        "t_at": {n: timeline[n - 1] for n in (1, 10, 25, 50, 75, 100) if len(timeline) >= n},
        "cpu_s": round(cpu, 1), "cpu_util_of_cap": round(cpu / wall / CPU_LIMIT, 2),
        "peak_ram_mb": round(pmc.PeakWorkingSetSize / 2**20),
        "peak_commit_mb": round(pmc.PeakPagefileUsage / 2**20),
        "serper_credits": c.get("serper_credits"), "serper_requests": c.get("search_requests"),
        "serper_avg_ms": round(1000 * sum(lat) / len(lat)) if lat else 0,
        "serper_max_ms": round(1000 * lat[-1]) if lat else 0,
        "serper_429": c.get("serper_429"), "serper_timeouts": c.get("serper_timeouts"),
        "organic_req": c.get("organic_requests"), "places_req": c.get("places_requests"),
        "valid_places": c.get("valid_places"), "valid_organic": c.get("valid_organic"),
        "candidates": c.get("candidate_urls"), "tasks_done": c.get("tasks_done"),
        "duplicates": c.get("duplicates"), "failed": c.get("failed"),
        "fail_reasons": dict(fail_reasons.most_common()),
        "engine_timings": tim, "stages": stages,
        "cpu_by_stage_s": {k: round(v, 2) for k, v in cpu_by.most_common(25)},
        "cpu_by_stage_pct": {k: round(100 * v / tot_cpu, 1) for k, v in cpu_by.most_common(25)},
        "thread_samples": dict(samples_by.most_common(20)),
        "out_dir": tmp,
    }
    with tape_lock:
        json.dump(TAPE, open(TAPE_PATH, "w", encoding="utf-8"))
    out["connections"] = dict(conn_stats)
    out["serper_live_calls"] = tape_stats["live"]
    out["serper_replayed"] = tape_stats["replayed"]
    res = os.path.join(BENCH_DIR, f"result_{LABEL}.json")
    with open(res, "w") as fh:
        json.dump(out, fh, indent=1)
    print(json.dumps(out, indent=1), flush=True)   # os._exit skips the flush
    os._exit(0)


if __name__ == "__main__":
    main()
