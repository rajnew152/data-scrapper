"""Crawl-path benchmark: the REAL Fetcher (TLS, robots.txt, redirects,
retries), extraction, validation, dedup, checkpoint saver and Excel export,
against a local HTTPS "internet" of simulated company sites. No Serper, no
real network, no credits. Optionally capped at a fraction of one CPU core
(Windows Job Object) to emulate a small host such as Render Free (0.1 CPU).

    python tests/loadtest/bench_crawl.py --root <dir containing backend/>
        --urls 100 [--cpu 0.1] [--state <demo_state.json to copy>] [--out r.json]

The simulated web (a separate, uncapped process) serves firmN.co.uk under a
throw-away CA; each host deterministically behaves as a normal site (60 %:
homepage with contact data, else a /contact page), slow (5-9 s), HTTP 403,
404, 429 or 503. deadN.co.uk hosts are black-holed (connect timeout) and
nxN.co.uk hosts do not resolve. ~12 % of the URL list are duplicates (same
URL, other paths / query strings of a known domain). robots.txt: 70 %
missing, 25 % partial rules, 5 % disallow everything.

--root lets the same benchmark run a saved copy of older code (e.g. a git
worktree of the previous commit) for before/after numbers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time

TLD = "co.uk"
BLACKHOLE = "10.255.255.1"


def hv(*parts) -> int:
    return int(hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()[:8], 16)


def site_kind(i: int) -> str:
    r = hv("kind", i) % 100
    return ("dead" if r < 6 else "nx" if r < 9 else "403" if r < 15 else
            "404" if r < 20 else "429" if r < 24 else "503" if r < 29 else
            "slow" if r < 33 else "ok")


def host_for(i: int) -> str:
    k = site_kind(i)
    return f"{'dead' if k == 'dead' else 'nx' if k == 'nx' else 'firm'}{i}.{TLD}"


# --------------------------------------------------------------------------- #
# Simulated web (separate process)
# --------------------------------------------------------------------------- #

FILLER = " ".join(f"<p>Our team delivers {w} services with a focus on quality, "
                  f"compliance and measurable outcomes for clients.</p>"
                  for w in ("advisory", "audit", "tax", "strategy", "people",
                            "operations", "technology", "risk") * 12)
NAV = "".join(f'<li><a href="/section-{k}">Section {k}</a></li>' for k in range(60))


def page(i: int, contact_page: bool) -> bytes:
    name = f"Harbor Advisory {i}"
    rich = hv("rich", i) % 10 < 6
    tel = f'<a href="tel:+1 312 555 {i % 10000:04d}">Call +1 312 555 {i % 10000:04d}</a>'
    if rich or contact_page:
        contact = (f'<a href="mailto:info@firm{i}.{TLD}">Email us</a> {tel}'
                   f"<address>{100 + i % 800} Main Street, Chicago, Illinois 60601, "
                   f"USA</address>")
    else:
        contact = tel
    return (f"<html><head><title>{name} | Advisory Services</title>"
            f'<meta name="description" content="{name} advisory services"></head>'
            f"<body><nav><ul>{NAV}</ul></nav><h1>{name}</h1>{contact}"
            f"<main>{FILLER}</main><a href=\"/contact\">Contact us</a> "
            f"<a href=\"/about\">About</a></body></html>").encode()


def serve(port: int, cert: str, key: str):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    counts: dict[str, int] = {}
    lock = threading.Lock()

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _send(self, status: int, body: bytes = b"", ctype: str = "text/html"):
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            host = (self.headers.get("Host") or "").split(":")[0]
            with lock:
                counts["requests"] = counts.get("requests", 0) + 1
                k = f"{host}{self.path}"
                counts[k] = counts.get(k, 0) + 1
            if host == f"stats.{TLD}":
                with lock:
                    total = counts.get("requests", 0)
                    repeats = sum(v - 1 for kk, v in counts.items()
                                  if kk != "requests" and v > 1)
                return self._send(200, json.dumps({"requests": total,
                                                   "repeated": repeats}).encode(),
                                  "application/json")
            digits = "".join(ch for ch in host.split(".")[0] if ch.isdigit())
            i = int(digits) if digits else 0
            if self.path == "/robots.txt":
                r = hv("robots", i) % 100
                if r < 70:
                    return self._send(404, b"not found")
                rules = "Disallow: /" if r >= 95 else "Disallow: /private/"
                return self._send(200, f"User-agent: *\n{rules}\n".encode(),
                                  "text/plain")
            kind = site_kind(i)
            if kind == "slow":
                time.sleep(random.uniform(5, 9))
            else:
                time.sleep(min(3.0, 0.15 * math.exp(random.gauss(0, 0.6))))
            if kind in ("403", "429", "503"):
                return self._send(int(kind), b"<html>error</html>")
            if kind == "404" and self.path in ("/", ""):
                return self._send(404, b"<html>not found</html>")
            self._send(200, page(i, "contact" in self.path))

    srv = ThreadingHTTPServer(("127.0.0.1", port), H)
    srv.daemon_threads = True
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    print("ready", flush=True)
    srv.serve_forever()


def make_certs(d: str) -> tuple[str, str, str]:
    """Throw-away CA + *.co.uk server certificate (openssl CLI)."""
    def run(*args):
        subprocess.run(["openssl", *args], cwd=d, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    with open(os.path.join(d, "ext.cnf"), "w") as fh:
        fh.write(f"subjectAltName=DNS:*.{TLD}\nbasicConstraints=CA:FALSE\n"
                 "keyUsage=digitalSignature,keyEncipherment\n"
                 "extendedKeyUsage=serverAuth\n")
    run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key",
        "-out", "ca.pem", "-days", "2", "-subj", "/CN=Bench CA",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    run("req", "-newkey", "rsa:2048", "-nodes", "-keyout", "srv.key", "-out",
        "srv.csr", "-subj", f"/CN=*.{TLD}")
    run("x509", "-req", "-in", "srv.csr", "-CA", "ca.pem", "-CAkey", "ca.key",
        "-CAcreateserial", "-out", "srv.pem", "-days", "2", "-extfile", "ext.cnf")
    return (os.path.join(d, "ca.pem"), os.path.join(d, "srv.pem"),
            os.path.join(d, "srv.key"))


# --------------------------------------------------------------------------- #
# Driver (the measured process)
# --------------------------------------------------------------------------- #

def cap_cpu(cores: float) -> str:
    """Cap this process at `cores` of one CPU (Windows Job Object hard cap)."""
    if os.name != "nt":
        return "no cap (Windows only)"
    import ctypes
    import ctypes.wintypes as w
    k = ctypes.WinDLL("kernel32", use_last_error=True)
    k.CreateJobObjectW.restype = w.HANDLE
    k.GetCurrentProcess.restype = w.HANDLE
    k.SetInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p,
                                          w.DWORD]
    k.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]

    class Rate(ctypes.Structure):
        _fields_ = [("ControlFlags", w.DWORD), ("CpuRate", w.DWORD)]
    job = k.CreateJobObjectW(None, None)
    rate = max(1, round(cores / os.cpu_count() * 10000))
    info = Rate(0x1 | 0x4, rate)                      # ENABLE | HARD_CAP
    if not (k.SetInformationJobObject(job, 15, ctypes.byref(info), ctypes.sizeof(info))
            and k.AssignProcessToJobObject(job, k.GetCurrentProcess())):
        return f"cap failed ({ctypes.get_last_error()})"
    globals()["_job_handle"] = job
    return f"{cores} core"


def peak_memory_mb() -> float:
    if os.name == "nt":
        import ctypes
        import ctypes.wintypes as w

        class PMC(ctypes.Structure):
            _fields_ = [("cb", w.DWORD), ("PageFaultCount", w.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t)] + \
                       [(f"f{n}", ctypes.c_size_t) for n in range(6)]
        pmc = PMC()
        pmc.cb = ctypes.sizeof(pmc)
        k = ctypes.WinDLL("kernel32")
        k.GetCurrentProcess.restype = w.HANDLE
        psapi = ctypes.WinDLL("psapi")
        psapi.GetProcessMemoryInfo.argtypes = [w.HANDLE, ctypes.c_void_p, w.DWORD]
        psapi.GetProcessMemoryInfo(k.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb)
        return pmc.PeakWorkingSetSize / 2**20
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def url_list(n: int, seed: int = 7) -> list[str]:
    rng = random.Random(seed)
    urls = [f"https://{host_for(i)}/" for i in range(n)]
    dups = []
    for _ in range(int(n * 0.12)):
        i = rng.randrange(n)
        dups.append(rng.choice([f"https://{host_for(i)}/",
                                f"https://{host_for(i)}/about",
                                f"https://{host_for(i)}/?utm_source=x"]))
    return urls + dups


def drive(args):
    out_dir = tempfile.mkdtemp(prefix="bench_out_")
    os.environ.update({
        "APP_OUTPUT_DIR": out_dir, "ANALYZE_PROCESSES": "0",
        # render.yaml (free plan) settings; older code ignores unknown ones
        "MAX_ACTIVE_JOBS": "1", "GLOBAL_CRAWL_WORKERS": "32",
        "SCRAPER_CONCURRENCY": "32", "EXPORT_MIN_INTERVAL": "60",
        "STATUS_PUBLISH_INTERVAL": "1", "ROBOTS_CACHE_HOSTS": "2000"})
    if args.state:
        shutil.copyfile(args.state, os.path.join(out_dir, "demo_state.json"))
    sys.path.insert(0, os.path.abspath(args.root))

    # the local "internet": every *.co.uk name resolves to the simulator,
    # deadN to a black hole, nxN to nothing
    real_gai = socket.getaddrinfo

    def gai(host, port, *a, **kw):
        if isinstance(host, str) and host.endswith("." + TLD):
            if host.startswith("nx"):
                raise socket.gaierror(11001, "host not found")
            ip = BLACKHOLE if host.startswith("dead") else "127.0.0.1"
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, args.port))]
        return real_gai(host, port, *a, **kw)
    socket.getaddrinfo = gai

    # trust the throw-away CA: the same bundle file for old and new code
    bundle = os.path.join(out_dir, "bundle.pem")
    import certifi
    with open(bundle, "wb") as fh:
        fh.write(open(certifi.where(), "rb").read() + b"\n" + open(args.ca, "rb").read())
    import requests.adapters
    import requests.utils
    requests.utils.DEFAULT_CA_BUNDLE_PATH = bundle
    requests.adapters.DEFAULT_CA_BUNDLE_PATH = bundle

    from backend import config
    config.OUTPUT_DIR = out_dir
    config.STATE_PATH = os.path.join(out_dir, "demo_state.json")
    from backend.collector import crawler
    crawler.url_block_reason = lambda url: ""   # loopback is the whole point here
    try:
        from backend.collector import tls
        tls.DEFAULT_CA_BUNDLE_PATH = bundle
    except ImportError:
        pass                                       # older code
    from backend.collector.engine import CollectionJob, StateStore
    from backend.worker import Worker
    import pickle

    state = StateStore.load(config.STATE_PATH)
    category = "Advisory"
    job = CollectionJob(state, category, [], "Chicago, Illinois, USA", 2000,
                        "serper", 12, geo={"city": "Chicago", "state": "Illinois",
                                           "country": "USA"})
    job.crawl_workers = config.job_crawl_workers()
    before = len(state.records[category])

    lat: list[float] = []
    reasons: dict[str, int] = {}
    fetch = job.fetcher.fetch

    def timed_fetch(url, *a, **kw):
        t = time.perf_counter()
        r = fetch(url, *a, **kw)
        lat.append(time.perf_counter() - t)
        return r
    job.fetcher.fetch = timed_fetch
    note = job._note_failure

    def note_failure(url, reason):
        key = reason.split("(")[0].strip()
        reasons[key] = reasons.get(key, 0) + 1
        note(url, reason)
    job._note_failure = note_failure

    cap = cap_cpu(args.cpu) if args.cpu else "uncapped"
    if os.name == "nt":   # a laptop sleeping mid-run would corrupt the wall times
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)  # CONTINUOUS|SYSTEM
    urls = url_list(args.urls)
    stop_pub = threading.Event()
    bundles = [0]

    def publisher():               # the worker's status bundle, same cadence
        last_rev = -1
        while not stop_pub.wait(config.STATUS_PUBLISH_INTERVAL):
            b = {"jobs": {job.id: job.snapshot(include_log=False)},
                 "logs": job.log_since(0)[1][-50:]}
            if state.rev != last_rev:
                last_rev = state.rev
                b["per_category"] = Worker.per_category(state)
            pickle.dumps(b)
            bundles[0] += 1

    c0, t0 = time.process_time(), time.perf_counter()
    threading.Thread(target=publisher, daemon=True).start()
    job.status = "running"
    job.run_started = time.time()
    job._pool = job._new_crawl_pool()
    submitted = skipped = 0
    for u in urls:
        if job._claim_result(job._crawl_key(u)):
            job._submit_crawl(u, "advisory firms Chicago", False)
            submitted += 1
        else:
            skipped += 1
    while job._crawl_backlog():
        time.sleep(0.25)
        job.checkpoint()
    t_crawl = time.perf_counter() - t0
    c_crawl = time.process_time() - c0
    t1, c1 = time.perf_counter(), time.process_time()
    job._finalize_outputs()
    t_final, c_final = time.perf_counter() - t1, time.process_time() - c1
    stop_pub.set()
    job._pool.shutdown(wait=False)
    wall, cpu = time.perf_counter() - t0, time.process_time() - c0

    import urllib.request
    ctx = ssl.create_default_context(cafile=args.ca)
    stats = json.loads(urllib.request.urlopen(
        f"https://stats.{TLD}:{args.port}/", context=ctx, timeout=10).read())
    lat.sort()
    added = len(state.records[category]) - before
    res = {
        "root": args.root, "urls": len(urls), "unique_submitted": submitted,
        "duplicates_skipped": skipped, "cpu_cap": cap,
        "wall_s": round(wall, 1), "cpu_s": round(cpu, 1),
        "crawl_wall_s": round(t_crawl, 1), "crawl_cpu_s": round(c_crawl, 1),
        "final_export_wall_s": round(t_final, 1), "final_export_cpu_s": round(c_final, 2),
        "sites_per_min": round(60 * submitted / t_crawl, 1),
        "records_added": added, "records_per_min": round(60 * added / t_crawl, 1),
        "page_fetches": len(lat),
        "fetch_avg_ms": round(1000 * sum(lat) / len(lat)) if lat else 0,
        "fetch_p50_ms": round(1000 * lat[len(lat) // 2]) if lat else 0,
        "fetch_p95_ms": round(1000 * lat[int(0.95 * (len(lat) - 1))]) if lat else 0,
        "failures": sum(reasons.values()), "failure_reasons": reasons,
        "timeouts": sum(v for k, v in reasons.items() if "timeout" in k),
        "http_requests_at_server": stats["requests"] - 1,
        "repeated_requests_at_server": stats["repeated"],
        "status_bundles": bundles[0], "checkpoint_saves": state.saves,
        "peak_memory_mb": round(peak_memory_mb(), 1),
    }
    print(json.dumps(res, indent=2), flush=True)
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(res, fh, indent=2)
    os._exit(0)   # crawl threads stuck on black-holed connects: don't wait


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))))
    ap.add_argument("--urls", type=int, default=100)
    ap.add_argument("--cpu", type=float, default=0.0, help="cap, cores (0 = none)")
    ap.add_argument("--state", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--ca", default="")
    ap.add_argument("--serve", nargs=2, metavar=("CERT", "KEY"))
    args = ap.parse_args()
    if args.serve:
        serve(args.port, *args.serve)
        return
    certs = tempfile.mkdtemp(prefix="bench_ca_")
    ca, cert, key = make_certs(certs)
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = subprocess.Popen([sys.executable, os.path.abspath(__file__), "--serve", cert,
                            key, "--port", str(port)], stdout=subprocess.PIPE, text=True)
    try:
        assert srv.stdout.readline().strip() == "ready"
        cmd = [sys.executable, os.path.abspath(__file__), "--root", args.root,
               "--urls", str(args.urls), "--cpu", str(args.cpu), "--port", str(port),
               "--ca", ca, "--state", args.state, "--out", args.out, "--drive"]
        subprocess.run(cmd, check=True)
    finally:
        srv.kill()


if __name__ == "__main__":
    if "--drive" in sys.argv:
        sys.argv.remove("--drive")
        ap = argparse.ArgumentParser()
        for a in ("--root", "--state", "--out", "--ca"):
            ap.add_argument(a, default="")
        ap.add_argument("--urls", type=int)
        ap.add_argument("--cpu", type=float)
        ap.add_argument("--port", type=int)
        drive(ap.parse_args())
    else:
        main()
