"""Per-user data isolation + abuse limits, end to end over HTTP against a
real server process (simulated Serper + websites: no credits are spent).

    python tests/test_isolation.py

  * user B never sees user A's records, counts or Excel files
  * requests without a valid X-Client-Id are rejected (no shared IP identity)
  * the client address is the X-Forwarded-For entry the proxy appended
    (rightmost); caller-written entries to its left are ignored
  * a new client id per request does not get around the per-network limits:
    collections at once (MAX_JOBS_PER_IP), Serper credits per day
"""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
APP = os.path.dirname(HERE)
PORT = 8271
BASE = f"http://127.0.0.1:{PORT}"


def req(method, path, cid=None, body=None, xff=None):
    h = {"Content-Type": "application/json"}
    if cid:
        h["X-Client-Id"] = cid
    if xff:
        h["X-Forwarded-For"] = xff
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=60) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def rid() -> str:
    return secrets.token_hex(16)


def wait(fn, timeout: float, step: float = 0.3):
    t_end = time.time() + timeout
    while time.time() < t_end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return None


def collect(cid, ip, category="Advisory", target=20):
    return req("POST", "/api/collect", cid,
               {"category": category, "city": "Chicago", "state": "Illinois",
                "country": "USA", "target": target}, xff=f"6.6.6.6, {ip}")


def status(cid, ip="10.0.0.1"):
    return req("GET", "/api/status", cid, xff=ip)[1]


def done(cid, ip):
    job = status(cid, ip)["job"]
    return job if job and job["status"] not in (
        "queued", "pending", "running", "recovering", "waiting") else None


def main():
    out = tempfile.mkdtemp(prefix="isolation_test_")
    env = dict(os.environ, TRUSTED_PROXY_HOPS="1", MAX_JOBS_PER_IP="2",
               MAX_CREDITS_PER_IP_PER_DAY="4", RATE_LIMIT_COLLECT_PER_IP_PER_MIN="1000",
               MAX_ACTIVE_JOBS="8",
               MONGODB_URI="")   # anonymous per-browser mode, even with a local .env login setup
    log = open(os.path.join(out, "server.txt"), "w")
    srv = subprocess.Popen([sys.executable, os.path.join(HERE, "loadtest", "server.py"),
                            "--root", APP, "--port", str(PORT), "--out", out],
                           env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        assert wait(lambda: _up(), 90), "server did not start"

        # -- identity -----------------------------------------------------------
        s, body = req("GET", "/api/status")
        assert s == 400 and "X-Client-Id" in body["detail"], (s, body)
        s, body = req("GET", "/api/records?category=RPO", "short")
        assert s == 400, s
        s, body = req("GET", "/api/health", xff="1.2.3.4, 5.6.7.8")
        assert s == 200 and body["client_ip"] == "5.6.7.8", body
        print("  missing client id -> 400; client ip = rightmost X-Forwarded-For: OK")

        # -- isolation ------------------------------------------------------------
        a, b = rid(), rid()
        s, _ = collect(a, "20.0.0.1")
        assert s == 200, s
        job = wait(lambda: done(a, "20.0.0.1"), 120)
        assert job and job["status"] == "completed", job
        ra = req("GET", "/api/records?category=Advisory&limit=50", a)[1]
        rb = req("GET", "/api/records?category=Advisory&limit=50", b)[1]
        fa = req("GET", "/api/files", a)[1]["files"]
        fb = req("GET", "/api/files", b)[1]["files"]
        sb = status(b)
        assert ra["total"] == 20 and len(fa) >= 2, (ra["total"], fa)
        assert rb["total"] == 0 and fb == [], (rb["total"], fb)
        assert sb["per_category"]["Advisory"]["count"] == 0 and sb["job"] is None, sb
        name = fa[0]["name"]
        s, _ = req("GET", f"/api/download/{name}", b)
        assert s == 404, f"user B downloaded A's {name}: {s}"
        print("  user B sees none of A's records / counts / files / downloads: OK")

        # -- jobs per network, with a new client id every time -----------------------
        ip = "30.0.0.1"
        ids = [rid() for _ in range(3)]
        codes = [collect(c, ip, "RPO", 2000)[0] for c in ids[:2]]
        assert codes == [200, 200], codes
        s, body = collect(ids[2], ip, "RPO", 2000)
        assert s == 429 and "network" in body["detail"], (s, body)
        s, _ = collect(rid(), "30.0.0.2", "RPO", 20)     # another network is fine
        assert s == 200, s
        for c in ids[:2]:
            req("POST", "/api/stop", c, {}, xff=ip)
        print("  3rd collection from one network (fresh client ids) -> 429: OK")

        # -- Serper credits per network per day ------------------------------------
        ip = "40.0.0.1"
        c1 = rid()
        s, _ = collect(c1, ip, "Finance", 2000)
        assert s == 200, s
        job = wait(lambda: done(c1, ip), 180)
        assert job and job["status"] == "exhausted", job
        assert "Daily search limit" in job["stop_reason"], job["stop_reason"]
        spent = job["counters"]["serper_credits"]
        assert 4 <= spent <= 4 + 6, spent      # + searches already in flight
        s, body = collect(rid(), ip, "Finance", 20)
        assert s == 429 and "daily search limit" in body["detail"], (s, body)
        print(f"  daily credit limit stops the run ({spent} credits) and blocks "
              f"new ones from that network: OK")
        print("\nALL ISOLATION TESTS PASSED")
    finally:
        srv.terminate()
        try:
            srv.wait(30)
        except subprocess.TimeoutExpired:
            srv.kill()
        log.close()


def _up():
    try:
        return req("GET", "/api/health")[1].get("worker") == "up"
    except Exception:
        return False


if __name__ == "__main__":
    main()
