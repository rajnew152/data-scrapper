"""End-to-end API smoke test of the multi-user server (simulated Serper +
websites, temporary data folder, no credits):

    python tests/loadtest/smoke.py --state <demo_state.json copy>

Checks: health, per-user job start, queueing when slots/categories are
busy, per-user isolation (status / stop), incremental live log, records
ETag -> 304, worker crash -> automatic restart + job resume.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
PORT = 8261


def req(method, path, client="", body=None, headers=None, timeout=30):
    h = dict(headers or {})
    if client:
        h["X-Client-Id"] = client
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        h["Content-Type"] = "application/json"
    r = urllib.request.Request(f"http://127.0.0.1:{PORT}{path}", data=data,
                               headers=h, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, dict(resp.headers), (json.loads(raw) if raw and
                                                      "json" in resp.headers.get("Content-Type", "") else raw)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, dict(e.headers), json.loads(raw)
        except ValueError:
            return e.code, dict(e.headers), raw


def wait(pred, timeout=60, step=0.5):
    t_end = time.time() + timeout
    while time.time() < t_end:
        try:
            v = pred()
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            v = None                     # server still starting / restarting
        if v:
            return v
        time.sleep(step)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", default="")
    a = ap.parse_args()
    out = tempfile.mkdtemp(prefix="smoke_")
    env = dict(os.environ, MAX_ACTIVE_JOBS="2")
    cmd = [sys.executable, os.path.join(HERE, "server.py"), "--root", ROOT,
           "--port", str(PORT), "--out", out]
    if a.state:
        cmd += ["--state", a.state]
    log = open(os.path.join(out, "console.txt"), "w")
    srv = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env)
    ok = False
    try:
        assert wait(lambda: req("GET", "/api/health")[0] == 200 if _alive() else None, 60), \
            "server did not start"
        s, _, cfg = req("GET", "/api/config", "c" * 32)
        assert s == 200 and cfg["limits"]["max_active_jobs"] == 2, cfg
        print("config OK:", len(cfg["categories"]), "categories")

        A, B, C, D = ("a" * 32, "b" * 32, "c" * 32, "d" * 32)
        s, _, ja = req("POST", "/api/collect", A, {"category": "Law_Firms",
                                                    "country": "USA", "target": 400})
        assert s == 200 and ja["status"] in ("pending", "running", "queued"), (s, ja)
        # B's target is larger than A's: B must still be running when the
        # worker is killed after A finishes (equal targets finished within
        # 0.5 s of each other, so the crash test raced B's completion)
        s, _, jb = req("POST", "/api/collect", B, {"category": "RPO", "country": "USA",
                                                    "target": 2000})
        assert s == 200, (s, jb)
        # third job: both slots busy -> queued
        s, _, jc = req("POST", "/api/collect", C, {"category": "Advisory",
                                                    "country": "USA", "target": 400})
        assert s == 200 and jc["status"] == "queued" and jc["queue_position"] == 1, jc
        # fourth: same category as A -> queued behind it
        s, _, jd = req("POST", "/api/collect", D, {"category": "Law_Firms",
                                                    "country": "USA", "target": 450})
        assert s == 200 and jd["status"] == "queued", jd
        # same client twice -> 409
        s, _, dup = req("POST", "/api/collect", A, {"category": "Finance",
                                                     "country": "USA", "target": 400})
        assert s == 409, (s, dup)
        print("submit/queue/per-user limit OK")

        # isolation: each client sees only its own job
        for cid, j in ((A, ja), (B, jb), (C, jc)):
            s, _, st = req("GET", "/api/status", cid)
            assert st["job"]["id"] == j["id"], (cid, st["job"]["id"], j["id"])
            assert "owner" not in st["job"]
        s, _, st = req("GET", "/api/status", "e" * 32)
        assert st["job"] is None
        print("per-user status isolation OK; queue:", st["queue"])

        # incremental log
        s, _, st = req("GET", f"/api/status?log_after=0&log_job={ja['id']}", A)
        seq = st["job"]["log_seq"]
        assert st["job"]["log_reset"] and st["job"]["log"], st["job"]
        time.sleep(2)
        s, _, st2 = req("GET", f"/api/status?log_after={seq}&log_job={ja['id']}", A)
        assert not st2["job"]["log_reset"], st2["job"]
        assert st2["job"]["log_seq"] - seq == len(st2["job"]["log"]) or \
            len(st2["job"]["log"]) == 300
        print(f"incremental log OK: {len(st2['job']['log'])} new lines "
              f"(full log {len(st['job']['log'])})")

        # C can't stop B's job; C stops its own queued job
        s, _, _ = req("POST", "/api/stop", C, {"job_id": jb["id"]})
        assert s == 404, s
        s, _, _ = req("POST", "/api/stop", C, {})
        assert s == 200
        st = wait(lambda: (lambda r: r if r["job"]["status"] == "stopped" else None)(
            req("GET", "/api/status", C)[2]), 10)
        assert st and st["job"]["stop_reason"] == "stopped before it started", st
        s, _, stb = req("GET", "/api/status", B)
        assert stb["job"]["status"] in ("running", "pending", "recovering"), stb["job"]["status"]
        print("stop isolation OK")

        # records: ETag -> 304
        s, h, rec = req("GET", "/api/records?category=RPO&limit=2000", A,
                        headers={"Accept-Encoding": "identity"})
        etag = h.get("ETag") or h.get("etag")
        assert s == 200 and etag, (s, h)
        s2, _, _ = req("GET", "/api/records?category=Finance&limit=2000", A)
        s_f, h_f, _ = req("GET", "/api/records?category=Finance&limit=2000", A)
        s3, _, _ = req("GET", "/api/records?category=Finance&limit=2000", A,
                       headers={"If-None-Match": h_f.get("ETag") or h_f.get("etag")})
        assert s3 == 304, s3
        print("records ETag/304 OK")

        # A finishes -> D (same category) starts automatically
        t0 = time.time()
        st = wait(lambda: (lambda r: r if r["job"]["status"] not in
                           ("queued", "pending", "running", "recovering") else None)(
            req("GET", "/api/status", A)[2]), 240, 1.0)
        assert st and st["job"]["status"] == "completed", st and st["job"]["status"]
        print(f"job A completed {st['job']['collected']}/{st['job']['target']} "
              f"in {time.time() - t0:.0f}s more")
        std = wait(lambda: (lambda r: r if r["job"]["status"] in
                            ("running", "completed") else None)(
            req("GET", "/api/status", D)[2]), 30)
        assert std, "queued same-category job did not start"
        print("queued same-category job started after the first finished:",
              std["job"]["status"])

        # worker crash -> restart + resume of B's running job
        s, _, hl = req("GET", "/api/health")
        pid_before = None
        s, _, stb = req("GET", "/api/status", B)
        if stb["job"]["status"] in ("running", "pending", "recovering"):
            # find the worker pid: a python child of the server process
            pid_before = _worker_pid(out)
            assert pid_before, "worker process not found"
            from loadgen import proc_tree
            parsers = [p for p in proc_tree(pid_before) if p != pid_before]
            subprocess.run(["taskkill", "/F", "/PID", str(pid_before)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            gone = wait(lambda: not any(_pid_exists(p) for p in parsers), 15)
            assert gone, f"orphaned parser processes after the worker died: {parsers}"
            print(f"dead worker's {len(parsers)} parser processes exited with it: OK")
            t0 = time.time()
            ok_restart = wait(lambda: (lambda p: p if p and p != pid_before else None)(
                _worker_pid(out)), 30)
            assert ok_restart, "worker was not restarted"
            stb2 = wait(lambda: (lambda r: r if r["job"] and r["job"]["id"] == stb["job"]["id"]
                                 and r["job"]["status"] in ("running", "completed")
                                 and r["job"]["log_seq"] < stb["job"]["log_seq"] else None)(
                req("GET", "/api/status", B)[2]), 60)
            assert stb2, "job was not resumed after the worker restart"
            print(f"worker crash -> restarted in {time.time() - t0:.1f}s, job B resumed "
                  f"at {stb2['job']['collected']}/{stb2['job']['target']} "
                  f"(was {stb['job']['collected']})")
        s, _, hl = req("GET", "/api/health")
        print("health:", hl)
        ok = True
    finally:
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(srv.pid)],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        log.close()
        print("server logs in", out)
    print("\nSMOKE TEST PASSED" if ok else "\nSMOKE TEST FAILED")
    sys.exit(0 if ok else 1)


def _alive():
    return True


def _pid_exists(pid: int) -> bool:
    import ctypes
    k32 = ctypes.windll.kernel32
    h = k32.OpenProcess(0x1000, False, pid)
    if not h:
        return False
    try:
        code = ctypes.c_ulong()
        k32.GetExitCodeProcess(h, ctypes.byref(code))
        return code.value == 259
    finally:
        k32.CloseHandle(h)


def _worker_pid(out: str):
    """The current worker's pid, from the server's own log line. (Walking
    the process tree picked the wrong process when the server runs from a
    Windows venv: its python.exe is a launcher whose child is the real
    server, so the "child with children" was the API server itself.)"""
    import re
    try:
        with open(os.path.join(out, "console.txt"), encoding="utf-8",
                  errors="replace") as fh:
            pids = re.findall(r"collection worker process started \(pid (\d+)\)",
                              fh.read())
    except OSError:
        return None
    return int(pids[-1]) if pids else None

if __name__ == "__main__":
    main()
