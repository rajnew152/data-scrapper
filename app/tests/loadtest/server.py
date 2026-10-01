"""Launch an app instance for load testing: simulated Serper + websites, its
own data folder (a copy of a real checkpoint for realistic sizes), its own
port. Works for the current code and for a saved baseline copy.

    python tests/loadtest/server.py --root <dir containing backend/> --port 8250
        --out <empty dir> [--state <demo_state.json to copy>] [--baseline]
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--port", type=int, default=8250)
    ap.add_argument("--out", required=True)
    ap.add_argument("--state", default="")
    ap.add_argument("--baseline", action="store_true",
                    help="old code: lift its per-IP rate limit (all simulated "
                         "users share 127.0.0.1) and run it the way "
                         "start_app.bat did (uvicorn defaults, access log on)")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    if args.state:
        shutil.copyfile(args.state, os.path.join(args.out, "demo_state.json"))
    os.environ["APP_OUTPUT_DIR"] = args.out
    os.environ["SERPER_API_KEY"] = "sim-key"          # never a real key
    # every simulated user comes from 127.0.0.1: the per-IP ceiling would
    # otherwise throttle the whole test (per-client limits stay in force)
    os.environ.setdefault("RATE_LIMIT_IP_PER_MIN", "10000000")
    # ... and so do the per-address collection limits
    os.environ.setdefault("RATE_LIMIT_COLLECT_PER_IP_PER_MIN", "100000")
    os.environ.setdefault("MAX_JOBS_PER_IP", "1000")
    os.environ.setdefault("MAX_CREDITS_PER_IP_PER_DAY", "0")

    sys.path.insert(0, os.path.abspath(args.root))
    sys.path.insert(0, HERE)
    from backend import config
    config.OUTPUT_DIR = args.out
    config.STATE_PATH = os.path.join(args.out, "demo_state.json")

    import sim
    if args.baseline:
        # old code: the engine runs inside this (API) process
        sim.install()
    from backend import main as app_main
    if args.baseline:
        app_main.RATE_LIMITS = {"api": (10**9, 60.0), "collect": (10**9, 60.0)}
    else:
        # new code: the engine runs in the worker process - install the
        # simulators there (a picklable module-level function)
        app_main.WORKER_INIT = sim.install

    with open(os.path.join(args.out, "server.pid"), "w") as fh:
        fh.write(str(os.getpid()))

    import uvicorn
    if args.baseline:
        uvicorn.run(app_main.app, host="127.0.0.1", port=args.port)
    else:
        from backend import serve
        serve.run(app_main.app, host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
