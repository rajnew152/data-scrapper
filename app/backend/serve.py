"""Production server runner (used by start_app.bat).

    python -m backend.serve [--host 127.0.0.1] [--port 8100]

Settings chosen for many concurrent browser sessions:
  * httptools HTTP parser when installed (several times faster than h11)
  * access log off - the app's middleware logs failures, slow requests and
    a per-minute summary instead of one line per status poll
  * a large accept backlog and keep-alive, so bursts of page loads queue
    instead of being refused
  * limit_concurrency: beyond this many open requests uvicorn answers 503
    at once instead of letting latency grow without bound
  * proxy headers are trusted only from FORWARDED_ALLOW_IPS (behind a
    reverse proxy set it to the proxy's address, so per-IP limits see the
    real client address)
One process by design: jobs live in the single collection worker process
the API supervises (see hub.py / worker.py).
"""

from __future__ import annotations

import argparse
import importlib.util
import os


def uvicorn_options(host: str, port: int) -> dict:
    opts = {
        "host": host, "port": port,
        "access_log": False,
        "log_level": "info",
        "backlog": 4096,
        "timeout_keep_alive": 30,
        "limit_concurrency": int(os.environ.get("HTTP_LIMIT_CONCURRENCY", "6000")),
        # Off: the app reads the client address itself (main.client_ip,
        # TRUSTED_PROXY_HOPS). uvicorn's proxy handling with the "*" that
        # Render needs took the LEFTMOST X-Forwarded-For entry, which the
        # caller writes - any per-IP limit could be dodged with a fake one.
        "proxy_headers": False,
        "server_header": False,
    }
    if importlib.util.find_spec("httptools"):
        opts["http"] = "httptools"
    return opts


def run(app, host: str = "127.0.0.1", port: int = 8100):
    import uvicorn
    uvicorn.run(app, **uvicorn_options(host, port))


def main():
    ap = argparse.ArgumentParser(description="Business Data Collector server")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8100)
    a = ap.parse_args()
    from backend.main import app
    run(app, a.host, a.port)


if __name__ == "__main__":
    main()
