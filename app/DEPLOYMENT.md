# Deployment

How to run this app beyond the local demo, deploy new versions safely, and
roll back. The app is two Python processes on one host - the web API
(FastAPI/uvicorn, started by you) and the collection worker (started and
supervised by the API) - with file state: no database, no build step.
Start it with `python -m backend.serve` (what the launchers do).

## Environment variables (production)

| Variable | Required | Purpose |
|---|---|---|
| `SERPER_API_KEY` | yes | Serper.dev key (search + Places) |
| `APP_AUTH_TOKEN` | strongly recommended | shared access code for all API calls |
| `SUPABASE_DB_URL` | on Render | Postgres connection string (Supabase: Connect → Session pooler, password URL-encoded). Turns on login / sign-up and keeps users, sessions and every user's records + Excel files in the database (schema `bdc`, created on first start), so a restart that wipes the disk loses nothing. Changed data is uploaded every `PERSIST_SYNC_INTERVAL` s (30) and at shutdown |
| `APP_ALLOWED_ORIGINS` | behind a domain | comma-separated CORS origins |
| `APP_FORCE_HTTPS` | behind TLS | `1` enables the HSTS header |
| `TRUSTED_PROXY_HOPS` | behind a proxy | number of proxies that append to `X-Forwarded-For` (Render: 1, nginx in front: 1, none: 0). The client IP is that entry from the right; check `client_ip` in `/api/health` shows your own address |
| `MAX_JOBS_PER_IP` / `MAX_CREDITS_PER_IP_PER_DAY` | no | per-network limits on paid collections (3 at once / 1500 Serper credits per 24 h, 0 = off) - a client id can be changed freely, an address cannot |
| `MAX_ACTIVE_JOBS` | no | collections running at once (default 4); others queue |
| `MAX_JOBS_PER_CLIENT` / `MAX_QUEUED_JOBS` | no | per-user and queue limits (1 / 500) |
| `RATE_LIMIT_CLIENT_PER_MIN` / `RATE_LIMIT_IP_PER_MIN` | no | per-user (240) and per-IP (60000) request limits |
| `HTTP_LIMIT_CONCURRENCY` | no | open requests before instant 503s (6000) |

Sizing: the defaults were load-tested with 2,000 simultaneous browser
sessions polling every 3 s while 4 collections ran and 4 waited (see
`tests/loadtest/`). Raising `MAX_ACTIVE_JOBS` adds crawl parallelism, not
Serper capacity - all collections share the account's request rate.

Put them in `app/.env` (never committed) or the service manager's
environment. Rotate `SERPER_API_KEY` from the serper.dev dashboard if it
ever leaks.

## Network layout

- The app binds `127.0.0.1:8100` by default (`start_app.bat`), or
  `0.0.0.0:8100` for LAN use (`start_app_network.bat` — set
  `APP_AUTH_TOKEN` first).
- For internet exposure, put a reverse proxy in front and keep uvicorn on
  localhost. Only the proxy's 443 should be reachable through the firewall.

Minimal nginx example:

```nginx
server {
    listen 443 ssl;
    server_name collector.example.com;
    # ssl_certificate ...; ssl_certificate_key ...;
    client_max_body_size 100k;
    location / {
        proxy_pass http://127.0.0.1:8100;
        proxy_http_version 1.1;
        proxy_set_header Connection "";          # keep-alive to the app
        proxy_set_header X-Forwarded-For $remote_addr;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Host $host;
    }
}
```

Then set `APP_FORCE_HTTPS=1`, `APP_ALLOWED_ORIGINS=https://collector.example.com`
and `TRUSTED_PROXY_HOPS=1` (nginx appends the caller's address) - without the
last one every user appears to come from the proxy's IP and shares its per-IP
limits.

## Versioning

- `backend/config.py` → `APP_VERSION`, surfaced at `GET /api/health`.
- Bump it on every release; keep a copy of each released app folder (see
  below) so rollback is a folder swap.

## Deployment procedure (BACKUP → BUILD → TEST → DEPLOY → VERIFY → KEEP OR ROLLBACK)

1. **Backup** the live version and its data:
   ```powershell
   Copy-Item app "app_backup_$(Get-Date -Format yyyyMMdd_HHmm)" -Recurse
   # data lives in app/output (demo_state.json + xlsx) - included above
   ```
2. **Install/refresh locked dependencies** on the new code:
   `python -m pip install -r requirements.txt`
3. **Test** the new code before switching: `test_pipeline`, `test_security`,
   `test_credits`, `test_concurrency` and `tests\loadtest\smoke.py` must
   pass (README section 8); `python -m pip_audit` must report no
   vulnerabilities. After changes to the API or engine, rerun the load test.
4. **Deploy**: stop the running instance (close its console window or stop
   the service), replace the `app` folder (keep `output/` and `.env`),
   start it again (`start_app.bat` or the service).
5. **Health check**: `GET /api/health` returns `{"status":"ok"}` with the
   new version; open the UI; run a small collection (target 20) if credits
   allow.
6. **Keep or roll back** (below). Never delete the previous backup until
   the new version has survived real use.

State compatibility: `demo_state.json` is loaded tolerantly (unknown keys
ignored, missing keys defaulted), so older state files load in newer
versions. Never edit it by hand while the app runs.

## Rollback

```powershell
# stop the app, then:
Remove-Item app -Recurse -Force
Rename-Item app_backup_<timestamp> app
# start it again; verify GET /api/health shows the previous version
```
Progress is preserved: state and Excel files live in `output/`, which the
backup contains. Nothing else holds state.

## Graceful shutdown / restart

- Ctrl+C / `stop_app.bat`: the worker stops every running collection,
  writes the final checkpoint and exits (`stop_app.bat` waits for that).
  While running, the checkpoint is written every ~5 s, so even a hard kill
  loses only seconds of work; collections resume when started again.
- If the collection worker crashes, the API restarts it (backoff; at most 5
  restarts in 10 minutes) and re-submits the collections that were running.
- Health checks (`/api/health`) are unauthenticated and cheap — point any
  uptime monitor at them. `"worker": "up"` means the collection worker is
  alive and publishing; `starting`/`down` means it is restarting / gave up.

## Monitoring

- `output/app.log` (API): failed (4xx/5xx), slow (>1 s) and mutating
  requests one by one, plus a per-minute summary line with request count,
  throughput and p50/p95 latency per endpoint; warnings for auth failures
  and rate-limit violations.
- `output/worker.log` (collections): every Serper request and page fetch,
  job lifecycle (queued / started / finished), checkpoint failures, worker
  restarts.
- Collection-level failures (failed URLs, rejected records, Serper errors)
  appear in the UI activity log and in the job counters.
- Watch: repeated 401/429 warnings (abuse), `unhandled error` entries
  (bugs), Serper credit counters (spend).

## Docker (optional — not used by default)

If containerizing later: base on `python:3.12-slim`, `pip install -r
requirements.txt`, run as a non-root user (`USER app`), expose only 8100,
pass secrets as env vars (never bake into the image), healthcheck
`CMD curl -f http://localhost:8100/api/health`, and mount `output/` as a
volume so state survives the container.
