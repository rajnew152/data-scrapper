# Security

Security controls in the Business Data Collector and how to operate them.
This is a small multi-user tool (one host, anonymous per-browser users,
optional shared access code) hardened for production use — defense in
depth, not a claim of being unhackable. See `SECURITY_AUDIT.md` for the audit
trail and known residual risks.

## Architecture facts that bound the attack surface

- No database (JSON state file + Excel files on disk) → no SQL injection
  surface, no DB credentials.
- No user uploads, no user accounts, no HTML rendering of crawled content.
- Only routes: `/` (the UI page) and 8 `/api/*` endpoints. API docs
  (`/docs`, `/redoc`, `/openapi.json`) are disabled.
- Static file serving is limited to two whitelists: `frontend/index.html`
  and generated `.xlsx` files matched by exact name. Backend source, `.env`,
  state and logs are never servable.

## Controls

### Secrets
- `SERPER_API_KEY` and `APP_AUTH_TOKEN` come only from environment variables
  or `app/.env` (gitignored). Never hardcoded, logged, sent to the frontend
  (only a boolean `serper_key_present`), or written into Excel files.
- `.env.example` contains placeholders only.

### Authentication (optional, recommended beyond localhost)
- Set `APP_AUTH_TOKEN` in `.env`. Every `/api` request (except
  `/api/health`) then requires the `X-Auth-Token` header; comparison is
  constant-time. The web page prompts once and stores the code in the
  browser's localStorage. Failures are logged with client IP.

### User separation (X-Client-Id)
- Each browser generates a random 128-bit id (`crypto.getRandomValues`),
  keeps it in localStorage and sends it as `X-Client-Id`. A user's status,
  live log and Stop apply only to collections started with their id; the
  id of a collection's owner is stripped from every API response, so users
  cannot learn each other's ids from the app.
- This separates cooperating users; it is **not authentication**. Anyone
  who can read another user's browser storage could act as them. Access
  control remains `APP_AUTH_TOKEN` (and, for internet exposure, the reverse
  proxy). Requests without a valid id are rejected (400): grouping them by
  IP would let anyone who can fake that IP read their data.
- Because an id costs nothing to change, everything that spends Serper
  credits is also limited per network address: `MAX_JOBS_PER_IP`,
  `MAX_CREDITS_PER_IP_PER_DAY`, `RATE_LIMIT_COLLECT_PER_IP_PER_MIN`. The
  address is read from `X-Forwarded-For` only as far as `TRUSTED_PROXY_HOPS`
  proxies wrote it; entries a caller adds are ignored.

### Rate limiting & abuse
- In-memory sliding-window limits per user (client id, or IP without one):
  240 requests/min on `/api`, 10/min on `/api/collect`; plus a per-IP
  ceiling (60,000/min) against single-address floods. Violations return
  429 with `Retry-After` and are logged. All limits are configurable.
- Request bodies over 64 KB are rejected (413).
- Collections: at most `MAX_ACTIVE_JOBS` run at once, `MAX_QUEUED_JOBS`
  wait (503 beyond), each user may have `MAX_JOBS_PER_CLIENT` (409 beyond).
- Serper spend is capped per run (credit safety cap scaled to the target,
  target ≤ 2000); duplicate queries are skipped; a run stops at its target.
  All runs share one account-wide Serper rate limiter.
- `HTTP_LIMIT_CONCURRENCY` (default 6000): beyond that many open requests
  the server answers 503 at once instead of queueing without bound.

### Process isolation
- The collection worker runs in its own process, connected to the web
  process by a private multiprocessing pipe created at startup (not a
  network port; nothing else can connect to it once established). A
  crash or memory blow-up in scraping cannot take the web API down; the
  API restarts the worker. HTML-parsing processes exit with their worker.

### SSRF (crawler)
The crawler fetches URLs originating from search results, so every fetch —
and every redirect hop (max 5, followed manually) — passes
`backend/collector/ssrf.py`:
- http/https only; web ports only (80, 443, 8080, 8443)
- blocked: localhost & `.local`/`.internal`/`.lan` names, cloud metadata
  hosts, and any hostname resolving to private / loopback / link-local /
  CGN / reserved / multicast / unspecified addresses (includes
  169.254.169.254)
- plus: 2 MB response cap, connect/read timeouts, robots.txt respected,
  page-depth limit (homepage + ≤2 contact pages per site), per-domain
  dedup (each domain fetched once per run). Downloaded content is parsed
  with BeautifulSoup only — never executed.

### XSS & output safety
- All dynamic values in the UI are inserted with `textContent`, never
  interpolated into HTML. Website links render only for `http(s)://` values
  and use `rel="noopener noreferrer"`.
- Content-Security-Policy (self + Google Fonts only), `X-Content-Type-Options:
  nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`,
  `Permissions-Policy` sent on every response; `Cache-Control: no-store` on
  API responses; HSTS when `APP_FORCE_HTTPS=1`.

### Excel / downloads
- Downloads accept only exact generated filenames from the category registry
  (path traversal impossible; arbitrary paths 404).
- Cell values are sanitized: illegal control characters stripped and leading
  `=`, `+`, `@` neutralized (formula-injection guard), 32k length cap.

### CORS
- Never `*`. Defaults to localhost origins (+`null` for the file-opened
  page); production origins via `APP_ALLOWED_ORIGINS`.

### Errors & logging
- A global handler converts unexpected exceptions to a generic 500; stack
  traces and paths are logged server-side only.
- Request log: every failed (4xx/5xx), slow (>1 s) or mutating request with
  method, path, status, duration, client IP, plus a per-minute summary of
  all traffic. Secrets and client ids are never logged in full.

## Reporting

This is demo software. If you find an issue, stop using the network-exposed
mode and fix before continuing.
