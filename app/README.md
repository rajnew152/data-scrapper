# Business Data Collector — Demo

A local demo app that searches for businesses (Serper.dev Google Search +
Google Places), crawls their public websites, extracts contact data,
deduplicates, categorizes, and exports one Excel file per category.

Built on the collection logic of the original `business_collector.py`
(Serper client, extraction, validation lists, directory mining,
checkpointing), restructured into a small FastAPI backend with a
single-page frontend.

```
SEARCH -> DISCOVER -> CRAWL -> EXTRACT -> DEDUPLICATE -> CATEGORIZE -> EXCEL
```

## 1. Requirements

- Python 3.10+ (tested with 3.12)
- A [Serper.dev](https://serper.dev) API key (each search or Places page = 1 credit)
- Windows/macOS/Linux; no database, no Redis, no build step

## 2. Installation

```powershell
cd app
python -m pip install -r requirements.txt
```

(On this machine: `& "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe" -m pip install -r requirements.txt`)

## 3. Environment / Serper key setup

```powershell
copy .env.example .env
# then edit .env and set:  SERPER_API_KEY=your_key_here
```

The key is read only from the environment or `app/.env`. It is never
hardcoded, never logged, never sent to the frontend, and `.env` is
gitignored. Setting the `SERPER_API_KEY` environment variable also works.

## 4. Start the backend (serves the frontend too)

The app **auto-starts in the background at Windows login** (a shortcut to
`start_app_hidden.vbs` in the Startup folder) — no console window, nothing
to accidentally close. Logs go to `output/app.log`. Stop it with
`stop_app.bat`; start it again with the Desktop shortcut.

Manual options:
- **`start_app.bat`** (or the Desktop shortcut): starts the app if needed
  and opens http://127.0.0.1:8100 — if it's already running it just opens
  the browser.
- **`start_app_hidden.vbs`**: starts it with no window (what autostart uses).
- **`stop_app.bat`**: stops any running instance (progress is saved).

To use the app from a **phone or tablet on the same Wi-Fi**, run
`start_app_network.bat` instead — it prints the address to open on the device
(e.g. `http://192.168.1.5:8100`) and allows connections from your network
(allow it if Windows Firewall asks). The UI is fully responsive on mobile.

Or manually:

```powershell
cd app
python -m backend.serve --port 8100
```

(`backend.serve` applies the production server settings - httptools parser,
large backlog, per-minute access summary instead of a log line per poll.
`python -m uvicorn backend.main:app --port 8100` still works for development.)

Then open **http://127.0.0.1:8100**. There is no separate frontend server —
the page is plain HTML/JS served by FastAPI. (Opening `frontend/index.html`
directly as a file also works now, as long as the backend is running.)

## 5. Run a demo collection

1. Pick a **Category** — one of the ten built-in ones, or choose
   "＋ Custom category…" and type any category (e.g. `Cybersecurity companies`,
   `Real Estate firms`). Custom categories get their own Excel file and card.
2. Optional **Keywords** (comma separated). Suggested keyword chips appear per
   category — Finance includes Accounting, Bookkeeping, Tax, Payroll, Audit,
   CPA, CFO Services, Forensic Accounting, etc. Click a chip to add/remove it.
3. **Location** — any combination of Country, State/Region and City, worldwide
   (e.g. USA → California, India → Maharashtra, UK → London, UAE → Dubai).
   Typing `USA` offers the top 20 US states as suggestions; all fields also
   accept free text.
4. **Target** records (demo default 100; try 20 first)
5. **Max queries** — the cost control; estimated Serper credits are shown
6. Click **Start collection**

Live progress, a live activity log (searching / found / crawling /
duplicate / rejected / failed), per-category totals, latest records, and
download links all update while it runs. **Stop** saves progress.

### Many users at once

Every browser is its own user (an anonymous random id kept in the browser):
each user starts, watches and stops **their own** collection, and one
user's collection never blocks another's.

- Up to `MAX_ACTIVE_JOBS` collections (default 4) run at the same time; more
  wait in a queue and start automatically ("Queued — #2 in line").
- A category is filled by one collection at a time (its target is shared);
  a second collection for a busy category waits for the first, while
  collections for other categories go ahead of it.
- Each user may have one queued/running collection (`MAX_JOBS_PER_CLIENT`).
- Running collections share the machine fairly: equal slices of the crawl
  threads, and one account-wide Serper rate limiter (the account's limit is
  per account, not per collection).
- The header shows how many collections are running / queued.

## 6. Where the Excel files are

`app/output/` — one file per category, only that category's records:

```
00_Master_Summary.xlsx   01_Finance.xlsx   02_CDS_Corporate_Compliance.xlsx
03_Forms.xlsx   04_Advisory.xlsx   05_Law_Firms.xlsx   06_Recruitment.xlsx
07_RPO.xlsx   08_Medical_Healthcare.xlsx   09_3D_Studios.xlsx   10_Other_B2B.xlsx
```

All are downloadable from the UI. The master summary has per-category
records found / unique / with website / email / phone / address /
duplicates removed / failed URLs, plus totals.

## 7. Checkpoint / resume

Every user (browser) has their own data: records, custom categories, dedup
registry, executed queries and Excel files live in
`app/output/users/<hash of the browser's client id>/` (`state.json` + the
`.xlsx` files). One user never sees another user's records or files. Only
raw Serper responses are shared (`app/output/search_cache.json`), so the
same search is never paid for twice. A user's checkpoint is saved every ~5 s
while collections run (however many run at once), and immediately when a
collection ends, pauses or fails - a crash loses at most a few seconds of
work. Starting a new collection for the same category resumes on top of
what is already collected; already-executed queries are skipped and known
companies are treated as duplicates. If the collection worker process
crashes, the web process restarts it within seconds and the collections
that were running resume automatically.
`POST /api/reset` starts the caller's own data fresh. Data collected before
per-user storage (`app/output/demo_state.json`) is no longer shown to anyone.

## 8. Testing without Serper credits

```powershell
cd app
python tests\test_pipeline.py
```

This drives the full engine (crawl, extraction, validation, directory
mining, Places enrichment, dedup, Excel export, checkpoint reload) with a
fake search provider and canned pages — no network, no credits.

All offline suites (no credits, no network):

```powershell
python tests\test_pipeline.py      # end-to-end engine
python tests\test_credits.py       # credit accounting / executed-query registry
python tests\test_resilience.py    # fault injection, crash + resume (~2 min)
python tests\test_concurrency.py   # multi-user scheduling + concurrent jobs
python tests\test_security.py      # SSRF / formula injection / download whitelist
python tests\loadtest\smoke.py     # real server: users, queue, isolation, worker crash
```

### Load testing

`tests/loadtest/` runs the real server (simulated Serper with a 5 req/s
account limit + simulated websites, a copy of a real checkpoint) against
N simulated browser users that behave like the real page:

```powershell
python tests\loadtest\run_levels.py --root . --state output\demo_state.json `
    --levels 100,500,1000,2000 --jobs 8 --force-poll-ms 3000 --tag mytest
```

It reports per-endpoint latency percentiles, errors, a 4 Hz health probe
(UI responsiveness), server CPU / memory / threads, and each collection's
progress. Results: `tests/loadtest/results/`.

## 9. Performance tuning

The pipeline streams: crawl tasks start the moment a search result or
directory link is discovered, the next query's searches are prefetched
while sites crawl, and one slow website never blocks the run. Concurrency
is tunable in `app/.env` without code changes (see `.env.example`):
`MAX_ACTIVE_JOBS`, `GLOBAL_CRAWL_WORKERS` (split evenly over running jobs,
at most `SCRAPER_CONCURRENCY` per job), `SERPER_CONCURRENCY` / `SERPER_RPS`
(whole Serper account), `ANALYZE_PROCESSES` (HTML parsing processes shared
by all jobs). robots.txt and per-site limits are always respected. Per-stage
timings are exposed in the job status (`timings`) for diagnosing slow runs.

Architecture: the web API and the collections run in **separate
processes**. The API process (one asyncio event loop) answers every read
from memory - status, category counts, record lists (ETag/304 + gzip),
index.html, Excel downloads - and never competes with crawl threads for
Python's GIL. The collection worker process runs all jobs, the crawl
threads, the parsing process pool, the checkpoint writer and the Excel
exports, and publishes a status snapshot to the API twice a second. Logs:
`output/app.log` (API, with a per-minute request summary) and
`output/worker.log` (collections).

## 10. Known limitations

- **Target** is capped at 2000 records per collection. A collection fills
  one category; several collections (different users / categories) run in
  parallel, up to `MAX_ACTIVE_JOBS`.
- **Throughput is bounded by the Serper account**, not the server: every
  collection shares the account's request rate. More simultaneous
  collections share it; they don't multiply it.
- **Serper credits are consumed** (~2 per query with the default settings).
  When the account is out of credits the run stops with a clear error and
  saved progress; nothing crashes.
- **No JavaScript rendering.** Plain HTTP + BeautifulSoup only; a small
  minority of JS-only sites will yield a name/website but no email.
- **City/State/Country parsing is heuristic** (comma-splitting the address,
  helped by your Location input). Unknown parts stay blank — nothing is
  fabricated.
- **robots.txt is respected**; CAPTCHAs, logins and paywalls are never
  bypassed — such pages are skipped and counted as failures.
- **One machine.** State is a file (no database); the API process and one
  collection worker process run on the same host. Users are anonymous
  browser ids (plus the optional shared access code) - there are no
  accounts; all users see the same collected data.
