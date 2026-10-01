"""Golden-output check for page analysis: speed work on dom.py / extractor.py
/ crawler.py must not change what is extracted.

Real pages are saved once into output/golden/ (git-ignored: other sites'
HTML stays out of the repo), their analysis is recorded, and every later
run compares field by field against it.

Usage (from app/):
    python tests/golden_extract.py fetch <urls.txt | records.json>   # save pages
    python tests/golden_extract.py record                           # save expected output
    python tests/golden_extract.py check                            # compare (exit 1 on a difference)
    python tests/golden_extract.py time                             # CPU ms per page

records.json is what /api/records returns (its websites and source pages
are fetched). `pytest tests/golden_extract.py` runs test_golden() (skipped
without a corpus).
"""
from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

APP = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, APP)
os.environ.setdefault("ANALYZE_PROCESSES", "0")

from backend.collector.analysis import analyze_html  # noqa: E402

GOLDEN = os.path.join(APP, "output", "golden")
PAGES = os.path.join(GOLDEN, "pages")
INDEX = os.path.join(PAGES, "index.json")
EXPECTED = os.path.join(GOLDEN, "expected.json")
TIMING_KEYS = ("parse_s", "extract_s")


def _urls(src: str) -> list[str]:
    with open(src, encoding="utf-8") as fh:
        if not src.endswith(".json"):
            return [u.strip() for u in fh if u.strip().startswith("http")]
        data = json.load(fh)
    rows = data.get("records", data) if isinstance(data, dict) else data
    return sorted({r.get(k) for r in rows for k in ("Official Website", "Source Page")
                   if str(r.get(k) or "").startswith("http")})


def fetch(src: str):
    from backend.collector.crawler import Fetcher
    urls = _urls(src)
    os.makedirs(PAGES, exist_ok=True)
    f = Fetcher()

    def get(item):
        i, url = item
        raw, final_url, _ = f.fetch(url, max_bytes=20_000_000)
        if not raw:
            return None
        with open(os.path.join(PAGES, f"{i:04d}.html"), "wb") as fh:
            fh.write(raw)
        return f"{i:04d}", final_url

    with ThreadPoolExecutor(32) as ex:
        got = dict(r for r in ex.map(get, enumerate(urls)) if r)
    with open(INDEX, "w", encoding="utf-8") as fh:
        json.dump(got, fh, indent=0)
    print(f"{len(got)} of {len(urls)} pages saved in {PAGES}")


def _pages():
    with open(INDEX, encoding="utf-8") as fh:
        index = json.load(fh)
    for key, url in sorted(index.items()):
        with open(os.path.join(PAGES, f"{key}.html"), "rb") as fh:
            yield key, url, fh.read()


def analyse_all() -> dict:
    out = {}
    for key, url, raw in _pages():
        for mine in (False, True):
            res = analyze_html(raw, url, True, True, mine, 15)
            for k in TIMING_KEYS:
                res.pop(k, None)
            out[f"{key}-{mine}"] = json.loads(json.dumps(res, sort_keys=True))
    return out


def record():
    out = analyse_all()
    with open(EXPECTED, "w", encoding="utf-8") as fh:
        json.dump(out, fh, sort_keys=True, indent=0)
    print(f"{len(out)} analyses recorded in {EXPECTED}")


def check() -> list[str]:
    with open(EXPECTED, encoding="utf-8") as fh:
        expected = json.load(fh)
    diffs = []
    for key, got in analyse_all().items():
        want = expected.get(key)
        if got != want:
            fields = sorted(f for f in set(got) | set(want or {})
                            if got.get(f) != (want or {}).get(f))
            diffs.append(f"{key}: {', '.join(fields)}")
    return diffs


def timing():
    pages = list(_pages())
    best = float("inf")
    for _ in range(3):
        t = time.process_time()
        for _key, url, raw in pages:
            analyze_html(raw, url, True, True, False, 15)
        best = min(best, time.process_time() - t)
    print(f"{len(pages)} pages: {1000 * best / len(pages):.1f} ms CPU per page (best of 3)")


def test_golden():
    if not (os.path.exists(INDEX) and os.path.exists(EXPECTED)):
        import pytest
        pytest.skip("no golden corpus (python tests/golden_extract.py fetch/record)")
    diffs = check()
    assert not diffs, f"{len(diffs)} analyses changed: {diffs[:10]}"


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    if cmd == "fetch":
        fetch(sys.argv[2])
    elif cmd == "record":
        record()
    elif cmd == "time":
        timing()
    else:
        diffs = check()
        for d in diffs[:50]:
            print("CHANGED", d)
        print(f"{len(diffs)} analyses changed" if diffs else "golden output: identical")
        sys.exit(1 if diffs else 0)


if __name__ == "__main__":
    main()
