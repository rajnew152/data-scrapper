"""Serper credit accounting + the persistent executed-query registry.

Every search the engine considers is recorded here with what it cost and what
it produced:

    query -> API call -> credits -> results -> new domains / duplicates
          -> valid records (attributed when the record is stored)

The ledger lives in the checkpoint (StateStore), so it doubles as the
EXECUTED-QUERY REGISTRY: a normalized request that was already paid for is
never sent again - not in this run, not after a restart, not from another
category - unless it is older than REEXECUTE_AFTER_S (search freshness).

Normalization is the planner's canonical form: lowercase, punctuation and
filler words ("top", "best", "in"...) dropped, plural/business-noun
variants folded and word order ignored, so "Top recruitment agencies in
St. Louis" and "recruitment agency St Louis" are the same registry entry.
"""

from __future__ import annotations

import csv
import os
import threading
import time

from .discovery import canon_geo, canon_phrase

LEDGER_MAX = 10_000                 # persisted query entries (oldest dropped)
REEXECUTE_AFTER_S = 30 * 24 * 3600  # an executed query may be re-run after this
KINDS = ("places", "organic")


def norm_query(query: str) -> str:
    """Canonical registry form of a query text ("<phrase> in <location>")."""
    q = " ".join((query or "").lower().split())
    phrase, sep, geo = q.rpartition(" in ")
    if not sep:
        phrase, geo = q, ""
    return f"{canon_phrase(phrase)}@{canon_geo(geo)}"


def ledger_key(kind: str, query: str, page: int = 1) -> str:
    return f"{kind}|{norm_query(query)}|p{int(page or 1)}"


def _blank_totals() -> dict:
    return {k: {"credits": 0, "requests": 0, "results": 0, "new": 0,
                "duplicates": 0, "valid": 0, "timeouts": 0} for k in KINDS}


class CreditLedger:
    """Thread-safe. `entries` is the executed-query registry + per-query
    efficiency log; `totals` are lifetime counters per search type."""

    def __init__(self, data: dict | None = None):
        data = data or {}
        self.entries: dict[str, dict] = {k: dict(v) for k, v in
                                         (data.get("entries") or {}).items()}
        self.totals: dict[str, dict] = _blank_totals()
        for k, v in (data.get("totals") or {}).items():
            if k in self.totals:
                self.totals[k].update({f: int(v.get(f, 0)) for f in self.totals[k]})
        self.saved = {"cache_hits": 0, "executed_skips": 0}
        self.saved.update({k: int(v) for k, v in (data.get("saved") or {}).items()
                           if k in self.saved})
        self.lock = threading.Lock()

    # -- registry -------------------------------------------------------------
    def executed(self, key: str) -> dict | None:
        """The earlier paid execution of this normalized request, if it is
        still fresh; None when the request may be sent."""
        with self.lock:
            e = self.entries.get(key)
            if not e or e.get("credits", 0) <= 0:
                return None
            if time.time() - e.get("ts", 0) > REEXECUTE_AFTER_S:
                return None
            return dict(e)

    def record(self, key: str, *, kind: str, query: str, page: int, category: str,
               credits: int, results: int, new: int, source: str = "api",
               timeouts: int = 0):
        """One search outcome. source: "api" (paid), "cache" (free re-use)."""
        with self.lock:
            e = self.entries.get(key)
            if e is None:
                if len(self.entries) >= LEDGER_MAX:
                    for k in sorted(self.entries, key=lambda k: self.entries[k].get("ts", 0)
                                    )[:LEDGER_MAX // 10]:
                        del self.entries[k]
                e = self.entries[key] = {
                    "kind": kind, "query": query, "page": page, "category": category,
                    "credits": 0, "results": 0, "new": 0, "duplicates": 0,
                    "valid": 0, "cache_hits": 0, "timeouts": 0, "ts": 0.0}
            if source == "cache":
                e["cache_hits"] += 1
                self.saved["cache_hits"] += 1
            else:
                e.update(query=query, category=category, ts=time.time())
                e["credits"] += credits
                e["results"] += results
                e["new"] += new
                e["duplicates"] += max(0, results - new)
                e["timeouts"] += timeouts
                t = self.totals.setdefault(kind, _blank_totals()[kind])
                t["credits"] += credits
                t["requests"] += 1
                t["results"] += results
                t["new"] += new
                t["duplicates"] += max(0, results - new)
                t["timeouts"] += timeouts

    def add_new(self, key: str, new: int):
        """New-business count of a search recorded moments earlier. (The
        entry is opened BEFORE the results are processed, so valid records
        from fast enrichments are never attributed to a missing entry.)"""
        with self.lock:
            e = self.entries.get(key)
            if e is None or new <= 0:
                return
            e["new"] += new
            e["duplicates"] = max(0, e["duplicates"] - new)
            t = self.totals.get(e["kind"])
            if t is not None:
                t["new"] += new
                t["duplicates"] = max(0, t["duplicates"] - new)

    def note_skip(self):
        with self.lock:
            self.saved["executed_skips"] += 1

    def attribute_valid(self, key: str):
        with self.lock:
            e = self.entries.get(key)
            if e is not None:
                e["valid"] += 1
                t = self.totals.get(e["kind"])
                if t is not None:
                    t["valid"] += 1

    def purge_category(self, category: str):
        """Category deleted: its queries may be executed again (re-collect)."""
        with self.lock:
            for k in [k for k, e in self.entries.items() if e.get("category") == category]:
                del self.entries[k]

    # -- reporting ------------------------------------------------------------
    @staticmethod
    def _eff(e: dict) -> dict:
        cr = e.get("credits", 0)
        return dict(e, valid_per_credit=round(e["valid"] / cr, 2) if cr else None,
                    duplicate_pct=round(100 * e["duplicates"] / e["results"], 1)
                    if e.get("results") else None)

    def summary(self, top: int = 10) -> dict:
        with self.lock:
            totals = {k: dict(v) for k, v in self.totals.items()}
            entries = [dict(e) for e in self.entries.values()]
            saved = dict(self.saved)
        credits = sum(t["credits"] for t in totals.values())
        valid = sum(t["valid"] for t in totals.values())
        results = sum(t["results"] for t in totals.values())
        new = sum(t["new"] for t in totals.values())
        paid = [e for e in entries if e["credits"] > 0]
        ranked = sorted(paid, key=lambda e: (-(e["valid"] / e["credits"]), -e["new"]))
        return {
            "total_credits": credits,
            "total_queries": sum(t["requests"] for t in totals.values()),
            "places_credits": totals["places"]["credits"],
            "organic_credits": totals["organic"]["credits"],
            "total_results": results,
            "new_domains": new,
            "duplicate_results": results - new,
            "valid_records": valid,
            "valid_per_credit": round(valid / credits, 2) if credits else None,
            "credits_per_valid": round(credits / valid, 3) if valid else None,
            "cache_hits": saved["cache_hits"],
            "executed_query_skips": saved["executed_skips"],
            "by_kind": {k: dict(v, valid_per_credit=round(v["valid"] / v["credits"], 2)
                             if v["credits"] else None) for k, v in totals.items()},
            "registry_size": len(entries),
            "best_queries": [self._eff(e) for e in ranked[:top]],
            "worst_queries": [self._eff(e) for e in ranked[::-1][:top]],
        }

    def write_csv(self, path: str):
        """Full per-query ledger for audit (opens in Excel)."""
        with self.lock:
            rows = sorted((dict(e) for e in self.entries.values()),
                          key=lambda e: e.get("ts", 0))
        cols = ["time", "category", "kind", "query", "page", "credits", "results",
                "new", "duplicates", "valid", "valid_per_credit", "cache_hits",
                "timeouts"]
        # a user's folder does not exist yet when their first run ends
        # before its first checkpoint
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            w.writerow(cols)
            for e in rows:
                cr = e.get("credits", 0)
                w.writerow([time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e.get("ts", 0)))
                            if e.get("ts") else "", e.get("category", ""), e["kind"],
                            e["query"], e["page"], cr, e["results"], e["new"],
                            e["duplicates"], e["valid"],
                            round(e["valid"] / cr, 2) if cr else "",
                            e.get("cache_hits", 0), e.get("timeouts", 0)])
        os.replace(tmp, path)

    def to_dict(self) -> dict:
        with self.lock:
            return {"entries": {k: dict(v) for k, v in self.entries.items()},
                    "totals": {k: dict(v) for k, v in self.totals.items()},
                    "saved": dict(self.saved)}
