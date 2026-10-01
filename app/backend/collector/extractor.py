"""HTML data extraction: JSON-LD, mailto/tel links, visible text, meta tags.
Ported from business_collector.py (bounded regex scans included — unbounded
patterns previously froze the process on pathological pages).
Nothing is fabricated: missing values stay blank."""

from __future__ import annotations

import json
import re
from urllib.parse import unquote

from . import dom

from .normalize import clean_text, norm_domain

MAX_SCAN_CHARS = 200_000

EMAIL_RE = re.compile(
    r"[A-Za-z0-9._%+\-]{1,64}@[A-Za-z0-9\-]{1,63}(?:\.[A-Za-z0-9\-]{1,63}){0,4}\.[A-Za-z]{2,12}"
)
# Longest possible match after the "@": label + 4 x ".label" + "." + TLD.
_EMAIL_TAIL = 63 + 4 * 64 + 1 + 12


def find_emails(s: str) -> list[str]:
    """EMAIL_RE.findall(s), without its worst case: on long runs of letters
    the plain scan retries the 64-character local part at every position.
    The local part cannot contain "@", so every match starts within 64
    characters before one - only those windows are searched."""
    out: list[str] = []
    cur, n = 0, len(s)
    while cur < n:
        at = s.find("@", cur)
        if at < 0:
            break
        m = EMAIL_RE.search(s, max(cur, at - 64), at + 1 + _EMAIL_TAIL)
        if m and m.start() < at:          # a match that uses this "@"
            out.append(m.group())
            cur = m.end()
        else:
            cur = at + 1
    return out


BAD_EMAIL_PARTS = (
    "example.", "sentry", "wixpress", "domain.com", "email.com", "yourdomain",
    "@2x", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", "schema.org",
    "w3.org", "noreply", "no-reply", "donotreply", "@sentry", "your@",
    "name@", "user@", "@test.", "@localhost", "jquery", "bootstrap",
)
PHONE_RE = re.compile(
    r"(?<![\w/#])(?:\+\s?\d{1,3}[\s.\-]?)?(?:\(\s?\d{1,5}\s?\)|\d{1,5})"
    r"(?:[\s.\-]?\d{2,5}){2,4}(?![\w])"
)
DATE_LIKE_RE = re.compile(r"^\d{1,4}[./\-\s]+\d{1,2}[./\-\s]+\d{1,4}$")
YEAR_RANGE_RE = re.compile(r"^(19|20)\d{2}\s*[-–.]\s*(19|20)\d{2}$")
PHONE_CONTEXT_RE = re.compile(
    r"tel|phone|call|mob|whatsapp|contact|ph\b|dial|hotline|toll", re.I
)
ADDR_CLASS_RE = re.compile(r"(^|[\s_\-])(address|addr|location|office)([\s_\-]|$)", re.I)
ADDR_CLASS_WORDS = ("addr", "location", "office")   # substrings any match contains
ADDR_WORD_RE = re.compile(
    r"\b(street|st\.|road|rd\.|avenue|ave\.?|suite|ste\.|floor|fl\.|building|"
    r"bldg|blvd|boulevard|lane|ln\.|drive|dr\.|plaza|tower|towers|sector|nagar|"
    r"park|way|square|centre|center|complex|house|block|estate|unit|level|"
    r"po box|p\.o\. box|highway|hwy|court|ct\.|place|pl\.|marg|colony|"
    r"industrial|business park|tech park)\b", re.I,
)
SERVICE_HREF_RE = re.compile(
    r"service|solution|practice|expertise|what-we-do|offering|capabilit|"
    r"specialt|industr|product|portfolio", re.I
)
GENERIC_LINK_TEXT = {
    "services", "our services", "solutions", "our solutions", "read more",
    "learn more", "view all", "more", "home", "about", "about us", "contact",
    "contact us", "products", "industries", "view more", "see all", "explore",
    "get started", "click here", "here", "menu", "login", "sign in",
}
GENERIC_TITLE_PARTS = {
    "home", "homepage", "home page", "welcome", "index", "official site",
    "official website", "main page", "start", "untitled", "contact",
    "contact us", "about", "about us", "services", "our services",
}

ORG_TYPE_HINTS = (
    "organization", "localbusiness", "corporation", "professionalservice",
    "legalservice", "financialservice", "medicalorganization",
    "medicalbusiness", "employmentagency", "accountingservice", "dentist",
    "physician", "hospital", "store", "attorney", "insuranceagency",
    "homeandconstructionbusiness", "healthandbeautybusiness",
)


class Extracted:
    __slots__ = ("name", "email", "phone", "address", "services")

    def __init__(self):
        self.name = ""
        self.email: list[str] = []
        self.phone: list[str] = []
        self.address = ""
        self.services = ""

    def merge_missing(self, other: "Extracted"):
        for e in other.email:
            if e not in self.email and len(self.email) < 3:
                self.email.append(e)
        for p in other.phone:
            if p not in self.phone and len(self.phone) < 2:
                self.phone.append(p)
        if not self.address:
            self.address = other.address
        if not self.services:
            self.services = other.services
        if not self.name:
            self.name = other.name


def meta_content(doc, *names: str) -> str:
    for name in names:
        tag = dom.meta(doc, prop=name)
        if tag is None:
            tag = dom.meta(doc, name=name)
        if tag is not None and tag.get("content"):
            return clean_text(tag.get("content"))
    return ""


def _iter_ld(node):
    if isinstance(node, list):
        for item in node:
            yield from _iter_ld(item)
    elif isinstance(node, dict):
        yield node
        for key in ("@graph", "publisher", "mainEntity", "itemListElement",
                    "item", "provider", "author"):
            if key in node:
                yield from _iter_ld(node[key])


def _format_ld_address(addr) -> str:
    if isinstance(addr, list):
        addr = addr[0] if addr else ""
    if isinstance(addr, str):
        return clean_text(addr)
    if isinstance(addr, dict):
        parts = []
        for key in ("streetAddress", "addressLocality", "addressRegion",
                    "postalCode", "addressCountry"):
            v = addr.get(key)
            if isinstance(v, dict):
                v = v.get("name")
            if v:
                parts.append(clean_text(str(v)))
        return ", ".join(parts)
    return ""


LD_JSON_RE = re.compile(r"ld\+json", re.I)


def parse_json_ld(doc) -> dict:
    out: dict = {}
    for script in dom.scripts_with_type(doc):
        if not LD_JSON_RE.search(script.get("type") or ""):
            continue
        raw = script.text or ""
        try:
            data = json.loads(raw.strip())
        except Exception:
            continue
        for obj in _iter_ld(data):
            t = obj.get("@type")
            types = [str(x).lower() for x in (t if isinstance(t, list) else [t]) if x]
            if "website" in types and obj.get("name") and not out.get("site_name"):
                out["site_name"] = clean_text(str(obj["name"]))
            if not any(any(h in x for h in ORG_TYPE_HINTS) for x in types):
                continue
            if obj.get("name") and not out.get("name"):
                out["name"] = clean_text(str(obj.get("name")))
            if obj.get("legalName") and not out.get("name"):
                out["name"] = clean_text(str(obj.get("legalName")))
            if obj.get("email") and not out.get("email"):
                out["email"] = clean_text(str(obj["email"]).replace("mailto:", "")).lower()
            if obj.get("telephone") and not out.get("phone"):
                tel = obj["telephone"]
                out["phone"] = clean_text(str(tel[0] if isinstance(tel, list) else tel))
            if obj.get("address") and not out.get("address"):
                out["address"] = _format_ld_address(obj["address"])
            if obj.get("description") and not out.get("description"):
                out["description"] = clean_text(str(obj["description"]))
    return out


def clean_title(title: str) -> str:
    title = clean_text(title)
    if not title:
        return ""
    parts = re.split(r"\s+[\|\-–—:•·»«/]\s+|\s+[\|•·»«]\s*|\s*[\|•·»«]\s+", title)
    strip_chars = " -|:•·»"
    cands = [p.strip(strip_chars) for p in parts if p and p.strip(strip_chars)]
    cands = [c for c in cands if c.lower() not in GENERIC_TITLE_PARTS and len(c) >= 2]
    if not cands:
        return ""
    # Titles conventionally lead with the brand ("Acme Corp | tagline"), so
    # prefer the first non-generic part (the old shortest-part rule kept
    # returning taglines instead of company names).
    return cands[0][:120]


def extract_emails(doc, text: str, raw_html: str) -> list[str]:
    found: list[str] = []
    for href in dom.hrefs_starting(doc, "mailto:"):
        addr = unquote(href[7:].split("?")[0])
        found.extend(EMAIL_RE.findall(addr[:500]))
    found.extend(find_emails(text[:MAX_SCAN_CHARS]))
    found.extend(find_emails(raw_html[:MAX_SCAN_CHARS]))
    out: list[str] = []
    for e in found:
        e = e.strip(".-_").lower()
        if len(e) > 80 or any(b in e for b in BAD_EMAIL_PARTS):
            continue
        if e not in out:
            out.append(e)
        if len(out) >= 3:
            break
    return out


def _valid_phone(cand: str) -> str:
    cand = clean_text(cand)
    digits = re.sub(r"\D", "", cand)
    if not 8 <= len(digits) <= 15:
        return ""
    if len(set(digits)) <= 2:
        return ""
    if DATE_LIKE_RE.match(cand) or YEAR_RANGE_RE.match(cand):
        return ""
    if digits in ("1234567890", "0123456789", "9876543210"):
        return ""
    return cand


def extract_phones(doc, text: str) -> list[str]:
    scored: list[tuple[int, str]] = []
    seen: set[str] = set()

    def push(cand: str, score: int):
        v = _valid_phone(cand)
        if not v:
            return
        key = re.sub(r"\D", "", v)
        if key in seen:
            return
        seen.add(key)
        scored.append((score, v))

    for href in dom.hrefs_starting(doc, "tel:"):
        push(unquote(href[4:]).replace("-", " ").strip(), 10)
    text = text[:MAX_SCAN_CHARS]
    for m in PHONE_RE.finditer(text):
        ctx = text[max(0, m.start() - 30):m.start()]
        score = 0
        if PHONE_CONTEXT_RE.search(ctx):
            score += 3
        if m.group(0).strip().startswith("+"):
            score += 2
        if "(" in m.group(0):
            score += 1
        push(m.group(0), score)
    scored.sort(key=lambda t: -t[0])
    return [v for _, v in scored[:2]]


def extract_address(doc, text: str) -> str:
    def ok(t: str) -> bool:
        return 10 <= len(t) <= 250 and any(ch.isdigit() for ch in t)

    for tag in dom.address_tags(doc):
        t = clean_text(dom.text(tag, " "))
        if 10 <= len(t) <= 250:
            return t
    for el in dom.with_itemprop(doc, "address"):
        t = clean_text(dom.text(el, " "))
        if ok(t):
            return t
    for el in dom.with_class_or_id(doc, ADDR_CLASS_RE, 20, 10, ADDR_CLASS_WORDS):
        t = clean_text(dom.text(el, " "))
        if ok(t) and "@" not in t and ADDR_WORD_RE.search(t):
            return t
    for line in text.splitlines():
        # cheap checks first: clean_text never adds commas or digits
        if line.count(",") < 2 or not any(ch.isdigit() for ch in line):
            continue
        line = clean_text(line)
        if 15 <= len(line) <= 200 and line.count(",") >= 2 and ok(line) and ADDR_WORD_RE.search(line):
            return line
    return ""


def build_services(doc, ld_desc: str) -> str:
    desc = clean_text(ld_desc or meta_content(doc, "description", "og:description"))[:220]
    items: list[str] = []
    for a in dom.anchors(doc):
        if not SERVICE_HREF_RE.search(a.get("href")):
            continue
        txt = clean_text(dom.text(a, " "))
        if 3 <= len(txt) <= 45 and txt.lower() not in GENERIC_LINK_TEXT:
            if txt not in items:
                items.append(txt)
        if len(items) >= 10:
            break
    if len(items) < 3:
        for h in dom.h2_h3(doc):
            txt = clean_text(dom.text(h, " "))
            if 3 <= len(txt) <= 60 and txt.lower() not in GENERIC_LINK_TEXT and txt not in items:
                items.append(txt)
            if len(items) >= 8:
                break
    parts = []
    if desc:
        parts.append(desc)
    if items:
        parts.append("Services: " + ", ".join(items))
    return " | ".join(parts)[:500]


def extract_company(doc, raw_html: str, url: str) -> Extracted:
    """doc: a page parsed by dom.parse()."""
    ex = Extracted()
    ld = parse_json_ld(doc)
    title = dom.title(doc) or ""
    ex.name = (
        ld.get("name") or meta_content(doc, "og:site_name") or ld.get("site_name")
        or clean_title(title) or norm_domain(url)
    )[:150]
    text = dom.text(doc, "\n")[:MAX_SCAN_CHARS]
    ex.email = extract_emails(doc, text, raw_html)
    if ld.get("email") and ld["email"] not in ex.email:
        ex.email.insert(0, ld["email"])
    ex.phone = extract_phones(doc, text)
    if ld.get("phone"):
        v = _valid_phone(ld["phone"])
        if v and v not in ex.phone:
            ex.phone.insert(0, v)
    ex.address = ld.get("address") or extract_address(doc, text)
    ex.services = build_services(doc, ld.get("description", ""))
    return ex


# --------------------------------------------------------------------------- #
# City / State / Country parsing (best effort, blank when unsure)
# --------------------------------------------------------------------------- #

COUNTRIES = {
    "india", "usa", "united states", "united states of america", "uk",
    "united kingdom", "canada", "australia", "uae", "united arab emirates",
    "singapore", "ireland", "germany", "netherlands", "south africa",
    "new zealand", "philippines", "malaysia", "hong kong", "switzerland",
    "france", "spain", "italy", "sweden", "poland", "saudi arabia", "qatar",
    "nigeria", "kenya", "japan", "china", "indonesia", "thailand", "brazil",
    "mexico", "belgium", "austria", "denmark", "norway", "finland", "portugal",
}
_POSTAL_RE = re.compile(r"\b\d{4,7}(?:[-\s]\d{3,4})?\b")


def split_address(address: str, location_hint: str) -> tuple[str, str, str]:
    """Return (city, state, country) parsed from a comma-separated address.
    location_hint (the user's location input) supplies the country when the
    address itself doesn't name one. Unknown parts stay blank."""
    city = state = country = ""
    hint = clean_text(location_hint)
    if hint.lower() in COUNTRIES:
        country = hint
    parts = [clean_text(p) for p in (address or "").split(",") if clean_text(p)]
    if parts:
        last = parts[-1]
        if last.lower() in COUNTRIES:
            country = last
            parts = parts[:-1]
        if parts:
            tail = _POSTAL_RE.sub("", parts[-1]).strip()
            if tail and not any(ch.isdigit() for ch in tail) and len(tail) <= 40:
                state = tail
                parts = parts[:-1]
        if parts:
            cand = _POSTAL_RE.sub("", parts[-1]).strip()
            if cand and not any(ch.isdigit() for ch in cand) and len(cand) <= 40:
                city = cand
    if hint and not country and not state and not city:
        # No parsable address: fall back to the location the user searched in.
        if hint.lower() in COUNTRIES:
            country = hint
        else:
            city = hint
    return city, state, country
