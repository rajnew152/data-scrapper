"""User accounts: sign-up, login, sessions.

Enabled when SUPABASE_DB_URL is set (users and sessions live in Postgres,
see db.py); without it the app keeps working with anonymous per-browser
client ids.

  * passwords are stored only as scrypt hashes (random salt per user)
  * a login returns a random session token; the database keeps only its
    SHA-256, so a leaked sessions table cannot be replayed
  * sessions expire after SESSION_DAYS (expired rows are purged at
    each login)
  * a logged-in user's identity ("u" + user id) replaces the browser
    client id: their records, jobs and Excel files follow the account
    to any browser

The store is pluggable: tests install MemoryStore with use_store().
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
import re
import secrets
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from . import config, db

log = logging.getLogger("auth")

EMAIL_RE = re.compile(r"^[^@\s]{1,64}@[^@\s]{1,190}\.[A-Za-z]{2,24}$")
PASSWORD_MIN, PASSWORD_MAX = 8, 128
NAME_MAX = 60
# scrypt cost: ~50 ms CPU / 16 MB per hash on a laptop core
SCRYPT_N, SCRYPT_R, SCRYPT_P = 2 ** 14, 8, 1
SESSION_CACHE_S = 60.0           # a session is re-checked in the DB at most this often
SESSION_CACHE_MAX = 5000


class AuthError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(detail)
        self.status, self.detail = status, detail


# ---------------------------------------------------------------- passwords
def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(password.encode(), salt=salt, n=SCRYPT_N, r=SCRYPT_R,
                        p=SCRYPT_P, maxmem=64 * 1024 * 1024, dklen=32)
    b64 = lambda b: base64.b64encode(b).decode()   # noqa: E731
    return f"scrypt${SCRYPT_N}${SCRYPT_R}${SCRYPT_P}${b64(salt)}${b64(dk)}"


def verify_password(password: str, stored: str) -> bool:
    try:
        algo, n, r, p, salt, dk = stored.split("$")
        if algo != "scrypt":
            return False
        want = base64.b64decode(dk)
        got = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt),
                             n=int(n), r=int(r), p=int(p),
                             maxmem=64 * 1024 * 1024, dklen=len(want))
        return hmac.compare_digest(got, want)
    except (ValueError, TypeError):
        return False


# A real-looking hash to verify against when the email is unknown, so a
# login for a missing account takes as long as a wrong password.
_DUMMY_HASH = hash_password(secrets.token_urlsafe(12))


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ------------------------------------------------------------------- stores
class DuplicateEmail(Exception):
    pass


class PgStore:
    """Users + sessions in Postgres (tables: db.py)."""

    def create_user(self, doc: dict) -> str:
        from psycopg.errors import UniqueViolation
        uid = secrets.token_hex(12)
        try:
            with db.connection() as conn:
                conn.execute(
                    f"INSERT INTO {db.SCHEMA}.users (id, name, email, password_hash,"
                    " created_at) VALUES (%s, %s, %s, %s, %s)",
                    (uid, doc["name"], doc["email"], doc["password_hash"],
                     doc["created_at"]))
        except UniqueViolation:
            raise DuplicateEmail() from None
        return uid

    def user_by_email(self, email: str) -> dict | None:
        with db.connection() as conn:
            row = conn.execute(
                f"SELECT id, name, email, password_hash FROM {db.SCHEMA}.users"
                " WHERE email = %s", (email,)).fetchone()
        return (dict(zip(("id", "name", "email", "password_hash"), row))
                if row else None)

    def create_session(self, token_hash: str, user: dict, expires: datetime):
        with db.connection() as conn:
            conn.execute(f"DELETE FROM {db.SCHEMA}.sessions WHERE expires_at < now()")
            conn.execute(
                f"INSERT INTO {db.SCHEMA}.sessions (token_hash, user_id, expires_at)"
                " VALUES (%s, %s, %s)", (token_hash, user["id"], expires))

    def session(self, token_hash: str) -> dict | None:
        with db.connection() as conn:
            row = conn.execute(
                f"SELECT u.id, u.name, u.email FROM {db.SCHEMA}.sessions s"
                f" JOIN {db.SCHEMA}.users u ON u.id = s.user_id"
                " WHERE s.token_hash = %s AND s.expires_at > now()",
                (token_hash,)).fetchone()
        return (dict(zip(("user_id", "name", "email"), row)) if row else None)

    def delete_session(self, token_hash: str):
        with db.connection() as conn:
            conn.execute(f"DELETE FROM {db.SCHEMA}.sessions WHERE token_hash = %s",
                         (token_hash,))


class MemoryStore:
    """Same interface, in memory (tests, or a demo without a database)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.users: dict[str, dict] = {}       # email -> doc
        self.sessions: dict[str, dict] = {}    # token hash -> doc

    def create_user(self, doc: dict) -> str:
        with self.lock:
            if doc["email"] in self.users:
                raise DuplicateEmail()
            uid = secrets.token_hex(12)
            self.users[doc["email"]] = dict(doc, id=uid)
            return uid

    def user_by_email(self, email: str) -> dict | None:
        with self.lock:
            doc = self.users.get(email)
            return dict(doc) if doc else None

    def create_session(self, token_hash: str, user: dict, expires: datetime):
        with self.lock:
            self.sessions[token_hash] = {"user_id": user["id"], "name": user["name"],
                                         "email": user["email"], "expires_at": expires}

    def session(self, token_hash: str) -> dict | None:
        with self.lock:
            s = self.sessions.get(token_hash)
            return dict(s) if s and s["expires_at"] > _now() else None

    def delete_session(self, token_hash: str):
        with self.lock:
            self.sessions.pop(token_hash, None)


_store = None
_store_lock = threading.Lock()
_cache: "OrderedDict[str, tuple[float, dict | None]]" = OrderedDict()


def enabled() -> bool:
    return _store is not None or db.enabled()


def use_store(store):
    """Install a store (tests); None = back to the database."""
    global _store
    with _store_lock:
        _store = store
        _cache.clear()


def _get_store():
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = PgStore()
    return _store


async def _call(fn, *args):
    try:
        return await asyncio.to_thread(fn, *args)
    except (AuthError, DuplicateEmail):
        raise
    except Exception:
        log.exception("user database unavailable")
        raise AuthError(503, "The user database is unavailable - try again "
                             "in a minute.") from None


def identity(user_id: str) -> str:
    """Per-user data key; matches the client-id format the app expects."""
    return f"u{user_id}"


def _public(user: dict) -> dict:
    return {"id": user["id"], "name": user["name"], "email": user["email"]}


# --------------------------------------------------------------- operations
def _clean_email(email: str) -> str:
    email = (email or "").strip().lower()
    if len(email) > 254 or not EMAIL_RE.match(email):
        raise AuthError(400, "Enter a valid email address.")
    return email


def _check_password(password: str):
    if not PASSWORD_MIN <= len(password or "") <= PASSWORD_MAX:
        raise AuthError(400, f"Password must be {PASSWORD_MIN}-{PASSWORD_MAX} characters.")


async def _new_session(user: dict) -> dict:
    token = secrets.token_urlsafe(32)
    expires = _now() + timedelta(days=config.SESSION_DAYS)
    await _call(_get_store().create_session, _token_hash(token), user, expires)
    return {"token": token, "user": _public(user),
            "expires_at": expires.isoformat(timespec="seconds")}


async def signup(name: str, email: str, password: str) -> dict:
    name = " ".join((name or "").split())[:NAME_MAX]
    if not name:
        raise AuthError(400, "Enter your name.")
    email = _clean_email(email)
    _check_password(password)
    pw_hash = await asyncio.to_thread(hash_password, password)
    doc = {"name": name, "email": email, "password_hash": pw_hash,
           "created_at": _now()}
    try:
        uid = await _call(_get_store().create_user, doc)
    except DuplicateEmail:
        raise AuthError(409, "An account with this email already exists - "
                             "log in instead.") from None
    log.info("signup: user %s", uid)
    return await _new_session({"id": uid, "name": name, "email": email})


async def login(email: str, password: str) -> dict:
    bad = AuthError(401, "Wrong email or password.")
    try:
        email = _clean_email(email)
    except AuthError:
        raise bad from None
    user = await _call(_get_store().user_by_email, email)
    ok = await asyncio.to_thread(verify_password, password or "",
                                 user["password_hash"] if user else _DUMMY_HASH)
    if not (user and ok):
        raise bad
    return await _new_session(user)


async def session_user(token: str) -> dict | None:
    """{id, name, email} for a valid session token, else None. Cached for
    SESSION_CACHE_S, so status polls do not hit the database."""
    if not token or len(token) > 200:
        return None
    th = _token_hash(token)
    hit = _cache.get(th)
    if hit is not None and hit[0] > time.monotonic():
        return hit[1]
    s = await _call(_get_store().session, th)
    user = ({"id": s["user_id"], "name": s["name"], "email": s["email"]}
            if s else None)
    _cache[th] = (time.monotonic() + SESSION_CACHE_S, user)
    _cache.move_to_end(th)
    while len(_cache) > SESSION_CACHE_MAX:
        _cache.popitem(last=False)
    return user


async def logout(token: str):
    th = _token_hash(token)
    _cache.pop(th, None)
    await _call(_get_store().delete_session, th)


def bearer(headers: dict) -> str:
    v = headers.get(b"authorization", b"").decode("latin-1")
    return v[7:].strip() if v[:7].lower() == "bearer " else ""
