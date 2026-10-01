"""Accounts: sign-up, login, sessions and the login wall, driven through the
real ASGI app with an in-memory user store (no MongoDB needed).
Run:  python tests/test_auth.py  (from the app/ directory)"""

from __future__ import annotations

import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import auth, main  # noqa: E402

CID = b"testclient0123456789"


async def request(method: str, path: str, body: dict | None = None,
                  token: str = "", ip: str = "10.0.0.1") -> tuple[int, dict, dict]:
    raw = json.dumps(body).encode() if body is not None else b""
    headers = [(b"host", b"testserver"), (b"x-client-id", CID)]
    if raw:
        headers += [(b"content-type", b"application/json"),
                    (b"content-length", str(len(raw)).encode())]
    if token:
        headers.append((b"authorization", f"Bearer {token}".encode()))
    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
             "method": method, "scheme": "http", "path": path,
             "raw_path": path.encode(), "query_string": b"", "root_path": "",
             "headers": headers, "client": (ip, 50000), "server": ("testserver", 80)}
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": raw, "more_body": False}

    out = {"status": 0, "headers": {}, "body": b""}

    async def send(msg):
        if msg["type"] == "http.response.start":
            out["status"] = msg["status"]
            out["headers"] = {k.decode().lower(): v.decode() for k, v in msg["headers"]}
        elif msg["type"] == "http.response.body":
            out["body"] += msg.get("body", b"")

    await main.app(scope, receive, send)
    try:
        data = json.loads(out["body"] or b"{}")
    except ValueError:
        data = {}
    return out["status"], data, out["headers"]


def call(*args, **kw):
    return asyncio.run(request(*args, **kw))


def main_():
    store = auth.MemoryStore()
    auth.use_store(store)

    # passwords: salted scrypt, verified, never stored in clear
    h1, h2 = auth.hash_password("correct horse"), auth.hash_password("correct horse")
    assert h1 != h2 and h1.startswith("scrypt$")
    assert auth.verify_password("correct horse", h1)
    assert not auth.verify_password("wrong", h1)
    assert not auth.verify_password("x", "garbage")
    print("  password hashing OK")

    st, health, _ = call("GET", "/api/health")
    assert st == 200 and health["login_required"] is True, health
    print("  health reports login_required")

    # the login wall: every API call needs a session
    for path in ("/api/status", "/api/config", "/api/auth/me"):
        st, data, hdrs = call("GET", path)
        assert st == 401 and hdrs.get("x-login-required") == "1", (path, st, data)
    st, _, hdrs = call("GET", "/api/status", token="not-a-real-session")
    assert st == 401 and hdrs.get("x-login-required") == "1"
    print("  API rejects callers without a valid session")

    # sign-up validation
    bad = [({"name": "", "email": "a@b.co", "password": "longenough"}, 400),
           ({"name": "A", "email": "not-an-email", "password": "longenough"}, 400),
           ({"name": "A", "email": "a@b.co", "password": "short"}, 400)]
    for body, want in bad:
        st, data, _ = call("POST", "/api/auth/signup", body, ip="10.0.1.1")
        assert st == want, (body, st, data)
    print("  sign-up validation OK")

    st, s1, _ = call("POST", "/api/auth/signup",
                     {"name": "  Raj   Kumar ", "email": "Raj@Example.com",
                      "password": "s3cret-pass"}, ip="10.0.1.2")
    assert st == 201 and s1["token"] and s1["user"]["email"] == "raj@example.com", s1
    assert s1["user"]["name"] == "Raj Kumar"
    doc = store.users["raj@example.com"]
    assert "s3cret-pass" not in json.dumps(doc, default=str)
    assert s1["token"] not in store.sessions            # only its hash is stored
    st, data, _ = call("POST", "/api/auth/signup",
                       {"name": "Other", "email": "raj@example.com",
                        "password": "another-pass"}, ip="10.0.1.3")
    assert st == 409, (st, data)
    print("  sign-up OK (email normalised, duplicate refused, nothing stored in clear)")

    # login
    st, data, _ = call("POST", "/api/auth/login",
                       {"email": "raj@example.com", "password": "wrong-pass"}, ip="10.0.2.1")
    assert st == 401 and data["detail"] == "Wrong email or password."
    st, data2, _ = call("POST", "/api/auth/login",
                        {"email": "nobody@example.com", "password": "wrong-pass"}, ip="10.0.2.1")
    assert st == 401 and data2["detail"] == data["detail"]   # no account probing
    st, s2, _ = call("POST", "/api/auth/login",
                     {"email": "RAJ@example.com", "password": "s3cret-pass"}, ip="10.0.2.1")
    assert st == 200 and s2["token"] != s1["token"], s2
    print("  login OK (same answer for unknown email and wrong password)")

    # a session opens the API; the account is the identity
    st, me, _ = call("GET", "/api/auth/me", token=s2["token"])
    assert st == 200 and me["user"]["email"] == "raj@example.com", me
    ident = auth.identity(me["user"]["id"])
    assert main.CLIENT_ID_RE.match(ident), ident
    st, _, hdrs = call("GET", "/api/health", token=s2["token"])
    assert st == 200
    print(f"  session accepted; per-user data key {ident[:8]}...")

    # logout ends only that session
    st, _, _ = call("POST", "/api/auth/logout", token=s2["token"])
    assert st == 200
    st, _, hdrs = call("GET", "/api/auth/me", token=s2["token"])
    assert st == 401 and hdrs.get("x-login-required") == "1"
    st, _, _ = call("GET", "/api/auth/me", token=s1["token"])
    assert st == 200
    print("  logout OK (other sessions stay valid)")

    # password guessing is rate limited per address
    codes = [call("POST", "/api/auth/login",
                  {"email": "raj@example.com", "password": f"guess-{i}"},
                  ip="10.0.9.9")[0] for i in range(main.config.RATE_LIMIT_AUTH_PER_IP_PER_MIN + 2)]
    assert codes[0] == 401 and codes[-1] == 429, codes
    print("  login attempts rate limited per address")

    auth.use_store(None)
    print("\nALL AUTH TESTS PASSED")


def test_auth():
    main_()


if __name__ == "__main__":
    main_()
