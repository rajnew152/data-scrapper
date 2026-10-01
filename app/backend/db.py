"""Postgres (Supabase) connection pool shared by accounts (auth.py) and
saved user data (persist.py). Enabled when SUPABASE_DB_URL is set.

The tables live in their own schema, "bdc", which Supabase does not
expose through its REST API, and have row level security on with no
policies: the publishable / anon key can never read them. Only this
server - the connection string's user, which owns the tables - can.

Each process (API, collection worker) opens its own small pool on first
use and creates the schema if it is missing.
"""

from __future__ import annotations

import logging
import threading

from . import config

log = logging.getLogger("db")

SCHEMA = "bdc"
POOL_SIZE = 3            # per process; Supabase's free pooler allows ~15 in all

_DDL = f"""
SELECT pg_advisory_xact_lock(727260412);
CREATE SCHEMA IF NOT EXISTS {SCHEMA};
CREATE TABLE IF NOT EXISTS {SCHEMA}.users (
    id            text PRIMARY KEY,
    name          text NOT NULL,
    email         text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {SCHEMA}.sessions (
    token_hash text PRIMARY KEY,
    user_id    text NOT NULL REFERENCES {SCHEMA}.users (id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_expires_at ON {SCHEMA}.sessions (expires_at);
CREATE TABLE IF NOT EXISTS {SCHEMA}.user_files (
    owner      text NOT NULL,
    name       text NOT NULL,
    data       bytea NOT NULL,
    size       integer NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (owner, name)
);
ALTER TABLE {SCHEMA}.users ENABLE ROW LEVEL SECURITY;
ALTER TABLE {SCHEMA}.sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE {SCHEMA}.user_files ENABLE ROW LEVEL SECURITY;
"""

_pool = None
_lock = threading.Lock()


def enabled() -> bool:
    return bool(config.DATABASE_URL)


def pool():
    """The process's connection pool (opened + schema created on first use;
    raises if the database cannot be reached - the next call retries)."""
    global _pool
    if _pool is None:
        with _lock:
            if _pool is None:
                from psycopg_pool import ConnectionPool
                p = ConnectionPool(
                    config.DATABASE_URL, min_size=1, max_size=POOL_SIZE,
                    # prepare_threshold=None: also works behind a
                    # transaction-mode pooler (Supabase port 6543)
                    kwargs={"connect_timeout": 10, "prepare_threshold": None},
                    check=ConnectionPool.check_connection, max_idle=300,
                    timeout=20, open=False, name="bdc")
                try:
                    p.open(wait=True, timeout=30)
                    with p.connection() as conn:
                        conn.execute(_DDL)
                except BaseException:
                    p.close()
                    raise
                _pool = p
                log.info("database connected")
    return _pool


def connection():
    """`with db.connection() as conn:` - one transaction, committed on exit."""
    return pool().connection()


def close():
    global _pool
    with _lock:
        if _pool is not None:
            _pool.close()
            _pool = None
