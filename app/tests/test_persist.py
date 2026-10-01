"""Saved user data (backend/persist.py): a user's records and Excel files
survive a wiped disk (Render's free plan wipes it on every restart).

  * records, custom categories and Excel files are uploaded by sync()
  * after the user's folder is wiped, the next load restores all of them
  * a folder that was never restored is never synced (a wipe cannot
    delete the saved copy)
  * a deleted custom category / reset checkpoint is dropped from the
    saved copy too
  * an unreachable database stops the load (no empty data shown)

Runs with an in-memory backend. Set TEST_DATABASE_URL to a Postgres
connection string to also round-trip the real tables (rows it creates
are deleted again).

Run (from app/):  python tests/test_persist.py
"""

from __future__ import annotations

import os
import secrets
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import config  # noqa: E402

_tmp = tempfile.mkdtemp(prefix="persist_test_")
config.OUTPUT_DIR = _tmp
config.STATE_PATH = os.path.join(_tmp, "state.json")

from backend import persist  # noqa: E402
from backend.collector.categories import SUMMARY_FILE  # noqa: E402
from backend.collector.stores import StoreRegistry  # noqa: E402

CLIENT = "u" + "a1" * 12


def record(i: int) -> dict:
    return {"Company Name": f"Company {i}", "Category": "Finance",
            "Official Website": f"https://company{i}.example.com",
            "Business Email": f"info@company{i}.example.com",
            "Business Phone": f"+1 555 010{i % 10}", "Country": "USA",
            "Confidence Score": 90}


def fresh_process(backend):
    """What a restart looks like to this module: nothing restored yet."""
    persist.use_backend(backend)
    return StoreRegistry()


def main_():
    backend = persist.MemoryFiles()
    reg = fresh_process(backend)
    st = reg.get(CLIENT)
    for i in range(100):
        assert st.append_record("Finance", record(i), 1000)
    custom = st.ensure_custom("Solar Installers")
    st.append_record(custom, record(500), 1000)
    st.save(st.path)
    st.export_categories(list(st.all_categories()))
    assert persist.sync(CLIENT)
    saved = backend.manifest(CLIENT)
    fin_file = st.all_categories()["Finance"]["file"]
    custom_file = st.all_categories()[custom]["file"]
    assert {"state.json", fin_file, custom_file, SUMMARY_FILE} <= saved, saved
    folder = config.client_dir(CLIENT)
    with open(os.path.join(folder, fin_file), "rb") as fh:
        fin_bytes = fh.read()
    print(f"  uploaded {len(saved)} files (checkpoint + Excel)")

    # unchanged files are not uploaded again
    puts = []
    orig_put = backend.put
    backend.put = lambda *a: (puts.append(a[1]), orig_put(*a))
    assert persist.sync(CLIENT) and puts == [], puts
    backend.put = orig_put
    print("  unchanged files are not re-uploaded")

    # a file that cannot be read for a moment (being replaced) is kept
    orig_sig = persist._sig
    persist._sig = lambda p: None if p.endswith(fin_file) else orig_sig(p)
    os.utime(os.path.join(folder, fin_file))
    assert persist.sync(CLIENT) and fin_file in backend.manifest(CLIENT)
    persist._sig = orig_sig
    print("  a file unreadable for a moment is neither uploaded nor deleted")

    # the disk is wiped (restart / redeploy); a new process loads the user
    reg.flush_all()
    shutil.rmtree(_tmp)
    reg = fresh_process(backend)
    st = reg.get(CLIENT)
    assert len(st.records["Finance"]) == 100, len(st.records["Finance"])
    assert st.records["Finance"][42]["Company Name"] == "Company 42"
    assert custom in st.all_categories() and len(st.records[custom]) == 1
    with open(os.path.join(folder, fin_file), "rb") as fh:
        assert fh.read() == fin_bytes
    assert os.path.exists(os.path.join(folder, SUMMARY_FILE))
    print("  after a wiped disk: 100 records, custom category and Excel files restored")

    # a folder never restored in this process is never synced
    shutil.rmtree(_tmp)
    persist.use_backend(backend)          # new process, user not loaded yet
    assert persist.sync(CLIENT) and persist.sync_all()
    assert {"state.json", fin_file} <= backend.manifest(CLIENT)
    print("  a wiped, not-yet-restored folder never deletes the saved copy")

    # deleting a custom category / resetting drops the saved copies too
    reg = fresh_process(backend)
    st = reg.get(CLIENT)
    st.delete_category(custom)
    os.remove(os.path.join(folder, custom_file))
    assert persist.sync(CLIENT)
    assert custom_file not in backend.manifest(CLIENT)
    reg.replace(CLIENT)
    assert persist.sync(CLIENT)
    left = backend.manifest(CLIENT)
    assert "state.json" not in left and fin_file in left, left
    print("  deleted category and reset are mirrored")

    # the database is unreachable: the load fails instead of showing nothing
    class Down(persist.MemoryFiles):
        def manifest(self, owner):
            raise OSError("connection refused")
    reg = fresh_process(Down())
    try:
        reg.get("u" + "b2" * 12)
    except persist.Unavailable:
        pass
    else:
        raise AssertionError("load went on without the saved data")
    print("  unreachable database -> Unavailable (no empty data)")

    persist.use_backend(None)
    if os.environ.get("TEST_DATABASE_URL"):
        live_db(os.environ["TEST_DATABASE_URL"])
    print("\nALL PERSIST TESTS PASSED")


def live_db(url: str):
    from backend import auth, db
    config.DATABASE_URL = url
    owner = "utest" + secrets.token_hex(8)
    files = persist.PgFiles()
    blob = os.urandom(50_000)
    files.put(owner, "01_Test.xlsx", blob, len(blob))
    files.put(owner, "01_Test.xlsx", blob[::-1], len(blob))   # upsert
    assert files.manifest(owner) == {"01_Test.xlsx"}
    assert files.get(owner, "01_Test.xlsx") == blob[::-1]
    files.delete(owner, "01_Test.xlsx")
    assert files.manifest(owner) == set()
    print("  live database: user_files round trip OK")

    store = auth.PgStore()
    email = f"test-{secrets.token_hex(6)}@example.com"
    uid = store.create_user({"name": "Test", "email": email,
                             "password_hash": "scrypt$x", "created_at": auth._now()})
    try:
        try:
            store.create_user({"name": "Dup", "email": email,
                               "password_hash": "scrypt$x", "created_at": auth._now()})
        except auth.DuplicateEmail:
            pass
        else:
            raise AssertionError("duplicate email accepted")
        user = store.user_by_email(email)
        assert user["id"] == uid and user["password_hash"] == "scrypt$x"
        th = secrets.token_hex(32)
        store.create_session(th, user, datetime.now(timezone.utc) + timedelta(days=1))
        s = store.session(th)
        assert s == {"user_id": uid, "name": "Test", "email": email}, s
        store.delete_session(th)
        assert store.session(th) is None
    finally:
        with db.connection() as conn:
            conn.execute(f"DELETE FROM {db.SCHEMA}.users WHERE id = %s", (uid,))
        db.close()
    print("  live database: users + sessions round trip OK")


def test_persist():
    main_()


if __name__ == "__main__":
    main_()
