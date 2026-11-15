import hashlib, json, sqlite3, threading
from contextlib import contextmanager
from dataclasses import asdict
from fastapi import HTTPException
from src.config.settings import DATABASE_URL
TABLES = ("datasets", "masks", "studies", "runs", "reviews", "reports", "users", "audit", "streams")
_LOCK = threading.RLock()
_STATION_LOCKS = {}
_STATION_LOCKS_GUARD = threading.Lock()
def connect():
    if DATABASE_URL.startswith("sqlite:///"):
        conn = sqlite3.connect(DATABASE_URL.removeprefix("sqlite:///"), timeout=30)
        return conn, "?"
    import psycopg
    return psycopg.connect(DATABASE_URL), "%s"
def init_db():
    conn,_ = connect()
    try:
        if not DATABASE_URL.startswith("sqlite:///"):
            conn.execute("SELECT pg_advisory_xact_lock(2309)")
        for table in TABLES:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
        conn.commit()
    finally: conn.close()
def all_rows(table):
    assert table in TABLES
    conn,_ = connect()
    try: return [json.loads(row[0]) for row in conn.execute(f"SELECT payload FROM {table} ORDER BY id").fetchall()]
    finally: conn.close()
def get(table, id):
    assert table in TABLES
    conn,q = connect()
    try:
        row = conn.execute(f"SELECT payload FROM {table} WHERE id={q}", (id,)).fetchone()
        if row is None: raise HTTPException(404, "记录不存在")
        return json.loads(row[0])
    finally: conn.close()
def find(table, id):
    assert table in TABLES
    conn,q = connect()
    try:
        row = conn.execute(f"SELECT payload FROM {table} WHERE id={q}", (id,)).fetchone()
        return json.loads(row[0]) if row is not None else None
    finally: conn.close()
def put(table, item, insert_only=False):
    assert table in TABLES
    if not isinstance(item, dict): item = asdict(item)
    conn,q = connect()
    clause = "DO NOTHING" if insert_only else "DO UPDATE SET payload=excluded.payload"
    try:
        with _LOCK:
            conn.execute(f"INSERT INTO {table} (id,payload) VALUES ({q},{q}) ON CONFLICT(id) {clause}", (item["id"], json.dumps(item, ensure_ascii=False, allow_nan=False)))
            conn.commit()
        return item
    finally: conn.close()
def put_many(table, items):
    """Insert/update several payload rows in one transaction; all or nothing."""
    assert table in TABLES
    conn,q = connect()
    try:
        with _LOCK:
            for item in items:
                if not isinstance(item, dict): item = asdict(item)
                conn.execute(f"INSERT INTO {table} (id,payload) VALUES ({q},{q}) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (item["id"], json.dumps(item, ensure_ascii=False, allow_nan=False)))
            conn.commit()
        return items
    except Exception:
        conn.rollback()
        raise
    finally: conn.close()
def station_lock(station):
    """Per-station lock: concurrent requests for the same station advance in arrival order."""
    with _STATION_LOCKS_GUARD:
        lock = _STATION_LOCKS.setdefault(station, threading.RLock())
    return lock
def _advisory_key(station):
    return int.from_bytes(hashlib.sha256(station.encode()).digest()[:8], "big", signed=True)
@contextmanager
def guard_stations(stations):
    """Serialize same-station work in this process and, on PostgreSQL, across processes.

    Sorted acquisition order keeps concurrent multi-station batches deadlock free.
    """
    ordered = sorted(set(stations))
    locks = [station_lock(station) for station in ordered]
    for lock in locks:
        lock.acquire()
    conn = None
    try:
        if not DATABASE_URL.startswith("sqlite:///"):
            conn, _ = connect()
            for station in ordered:
                conn.execute(f"SELECT pg_advisory_lock({_advisory_key(station)})")
        yield
    finally:
        if conn is not None:
            try:
                for station in ordered:
                    conn.execute(f"SELECT pg_advisory_unlock({_advisory_key(station)})")
                conn.commit()
            finally:
                conn.close()
        for lock in reversed(locks):
            lock.release()
def replace_rows(table, items):
    """Atomically swap every row of a table (single transaction, rollback on failure)."""
    assert table in TABLES
    conn,q = connect()
    try:
        with _LOCK:
            conn.execute(f"DELETE FROM {table}")
            for item in items:
                if not isinstance(item, dict): item = asdict(item)
                conn.execute(f"INSERT INTO {table} (id,payload) VALUES ({q},{q})", (item["id"], json.dumps(item, ensure_ascii=False, allow_nan=False)))
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally: conn.close()
