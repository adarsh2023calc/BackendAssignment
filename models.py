
import sqlite3


def get_conn(db_path):
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)  # autocommit mode off if begin is used
    conn.execute("PRAGMA journal_mode=WAL;")  # allow concurrent readers/writers
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path,logger):
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      command TEXT NOT NULL,
      state TEXT NOT NULL,
      attempts INTEGER NOT NULL,
      max_retries INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      next_run_at INTEGER NOT NULL,
      last_error TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dlq (
      id TEXT PRIMARY KEY,
      payload TEXT NOT NULL,
      moved_at TEXT NOT NULL,
      reason TEXT
    );
    """)
    conn.commit()
    conn.close()
    logger.info(f"Initialized DB at {db_path}")




