
import sqlite3


def get_conn(db_path,logger):
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)  # autocommit mode off if begin is used
    conn.execute("PRAGMA journal_mode=DELETE;")
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
    logger.info(f"Initialized DB at {db_path}")

    conn.row_factory = sqlite3.Row
    return conn
