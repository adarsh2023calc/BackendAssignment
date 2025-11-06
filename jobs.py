
from models import get_conn 
import json 
import uuid
from util import now_iso,unix_now,compute_next_run
import sqlite3
import subprocess
import time 
import os 
import datetime



def enqueue_job(db_path,logger,command=None, job_json=None, max_retries=3):
    conn = get_conn(db_path,logger=logger)
    cur = conn.cursor()
    print(job_json)
    if job_json:
        # user provided custom json (string)
        try:
            payload = json.loads(job_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")
        job_id = payload.get("id", str(uuid.uuid4()))
        command = payload.get("command", command)
        print(payload)
        if not command:
            raise ValueError("Job JSON must include 'command' or pass --command")
        max_retries = int(payload.get("max_retries", max_retries))
    else:
        if not command:
            raise ValueError("Either --command or --json must be provided")
        job_id = str(uuid.uuid4())

    now = now_iso()
    next_run_at = unix_now()
    cur.execute("""
    INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at, last_error)
    VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, NULL)
    """, (job_id, command, max_retries, now, now, next_run_at))
    conn.commit()
    conn.close()
    logger.info(f"Enqueued job {job_id}: {command}")
    return job_id


def list_jobs(db_path,logging, state=None, limit=100):
    conn = get_conn(db_path,logging)
    cur = conn.cursor()
    if state:
        cur.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at DESC LIMIT ?", (state, limit))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows



def show_job(db_path, job_id):
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    r = cur.fetchone()
    conn.close()
    return dict(r) if r else None

def show_status(db_path,logger):
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)  # autocommit mode off if begin is used
    conn.execute("PRAGMA journal_mode=WAL;")  # allow concurrent readers/writers

    conn.row_factory = sqlite3.Row  
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs")
    rows = cur.fetchall()
    
    jobs = []
    col_names = [desc[0] for desc in cur.description]
    for row in rows:
        jobs.append(dict(zip(col_names, row)))

    conn.close()
    return jobs


def move_to_dlq(db_path, logger,job_row, reason=None):
    conn = get_conn(db_path)
    cur = conn.cursor()
    payload = json.dumps({k: job_row[k] for k in job_row.keys()})
    moved_at = now_iso()
    cur.execute("INSERT OR REPLACE INTO dlq (id, payload, moved_at, reason) VALUES (?, ?, ?, ?)", (job_row["id"], payload, moved_at, reason))
    cur.execute("DELETE FROM jobs WHERE id = ?", (job_row["id"],))
    conn.commit()
    conn.close()
    logger.warning(f"Moved job {job_row['id']} to DLQ. Reason: {reason}")

def dlq_list(db_path,logger, limit=100):
    conn = get_conn(db_path,logger=logger)
    cur = conn.cursor()
    cur.execute("SELECT id, moved_at, reason FROM dlq ORDER BY moved_at DESC LIMIT ?", (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def requeue_dlq(db_path,logger, job_id):
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT payload FROM dlq WHERE id = ?", (job_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        raise ValueError("DLQ job not found")
    payload = json.loads(r[0])
    # Reset attempts and timestamps
    payload["attempts"] = 0
    payload["state"] = "pending"
    payload["updated_at"] = now_iso()
    payload["created_at"] = payload.get("created_at", now_iso())
    # Insert into jobs table
    cur.execute("""
    INSERT OR REPLACE INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at, last_error)
    VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, NULL)
    """, (
        payload["id"],
        payload["command"],
        payload.get("max_retries", 3),
        payload["created_at"],
        payload["updated_at"],
        unix_now()
    ))
    cur.execute("DELETE FROM dlq WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()
    logger.info(f"Requeued DLQ job {job_id} back to jobs")

def retry_job_force(db_path, logger,job_id):
    conn = get_conn(db_path=db_path,logger=logger)
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET state='pending', next_run_at=?, updated_at=? WHERE id=?", (unix_now(), now_iso(), job_id))
    conn.commit()
    changed = cur.rowcount
    conn.close()
    if changed:
        logger.info(f"Forced retry: job {job_id}")
    else:
        raise ValueError("Job not found")

# ----------------------------
# Worker logic
# ----------------------------
def claim_job(logger,conn):
    """
    Atomically claim the next available job:
      - state == 'pending'
      - next_run_at <= now
    Returns row dict if claimed else None.

    Implementation: UPDATE jobs SET state='running', updated_at=?, WHERE id = (
        SELECT id FROM jobs WHERE state='pending' AND next_run_at <= ? ORDER BY next_run_at ASC, created_at ASC LIMIT 1
    );
    Then SELECT that job row. Use a transaction so UPDATE is atomic.
    """
    cur = conn.cursor()
    now_ts = unix_now()
    now_iso_str = now_iso()
    try:
        # Begin immediate transaction to avoid race conditions
        cur.execute("BEGIN IMMEDIATE;")
        # Try to update a single candidate job to 'running' and set updated_at
        cur.execute("""
            UPDATE jobs
            SET state='running', updated_at=?
            WHERE id = (
                SELECT id FROM jobs
                WHERE state='pending' AND next_run_at <= ?
                ORDER BY next_run_at ASC, created_at ASC
                LIMIT 1
            );
        """, (now_iso_str, now_ts))
        if cur.rowcount == 0:
            conn.commit()
            return None
        # fetch the job we just claimed
        cur.execute("SELECT * FROM jobs WHERE state='running' AND updated_at = ? LIMIT 1", (now_iso_str,))
        row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return dict(row)
    except sqlite3.OperationalError as e:
        conn.rollback()
        logger.error(f"SQLite operational error during claim: {e}")
        return None

def run_command(job_row, timeout=300):
    """
    Execute the job's command in a shell. Return (success: bool, exit_code:int, stdout, stderr).
    """
    command = job_row["command"]
    try:
        proc = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
        success = proc.returncode == 0
        stdout = proc.stdout.strip()
        stderr = proc.stderr.strip()
        return success, proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired as e:
        return False, -1, "", f"timeout: {e}"
    except Exception as e:
        return False, -2, "", f"exception: {e}"

def worker_loop(db_path,logger, stop_event, poll_interval,worker_id=None):
    conn = get_conn(db_path,logger)
    pid = os.getpid()
    logger.info(f"Worker started (pid={pid}, worker={worker_id})")
    while not stop_event.is_set():
        try:
            job = claim_job(logger=logger,conn=conn)
            if not job:
                # no job now
                time.sleep(poll_interval)
                continue
            logger.info(f"Worker {worker_id} claimed job {job['id']} (attempts={job['attempts']})")
            success, exit_code, stdout, stderr = run_command(job)
            cur = conn.cursor()
            now = now_iso()
            if success:
                cur.execute("UPDATE jobs SET state='succeeded', attempts=attempts+1, updated_at=?, last_error=NULL WHERE id=?", (now, job["id"]))
                conn.commit()
                logger.info(f"Job {job['id']} succeeded (exit={exit_code}). stdout={stdout}")
            else:
                attempts = job["attempts"] + 1
                if attempts > job["max_retries"]:
                    # move to dlq
                    # fetch latest job row for payload
                    cur.execute("SELECT * FROM jobs WHERE id=?", (job["id"],))
                    full = cur.fetchone()
                    if full:
                        move_to_dlq(db_path, full, reason=f"Exceeded retries. Last error: {stderr or 'exit:'+str(exit_code)}")
                    else:
                        logger.error(f"Job {job['id']} missing when trying to move to DLQ")
                else:
                    # compute next_run_at with exponential backoff
                    next_run = compute_next_run(attempts)
                    cur.execute("""
                        UPDATE jobs
                        SET state='pending', attempts=?, updated_at=?, next_run_at=?, last_error=?
                        WHERE id = ?
                    """, (attempts, now, next_run, stderr or f"exit:{exit_code}", job["id"]))
                    conn.commit()
                    logger.warning(f"Job {job['id']} failed (exit={exit_code}), attempts={attempts}, next_run_at={datetime.datetime.utcfromtimestamp(next_run).isoformat()}Z")
        except Exception as e:
            logger.exception(f"Worker loop error: {e}")
            time.sleep(poll_interval)
    conn.close()
    logger.info(f"Worker {worker_id} stopping (pid={pid})")
