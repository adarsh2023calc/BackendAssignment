import os
import sqlite3
import subprocess
import time
import json
import tempfile
import pytest

DB_FILE = "test.db"

@pytest.fixture(autouse=True)
def cleanup_db():
    """Removes test database before/after each test."""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    yield
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

def run_cli(cmd):
    """Helper to run the queuectl command."""
    if isinstance(cmd, str):
        cmd = cmd.split()
    process = subprocess.run(
        ["./queuectl"] + cmd,
        capture_output=True,
        text=True
    )
    if process.returncode != 0:
        print(process.stderr)
    return process

def test_enqueue_job():
    """Ensure a job can be enqueued."""
    result = run_cli([
        "enqueue","--json",
        f'{{"id":"job20","command":"echo hello","max_retries":1}}',
        "--db", f'{DB_FILE}'
        
    ])

    assert result.returncode == 0

    # Verify DB entry exists
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id, command, max_retries FROM jobs WHERE id='job20'")
    row = cur.fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "job20"

def test_failed_job_moves_to_dlq():
    """Enqueue a failing job and verify it ends up in DLQ."""
    run_cli([
    "enqueue", "--json",
    f'{{"id":"failjob2","command":"find / -name \\"filename\\"","max_retries":1}}',
    "--db", DB_FILE
    ])


    # Start worker (should fail and push to DLQ)
    proc = subprocess.Popen(
        ["./queuectl", "worker", "start", "--count", "2", "--db", DB_FILE],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    time.sleep(5)
    proc.terminate()
    proc.wait()

    # Verify job is in DLQ
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT id FROM dlq WHERE id='failjob2'")
    row = cur.fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "failjob2"

def test_list_jobs_command():
    job_json = json.dumps({"id": "job_json", "command": "echo test", "max_retries": 1})
    result = run_cli(["enqueue", "--json", job_json, "--db",DB_FILE])
    assert result.returncode == 0
    assert "enqueued" in result.stdout.lower()
