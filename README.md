
#Backend Assignment

Skip to content
Chat history

You said:
🎯 Objective
Build a CLI-based background job queue system called queuectl.
This system should manage background jobs with worker processes, handle retries using exponential backoff, and maintain a Dead Letter Queue (DLQ) for permanently failed jobs.
🧩 Problem Overview
You need to implement a minimal, production-grade job queue system that supports:
Enqueuing and managing background jobs
Running multiple worker processes
Retrying failed jobs automatically with exponential backoff
Moving jobs to a Dead Letter Queue after exhausting retries
Persistent job storage across restarts
All operations accessible through a CLI interface
📦 Job Specification
Each job must contain at least the following fields:
{
  "id": "unique-job-id",
  "command": "echo 'Hello World'",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  "created_at": "2025-11-04T10:30:00Z",
  "updated_at": "2025-11-04T10:30:00Z"
}


give me python code
ChatGPT said:

Thought for 4 seconds
Nice — here's a single-file, production-minded Python implementation of queuectl that meets your requirements:

CLI-based (argparse)

Persistent storage (SQLite)

Multiple worker processes (multiprocessing)

Atomic job claiming (SQLite UPDATE-then-SELECT pattern)

Exponential backoff with jitter

Dead Letter Queue (DLQ) table for permanently failed jobs

Job JSON schema matches your spec fields

Graceful shutdown, logging, basic metrics

Save this as queuectl.py and run with python queuectl.py --help.

#!/usr/bin/env python3
"""
queuectl.py - Minimal production-grade background job queue CLI

Usage examples:
  # Initialize DB
  python queuectl.py init-db --db ./queue.db

  # Enqueue a job (command string)
  python queuectl.py enqueue --db ./queue.db --command "echo 'Hello World'" --max-retries 3

  # Enqueue job with JSON payload
  python queuectl.py enqueue --db ./queue.db --json '{"id":"job-1","command":"ls -la","max_retries":2}'

  # Start 4 workers
  python queuectl.py start-workers --db ./queue.db --workers 4

  # List pending/running jobs
  python queuectl.py list --db ./queue.db --state pending

  # Show DLQ
  python queuectl.py dlq-list --db ./queue.db

  # Requeue a DLQ job
  python queuectl.py requeue-dlq --db ./queue.db --job-id <JOB_ID>

  # Retry a specific job (force)
  python queuectl.py retry-job --db ./queue.db --job-id <JOB_ID>

Notes:
 - SQLite is used for persistence (file DB).
 - Workers claim jobs atomically using an UPDATE ... WHERE ... AND state='pending' AND next_run_at<=now
 - Exponential backoff: base_delay * (2 ** attempts) with optional jitter (randomized small fraction)
 - When attempts > max_retries job is moved to DLQ.
