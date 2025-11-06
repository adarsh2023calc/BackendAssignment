


from logger import get_logger
from models import init_db
from jobs import enqueue_job,list_jobs,show_job,dlq_list,requeue_dlq,retry_job_force,worker_loop
import json  
import sys  
from multiprocessing import Process, Event
import time  
import argparse


logging = get_logger()

def cmd_init_db(args):
    init_db(args.db,logging)

def cmd_enqueue(args):
    job_id = enqueue_job(args.db,logging,command=args.command, job_json=args.json, max_retries=args.max_retries)
    print(job_id)

def cmd_list(args):
    rows = list_jobs(args.db,state=args.state, limit=args.limit)
    print(json.dumps(rows, indent=2, default=str))

def cmd_show(args):
    job = show_job(args.db, args.job_id)
    if not job:
        print(f"Job {args.job_id} not found", file=sys.stderr)
        sys.exit(2)
    print(json.dumps(job, indent=2, default=str))

def cmd_dlq_list(args):
    rows = dlq_list(args.db, limit=args.limit)
    print(json.dumps(rows, indent=2, default=str))

def cmd_requeue_dlq(args):
    try:
        requeue_dlq(args.db,logging, args.job_id)
        print(f"Requeued {args.job_id}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

def cmd_retry_job(args):
    try:
        retry_job_force(args.db,logging, args.job_id)
        print(f"Retry scheduled for {args.job_id}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

def cmd_start_workers(args):
    # spawn worker processes and handle SIGINT to stop them
    stop_event = Event()
    processes = []
    logging.info(f"Starting {args.workers} worker(s) (db={args.db})")
    try:
        for i in range(args.workers):
            p = Process(target=worker_loop, args=(args.db, logging,stop_event, i, args.poll_interval), name=f"worker-{i}")
            p.start()
            processes.append(p)
        # main process waits until terminated
        while True:
            alive = any(p.is_alive() for p in processes)
            if not alive:
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.info("Shutdown requested, stopping workers...")
        stop_event.set()
        # give grace time
        for p in processes:
            p.join(timeout=5)
        logging.info("All workers stopped gracefully.")


def main():
    parser = argparse.ArgumentParser(description="queuectl - simple background job queue")
    subparsers = parser.add_subparsers(dest="command")

    # Enqueue
    enqueue_parser = subparsers.add_parser("enqueue", help="Enqueue a new job")
    enqueue_parser.add_argument("command", help="Command to run")
    enqueue_parser.add_argument("--max_retries", type=int, default=3, help="Max retries per job")

    # Start workers
    worker_parser = subparsers.add_parser("start", help="Start worker processes")
    worker_parser.add_argument("--workers", type=int, default=2, help="Number of workers")
    worker_parser.add_argument("--poll_interval", type=int, default=2, help="Polling interval (seconds)")

    # List jobs
    subparsers.add_parser("list", help="List all jobs")
    subparsers.add_parser("dlq", help="Show Dead Letter Queue")

    args = parser.parse_args()

    if args.command == "enqueue":
        cmd_enqueue(args)
    elif args.command == "start":
        cmd_start_workers(args)
    elif args.command == "list":
        jobs=cmd_list(args)
        print(json.dumps(jobs, indent=2))
    elif args.command == "dlq":
        dlq = cmd_dlq_list(args)
        print(json.dumps(dlq, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()



