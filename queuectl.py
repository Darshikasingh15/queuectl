#!/usr/bin/env python3
"""CLI entry point for QueueCTL."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from queuectl.db import QueueStore
from queuectl.worker import run_workers


def parse_run_at(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("use ISO-8601, e.g. 2026-07-17T10:30:00Z") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="queuectl", description="Durable SQLite background job queue")
    result.add_argument("--db", default="queuectl.db", help="SQLite database path (default: queuectl.db)")
    commands = result.add_subparsers(dest="command", required=True)

    enqueue = commands.add_parser("enqueue", help="queue a shell command")
    enqueue.add_argument("command", help="shell command to execute")
    enqueue.add_argument("--priority", type=int, default=0, help="higher numbers run first")
    schedule = enqueue.add_mutually_exclusive_group()
    schedule.add_argument("--delay", type=float, default=0, help="delay eligibility by seconds")
    schedule.add_argument("--run-at", type=parse_run_at, help="UTC ISO-8601 eligibility time")
    enqueue.add_argument("--max-attempts", type=int, default=3)
    enqueue.add_argument("--timeout", type=float, default=None, help="command timeout in seconds")

    start = commands.add_parser("start", help="start worker processes")
    start.add_argument("--workers", type=int, default=1)
    start.add_argument("--poll-interval", type=float, default=0.25)
    start.add_argument("--once", action="store_true", help="process available jobs then exit")

    listing = commands.add_parser("list", help="list jobs")
    listing.add_argument("--status", choices=["queued", "retrying", "running", "succeeded", "dead", "cancelled"])
    listing.add_argument("--limit", type=int, default=50)
    commands.add_parser("metrics", help="show queue counts")
    show = commands.add_parser("show", help="show a job and captured output")
    show.add_argument("job_id")
    cancel = commands.add_parser("cancel", help="cancel a queued/retrying job")
    cancel.add_argument("job_id")
    retry = commands.add_parser("retry-dlq", help="return a dead-letter job to the queue")
    retry.add_argument("job_id")
    return result


def emit(value: object) -> None:
    print(json.dumps(value, indent=2, default=str))


def main() -> int:
    args = parser().parse_args()
    database = Path(args.db)
    store = QueueStore(database)
    store.initialize()

    if args.command == "enqueue":
        if args.max_attempts < 1:
            parser().error("--max-attempts must be at least 1")
        if args.timeout is not None and args.timeout <= 0:
            parser().error("--timeout must be positive")
        run_at = args.run_at or (datetime.now(timezone.utc) + timedelta(seconds=args.delay)).isoformat()
        job = store.enqueue(args.command, args.priority, run_at, args.max_attempts, args.timeout)
        emit({"enqueued": job})
        return 0
    if args.command == "start":
        if args.workers < 1 or args.poll_interval <= 0:
            parser().error("workers and poll interval must be positive")
        return run_workers(database, args.workers, args.poll_interval, args.once)
    if args.command == "list":
        emit(store.list_jobs(args.status, args.limit))
        return 0
    if args.command == "metrics":
        emit(store.metrics())
        return 0
    if args.command == "show":
        job = store.get_job(args.job_id)
        if not job:
            print(f"job not found: {args.job_id}", file=sys.stderr)
            return 1
        emit(job)
        return 0
    if args.command == "cancel":
        if not store.cancel(args.job_id):
            print("only queued or retrying jobs can be cancelled", file=sys.stderr)
            return 1
        print(f"cancelled {args.job_id}")
        return 0
    if args.command == "retry-dlq":
        if not store.retry_dead(args.job_id):
            print("job is not in the dead-letter queue", file=sys.stderr)
            return 1
        print(f"requeued {args.job_id}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
