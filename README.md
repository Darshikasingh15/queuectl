# QueueCTL

QueueCTL is a durable, local background-job queue implemented with Python's
standard library. Jobs are shell commands persisted in SQLite, so pending work
survives process restarts.

## Features

- Atomic SQLite job claiming (`BEGIN IMMEDIATE`) prevents duplicate execution.
- One or more worker **processes** execute jobs in parallel.
- Priorities and delayed/scheduled jobs.
- Retries with exponential backoff; exhausted jobs are moved to the DLQ.
- Per-job output/error logs, timeouts, cancellation, worker heartbeats, and
  queue metrics.
- No third-party runtime dependencies.

## Requirements

Python 3.11 or newer. On this machine, use:

```powershell
$py = 'C:\Users\Darshika Singh\AppData\Local\Programs\Python\Python313\python.exe'
```

## Quick start

```powershell
$py = 'C:\Users\Darshika Singh\AppData\Local\Programs\Python\Python313\python.exe'
& $py queuectl.py enqueue "echo hello"
& $py queuectl.py start --workers 2
& $py queuectl.py list
& $py queuectl.py metrics
```

The database defaults to `queuectl.db` in the current directory. Override it
for every command with `--db path\to\queue.db`.

## Commands

```text
enqueue COMMAND [--priority N] [--delay SECONDS | --run-at ISO-8601]
                [--max-attempts N] [--timeout SECONDS]
start [--workers N] [--poll-interval SECONDS] [--once]
list [--status STATUS] [--limit N]
show JOB_ID
cancel JOB_ID
retry-dlq JOB_ID
metrics
```

Examples:

```powershell
# Fail once, then retry after the default exponential delay.
& $py queuectl.py enqueue "cmd /c exit 1" --max-attempts 3

# Run a task 30 seconds from now with high priority.
& $py queuectl.py enqueue "echo scheduled" --delay 30 --priority 10

# Inspect a completed job's captured streams.
& $py queuectl.py show <job-id>
```

## Job lifecycle

`queued -> running -> succeeded`

On failure, a job becomes `retrying` and is made eligible after
`backoff_base * 2^(attempt - 1)` seconds. Once its attempt limit is reached it
moves to `dead` (the dead-letter queue). `retry-dlq` explicitly requeues a dead
job after correcting the cause.

## Tests

```powershell
& $py -m unittest discover -s tests -v
```
