# QueueCTL

QueueCTL is a lightweight Python CLI for durable local job queuing. It stores
shell commands in an SQLite database and executes them reliably with worker
processes.

## What this project does

- Adds shell commands to a queue using `enqueue`
- Runs queued jobs with one or more worker processes
- Persists jobs in `queuectl.db` so work survives restarts
- Handles retries, delays, priorities, and dead-letter queue (DLQ)
- Provides job inspection, cancellation, retry, and metrics commands

## Features

- Durable SQLite-backed queue
- Safe job claiming to prevent duplicate execution
- Retry with exponential backoff
- Job priorities, scheduling, and timeouts
- CLI commands for enqueue, start, list, show, cancel, retry, and metrics
- No external dependencies beyond Python standard library

## Requirements

- Python 3.11 or newer
- A terminal on Windows

## How to use

Open the project folder:

```powershell
cd C:\abc\OneDrive\Documents\New
```



### Run tests

```powershell
 & 'C:\Users\Darshika Singh\AppData\Local\Programs\Python\Python313\python.exe' -m unittest discover -s tests -v
python -m unittest discover -s tests
```

You should see `OK` if the project is working correctly.

### Enqueue a job

```powershell
& $py queuectl.py enqueue "echo hello"
```

### Start the worker once

```powershell
& $py queuectl.py start --workers 1 --once
```

### List jobs

```powershell
& $py queuectl.py list
```

### View metrics

```powershell
& $py queuectl.py metrics
```

## Commands

- `enqueue COMMAND` — add a shell command to the queue
- `start` — begin processing jobs
- `list` — show queued jobs
- `show JOB_ID` — display details of a specific job
- `cancel JOB_ID` — cancel a queued or retrying job
- `retry-dlq JOB_ID` — requeue a dead job
- `metrics` — display queue counts

## Demo recording instructions

Record the following steps in your terminal:

1. Run tests:
   ```powershell
   & $py -m unittest discover -s tests
   ```
   Show that it prints `OK`.
2. Enqueue a job:
   ```powershell
   & $py queuectl.py enqueue "echo hello"
   ```
3. Start the worker once:
   ```powershell
   & $py queuectl.py start --workers 1 --once
   ```
4. List jobs:
   ```powershell
   & $py queuectl.py list
   ```
5. Optionally show metrics:
   ```powershell
   & $py queuectl.py metrics
   ```

Narrate each step by saying the command name and what it does.

## What I did

I built a complete CLI tool in Python that:

- stores background jobs in SQLite
- executes queued shell commands reliably
- supports retries and job life-cycle management
- exposes inspection and management commands for job control

This repository contains the full implementation along with tests to verify
correct behavior.

## Demo Video

🎥 **Watch the demo video here:**  
[QueueCTL Demo Video](https://drive.google.com/file/d/1F0qi33lnJxClYOej2lZNhRLgYoME7v99/view?usp=sharing)
