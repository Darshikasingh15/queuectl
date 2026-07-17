"""Process-based workers for QueueCTL."""

from __future__ import annotations

import multiprocessing
import os
import signal
import subprocess
import time
import uuid
from pathlib import Path

from queuectl.db import QueueStore


def output_text(value: object) -> str:
    """Normalize subprocess output, including timeout partial output."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode(errors="replace")
    return str(value)


def execute(command: str, timeout: float | None) -> tuple[int, str, str, str | None]:
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout, result.stderr, None
    except subprocess.TimeoutExpired as error:
        stdout = output_text(error.stdout)
        stderr = output_text(error.stderr)
        return 124, stdout, stderr, f"job timed out after {timeout} seconds"
    except OSError as error:
        return 127, "", "", str(error)


def worker_main(database: str, poll_interval: float, once: bool) -> None:
    store = QueueStore(database)
    store.initialize()
    worker_id = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
    store.register_worker(worker_id, os.getpid())
    running = True

    def stop(_: int, __: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        while running:
            store.heartbeat(worker_id)
            job = store.claim_next(worker_id)
            if job is None:
                if once:
                    break
                time.sleep(poll_interval)
                continue
            code, stdout, stderr, error = execute(job["command"], job["timeout_seconds"])
            status = store.complete(job["id"], code, stdout, stderr, error)
            print(f"[{worker_id}] {job['id']} -> {status}", flush=True)
    finally:
        store.stop_worker(worker_id)


def run_workers(database: Path, count: int, poll_interval: float, once: bool) -> int:
    processes = [multiprocessing.Process(target=worker_main, args=(str(database), poll_interval, once), daemon=False) for _ in range(count)]
    for process in processes:
        process.start()
    try:
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        for process in processes:
            process.terminate()
        for process in processes:
            process.join()
    return 0
