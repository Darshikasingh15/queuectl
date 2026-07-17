"""SQLite persistence and atomic queue state transitions."""

from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class QueueStore:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=10000")
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, command TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('queued','retrying','running','succeeded','dead','cancelled')),
                    priority INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL, run_at TEXT NOT NULL,
                    timeout_seconds REAL, created_at TEXT NOT NULL, started_at TEXT,
                    finished_at TEXT, worker_id TEXT, exit_code INTEGER,
                    stdout TEXT, stderr TEXT, error TEXT
                );
                CREATE INDEX IF NOT EXISTS jobs_ready_idx ON jobs(status, run_at, priority DESC, created_at);
                CREATE TABLE IF NOT EXISTS workers (
                    id TEXT PRIMARY KEY, pid INTEGER NOT NULL, status TEXT NOT NULL,
                    started_at TEXT NOT NULL, heartbeat_at TEXT NOT NULL
                );
                """
            )

    def enqueue(self, command: str, priority: int, run_at: str, max_attempts: int, timeout: float | None) -> dict[str, Any]:
        job_id = str(uuid.uuid4())
        created = now()
        with self.connection() as connection:
            connection.execute(
                "INSERT INTO jobs(id,command,status,priority,max_attempts,run_at,timeout_seconds,created_at) VALUES(?,?,'queued',?,?,?,?,?)",
                (job_id, command, priority, max_attempts, run_at, timeout, created),
            )
        return {"id": job_id, "status": "queued", "run_at": run_at}

    def claim_next(self, worker_id: str) -> dict[str, Any] | None:
        """Atomically claim one eligible job. BEGIN IMMEDIATE serializes claimers."""
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT * FROM jobs WHERE status IN ('queued','retrying') AND run_at <= ?
                   ORDER BY priority DESC, run_at ASC, created_at ASC LIMIT 1""",
                (now(),),
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            started = now()
            updated = connection.execute(
                "UPDATE jobs SET status='running', attempts=attempts+1, worker_id=?, started_at=?, error=NULL WHERE id=? AND status IN ('queued','retrying')",
                (worker_id, started, row["id"]),
            )
            connection.execute("COMMIT")
            if updated.rowcount != 1:
                return None
            job = dict(row)
            job["attempts"] += 1
            job["started_at"] = started
            return job

    def complete(self, job_id: str, exit_code: int, stdout: str, stderr: str, error: str | None, backoff_base: float = 2) -> str:
        with self.connection() as connection:
            job = connection.execute("SELECT attempts,max_attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
            if job is None:
                raise KeyError(job_id)
            finished = now()
            if exit_code == 0 and error is None:
                status, run_at = "succeeded", finished
            elif job["attempts"] >= job["max_attempts"]:
                status, run_at = "dead", finished
            else:
                delay = backoff_base * (2 ** (job["attempts"] - 1))
                status = "retrying"
                run_at = (datetime.now(timezone.utc).timestamp() + delay)
                run_at = datetime.fromtimestamp(run_at, timezone.utc).isoformat()
            connection.execute(
                """UPDATE jobs SET status=?,run_at=?,finished_at=?,exit_code=?,stdout=?,stderr=?,error=?
                   WHERE id=?""",
                (status, run_at, finished, exit_code, stdout, stderr, error, job_id),
            )
            return status

    def register_worker(self, worker_id: str, pid: int) -> None:
        timestamp = now()
        with self.connection() as connection:
            connection.execute("INSERT OR REPLACE INTO workers VALUES(?,?, 'running',?,?)", (worker_id, pid, timestamp, timestamp))

    def heartbeat(self, worker_id: str) -> None:
        with self.connection() as connection:
            connection.execute("UPDATE workers SET heartbeat_at=? WHERE id=?", (now(), worker_id))

    def stop_worker(self, worker_id: str) -> None:
        with self.connection() as connection:
            connection.execute("UPDATE workers SET status='stopped', heartbeat_at=? WHERE id=?", (now(), worker_id))

    def list_jobs(self, status: str | None, limit: int) -> list[dict[str, Any]]:
        with self.connection() as connection:
            query = "SELECT id,command,status,priority,attempts,max_attempts,run_at,created_at,worker_id,exit_code,error FROM jobs"
            arguments: tuple[Any, ...] = ()
            if status:
                query += " WHERE status=?"
                arguments = (status,)
            rows = connection.execute(query + " ORDER BY created_at DESC LIMIT ?", arguments + (limit,)).fetchall()
            return [dict(row) for row in rows]

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self.connection() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None

    def cancel(self, job_id: str) -> bool:
        with self.connection() as connection:
            result = connection.execute("UPDATE jobs SET status='cancelled',finished_at=? WHERE id=? AND status IN ('queued','retrying')", (now(), job_id))
            return result.rowcount == 1

    def retry_dead(self, job_id: str) -> bool:
        with self.connection() as connection:
            result = connection.execute("UPDATE jobs SET status='queued',attempts=0,run_at=?,error=NULL,finished_at=NULL WHERE id=? AND status='dead'", (now(), job_id))
            return result.rowcount == 1

    def metrics(self) -> dict[str, Any]:
        with self.connection() as connection:
            counts = {row["status"]: row["count"] for row in connection.execute("SELECT status,COUNT(*) count FROM jobs GROUP BY status")}
            workers = [dict(row) for row in connection.execute("SELECT * FROM workers ORDER BY started_at DESC")]
            return {"jobs": counts, "workers": workers}
