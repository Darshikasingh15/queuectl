from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from queuectl.db import QueueStore


class QueueStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.store = QueueStore(Path(self.directory.name) / "test.db")
        self.store.initialize()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def enqueue_ready(self, attempts: int = 3) -> str:
        return self.store.enqueue("echo test", 0, datetime.now(timezone.utc).isoformat(), attempts, None)["id"]

    def test_claim_is_atomic_and_marks_running(self) -> None:
        job_id = self.enqueue_ready()
        job = self.store.claim_next("worker-a")
        self.assertEqual(job["id"], job_id)
        self.assertEqual(job["attempts"], 1)
        self.assertIsNone(self.store.claim_next("worker-b"))

    def test_failure_retries_then_moves_to_dead_letter_queue(self) -> None:
        job_id = self.enqueue_ready(attempts=2)
        self.store.claim_next("worker-a")
        self.assertEqual(self.store.complete(job_id, 1, "", "bad", "exit 1"), "retrying")
        with self.store.connection() as connection:
            connection.execute("UPDATE jobs SET run_at=? WHERE id=?", (datetime.now(timezone.utc).isoformat(), job_id))
        self.store.claim_next("worker-a")
        self.assertEqual(self.store.complete(job_id, 1, "", "bad", "exit 1"), "dead")
        self.assertTrue(self.store.retry_dead(job_id))
        self.assertEqual(self.store.get_job(job_id)["status"], "queued")

    def test_cancel_only_applies_before_execution(self) -> None:
        job_id = self.enqueue_ready()
        self.assertTrue(self.store.cancel(job_id))
        self.assertFalse(self.store.cancel(job_id))


if __name__ == "__main__":
    unittest.main()
