import hashlib
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from excel_io import input_rows_fingerprint

TERMINAL_STATUSES = {"succeeded", "failed_final", "invalid", "cancelled"}


def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def default_state_db(base_dir: Optional[str] = None) -> str:
    root = base_dir or os.path.dirname(os.path.abspath(__file__))
    return os.path.join(root, "worten_runs.sqlite")


def normalize_url(url: str) -> str:
    parsed = urlsplit(str(url).strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/") or parsed.path
    query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    return urlunsplit((scheme, netloc, path, query, ""))


def task_key_for(mode: str, task_type: str, url: str, extra: Optional[Dict[str, Any]] = None) -> str:
    payload = {"mode": mode, "type": task_type, "url": normalize_url(url)}
    if extra:
        payload["extra"] = extra
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def input_fingerprint(path: str) -> str:
    try:
        return input_rows_fingerprint(path, include_pages=True)
    except Exception:
        abs_path = os.path.abspath(path)
        stat = os.stat(abs_path)
        digest = hashlib.sha256()
        digest.update(abs_path.lower().encode("utf-8"))
        digest.update(str(stat.st_size).encode("ascii"))
        digest.update(str(int(stat.st_mtime)).encode("ascii"))
        with open(abs_path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


class StateStore:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or default_state_db()
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        self.init_db()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    input_file TEXT NOT NULL,
                    output_file TEXT NOT NULL,
                    input_fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    error_message TEXT,
                    discovery_status TEXT NOT NULL DEFAULT 'pending',
                    discovery_started_at TEXT,
                    discovery_finished_at TEXT,
                    discovery_error TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_runs_resume
                    ON runs(mode, input_fingerprint, output_file, status);

                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    task_key TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    url TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    result_group TEXT,
                    result_json TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finished_at TEXT,
                    UNIQUE(run_id, task_key)
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_status
                    ON tasks(run_id, status, lease_expires_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_claim_next
                    ON tasks(run_id, status, lease_expires_at, created_at);

                CREATE TABLE IF NOT EXISTS run_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    task_key TEXT,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._ensure_column(conn, "runs", "discovery_status", "TEXT NOT NULL DEFAULT 'pending'")
            self._ensure_column(conn, "runs", "discovery_started_at", "TEXT")
            self._ensure_column(conn, "runs", "discovery_finished_at", "TEXT")
            self._ensure_column(conn, "runs", "discovery_error", "TEXT")

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {row["name"] for row in rows}

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        if column not in self._table_columns(conn, table):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def create_or_resume_run(self, mode: str, input_file: str, output_file: str) -> Tuple[str, bool]:
        fingerprint = input_fingerprint(input_file)
        abs_input = os.path.abspath(input_file)
        abs_output = os.path.abspath(output_file)
        now = utc_now()
        resumable_statuses = "'running', 'crashed', 'failed', 'cancelled'"
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT id, output_file FROM runs
                WHERE mode = ? AND input_fingerprint = ? AND output_file = ?
                  AND status IN ({resumable_statuses})
                ORDER BY updated_at DESC LIMIT 1
                """,
                (mode, fingerprint, abs_output),
            ).fetchone()

            if row is None:
                row = conn.execute(
                    f"""
                    SELECT id, output_file FROM runs
                    WHERE mode = ? AND input_fingerprint = ?
                      AND status IN ({resumable_statuses})
                    ORDER BY updated_at DESC LIMIT 1
                    """,
                    (mode, fingerprint),
                ).fetchone()

            if row:
                previous_output = row["output_file"]
                conn.execute(
                    """
                    UPDATE runs
                    SET status = 'running', input_file = ?, output_file = ?, updated_at = ?, error_message = NULL
                    WHERE id = ?
                    """,
                    (abs_input, abs_output, now, row["id"]),
                )
                if previous_output != abs_output:
                    self.add_event(row["id"], "INFO", f"继续未完成任务，输出文件从 {previous_output} 更新为 {abs_output}")
                return row["id"], True

            run_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO runs(id, mode, input_file, output_file, input_fingerprint, status, created_at, updated_at, started_at)
                VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?)
                """,
                (run_id, mode, abs_input, abs_output, fingerprint, now, now, now),
            )
            return run_id, False

    def set_run_status(self, run_id: str, status: str, error_message: Optional[str] = None) -> None:
        now = utc_now()
        finished_at = now if status in {"completed", "failed", "cancelled"} else None
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET status = ?, error_message = ?, updated_at = ?, finished_at = COALESCE(?, finished_at)
                WHERE id = ?
                """,
                (status, error_message, now, finished_at, run_id),
            )

    def mark_discovery_started(self, run_id: str) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET discovery_status = 'running', discovery_started_at = ?, discovery_finished_at = NULL,
                    discovery_error = NULL, updated_at = ?
                WHERE id = ?
                """,
                (now, now, run_id),
            )

    def mark_discovery_finished(self, run_id: str) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET discovery_status = 'finished', discovery_finished_at = ?, discovery_error = NULL, updated_at = ?
                WHERE id = ?
                """,
                (now, now, run_id),
            )

    def mark_discovery_failed(self, run_id: str, error: str) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET discovery_status = 'failed', discovery_finished_at = ?, discovery_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (now, error, now, run_id),
            )

    def discovery_status(self, run_id: str) -> str:
        with self.connect() as conn:
            row = conn.execute("SELECT discovery_status FROM runs WHERE id = ?", (run_id,)).fetchone()
        return row["discovery_status"] if row else "pending"

    def is_discovery_finished(self, run_id: str) -> bool:
        return self.discovery_status(run_id) in {"finished", "failed"}

    def add_event(self, run_id: str, level: str, message: str, task_key: Optional[str] = None) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO run_events(run_id, task_key, level, message, created_at) VALUES (?, ?, ?, ?, ?)",
                (run_id, task_key, level, message, utc_now()),
            )

    def add_task(self, run_id: str, task: Dict[str, Any], mode: str, max_attempts: int = 3) -> bool:
        url = str(task.get("url", "")).strip()
        task_type = str(task.get("type", "task"))
        if not url:
            return False
        key = task.get("task_key") or task_key_for(mode, task_type, url, {k: v for k, v in task.items() if k not in {"url", "type", "task_key"}})
        payload = dict(task)
        payload["task_key"] = key
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        now = utc_now()
        with self.connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO tasks(id, run_id, task_key, task_type, url, payload_json, status, max_attempts, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (str(uuid.uuid4()), run_id, key, task_type, url, payload_json, max_attempts, now, now),
                )
                task["task_key"] = key
                return True
            except sqlite3.IntegrityError:
                task["task_key"] = key
                return False

    def recover_stale_tasks(self, run_id: str) -> int:
        now = utc_now()
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE tasks
                SET status = 'pending',
                    last_error = COALESCE(last_error, '程序中断，任务租约已过期'),
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?
                WHERE run_id = ? AND status = 'running'
                  AND (lease_expires_at IS NULL OR lease_expires_at < ?)
                """,
                (now, run_id, now),
            )
            return cur.rowcount

    def load_unfinished_tasks(self, run_id: str) -> List[Dict[str, Any]]:
        self.recover_stale_tasks(run_id)
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT payload_json FROM tasks
                WHERE run_id = ? AND status NOT IN ('succeeded', 'failed_final', 'invalid', 'cancelled')
                """,
                (run_id,),
            ).fetchall()
        return [json.loads(row["payload_json"]) for row in rows]

    def recover_running_tasks(self, run_id: str, reason: str = "Worker 已退出，任务重新排队") -> int:
        now = utc_now()
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE tasks
                SET status = 'pending',
                    last_error = COALESCE(last_error, ?),
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?
                WHERE run_id = ? AND status = 'running'
                """,
                (reason, now, run_id),
            )
            return cur.rowcount

    def claim_task(self, run_id: str, task: Dict[str, Any], owner: str, lease_seconds: int = 900) -> bool:
        key = task.get("task_key")
        if not key:
            return False
        now = utc_now()
        lease_until = (datetime.utcnow() + timedelta(seconds=lease_seconds)).isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, attempts, max_attempts, lease_expires_at FROM tasks WHERE run_id = ? AND task_key = ?",
                (run_id, key),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return False
            status = row["status"]
            if status in TERMINAL_STATUSES:
                conn.execute("COMMIT")
                return False
            if status == "running" and row["lease_expires_at"] and row["lease_expires_at"] >= now:
                conn.execute("COMMIT")
                return False
            conn.execute(
                """
                UPDATE tasks
                SET status = 'running', attempts = attempts + 1, lease_owner = ?, lease_expires_at = ?, updated_at = ?
                WHERE run_id = ? AND task_key = ?
                """,
                (owner, lease_until, now, run_id, key),
            )
            conn.execute("COMMIT")
            return True

    def claim_next_task(
        self,
        run_id: str,
        owner: str,
        lease_seconds: int = 900,
        task_type: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        now = utc_now()
        lease_until = (datetime.utcnow() + timedelta(seconds=lease_seconds)).isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'failed_final',
                        last_error = COALESCE(last_error, '任务租约过期且达到最大尝试次数'),
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        updated_at = ?,
                        finished_at = ?
                    WHERE run_id = ? AND status = 'running'
                      AND (lease_expires_at IS NULL OR lease_expires_at < ?)
                      AND attempts >= max_attempts
                    """,
                    (now, now, run_id, now),
                )
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'pending',
                        last_error = COALESCE(last_error, '程序中断，任务租约已过期'),
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        updated_at = ?
                    WHERE run_id = ? AND status = 'running'
                      AND (lease_expires_at IS NULL OR lease_expires_at < ?)
                      AND attempts < max_attempts
                    """,
                    (now, run_id, now),
                )
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'failed_final',
                        last_error = COALESCE(last_error, '达到最大尝试次数'),
                        updated_at = ?,
                        finished_at = ?
                    WHERE run_id = ? AND status = 'pending'
                      AND attempts >= max_attempts
                    """,
                    (now, now, run_id),
                )
                params: Tuple[Any, ...] = (run_id,)
                type_filter = ""
                if task_type is not None:
                    type_filter = " AND task_type = ?"
                    params = (run_id, task_type)
                row = conn.execute(
                    f"""
                    SELECT task_key, payload_json FROM tasks
                    WHERE run_id = ? AND status = 'pending'{type_filter}
                      AND attempts < max_attempts
                    ORDER BY created_at, id
                    LIMIT 1
                    """,
                    params,
                ).fetchone()
                if row is None:
                    conn.execute("COMMIT")
                    return None
                cur = conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'running', attempts = attempts + 1, lease_owner = ?, lease_expires_at = ?, updated_at = ?
                    WHERE run_id = ? AND task_key = ? AND status = 'pending' AND attempts < max_attempts
                    """,
                    (owner, lease_until, now, run_id, row["task_key"]),
                )
                if cur.rowcount != 1:
                    conn.execute("COMMIT")
                    return None
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        payload = json.loads(row["payload_json"])
        payload["task_key"] = row["task_key"]
        return payload

    def release_task(
        self,
        run_id: str,
        task_key: str,
        owner: str,
        error: Optional[str] = None,
        consume_attempt: bool = False,
    ) -> bool:
        now = utc_now()
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT attempts FROM tasks
                WHERE run_id = ? AND task_key = ? AND status = 'running' AND lease_owner = ?
                """,
                (run_id, task_key, owner),
            ).fetchone()
            if row is None:
                return False
            attempts = int(row["attempts"] or 0)
            next_attempts = attempts if consume_attempt or attempts <= 0 else attempts - 1
            conn.execute(
                """
                UPDATE tasks
                SET status = 'pending', attempts = ?, lease_owner = NULL, lease_expires_at = NULL,
                    last_error = ?, updated_at = ?
                WHERE run_id = ? AND task_key = ? AND status = 'running' AND lease_owner = ?
                """,
                (next_attempts, error, now, run_id, task_key, owner),
            )
            return True

    def complete_task(
        self,
        run_id: str,
        task_key: str,
        result_group: str,
        rows: Iterable[Dict[str, Any]],
        status: str = "succeeded",
        error: Optional[str] = None,
        owner: Optional[str] = None,
    ) -> bool:
        result_rows = list(rows)
        now = utc_now()
        payload = json.dumps(result_rows, ensure_ascii=False)
        with self.connect() as conn:
            if owner is None:
                cur = conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, result_group = ?, result_json = ?, last_error = ?, lease_owner = NULL,
                        lease_expires_at = NULL, updated_at = ?, finished_at = ?
                    WHERE run_id = ? AND task_key = ?
                    """,
                    (status, result_group, payload, error, now, now, run_id, task_key),
                )
            else:
                cur = conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, result_group = ?, result_json = ?, last_error = ?, lease_owner = NULL,
                        lease_expires_at = NULL, updated_at = ?, finished_at = ?
                    WHERE run_id = ? AND task_key = ? AND status = 'running' AND lease_owner = ?
                    """,
                    (status, result_group, payload, error, now, now, run_id, task_key, owner),
                )
        return cur.rowcount == 1

    def fail_task(
        self,
        run_id: str,
        task_key: str,
        result_group: str,
        rows: Iterable[Dict[str, Any]],
        error: str,
        owner: Optional[str] = None,
    ) -> bool:
        return self.complete_task(run_id, task_key, result_group, rows, status="failed_final", error=error, owner=owner)

    def grouped_result_rows(self, run_id: str) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT result_group, result_json FROM tasks
                WHERE run_id = ? AND status IN ('succeeded', 'failed_final', 'invalid')
                  AND result_group IS NOT NULL AND result_json IS NOT NULL
                ORDER BY finished_at, created_at
                """,
                (run_id,),
            ).fetchall()
        for row in rows:
            result_group = row["result_group"]
            payload = json.loads(row["result_json"] or "[]")
            grouped.setdefault(result_group, []).extend(payload)
        return grouped

    def progress(self, run_id: str) -> Dict[str, int]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status IN ('succeeded', 'failed_final', 'invalid', 'cancelled') THEN 1 ELSE 0 END) AS processed,
                    SUM(CASE WHEN status = 'failed_final' THEN 1 ELSE 0 END) AS failed
                FROM tasks WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        return {
            "total": int(row["total"] or 0),
            "processed": int(row["processed"] or 0),
            "failed": int(row["failed"] or 0),
        }

    def queue_stats(self, run_id: str) -> Dict[str, int]:
        stats = {
            "total": 0,
            "pending": 0,
            "running": 0,
            "succeeded": 0,
            "failed_final": 0,
            "invalid": 0,
            "cancelled": 0,
            "active_workers": 0,
        }
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM tasks
                WHERE run_id = ?
                GROUP BY status
                """,
                (run_id,),
            ).fetchall()
            for row in rows:
                status = row["status"]
                count = int(row["count"] or 0)
                stats["total"] += count
                stats[status] = count

            active_workers = conn.execute(
                """
                SELECT COUNT(DISTINCT lease_owner) AS count
                FROM tasks
                WHERE run_id = ? AND status = 'running' AND lease_owner IS NOT NULL
                """,
                (run_id,),
            ).fetchone()
            stats["active_workers"] = int(active_workers["count"] or 0)
        return stats

    def has_incomplete_tasks(self, run_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM tasks
                WHERE run_id = ? AND status NOT IN ('succeeded', 'failed_final', 'invalid', 'cancelled')
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        return row is not None
