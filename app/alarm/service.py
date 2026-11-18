from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

DEVICE_NAME = "供电柜 #1"

HANDOVER_SCOPE = "已结束检修窗口内的告警轮次"
VERDICT_LABELS = {
    "consistent": "一致",
    "conflict": "两端冲突",
    "undecidable": "暂无法判定",
}
HANDOVER_MAX_CHECKS = 50
HANDOVER_MAX_REPORTS = 20
HANDOVER_WORKERS = 8
TIMESTAMP_TOLERANCE = 0.001

STATE_LABELS = {
    "suppressed": "抑制中",
    "pending": "待送",
    "delivering": "待送（投递中）",
    "sent": "已送",
    "skipped": "未补送",
    "cancelled": "已取消",
}

GROUP_OF_STATE = {
    "suppressed": "suppressed",
    "pending": "pending",
    "delivering": "pending",
    "sent": "sent",
    "skipped": "skipped",
    "cancelled": "cancelled",
}


class AlarmConflict(Exception):
    """The requested operation conflicts with current alarm state."""


class AlarmBadRequest(Exception):
    """Invalid input for an alarm operation."""


class NotifyOutcome:
    def __init__(
        self,
        kind: str,
        accepted_at: float | None = None,
        detail: str = "",
        duplicate: bool = False,
        record: dict[str, Any] | None = None,
    ) -> None:
        # kind: accepted | not_found | unknown
        self.kind = kind
        self.accepted_at = accepted_at
        self.detail = detail
        self.duplicate = duplicate
        # Full notifier-side record, when the lookup returned one.
        self.record = record


class NotifierClient:
    """HTTP client for the local notifier. Never guesses that a send succeeded."""

    def __init__(self, base_url: str, timeout: float = 2.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _timeout(self) -> httpx.Timeout:
        return httpx.Timeout(self.timeout, connect=min(1.0, self.timeout))

    def notify(self, payload: dict[str, Any]) -> NotifyOutcome:
        try:
            resp = httpx.post(
                f"{self.base_url}/notify",
                json=payload,
                timeout=self._timeout(),
            )
        except httpx.HTTPError as exc:
            return NotifyOutcome("unknown", detail=f"通知请求失败（{type(exc).__name__}）")
        if 200 <= resp.status_code < 300:
            try:
                data = resp.json()
            except ValueError:
                return NotifyOutcome("unknown", detail="通知端返回了无法解析的响应")
            if data.get("accepted") is True:
                accepted_at = data.get("accepted_at")
                if not isinstance(accepted_at, (int, float)):
                    accepted_at = None
                return NotifyOutcome(
                    "accepted",
                    accepted_at=float(accepted_at) if accepted_at is not None else None,
                    duplicate=bool(data.get("duplicate")),
                )
            return NotifyOutcome("unknown", detail="通知端未确认受理")
        return NotifyOutcome("unknown", detail=f"通知端 HTTP {resp.status_code}")

    def lookup(self, notification_no: str) -> NotifyOutcome:
        """Fetch the notifier's saved record for a notification number."""
        url = f"{self.base_url}/notifications/{quote(notification_no, safe='')}"
        try:
            resp = httpx.get(url, timeout=self._timeout())
        except httpx.HTTPError as exc:
            return NotifyOutcome("unknown", detail=f"按通知号核实失败（{type(exc).__name__}）")
        if resp.status_code == 404:
            return NotifyOutcome("not_found", detail="通知端无此通知号")
        if resp.status_code == 200:
            try:
                data = resp.json()
            except ValueError:
                return NotifyOutcome("unknown", detail="核实响应无法解析")
            if not isinstance(data, dict):
                return NotifyOutcome("unknown", detail="核实响应格式异常")
            if data.get("accepted") is True:
                accepted_at = data.get("accepted_at")
                if not isinstance(accepted_at, (int, float)):
                    accepted_at = None
                return NotifyOutcome(
                    "accepted",
                    accepted_at=float(accepted_at) if accepted_at is not None else None,
                    record=data,
                )
            return NotifyOutcome(
                "not_found", detail="通知端记录未受理", record=data
            )
        return NotifyOutcome("unknown", detail=f"核实请求 HTTP {resp.status_code}")

    def verify(self, notification_no: str) -> NotifyOutcome:
        return self.lookup(notification_no)


class AlarmService:
    """Single-device alarm rounds with maintenance suppression and reliable delivery."""

    def __init__(
        self,
        db_path: str | Path,
        notifier_url: str,
        *,
        notifier_timeout: float = 2.0,
        clock: Callable[[], float] = time.time,
        retry_base: float = 0.5,
        retry_max: float = 8.0,
        worker_interval: float = 0.25,
        start_worker: bool = True,
    ) -> None:
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
        self.retry_base = retry_base
        self.retry_max = retry_max
        self.worker_interval = worker_interval
        self._lock = threading.RLock()
        self._client = NotifierClient(notifier_url, timeout=notifier_timeout)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._worker_enabled = start_worker
        self._handover_reports: OrderedDict[str, dict[str, Any]] = OrderedDict()

        with self._lock:
            self._init_schema_locked()
            self._startup_recovery_locked()
        if start_worker:
            self.start_worker()

    # ---------- persistence helpers ----------

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 5000")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema_locked(self) -> None:
        with self._db() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS maintenance_windows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at REAL NOT NULL,
                    ends_at REAL NOT NULL,
                    closed_at REAL,
                    close_reason TEXT,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alarm_rounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    notification_no TEXT UNIQUE,
                    triggered_at REAL NOT NULL,
                    recovered_at REAL,
                    maintenance_id INTEGER REFERENCES maintenance_windows(id),
                    state TEXT NOT NULL,
                    scheduled_at REAL,
                    accepted_at REAL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_attempt_at REAL,
                    next_attempt_at REAL,
                    delivery_started_at REAL,
                    needs_verify INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    note TEXT,
                    updated_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alarm_rounds_state ON alarm_rounds(state)"
            )

    def _startup_recovery_locked(self) -> None:
        """A crash may leave rows in delivering: verify by number before re-sending."""
        now = self._clock()
        with self._db() as conn:
            cancelled = conn.execute(
                """
                UPDATE alarm_rounds
                   SET state = 'cancelled',
                       needs_verify = 0,
                       next_attempt_at = NULL,
                       note = '服务重启时通知尚未发出，且告警已恢复，取消送达',
                       updated_at = ?
                 WHERE state = 'delivering'
                   AND recovered_at IS NOT NULL
                   AND delivery_started_at IS NULL
                """,
                (now,),
            ).rowcount
            changed = conn.execute(
                """
                UPDATE alarm_rounds
                   SET state = 'pending',
                       needs_verify = 1,
                       next_attempt_at = ?,
                       note = '服务重启时投递结果未确认，先按通知号核实',
                       updated_at = ?
                 WHERE state = 'delivering'
                """,
                (now, now),
            ).rowcount
            if changed or cancelled:
                logger.info(
                    "startup recovery: %s in-flight row(s) to verify, %s cancelled",
                    changed,
                    cancelled,
                )
        self._expire_windows_locked(now)

    # ---------- state machine (call with lock held) ----------

    def _expire_windows_locked(self, now: float) -> None:
        with self._db() as conn:
            windows = conn.execute(
                "SELECT id FROM maintenance_windows WHERE closed_at IS NULL AND ends_at <= ?",
                (now,),
            ).fetchall()
        for window in windows:
            self._close_window_locked(int(window["id"]), "cutoff", now)

    def _close_window_locked(self, window_id: int, reason: str, now: float) -> None:
        with self._db() as conn:
            closed = conn.execute(
                """
                UPDATE maintenance_windows
                   SET closed_at = ?, close_reason = ?
                 WHERE id = ? AND closed_at IS NULL
                """,
                (now, reason, window_id),
            ).rowcount
            if not closed:
                return
            ends_at = float(
                conn.execute(
                    "SELECT ends_at FROM maintenance_windows WHERE id = ?", (window_id,)
                ).fetchone()["ends_at"]
            )
            rounds = conn.execute(
                """
                SELECT * FROM alarm_rounds
                 WHERE maintenance_id = ? AND state = 'suppressed'
                """,
                (window_id,),
            ).fetchall()
            reason_text = "检修截止" if reason == "cutoff" else "检修结束"
            for row in rounds:
                if row["recovered_at"] is not None:
                    conn.execute(
                        """
                        UPDATE alarm_rounds
                           SET state = 'skipped', updated_at = ?,
                               note = ?
                         WHERE id = ?
                        """,
                        (now, f"{reason_text}前已恢复，不补送通知", row["id"]),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE alarm_rounds
                           SET state = 'pending',
                               scheduled_at = ?,
                               next_attempt_at = ?,
                               updated_at = ?,
                               note = ?
                         WHERE id = ?
                        """,
                        (
                            ends_at,
                            now,
                            now,
                            f"{reason_text}时仍活跃，按固定通知号 {row['notification_no']} 送达",
                            row["id"],
                        ),
                    )

    def _claim_next_locked(self, now: float) -> dict[str, Any] | None:
        with self._db() as conn:
            row = conn.execute(
                """
                SELECT * FROM alarm_rounds
                 WHERE state = 'pending'
                   AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                 ORDER BY id ASC
                 LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            claimed = conn.execute(
                """
                UPDATE alarm_rounds
                   SET state = 'delivering',
                       updated_at = ?
                 WHERE id = ? AND state = 'pending'
                """,
                (now, row["id"]),
            ).rowcount
            if not claimed:
                return None
            return dict(
                conn.execute("SELECT * FROM alarm_rounds WHERE id = ?", (row["id"],)).fetchone()
            )

    def _backoff(self, attempts: int) -> float:
        exponent = max(0, min(attempts - 1, 10))
        return min(self.retry_max, self.retry_base * (2**exponent))

    def _mark_sent(self, round_id: int, accepted_at: float | None, note: str) -> None:
        now = self._clock()
        with self._db() as conn:
            row = conn.execute(
                "SELECT recovered_at FROM alarm_rounds WHERE id = ?", (round_id,)
            ).fetchone()
            if row is not None and row["recovered_at"] is not None:
                note = f"{note}；告警已恢复，此通知在恢复前/投递中已受理，保留记录"
            conn.execute(
                """
                UPDATE alarm_rounds
                   SET state = 'sent',
                       accepted_at = ?,
                       needs_verify = 0,
                       next_attempt_at = NULL,
                       last_error = NULL,
                       note = ?,
                       updated_at = ?
                 WHERE id = ?
                """,
                (accepted_at if accepted_at is not None else now, note, now, round_id),
            )

    def _is_recovered(self, round_id: int) -> bool:
        with self._db() as conn:
            row = conn.execute(
                "SELECT recovered_at FROM alarm_rounds WHERE id = ?", (round_id,)
            ).fetchone()
        return row is not None and row["recovered_at"] is not None

    def _cancel(self, round_id: int, note: str) -> None:
        now = self._clock()
        with self._db() as conn:
            conn.execute(
                """
                UPDATE alarm_rounds
                   SET state = 'cancelled',
                       needs_verify = 0,
                       next_attempt_at = NULL,
                       note = ?,
                       updated_at = ?
                 WHERE id = ? AND state IN ('pending', 'delivering')
                """,
                (note, now, round_id),
            )

    def _retry(
        self,
        round_id: int,
        error: str,
        *,
        needs_verify: bool,
        note_prefix: str,
        definitive: bool = False,
    ) -> None:
        if self._is_recovered(round_id) and definitive:
            self._cancel(
                round_id,
                f"{note_prefix}；告警已恢复且通知未被受理，取消本轮的后续送达",
            )
            return
        now = self._clock()
        with self._db() as conn:
            row = conn.execute(
                "SELECT attempts, notification_no, recovered_at FROM alarm_rounds WHERE id = ?",
                (round_id,),
            ).fetchone()
            if row is None:
                return
            delay = self._backoff(int(row["attempts"]))
            if row["recovered_at"] is not None:
                note = (
                    f"{note_prefix}；告警已恢复，但投递结果未知，先按通知号 "
                    f"{row['notification_no']} 核实，不重复通知"
                )
            else:
                note = f"{note_prefix}；将按通知号 {row['notification_no']} 重试"
            conn.execute(
                """
                UPDATE alarm_rounds
                   SET state = 'pending',
                       needs_verify = ?,
                       next_attempt_at = ?,
                       last_error = ?,
                       note = ?,
                       updated_at = ?
                 WHERE id = ? AND state = 'delivering'
                """,
                (1 if needs_verify else 0, now + delay, error, note, now, round_id),
            )

    # ---------- delivery ----------

    def _perform_delivery(self, row: dict[str, Any]) -> None:
        round_id = int(row["id"])
        notification_no = str(row["notification_no"])

        # The network call happens outside the lock. Recovery may therefore race
        # with it: only a delivery that has actually started at the moment of
        # recovery is allowed to continue; one that never started is cancelled.
        now = self._clock()
        with self._lock, self._db() as conn:
            current = conn.execute(
                "SELECT state, recovered_at, delivery_started_at FROM alarm_rounds WHERE id = ?",
                (round_id,),
            ).fetchone()
            if current is None or current["state"] != "delivering":
                return
            if current["recovered_at"] is not None and not row["needs_verify"]:
                # A fresh notify is a delivery that has not started: recovery cancels it.
                self._cancel(round_id, "告警恢复时投递尚未开始，已取消送达")
                return
            # Every network attempt counts; delivery_started_at marks the first one,
            # which is what recovery uses to decide "cancelled" vs "keep reconciling".
            conn.execute(
                """
                UPDATE alarm_rounds
                   SET delivery_started_at = COALESCE(delivery_started_at, ?),
                       last_attempt_at = ?,
                       attempts = attempts + 1,
                       updated_at = ?
                 WHERE id = ?
                """,
                (now, now, now, round_id),
            )

        if row["needs_verify"]:
            outcome = self._client.verify(notification_no)
            if outcome.kind == "accepted":
                self._mark_sent(
                    round_id,
                    outcome.accepted_at,
                    "故障/重启后按通知号核实：通知端已受理，保留送达记录",
                )
                return
            if outcome.kind == "unknown":
                self._retry(
                    round_id,
                    outcome.detail or "核实结果未知",
                    needs_verify=True,
                    note_prefix="核实受理状态失败",
                )
                return
            if self._is_recovered(round_id):
                self._cancel(
                    round_id,
                    "告警已恢复，且按通知号核实通知端未受理；取消本轮送达",
                )
                return
            with self._db() as conn:
                conn.execute(
                    "UPDATE alarm_rounds SET needs_verify = 0 WHERE id = ?", (round_id,)
                )

        payload = {
            "notification_no": notification_no,
            "device": DEVICE_NAME,
            "round_id": round_id,
            "triggered_at": row["triggered_at"],
            "recovered_at": row["recovered_at"],
            "maintenance_id": row["maintenance_id"],
            "message": "供电柜过热告警",
        }
        outcome = self._client.notify(payload)
        if outcome.kind == "accepted":
            note = "通知端已受理"
            if outcome.duplicate:
                note = "通知端确认此前已按该通知号受理，未重复通知"
            self._mark_sent(round_id, outcome.accepted_at, note)
            return

        # Any non-confirmation is resolved by notification number, never assumed sent.
        verify = self._client.verify(notification_no)
        if verify.kind == "accepted":
            self._mark_sent(
                round_id,
                verify.accepted_at,
                f"回执未确认（{outcome.detail or '无回执'}），按通知号核实已受理，保留记录",
            )
            return
        if verify.kind == "unknown":
            self._retry(
                round_id,
                f"回执未确认（{outcome.detail or '无回执'}）；{verify.detail or '核实结果未知'}",
                needs_verify=True,
                note_prefix="投递结果未确认",
            )
            return

        if self._is_recovered(round_id):
            self._cancel(
                round_id,
                f"回执未确认（{outcome.detail or '无回执'}），按通知号核实未受理；"
                "告警已恢复，取消本轮送达",
            )
            return

        # Definitively not accepted: bounded backoff, same notification number.
        self._retry(
            round_id,
            outcome.detail or "通知端未受理",
            needs_verify=False,
            note_prefix="通知端未受理",
            definitive=True,
        )

    def tick(self) -> None:
        # Bounded so one tick can never spin forever; the worker calls this repeatedly.
        for _ in range(50):
            now = self._clock()
            with self._lock:
                self._expire_windows_locked(now)
                claimed = self._claim_next_locked(now)
            if claimed is None:
                return
            try:
                self._perform_delivery(claimed)
            except Exception:
                logger.exception("delivery round %s failed unexpectedly", claimed.get("id"))
                self._retry(
                    int(claimed["id"]),
                    "内部错误，投递未完成",
                    needs_verify=True,
                    note_prefix="投递异常",
                )

    # ---------- worker ----------

    def start_worker(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._worker_loop, name="alarm-worker", daemon=True)
        self._thread.start()

    def _worker_loop(self) -> None:
        while not self._stop.wait(self.worker_interval):
            try:
                self.tick()
            except Exception:
                logger.exception("alarm worker tick failed")

    def stop_worker(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=max(2.0, self.worker_interval * 4))
        self._thread = None

    @property
    def worker_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ---------- commands ----------

    def set_maintenance(self, cutoff_ts: float) -> dict[str, Any]:
        now = self._clock()
        with self._lock:
            self._expire_windows_locked(now)
            if cutoff_ts <= now:
                raise AlarmBadRequest("检修截止时刻必须晚于当前时间")
            with self._db() as conn:
                active = conn.execute(
                    """
                    SELECT * FROM maintenance_windows
                     WHERE closed_at IS NULL AND ends_at > ?
                     ORDER BY id DESC LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                if active is not None:
                    conn.execute(
                        "UPDATE maintenance_windows SET ends_at = ? WHERE id = ?",
                        (cutoff_ts, active["id"]),
                    )
                    window_id = int(active["id"])
                else:
                    cur = conn.execute(
                        """
                        INSERT INTO maintenance_windows
                            (started_at, ends_at, created_at)
                        VALUES (?, ?, ?)
                        """,
                        (now, cutoff_ts, now),
                    )
                    window_id = int(cur.lastrowid)
                row = conn.execute(
                    "SELECT * FROM maintenance_windows WHERE id = ?", (window_id,)
                ).fetchone()
            return self._maintenance_dict(row, now)

    def end_maintenance(self) -> dict[str, Any]:
        now = self._clock()
        with self._lock:
            self._expire_windows_locked(now)
            with self._db() as conn:
                active = conn.execute(
                    """
                    SELECT * FROM maintenance_windows
                     WHERE closed_at IS NULL AND ends_at > ?
                     ORDER BY id DESC LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                if active is None:
                    raise AlarmConflict("当前没有进行中的检修")
                window_id = int(active["id"])
                conn.execute(
                    "UPDATE maintenance_windows SET ends_at = ? WHERE id = ?",
                    (now, window_id),
                )
            self._close_window_locked(window_id, "manual", now)
            window = self._latest_window_locked()
            return self._maintenance_dict(window, now) if window else {}

    def trigger(self) -> dict[str, Any]:
        now = self._clock()
        with self._lock:
            self._expire_windows_locked(now)
            with self._db() as conn:
                active_round = conn.execute(
                    "SELECT * FROM alarm_rounds WHERE recovered_at IS NULL ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if active_round is not None:
                    raise AlarmConflict(
                        f"第 {active_round['id']} 轮告警尚未恢复，不能重复触发"
                    )
                window = conn.execute(
                    """
                    SELECT * FROM maintenance_windows
                     WHERE closed_at IS NULL AND ends_at > ?
                     ORDER BY id DESC LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                if window is not None:
                    state = "suppressed"
                    note = "检修中触发，暂缓通知；截止时仍活跃则按固定通知号送达"
                    scheduled_at = None
                    next_attempt_at = None
                else:
                    state = "pending"
                    note = "非检修时段触发，立即安排通知"
                    scheduled_at = now
                    next_attempt_at = now
                cur = conn.execute(
                    """
                    INSERT INTO alarm_rounds
                        (triggered_at, maintenance_id, state, scheduled_at,
                         next_attempt_at, note, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now,
                        int(window["id"]) if window is not None else None,
                        state,
                        scheduled_at,
                        next_attempt_at,
                        note,
                        now,
                    ),
                )
                round_id = int(cur.lastrowid)
                notification_no = f"ALM-{round_id:06d}"
                conn.execute(
                    "UPDATE alarm_rounds SET notification_no = ? WHERE id = ?",
                    (notification_no, round_id),
                )
                row = conn.execute(
                    "SELECT * FROM alarm_rounds WHERE id = ?", (round_id,)
                ).fetchone()
            return self._round_dict(row)

    def recover(self) -> dict[str, Any]:
        now = self._clock()
        with self._lock:
            self._expire_windows_locked(now)
            with self._db() as conn:
                row = conn.execute(
                    "SELECT * FROM alarm_rounds WHERE recovered_at IS NULL ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if row is None:
                    raise AlarmConflict("当前没有未恢复的告警")
                state = row["state"]
                note = row["note"]
                needs_verify = int(row["needs_verify"])
                if state == "suppressed":
                    # Recovered inside maintenance: never backfill this round.
                    state = "skipped"
                    note = "检修结束前恢复，不补送通知"
                elif state == "delivering" and row["delivery_started_at"] is None:
                    # Claimed but never sent: safe to cancel.
                    state = "cancelled"
                    needs_verify = 0
                    note = "告警恢复时投递尚未开始，已取消送达"
                elif state == "delivering":
                    # An attempt is already in flight: resolve by number, never re-send.
                    state = "pending"
                    needs_verify = 1
                    note = "告警恢复时投递已开始，只按原通知号确认结果，不再新发通知"
                elif state == "pending" and needs_verify:
                    note = "告警恢复时已有投递尝试，仅按原通知号核实结果，不再新发通知"
                elif state == "pending":
                    # Not started: cancellable.
                    state = "cancelled"
                    note = "告警恢复时投递尚未开始，已取消送达"
                elif state == "sent":
                    note = "告警恢复；此前通知已受理，保留送达记录"
                conn.execute(
                    """
                    UPDATE alarm_rounds
                       SET recovered_at = ?,
                           state = ?,
                           needs_verify = ?,
                           next_attempt_at = CASE
                               WHEN ? = 'cancelled' THEN NULL
                               WHEN ? = 'pending' THEN ?
                               ELSE next_attempt_at
                           END,
                           note = ?,
                           updated_at = ?
                     WHERE id = ?
                    """,
                    (now, state, needs_verify, state, state, now, note, now, row["id"]),
                )
                updated = conn.execute(
                    "SELECT * FROM alarm_rounds WHERE id = ?", (row["id"],)
                ).fetchone()
            return self._round_dict(updated)

    # ---------- handover verification ----------

    @staticmethod
    def _handover_fingerprint(row: dict[str, Any]) -> tuple[Any, ...]:
        """Fields whose change invalidates a handover conclusion."""
        return (
            row["notification_no"],
            row["state"],
            int(row["needs_verify"]),
            row["accepted_at"],
            row["attempts"],
            row["last_attempt_at"],
            row["delivery_started_at"],
            row["recovered_at"],
        )

    def _handover_snapshot_locked(
        self, maintenance_id: int | None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
        """Read ended-maintenance rounds; the digest covers all conclusion inputs."""
        with self._db() as conn:
            windows, rounds = self._handover_rows(conn, maintenance_id)
        window_dicts = [dict(w) for w in windows]
        round_dicts = [dict(r) for r in rounds]
        return window_dicts, round_dicts, self._handover_digest(
            window_dicts, round_dicts
        )

    @staticmethod
    def _handover_rows(
        conn: sqlite3.Connection, maintenance_id: int | None
    ) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
        if maintenance_id is None:
            windows_where = "closed_at IS NOT NULL"
            rounds_where = "w.closed_at IS NOT NULL"
            where_params: tuple[Any, ...] = ()
        else:
            windows_where = "closed_at IS NOT NULL AND id = ?"
            rounds_where = "w.closed_at IS NOT NULL AND w.id = ?"
            where_params = (maintenance_id,)
        windows = conn.execute(
            f"""
            SELECT * FROM maintenance_windows
             WHERE {windows_where}
             ORDER BY id ASC
            """,
            where_params,
        ).fetchall()
        rounds = conn.execute(
            f"""
            SELECT r.*, w.id AS window_id
              FROM alarm_rounds r
              JOIN maintenance_windows w ON r.maintenance_id = w.id
             WHERE {rounds_where}
             ORDER BY r.id ASC
            """,
            where_params,
        ).fetchall()
        return windows, rounds

    @staticmethod
    def _handover_digest(
        window_dicts: list[dict[str, Any]], round_dicts: list[dict[str, Any]]
    ) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "windows": window_dicts,
                    "rounds": [
                        {k: r[k] for k in r if k != "note"} for r in round_dicts
                    ],
                },
                sort_keys=True,
                default=str,
            ).encode("utf-8")
        ).hexdigest()

    def _handover_signature_atomic(self, maintenance_id: int | None) -> str:
        """Compute the signature under an exclusive write lock.

        Worker deliveries write with their own connections and do not take the
        service lock, so an immediate transaction is what makes "check then act"
        atomic with respect to concurrent delivery updates.
        """
        conn = sqlite3.connect(self.db_path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        try:
            conn.execute("BEGIN IMMEDIATE")
            windows, rounds = self._handover_rows(conn, maintenance_id)
            window_dicts = [dict(w) for w in windows]
            round_dicts = [dict(r) for r in rounds]
            return self._handover_digest(window_dicts, round_dicts)
        finally:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            conn.close()

    @staticmethod
    def _same_ts(left: Any, right: Any) -> bool:
        if left is None or right is None:
            return False
        try:
            return abs(float(left) - float(right)) <= TIMESTAMP_TOLERANCE
        except (TypeError, ValueError):
            return False

    def _query_handover_rounds(
        self, rows: list[dict[str, Any]]
    ) -> dict[int, NotifyOutcome | str]:
        outcomes: dict[int, NotifyOutcome | str] = {}
        to_query: list[dict[str, Any]] = []
        for row in rows:
            if row["notification_no"]:
                to_query.append(row)
            else:
                outcomes[int(row["id"])] = "missing_number"
        if not to_query:
            return outcomes
        workers = max(1, min(HANDOVER_WORKERS, len(to_query)))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="handover") as pool:
            futures = {
                pool.submit(self._client.lookup, str(row["notification_no"])): int(row["id"])
                for row in to_query
            }
            for future, round_id in futures.items():
                try:
                    outcomes[round_id] = future.result()
                except Exception as exc:  # never let one lookup break the whole check
                    outcomes[round_id] = NotifyOutcome(
                        "unknown", detail=f"核查异常（{type(exc).__name__}）"
                    )
        return outcomes

    def _judge_handover_round(
        self,
        row: dict[str, Any],
        outcome: NotifyOutcome | str,
        changed: bool,
    ) -> dict[str, Any]:
        state = str(row["state"])
        reasons: list[str] = []

        def result(verdict: str, *items: str) -> dict[str, Any]:
            reasons.extend(items)
            return {
                "verdict": verdict,
                "verdict_label": VERDICT_LABELS[verdict],
                "reasons": reasons,
            }

        if changed:
            return result(
                "undecidable",
                "核查期间该轮告警状态或投递状态发生变化，旧结论不能用于交接，请重新核查",
            )
        if isinstance(outcome, str):
            return result("undecidable", "该轮缺少通知号，无法向通知端核对")

        if outcome.kind == "unknown":
            return result(
                "undecidable",
                f"通知端查询未能完成（{outcome.detail or '结果未知'}），请稍后重新核查",
            )

        if outcome.kind == "not_found":
            if state == "sent":
                return result(
                    "conflict",
                    "告警库记录该轮已送，但通知端按通知号查无记录",
                )
            if state in ("skipped", "cancelled"):
                return result(
                    "consistent",
                    f"告警库记录本轮未送达（{STATE_LABELS.get(state, state)}），通知端也无受理记录",
                )
            if state == "suppressed":
                return result(
                    "undecidable",
                    "检修窗口已结束，但该轮仍为抑制中，状态异常，请重新核查",
                )
            if state in ("pending", "delivering"):
                if state == "delivering" or bool(row["needs_verify"]):
                    return result(
                        "undecidable",
                        "通知端查无记录，但该轮仍在投递或待按通知号核实，只能暂无法判定",
                    )
                return result(
                    "undecidable",
                    "通知端查无记录，但该轮仍待送、尚未完成送达，暂无法判定",
                )
            return result("undecidable", f"告警状态 {state} 无法与通知端记录比对")

        # Notifier has an accepted record for this notification number.
        record = outcome.record or {}
        identity_conflicts: list[str] = []
        identity_missing: list[str] = []

        notifier_no = record.get("notification_no")
        if notifier_no is not None and str(notifier_no) != str(row["notification_no"]):
            identity_conflicts.append(
                f"通知号不一致（告警库 {row['notification_no']}，通知端 {notifier_no}）"
            )

        notifier_round = record.get("round_id")
        if notifier_round is None:
            identity_missing.append("通知端未保存告警轮次")
        else:
            try:
                round_matches = int(notifier_round) == int(row["id"])
            except (TypeError, ValueError):
                round_matches = False
            if not round_matches:
                identity_conflicts.append(
                    f"告警轮次不一致（告警库 #{row['id']}，通知端 #{notifier_round}）"
                )

        notifier_triggered = record.get("triggered_at")
        if notifier_triggered is None:
            identity_missing.append("通知端未保存触发时间")
        elif not self._same_ts(notifier_triggered, row["triggered_at"]):
            identity_conflicts.append(
                f"触发时间不一致（告警库 {iso_z(row['triggered_at'])}，"
                f"通知端 {iso_z(notifier_triggered)}）"
            )

        if identity_conflicts:
            return result("conflict", *identity_conflicts)
        if identity_missing:
            return result("undecidable", *identity_missing)

        if state == "sent":
            notifier_accepted = record.get("accepted_at")
            if row["accepted_at"] is None:
                return result("undecidable", "告警库已送记录缺少受理时间")
            if notifier_accepted is None:
                return result("undecidable", "通知端未返回受理时间")
            if not self._same_ts(notifier_accepted, row["accepted_at"]):
                return result(
                    "conflict",
                    f"受理时间不一致（告警库 {iso_z(row['accepted_at'])}，"
                    f"通知端 {iso_z(notifier_accepted)}）",
                )
            return result(
                "consistent",
                "告警轮次、触发时间与受理时间在两端一致",
            )

        if state in ("skipped", "cancelled"):
            return result(
                "conflict",
                f"告警库记录本轮未送达（{STATE_LABELS.get(state, state)}），"
                "但通知端已有受理记录",
            )
        if state == "suppressed":
            return result(
                "undecidable",
                "检修窗口已结束，但该轮仍为抑制中，状态异常，请重新核查",
            )
        # pending / delivering: the alarm side has not reconciled acceptance yet.
        return result(
            "undecidable",
            "通知端已有受理记录，但告警库仍为待送/投递中，等待告警库完成核实",
        )

    def check_handover(self, maintenance_id: int | None = None) -> dict[str, Any]:
        """Compare alarm-DB rounds of ended maintenance windows with notifier records.

        Notifier queries run outside the service lock and each one is bounded by the
        notifier client timeout, so alarm trigger/recover keep working even when the
        notifier is slow or unreachable.
        """
        generated_with_clock = self._clock()
        with self._lock:
            # A window whose cutoff has passed is part of the handover scope even
            # if no tick/read has closed it yet.
            self._expire_windows_locked(generated_with_clock)
            if maintenance_id is not None:
                with self._db() as conn:
                    window = conn.execute(
                        "SELECT * FROM maintenance_windows WHERE id = ?",
                        (maintenance_id,),
                    ).fetchone()
                if window is None:
                    raise AlarmBadRequest("指定的检修窗口不存在")
                if window["closed_at"] is None:
                    raise AlarmBadRequest("指定的检修窗口尚未结束，无法交接核查")
            windows, rows, signature = self._handover_snapshot_locked(maintenance_id)
            fingerprints = {int(r["id"]): self._handover_fingerprint(r) for r in rows}

        truncated = len(rows) > HANDOVER_MAX_CHECKS
        to_check = rows[:HANDOVER_MAX_CHECKS]
        outcomes = self._query_handover_rounds(to_check)

        # Re-read after the notifier round-trips: any state change means the
        # conclusion is no longer trustworthy.
        with self._lock:
            windows_after, rows_after, signature_after = self._handover_snapshot_locked(
                maintenance_id
            )
        after_fingerprints = {
            int(r["id"]): self._handover_fingerprint(r) for r in rows_after
        }
        changed_ids = {
            round_id
            for round_id, fingerprint in fingerprints.items()
            if after_fingerprints.get(round_id) != fingerprint
        }

        reports: list[dict[str, Any]] = []
        checked_ids = {int(r["id"]) for r in to_check}
        windows_changed = windows_after != windows
        for row in rows:
            round_id = int(row["id"])
            if round_id in checked_ids:
                outcome: NotifyOutcome | str = outcomes.get(
                    round_id, NotifyOutcome("unknown", detail="未查询")
                )
            else:
                outcome = NotifyOutcome("unknown", detail="本次核查轮次过多，未查询")
            judged = self._judge_handover_round(
                row,
                outcome,
                changed=windows_changed or round_id in changed_ids,
            )
            notifier_view: dict[str, Any] = {"status": "unknown", "detail": "未查询"}
            if isinstance(outcome, NotifyOutcome):
                notifier_view = {
                    "status": outcome.kind,
                    "detail": outcome.detail,
                    "accepted_at": iso_z(outcome.accepted_at),
                }
                if outcome.record:
                    notifier_view.update(
                        {
                            "notification_no": outcome.record.get("notification_no"),
                            "round_id": outcome.record.get("round_id"),
                            "triggered_at": iso_z(outcome.record.get("triggered_at")),
                            "deliveries": outcome.record.get("deliveries"),
                        }
                    )
            reports.append(
                {
                    "round_id": round_id,
                    "window_id": row["window_id"],
                    "notification_no": row["notification_no"],
                    "state": str(row["state"]),
                    "state_label": STATE_LABELS.get(str(row["state"]), str(row["state"])),
                    "recovered_at": iso_z(row["recovered_at"]),
                    "alarm_triggered_at": iso_z(row["triggered_at"]),
                    "alarm_accepted_at": iso_z(row["accepted_at"]),
                    "needs_verify": bool(row["needs_verify"]),
                    "notifier": notifier_view,
                    "verdict": judged["verdict"],
                    "verdict_label": judged["verdict_label"],
                    "reasons": judged["reasons"],
                }
            )

        summary = {"consistent": 0, "conflict": 0, "undecidable": 0}
        for item in reports:
            summary[item["verdict"]] += 1
        changed_during_check = bool(changed_ids) or signature_after != signature
        report = {
            "check_id": uuid.uuid4().hex,
            "generated_at": iso_z(generated_with_clock),
            "scope": HANDOVER_SCOPE,
            "maintenance_id": maintenance_id,
            "windows": [
                {
                    "id": w["id"],
                    "started_at": iso_z(w["started_at"]),
                    "ends_at": iso_z(w["ends_at"]),
                    "closed_at": iso_z(w["closed_at"]),
                    "close_reason": w["close_reason"],
                    "round_count": sum(
                        1 for r in reports if r["window_id"] == w["id"]
                    ),
                }
                for w in windows_after
            ],
            "rounds": reports,
            "summary": summary,
            "changed_during_check": changed_during_check,
            "truncated": truncated,
            "stale": False,
            "can_handover": (
                not changed_during_check
                and summary["conflict"] == 0
                and summary["undecidable"] == 0
            ),
        }
        if changed_during_check:
            report["notice"] = (
                "核查期间告警恢复或投递状态发生变化，部分结论已失效；"
                "旧结论不能用于完成交接，请重新核查"
            )
        elif summary["conflict"] or summary["undecidable"]:
            report["notice"] = "存在两端冲突或暂无法判定的轮次，核实前不能完成交接"
        else:
            report["notice"] = "全部轮次两端一致，可用于交接"

        with self._lock:
            self._handover_reports[report["check_id"]] = {
                "report": report,
                "signature": signature,
                "maintenance_id": maintenance_id,
            }
            while len(self._handover_reports) > HANDOVER_MAX_REPORTS:
                self._handover_reports.popitem(last=False)
        return report

    def get_handover_report(self, check_id: str) -> dict[str, Any]:
        """Return a stored report plus whether its conclusions are still current."""
        with self._lock:
            stored = self._handover_reports.get(check_id)
            if stored is None:
                raise AlarmBadRequest("核查记录不存在或已过期，请重新核查")
            current = self._handover_signature_atomic(stored["maintenance_id"])
            report = dict(stored["report"])
            stale = current != stored["signature"]
            report["stale"] = stale
            report["can_handover"] = bool(report["can_handover"]) and not stale
            if stale:
                report["notice"] = (
                    "核查后告警或投递状态已变化，旧结论不能用于完成交接，请重新核查"
                )
            return report

    def confirm_handover(self, check_id: str) -> dict[str, Any]:
        """Complete handover only if the checked conclusions are still current."""
        with self._lock:
            stored = self._handover_reports.get(check_id)
            if stored is None:
                raise AlarmBadRequest("核查记录不存在或已过期，请重新核查")
            current = self._handover_signature_atomic(stored["maintenance_id"])
            report = stored["report"]
            if current != stored["signature"]:
                raise AlarmConflict(
                    "核查后告警恢复或投递状态已变化，旧结论不能用于完成交接，请重新核查"
                )
            if report["changed_during_check"]:
                raise AlarmConflict("核查期间状态发生了变化，请重新核查后再交接")
            summary = report["summary"]
            if summary["conflict"] or summary["undecidable"]:
                raise AlarmConflict(
                    f"仍有 {summary['conflict']} 轮两端冲突、"
                    f"{summary['undecidable']} 轮暂无法判定，不能完成交接"
                )
            stored["confirmed_at"] = iso_z(self._clock())
            report["confirmed_at"] = stored["confirmed_at"]
            return {
                "ok": True,
                "check_id": check_id,
                "confirmed_at": stored["confirmed_at"],
                "summary": summary,
            }

    # ---------- read models ----------

    def _latest_window_locked(self) -> sqlite3.Row | None:
        with self._db() as conn:
            return conn.execute(
                "SELECT * FROM maintenance_windows ORDER BY id DESC LIMIT 1"
            ).fetchone()

    def _maintenance_dict(self, row: sqlite3.Row, now: float) -> dict[str, Any]:
        active = row["closed_at"] is None and row["ends_at"] > now
        return {
            "id": row["id"],
            "started_at": iso_z(row["started_at"]),
            "ends_at": iso_z(row["ends_at"]),
            "closed_at": iso_z(row["closed_at"]),
            "close_reason": row["close_reason"],
            "active": active,
            "remaining_seconds": max(0.0, float(row["ends_at"]) - now) if active else 0.0,
        }

    def _round_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        state = str(row["state"])
        return {
            "id": row["id"],
            "notification_no": row["notification_no"],
            "triggered_at": iso_z(row["triggered_at"]),
            "recovered_at": iso_z(row["recovered_at"]),
            "maintenance_id": row["maintenance_id"],
            "state": state,
            "state_label": STATE_LABELS.get(state, state),
            "group": GROUP_OF_STATE.get(state, state),
            "is_active": row["recovered_at"] is None,
            "scheduled_at": iso_z(row["scheduled_at"]),
            "accepted_at": iso_z(row["accepted_at"]),
            "attempts": row["attempts"],
            "last_attempt_at": iso_z(row["last_attempt_at"]),
            "delivery_started_at": iso_z(row["delivery_started_at"]),
            "needs_verify": bool(row["needs_verify"]),
            "last_error": row["last_error"],
            "note": row["note"],
        }

    def get_state(self) -> dict[str, Any]:
        now = self._clock()
        with self._lock:
            self._expire_windows_locked(now)
            with self._db() as conn:
                rounds = conn.execute(
                    "SELECT * FROM alarm_rounds ORDER BY id DESC LIMIT 100"
                ).fetchall()
                active = conn.execute(
                    "SELECT * FROM alarm_rounds WHERE recovered_at IS NULL ORDER BY id DESC LIMIT 1"
                ).fetchone()
                active_window = conn.execute(
                    """
                    SELECT * FROM maintenance_windows
                     WHERE closed_at IS NULL AND ends_at > ?
                     ORDER BY id DESC LIMIT 1
                    """,
                    (now,),
                ).fetchone()
                latest_window = conn.execute(
                    "SELECT * FROM maintenance_windows ORDER BY id DESC LIMIT 1"
                ).fetchone()
                grouped = conn.execute(
                    """
                    SELECT state, COUNT(*) AS n FROM alarm_rounds GROUP BY state
                    """
                ).fetchall()
                totals = conn.execute(
                    "SELECT COUNT(*) AS total FROM alarm_rounds"
                ).fetchone()
            counts = {
                "suppressed": 0,
                "pending": 0,
                "sent": 0,
                "skipped": 0,
                "cancelled": 0,
            }
            for item in grouped:
                group = GROUP_OF_STATE.get(str(item["state"]))
                if group is not None:
                    counts[group] += int(item["n"])
            return {
                "device": DEVICE_NAME,
                "now": iso_z(now),
                "maintenance": self._maintenance_dict(active_window, now)
                if active_window is not None
                else None,
                "latest_maintenance": self._maintenance_dict(latest_window, now)
                if latest_window is not None
                else None,
                "active_round": self._round_dict(active) if active is not None else None,
                "rounds": [self._round_dict(row) for row in rounds],
                "counts": counts,
                "total_rounds": int(totals["total"]) if totals is not None else 0,
                "worker_running": self.worker_running,
            }

    def close(self) -> None:
        self.stop_worker()


def iso_z(ts: float | None) -> str | None:
    if ts is None:
        return None
    from datetime import datetime, timezone

    return (
        datetime.fromtimestamp(float(ts), tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
