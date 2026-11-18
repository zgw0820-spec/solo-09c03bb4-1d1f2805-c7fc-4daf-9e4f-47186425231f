from __future__ import annotations

import sqlite3
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from app.alarm.main import create_app
from app.alarm.service import AlarmService, NotifyOutcome
from app.notifier.main import NotifyIn


def round_by_id(state: dict, round_id: int) -> dict:
    return next(r for r in state["rounds"] if r["id"] == round_id)


def make_alarm(tmp_path, notifier, **kwargs) -> AlarmService:
    _store, url = notifier
    return AlarmService(
        tmp_path / "handover-alarm.db",
        url,
        start_worker=False,
        retry_base=0.01,
        **kwargs,
    )


def closed_window_with_sent_round(alarm, clock, minutes: float = 1.0) -> dict:
    """One round suppressed by maintenance, delivered at cutoff, then recovered."""
    alarm.set_maintenance(clock() + minutes * 60)
    created = alarm.trigger()
    clock.advance(minutes * 60 + 1)
    alarm.tick()
    assert round_by_id(alarm.get_state(), created["id"])["state"] == "sent"
    alarm.recover()
    return round_by_id(alarm.get_state(), created["id"])


# ---------- verdicts ----------


def test_consistent_round_reports_consistent(alarm, notifier, clock):
    _store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)

    report = alarm.check_handover()
    assert report["summary"] == {"consistent": 1, "conflict": 0, "undecidable": 0}
    assert report["can_handover"] is True
    item = report["rounds"][0]
    assert item["round_id"] == row["id"]
    assert item["verdict"] == "consistent"
    assert item["notifier"]["status"] == "accepted"
    assert item["notifier"]["round_id"] == row["id"]
    assert item["notifier"]["triggered_at"] == row["triggered_at"]
    assert item["notifier"]["accepted_at"] == row["accepted_at"]


def test_skipped_and_cancelled_rounds_are_consistent_when_notifier_has_nothing(
    alarm, notifier, clock
):
    _store, _url = notifier
    alarm.set_maintenance(clock() + 600)
    skipped = alarm.trigger()
    alarm.recover()

    clock.advance(601)
    alarm.get_state()
    outside = alarm.trigger()
    alarm.recover()  # delivery never started -> cancelled
    assert round_by_id(alarm.get_state(), skipped["id"])["state"] == "skipped"
    assert round_by_id(alarm.get_state(), outside["id"])["state"] == "cancelled"

    report = alarm.check_handover()
    # Only rounds tied to an ended maintenance window are in handover scope.
    assert [r["round_id"] for r in report["rounds"]] == [skipped["id"]]
    assert report["summary"] == {"consistent": 1, "conflict": 0, "undecidable": 0}
    assert report["rounds"][0]["verdict"] == "consistent"
    assert report["can_handover"] is True


def test_cancelled_round_inside_ended_window_is_consistent(alarm, notifier, clock):
    _store, _url = notifier
    # A round triggered outside maintenance has no window; force one inside a window
    # by ending maintenance while the round is still suppressed is impossible, so
    # this covers the cancelled-in-window path via a manual DB state.
    alarm.set_maintenance(clock() + 600)
    created = alarm.trigger()
    alarm.recover()
    clock.advance(601)
    alarm.get_state()
    db = sqlite3.connect(alarm.db_path)
    db.execute(
        "UPDATE alarm_rounds SET state = 'cancelled' WHERE id = ?", (created["id"],)
    )
    db.commit()
    db.close()

    report = alarm.check_handover()
    assert report["rounds"][0]["verdict"] == "consistent"
    assert "通知端也无受理记录" in " ".join(report["rounds"][0]["reasons"])


def test_conflict_when_alarm_says_sent_but_notifier_has_no_record(alarm, notifier, clock):
    store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)

    # Simulate the notifier having lost the record (e.g. a wiped store).
    db = sqlite3.connect(store.path)
    db.execute("DELETE FROM notifications WHERE notification_no = ?", (row["notification_no"],))
    db.commit()
    db.close()

    report = alarm.check_handover()
    item = report["rounds"][0]
    assert item["verdict"] == "conflict"
    assert item["notifier"]["status"] == "not_found"
    assert report["can_handover"] is False
    assert "查无记录" in " ".join(item["reasons"])


def test_conflict_when_notifier_accepted_but_alarm_says_cancelled(alarm, notifier, clock):
    store, _url = notifier
    alarm.set_maintenance(clock() + 600)
    created = alarm.trigger()
    alarm.recover()  # suppressed -> skipped
    clock.advance(601)
    alarm.get_state()

    # The notifier somehow accepted this number even though the alarm side never sent it.
    store.accept(
        NotifyIn(
            notification_no=created["notification_no"],
            round_id=created["id"],
            triggered_at=float(
                __import__("datetime").datetime.fromisoformat(
                    created["triggered_at"].replace("Z", "+00:00")
                ).timestamp()
            ),
        )
    )

    report = alarm.check_handover()
    item = report["rounds"][0]
    assert item["verdict"] == "conflict"
    assert report["can_handover"] is False


def test_conflict_when_trigger_time_differs(alarm, notifier, clock):
    store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)

    db = sqlite3.connect(store.path)
    db.execute(
        "UPDATE notifications SET triggered_at = triggered_at + 5 WHERE notification_no = ?",
        (row["notification_no"],),
    )
    db.commit()
    db.close()

    report = alarm.check_handover()
    item = report["rounds"][0]
    assert item["verdict"] == "conflict"
    assert "触发时间不一致" in " ".join(item["reasons"])


def test_conflict_when_round_id_differs(alarm, notifier, clock):
    store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)

    db = sqlite3.connect(store.path)
    db.execute(
        "UPDATE notifications SET round_id = 999 WHERE notification_no = ?",
        (row["notification_no"],),
    )
    db.commit()
    db.close()

    report = alarm.check_handover()
    assert report["rounds"][0]["verdict"] == "conflict"
    assert "告警轮次不一致" in " ".join(report["rounds"][0]["reasons"])


def test_conflict_when_accepted_time_differs(alarm, notifier, clock):
    store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)

    db = sqlite3.connect(store.path)
    db.execute(
        "UPDATE notifications SET accepted_at = accepted_at + 7 WHERE notification_no = ?",
        (row["notification_no"],),
    )
    db.commit()
    db.close()

    report = alarm.check_handover()
    assert report["rounds"][0]["verdict"] == "conflict"
    assert "受理时间不一致" in " ".join(report["rounds"][0]["reasons"])


def test_undecidable_when_notifier_has_no_record_but_round_still_delivering(
    alarm, notifier, clock
):
    store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)

    # Force the round back into "delivering / needs_verify" while the notifier
    # genuinely has no record for it.
    db = sqlite3.connect(store.path)
    db.execute("DELETE FROM notifications WHERE notification_no = ?", (row["notification_no"],))
    db.commit()
    db.close()
    db = sqlite3.connect(alarm.db_path)
    db.execute(
        """
        UPDATE alarm_rounds
           SET state = 'delivering', accepted_at = NULL, needs_verify = 1,
               delivery_started_at = ?
         WHERE id = ?
        """,
        (clock(), row["id"]),
    )
    db.commit()
    db.close()

    report = alarm.check_handover()
    item = report["rounds"][0]
    assert item["verdict"] == "undecidable"
    assert "仍在投递或待按通知号核实" in " ".join(item["reasons"])
    assert report["can_handover"] is False


def test_undecidable_when_notifier_has_no_record_and_round_pending_needs_verify(
    alarm, notifier, clock
):
    _store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)
    db = sqlite3.connect(alarm.db_path)
    db.execute(
        "UPDATE alarm_rounds SET state='pending', needs_verify=1, accepted_at=NULL WHERE id=?",
        (row["id"],),
    )
    db.commit()
    db.close()

    report = alarm.check_handover()
    assert report["rounds"][0]["verdict"] == "undecidable"


def test_undecidable_when_notifier_unreachable(alarm, notifier, clock):
    _store, _url = notifier
    closed_window_with_sent_round(alarm, clock)

    alarm._client.base_url = "http://127.0.0.1:1"
    report = alarm.check_handover()
    item = report["rounds"][0]
    assert item["verdict"] == "undecidable"
    assert item["notifier"]["status"] == "unknown"
    assert report["can_handover"] is False


# ---------- freshness / interleaving ----------


def test_state_change_during_check_marks_report_stale(alarm, notifier, clock):
    _store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)

    original_lookup = alarm._client.lookup

    def lookup_then_mutate(number: str):
        outcome = original_lookup(number)
        # A recovery/bounded state change lands while the notifier lookup is in flight.
        db = sqlite3.connect(alarm.db_path)
        db.execute(
            "UPDATE alarm_rounds SET attempts = attempts + 1, updated_at = updated_at + 1 WHERE id = ?",
            (row["id"],),
        )
        db.commit()
        db.close()
        return outcome

    alarm._client.lookup = lookup_then_mutate
    report = alarm.check_handover()
    assert report["changed_during_check"] is True
    assert report["can_handover"] is False
    assert report["rounds"][0]["verdict"] == "undecidable"
    assert "重新核查" in report["notice"]

    stored = alarm.get_handover_report(report["check_id"])
    assert stored["stale"] is True
    assert stored["can_handover"] is False
    with pytest.raises(Exception) as excinfo:
        alarm.confirm_handover(report["check_id"])
    assert "重新核查" in str(excinfo.value)


def test_report_becomes_stale_after_later_state_change(alarm, notifier, clock):
    _store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)

    report = alarm.check_handover()
    assert report["can_handover"] is True
    assert alarm.get_handover_report(report["check_id"])["stale"] is False

    db = sqlite3.connect(alarm.db_path)
    db.execute("UPDATE alarm_rounds SET attempts = attempts + 1 WHERE id = ?", (row["id"],))
    db.commit()
    db.close()

    later = alarm.get_handover_report(report["check_id"])
    assert later["stale"] is True
    assert later["can_handover"] is False
    assert "重新核查" in later["notice"]
    with pytest.raises(Exception):
        alarm.confirm_handover(report["check_id"])


def test_confirm_handover_succeeds_for_clean_report(alarm, notifier, clock):
    _store, _url = notifier
    closed_window_with_sent_round(alarm, clock)
    report = alarm.check_handover()
    result = alarm.confirm_handover(report["check_id"])
    assert result["ok"] is True
    assert result["summary"]["consistent"] == 1


def test_confirm_refused_when_conflicts_exist(alarm, notifier, clock):
    store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)
    db = sqlite3.connect(store.path)
    db.execute("DELETE FROM notifications WHERE notification_no = ?", (row["notification_no"],))
    db.commit()
    db.close()

    report = alarm.check_handover()
    with pytest.raises(Exception) as excinfo:
        alarm.confirm_handover(report["check_id"])
    assert "不能完成交接" in str(excinfo.value)


def test_check_does_not_block_trigger_and_recover_when_notifier_slow(
    alarm, notifier, clock, monkeypatch
):
    """Notifier lookups run outside the lock and are timeout-bounded."""
    _store, _url = notifier
    closed_window_with_sent_round(alarm, clock)

    def slow_lookup(number: str):
        time.sleep(0.3)
        return NotifyOutcome("unknown", detail="查询超时")

    monkeypatch.setattr(alarm._client, "lookup", slow_lookup)

    import threading

    started = threading.Event()
    done = threading.Event()

    def run_check():
        started.set()
        alarm.check_handover()
        done.set()

    thread = threading.Thread(target=run_check)
    thread.start()
    started.wait(1)

    # Alarm commands must still complete while the notifier queries are in flight.
    started_at = time.monotonic()
    created = alarm.trigger()
    assert created["id"] == 2
    alarm.recover()
    elapsed = time.monotonic() - started_at
    assert elapsed < 0.2, f"trigger/recover blocked for {elapsed:.2f}s"

    assert done.wait(5) is True
    thread.join(5)


def test_notifier_down_check_is_undecidable_but_alarm_commands_still_work(
    alarm, notifier, clock
):
    _store, _url = notifier
    closed_window_with_sent_round(alarm, clock)

    alarm._client.base_url = "http://127.0.0.1:1"
    started_at = time.monotonic()
    report = alarm.check_handover()
    elapsed = time.monotonic() - started_at
    assert report["summary"]["undecidable"] == 1
    assert elapsed < 5

    # Alarm operations are unaffected by the notifier outage.
    created = alarm.trigger()
    assert created["state"] == "pending"
    alarm.tick()  # attempts delivery, stays undelivered
    row = round_by_id(alarm.get_state(), created["id"])
    assert row["state"] == "pending"
    assert row["accepted_at"] is None
    assert alarm.recover()["state"] in ("cancelled", "pending")


# ---------- restart semantics ----------


def test_both_sides_restart_and_recheck_uses_current_records(
    alarm_factory, notifier, clock
):
    store, _url = notifier
    first = alarm_factory()
    first.set_maintenance(clock() + 60)
    created = first.trigger()
    clock.advance(61)
    first.tick()
    first.recover()
    first.close()

    # Restarting the notifier store over the same DB must not lose the record.
    restarted_store = type(store)(store.path)
    restarted = alarm_factory()
    report = restarted.check_handover()
    assert report["summary"]["consistent"] == 1
    assert report["rounds"][0]["round_id"] == created["id"]

    # If the notifier genuinely has no record after its restart, the verdict
    # must follow the current record, i.e. conflict for a round alarm calls sent.
    db = sqlite3.connect(restarted_store.path)
    db.execute("DELETE FROM notifications")
    db.commit()
    db.close()
    report2 = restarted.check_handover()
    assert report2["rounds"][0]["verdict"] == "conflict"


# ---------- HTTP API ----------


def make_client(tmp_path, notifier) -> tuple[TestClient, AlarmService]:
    _store, url = notifier
    service = AlarmService(
        tmp_path / "handover-api.db", url, start_worker=False, retry_base=0.01
    )
    app = create_app(
        service=service, db_path=str(tmp_path / "handover-api.db"), notifier_url=url
    )
    return TestClient(app), service


def test_api_handover_check_confirm_and_validation(tmp_path, notifier):
    client, service = make_client(tmp_path, notifier)
    with client:
        now = time.time()
        window = client.post("/api/maintenance", json={"ends_at": now + 2}).json()
        created = client.post("/api/alarm/trigger").json()
        assert created["state"] == "suppressed"

        # An explicitly targeted window that has not ended cannot be checked.
        active = client.post(f"/api/handover/check?maintenance_id={window['id']}")
        assert active.status_code == 400
        assert "尚未结束" in active.json()["detail"]

        time.sleep(2.1)
        client.post("/api/tick")
        client.post("/api/alarm/recover")

        checked = client.post("/api/handover/check")
        assert checked.status_code == 200
        report = checked.json()
        assert report["check_id"]
        assert report["summary"] == {"consistent": 1, "conflict": 0, "undecidable": 0}
        assert report["can_handover"] is True

        fetched = client.get(f"/api/handover/reports/{report['check_id']}")
        assert fetched.status_code == 200
        assert fetched.json()["stale"] is False

        confirmed = client.post(f"/api/handover/reports/{report['check_id']}/confirm")
        assert confirmed.status_code == 200
        assert confirmed.json()["ok"] is True

        missing = client.get("/api/handover/reports/does-not-exist")
        assert missing.status_code == 400

        bad_window = client.post("/api/handover/check?maintenance_id=999")
        assert bad_window.status_code == 400
    service.close()


def test_page_exposes_handover_controls(tmp_path, notifier):
    client, service = make_client(tmp_path, notifier)
    with client:
        page = client.get("/")
        assert page.status_code == 200
        assert "交接核查" in page.text
        assert 'id="run-handover"' in page.text
        js = client.get("/static/app.js").text
        assert "/api/handover/check" in js
    service.close()


def test_concurrent_delivery_during_check_invalidates_conclusion(
    alarm, notifier, clock, monkeypatch
):
    """A delivery completing while the notifier lookup is in flight must invalidate it."""
    _store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)
    db = sqlite3.connect(alarm.db_path)
    db.execute(
        "UPDATE alarm_rounds SET state='pending', needs_verify=1, accepted_at=NULL WHERE id=?",
        (row["id"],),
    )
    db.commit()
    db.close()

    original_lookup = alarm._client.lookup

    def lookup_and_complete(number: str):
        outcome = original_lookup(number)
        # The alarm worker finishes the round while the check is comparing.
        db = sqlite3.connect(alarm.db_path)
        db.execute(
            "UPDATE alarm_rounds SET state='sent', needs_verify=0, accepted_at=? WHERE id=?",
            (clock(), row["id"]),
        )
        db.commit()
        db.close()
        return outcome

    monkeypatch.setattr(alarm._client, "lookup", lookup_and_complete)
    report = alarm.check_handover()
    assert report["changed_during_check"] is True
    assert report["can_handover"] is False
    assert report["rounds"][0]["verdict"] == "undecidable"
    assert "旧结论不能用于完成交接" in report["notice"]


def test_fingerprint_change_only_in_note_does_not_invalidate(alarm, notifier, clock):
    """The digest excludes free-text notes, which carry no conclusion inputs."""
    _store, _url = notifier
    row = closed_window_with_sent_round(alarm, clock)
    report = alarm.check_handover()
    db = sqlite3.connect(alarm.db_path)
    db.execute("UPDATE alarm_rounds SET note='无关的说明文字' WHERE id=?", (row["id"],))
    db.commit()
    db.close()
    assert alarm.get_handover_report(report["check_id"])["stale"] is False
