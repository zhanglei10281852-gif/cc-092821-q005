from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.core.clock import FrozenClock
from app.core.errors import ConflictError, ValidationError
from app.database import get_connection, transaction
from app.germplasm.service import GermplasmService
from app.services.jobs import JobService
from tests.test_germplasm_workflow import create_stored_lot


@pytest.fixture()
def service(client) -> GermplasmService:
    clock = FrozenClock(datetime(2026, 9, 25, 8, 0, tzinfo=UTC))
    return GermplasmService(get_connection(), clock)


def prepare_lot(service: GermplasmService, suffix: str = "ENV") -> tuple[dict, dict]:
    accession, lot, _placement = create_stored_lot(service, suffix)
    location = service.repository.require_location(lot["placements"][0]["location_id"])
    # 摆放发生在异常窗口之前，评估时钟位于读数之后。
    service.clock.current = datetime(2026, 9, 27, 8, 0, tzinfo=UTC)
    return location, lot


def reading(location_id: int, at: datetime, *, temperature_c: float = -5.0, humidity: float = 31.0, key: str | None = None) -> dict:
    return {
        "location_id": location_id,
        "observed_at": at,
        "temperature_c": temperature_c,
        "humidity_percent": humidity,
        "source_key": key or f"sensor-{at.isoformat()}",
    }


def test_consecutive_violations_merge_into_windowed_event(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        for minutes in (0, 60, 120, 180):
            service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=minutes)))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=240), temperature_c=-18))

        events, total = service.environment.list_events()
        assert total == 1
        event = events[0]
        assert event["started_at"] == t0.isoformat(timespec="seconds")
        assert event["ended_at"] == (t0 + timedelta(minutes=240)).isoformat(timespec="seconds")
        assert event["duration_minutes"] == 180
        assert event["temperature_peak_c"] == -5.0
        assert event["peak_at"] is not None
        assert event["status"] == "closed"
        assert len(event["reading_ids"]) == 4
        # 一个事件只有一条聚合告警
        alerts = service.quality.open_alerts()
        assert not any(alert["alert_type"] == "environment_event" for alert in alerts)


def test_out_of_order_backfill_deterministically_revises_window(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        for minutes in (120, 180):
            service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=minutes)))
        event_first = service.environment.list_events()[0][0]
        assert event_first["started_at"] == (t0 + timedelta(minutes=120)).isoformat(timespec="seconds")

        # 迟到补传更早与中间的读数：窗口提前、延长，但仍是同一个事件
        service.quality.add_reading(reading(location["id"], t0))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=60)))
        event = service.environment.list_events()[0][0]
        assert event["id"] == event_first["id"]
        assert event["event_no"] == event_first["event_no"]
        assert event["started_at"] == t0.isoformat(timespec="seconds")
        assert event["revision"] == event_first["revision"] + 2
        assert len(event["reading_ids"]) == 4


def test_repeated_readings_and_reevaluation_do_not_create_events(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        payload = reading(location["id"], t0)
        first = service.quality.add_reading(payload)
        replay = service.quality.add_reading(payload)
        assert replay["replayed"] is True and replay["events"] == []

        service.environment.sync_location(location["id"])
        service.environment.sync_location(location["id"])
        assert service.environment.list_events()[1] == 1
        event = service.environment.list_events()[0][0]
        assert event["revision"] == first["events"][0]["revision"]


def test_batch_import_merges_once_per_location(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        rows = [
            reading(location["id"], t0, key="imp-0"),
            reading(location["id"], t0 + timedelta(minutes=60), key="imp-1"),
            reading(location["id"], t0 + timedelta(minutes=120), key="imp-2"),
            reading(location["id"], t0 + timedelta(minutes=180), temperature_c=-18, key="imp-n"),
        ]
        result = service.quality.import_readings(rows)
        assert result["accepted_count"] == 4 and result["rejected_count"] == 0
        events = service.environment.list_events()[0]
        assert len(events) == 1
        assert len(events[0]["reading_ids"]) == 3
        assert events[0]["status"] == "closed"


def test_gap_splits_into_separate_events(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        service.quality.add_reading(reading(location["id"], t0 + timedelta(hours=8), key="a"))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(hours=13), key="b"))  # 间隔 5 小时

        events = service.environment.list_events()[0]
        assert len(events) == 2


def test_late_normal_reading_splits_window_deterministically(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 6, 0, tzinfo=UTC)
        for minutes in (0, 30, 45):
            service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=minutes), key=f"v{minutes}"))
        # 迟到补传夹在中间的正常读数，确定性地把窗口切成两段
        service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=35), temperature_c=-18, key="n35"))

        events = service.environment.list_events()[0]
        assert len(events) == 2
        assert {len(event["reading_ids"]) for event in events} == {1, 2}
        assert all(event["status"] == "closed" for event in events)
        # 重复归并结果稳定
        service.environment.sync_location(location["id"])
        assert service.environment.list_events()[1] == 2


def test_late_backfill_merges_events_and_keeps_superseded_history(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        service.quality.add_reading(reading(location["id"], t0, key="a"))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(hours=5), key="b"))  # 间隔 5 小时 -> 两事件
        assert service.environment.list_events()[1] == 2

        # 迟到补传把间隔填上（每 30 分钟一条越界读数），两个窗口合并为一个事件
        for minutes in range(30, 300, 30):
            service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=minutes), key=f"m{minutes}"))
        events = service.environment.list_events()[0]
        assert len(events) == 1
        alive = events
        superseded = service.environment.list_events(status="superseded")[0]
        assert len(superseded) == 1
        assert superseded[0]["superseded_by"] == alive[0]["id"]
        # 被合并事件的读数仍可经由存活事件追到
        assert len(alive[0]["reading_ids"]) == 11


def test_affected_lots_follow_placement_history(service):
    with transaction(immediate=True):
        location, lot = prepare_lot(service)
        # 容器 03:00 移到第二个库位，只经历异常窗口的前一小时
        other = service.inventory.create_location({
            "location_code": "COLD-ENV-B", "facility": "长期库", "room": "低温二室", "rack": "R2", "shelf": "S1",
            "capacity_grams": 1000, "temperature_c": -18, "humidity_percent": 30,
        })
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        service.quality.add_reading(reading(location["id"], t0 + timedelta(hours=2), key="v1"))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(hours=3), key="v2"))
        service.clock.current = t0 + timedelta(hours=3)
        service.inventory.move_placement(lot["placements"][0]["id"], {
            "target_location_id": other["id"], "expected_version": 1,
            "idempotency_key": "move-ENV-0001", "actor": "保管员", "reason": "库位整理",
        })
        service.quality.add_reading(reading(location["id"], t0 + timedelta(hours=6), temperature_c=-18, key="n1"))
        event = service.environment.list_events()[0][0]
        affected = service.environment.affected_lots(event)
        assert len(affected) == 1
        assert affected[0]["lot_no"] == lot["lot_no"]
        assert affected[0]["exposed_minutes"] == 60  # 02:00 到 03:00


def test_recommendations_cover_freeze_retest_and_observe(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        base = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        service.clock.current = base + timedelta(days=6)
        rule = service.environment.active_rule()

        def window(day: int, length_minutes: int, temperature_c: float) -> dict:
            t0 = base + timedelta(days=day)
            # 每 60 分钟一条越界读数，保证连续窗口不被最大间隔（120 分钟）切断
            steps = max(1, length_minutes // 60)
            for step in range(steps + 1):
                service.quality.add_reading(reading(
                    location["id"], t0 + timedelta(minutes=60 * step), temperature_c=temperature_c,
                    key=f"{day}-v{step}"))
            service.quality.add_reading(reading(
                location["id"], t0 + timedelta(minutes=length_minutes + 5), temperature_c=-18, key=f"{day}-n"))
            return next(
                event for event in service.environment.list_events(limit=200)[0]
                if event["started_at"] == t0.isoformat(timespec="seconds")
            )

        short = window(1, 30, -23.5)    # 偏离 5.5°C（warning），暴露不足 -> none
        medium = window(2, 120, -23.5)  # 120 分钟 -> retest
        long_event = window(3, 240, -23.5)  # 240 分钟 -> freeze
        assert service.environment.recommend(short, rule)[0] == "none"
        assert service.environment.recommend(medium, rule)[0] == "retest"
        assert service.environment.recommend(long_event, rule)[0] == "freeze"

        critical = window(4, 10, -30.0)  # 10 分钟但严重偏离 -> 至少 retest
        assert critical["severity"] == "critical"
        assert service.environment.recommend(critical, rule)[0] == "retest"


def _assess(service: GermplasmService, event: dict) -> dict:
    service.environment.request_assessment(event["id"], "值班员")
    return service.environment.run_due_assessment_job()


def test_confirm_atomically_writes_freeze_retest_and_audit(service):
    with transaction(immediate=True):
        location, lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        for minutes in (0, 60, 120, 180, 240):
            service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=minutes), key=f"v{minutes}"))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=300), temperature_c=-18, key="n"))
        event = service.environment.list_events()[0][0]
        assessment = _assess(service, event)
        assert assessment["status"] == "awaiting_confirmation"
        assert assessment["recommendation"] == "freeze"
        assert {item["lot_id"] for item in assessment["affected"]} == {lot["id"]}

        confirmed = service.environment.confirm_assessment(assessment["id"], "审核员")
        assert confirmed["status"] == "confirmed"
        assert confirmed["applied_result"]["holds_created"]
        assert confirmed["applied_result"]["retest_orders_created"]

        held = service.repository.require_lot(lot["id"])
        assert held["status"] == "held"
        audit = get_connection().execute(
            "SELECT action FROM audit_events WHERE resource_type='environment_event' AND resource_id=? "
            "AND action='environment.disposition.apply'", (event["id"],)
        ).fetchall()
        assert len(audit) == 1

        # 重复确认是幂等的，不产生额外冻结、复检单或审计
        again = service.environment.confirm_assessment(assessment["id"], "审核员")
        assert again["status"] == "confirmed"
        assert get_connection().execute("SELECT COUNT(*) FROM lot_holds").fetchone()[0] == 1
        assert get_connection().execute("SELECT COUNT(*) FROM event_retest_orders").fetchone()[0] == 1
        assert get_connection().execute(
            "SELECT COUNT(*) FROM audit_events WHERE action='environment.disposition.apply' AND resource_id=?",
            (event["id"],),
        ).fetchone()[0] == 1


def test_human_override_requires_reason_and_is_preserved(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        service.quality.add_reading(reading(location["id"], t0, temperature_c=-23.5, key="v"))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=30), temperature_c=-18, key="n"))
        event = service.environment.list_events()[0][0]
        assessment = _assess(service, event)
        assert assessment["recommendation"] == "none"

        with pytest.raises(ValidationError):
            service.environment.confirm_assessment(assessment["id"], "审核员", decision="retest")
        confirmed = service.environment.confirm_assessment(
            assessment["id"], "审核员", decision="retest", reason="主管要求提前复检"
        )
        assert confirmed["applied_result"]["decision"] == "retest"
        assert confirmed["applied_result"]["override"] is True
        assert confirmed["overrides"][0]["system_recommendation"] == "none"
        assert confirmed["overrides"][0]["rule_version"] == event["rule_version"]


def test_cannot_confirm_before_assessment_completes(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        service.quality.add_reading(reading(location["id"], t0, key="v"))
        event = service.environment.list_events()[0][0]
        assessment = service.environment.request_assessment(event["id"], "值班员")
        with pytest.raises(ConflictError):
            service.environment.confirm_assessment(assessment["id"], "审核员")


def test_assessment_job_resumes_after_restart(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        for minutes in (0, 60, 120, 180):
            service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=minutes), key=f"v{minutes}"))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=240), temperature_c=-18, key="n"))
        event = service.environment.list_events()[0][0]
        service.environment.request_assessment(event["id"], "值班员")
        JobService(get_connection(), service.clock).claim("dead-worker")  # 模拟进程崩溃于认领之后

    service.clock.advance(seconds=61)  # 租约过期
    restarted = GermplasmService(get_connection(), service.clock)
    with transaction(immediate=True):
        result = restarted.environment.run_due_assessment_job("new-worker")
    assert result is not None and result["status"] == "awaiting_confirmation"


def test_late_reading_after_confirmation_never_duplicates_disposition(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        for minutes in (0, 60, 120):
            service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=minutes), key=f"v{minutes}"))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=180), temperature_c=-18, key="n"))
        event = service.environment.list_events()[0][0]
        first = _assess(service, event)
        service.environment.confirm_assessment(first["id"], "审核员")

        # 迟到读数延长已关闭窗口：事件修订，旧评估仍为 confirmed 并保留
        service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=150), key="late"))
        revised = service.environment.list_events()[0][0]
        assert revised["revision"] == event["revision"] + 1
        assert service.environment._require_assessment(first["id"])["status"] == "confirmed"

        second = _assess(service, revised)
        result = service.environment.confirm_assessment(second["id"], "审核员")
        assert result["applied_result"]["holds_created"] == []
        assert result["applied_result"]["retest_orders_created"] == []
        assert get_connection().execute("SELECT COUNT(*) FROM event_retest_orders").fetchone()[0] == 1
        assert get_connection().execute("SELECT COUNT(*) FROM lot_holds").fetchone()[0] == 1


def test_event_detail_traces_readings_containers_lots_and_disposition(service):
    with transaction(immediate=True):
        location, lot = prepare_lot(service)
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        for minutes in (0, 60, 120, 180, 240):
            service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=minutes), key=f"v{minutes}"))
        service.quality.add_reading(reading(location["id"], t0 + timedelta(minutes=300), temperature_c=-18, key="n"))
        event = service.environment.list_events()[0][0]
        assessment = _assess(service, event)
        assert assessment["recommendation"] == "freeze"
        service.environment.confirm_assessment(assessment["id"], "审核员")

        detail = service.environment.event_detail(event["id"])
        assert len(detail["readings"]) == 5
        assert detail["affected_lots"][0]["container_code"] == lot["placements"][0]["container_code"]
        assert detail["assessments"][0]["status"] == "confirmed"
        assert len(detail["holds"]) == 1
        assert detail["retest_orders"][0]["lot_no"] == lot["lot_no"]
        assert detail["rule_version"] == "env-rule-v1"


def test_new_rule_version_is_retained_on_future_events(service):
    with transaction(immediate=True):
        location, _lot = prepare_lot(service)
        rule = service.environment.create_rule({
            "version": "env-rule-v2", "temperature_tolerance_c": 4, "temperature_critical_c": 8,
            "humidity_tolerance_percent": 10, "humidity_critical_percent": 20, "max_gap_minutes": 60,
            "freeze_minutes": 180, "retest_minutes": 90, "retest_due_days": 14, "created_by": "管理员",
        })
        assert rule["version"] == "env-rule-v2"
        t0 = datetime(2026, 9, 26, 0, 0, tzinfo=UTC)
        # 偏离 4.5°C：按 v1（容差5）正常，按 v2（容差4）越界
        service.quality.add_reading(reading(location["id"], t0, temperature_c=-22.5, key="v"))
        event = service.environment.list_events()[0][0]
        assert event["rule_version"] == "env-rule-v2"
        assessment = _assess(service, event)
        assert assessment["rule_version"] == "env-rule-v2"
