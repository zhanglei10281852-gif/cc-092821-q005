from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import get_connection, transaction
from app.germplasm.service import GermplasmService


@pytest.fixture()
def service(client) -> GermplasmService:
    clock = FrozenClock(datetime(2026, 9, 1, 0, 0, tzinfo=UTC))
    return GermplasmService(get_connection(), clock)


def _prepare_location(service: GermplasmService, code: str = "COLD-E1", *, temp: float = -18, humidity: float = 30):
    source = service.accessions.create_source({
        "source_code": f"SRC-{code}", "provider_name": "省站", "country_code": "CN",
    })
    accession = service.accessions.create_accession({
        "accession_no": f"ACC-{code}", "scientific_name": "Oryza sativa", "crop_name": "水稻",
        "source_id": source["id"], "acquisition_type": "采集", "received_on": "2026-08-01",
        "created_by": "登记员",
    })
    service.accessions.transition(accession["id"], {
        "target_status": "accepted", "reason": "齐全", "expected_version": 1, "actor": "审核员",
    })
    location = service.inventory.create_location({
        "location_code": code, "facility": "长期库", "room": "低温一室", "rack": "R1", "shelf": "S1",
        "capacity_grams": 5000, "temperature_c": temp, "humidity_percent": humidity,
    })
    lot = service.inventory.create_lot({
        "lot_no": f"LOT-{code}", "accession_id": accession["id"], "harvest_year": 2025,
        "initial_weight_grams": 500, "created_by": "登记员",
    })
    service.inventory.place_lot({
        "lot_id": lot["id"], "location_id": location["id"], "weight_grams": 500,
        "container_code": f"BOX-{code}", "idempotency_key": f"place-{code}", "actor": "保管员",
    })
    protocol = service.viability.create_protocol({
        "protocol_code": f"PROTO-{code}", "crop_name": "水稻", "sample_size": 100, "replicate_count": 2,
        "temperature_c": 25, "duration_days": 14, "normal_seedling_rule": "根芽完整", "created_by": "负责人",
    })
    return location, lot, accession, protocol


def _add_reading(service, location_id, when: datetime, *, temp=-5.0, humidity=31.0, key=None):
    return service.quality.add_reading({
        "location_id": location_id, "observed_at": when,
        "temperature_c": temp, "humidity_percent": humidity,
        "source_key": key or f"k-{when.isoformat()}-{temp}",
    })


def test_consecutive_excursions_merge_into_one_event(service):
    with transaction(immediate=True):
        location, lot, _, _ = _prepare_location(service)
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        for i in range(4):  # 间隔 30 分钟，小于默认 60 分钟归并间隔
            _add_reading(service, location["id"], base + timedelta(minutes=30 * i))
        # 正常读数打断
        _add_reading(service, location["id"], base + timedelta(hours=3), temp=-18.0, key="normal-1")
        result = service.environment.detect_location(location["id"])
        assert result["created"] == 1
        event = service.environment.event_detail(result["events"][0])
        assert event["started_at"] == base.isoformat(timespec="seconds")
        assert event["ended_at"] == (base + timedelta(minutes=90)).isoformat(timespec="seconds")
        assert event["reading_count"] == 4
        assert event["metrics"] == "temperature"
        assert event["severity"] == "critical"
        assert event["peak_temperature_c"] == -5.0
        assert event["rule"]["rule_code"] == "cold-storage-default"
        assert event["rule"]["version"] == 1


def test_out_of_order_backfill_deterministically_revises_window(service):
    with transaction(immediate=True):
        location, _, _, _ = _prepare_location(service)
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        _add_reading(service, location["id"], base, key="a")
        _add_reading(service, location["id"], base + timedelta(minutes=30), key="b")
        event_id = service.environment.detect_location(location["id"])["events"][0]

        # 迟到补传：更早的起点、更晚的终点，以及一个改变峰值的中间点
        _add_reading(service, location["id"], base - timedelta(minutes=20), key="late-head")
        _add_reading(service, location["id"], base + timedelta(minutes=10), temp=8.0, key="late-peak")
        _add_reading(service, location["id"], base + timedelta(minutes=60), key="late-tail")
        result = service.environment.detect_location(location["id"])
        assert result["created"] == 0
        assert result["revised"] == 1
        event = service.environment.event_detail(event_id)
        assert event["started_at"] == (base - timedelta(minutes=20)).isoformat(timespec="seconds")
        assert event["ended_at"] == (base + timedelta(minutes=60)).isoformat(timespec="seconds")
        assert event["peak_temperature_c"] == 8.0
        assert event["reading_count"] == 5
        assert event["window_revised"] == 1
        assert event["version"] == 2


def test_backfill_merges_separate_events(service):
    with transaction(immediate=True):
        location, _, _, _ = _prepare_location(service)
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        _add_reading(service, location["id"], base, key="run1")
        _add_reading(service, location["id"], base + timedelta(hours=2), key="run2")
        first = service.environment.detect_location(location["id"])
        assert first["created"] == 2
        # 补传中间读数，把两段连成一段（相邻间隔恰好 60 分钟，不超过归并间隔）
        _add_reading(service, location["id"], base + timedelta(hours=1), key="bridge")
        result = service.environment.detect_location(location["id"])
        assert result["created"] == 0
        assert result["merged"] == 1
        events, total = service.environment.list_events(location_id=location["id"])
        assert total == 2
        statuses = sorted(event["status"] for event in events)
        assert statuses == ["merged", "open"]
        open_event = next(event for event in events if event["status"] == "open")
        detail = service.environment.event_detail(open_event["id"])
        assert detail["reading_count"] == 3
        merged = next(event for event in events if event["status"] == "merged")
        assert merged["merged_into_event_id"] == open_event["id"]

        # 合并完成后再次评估必须零写入、零新增
        again = service.environment.detect_location(location["id"])
        assert again["created"] == 0 and again["revised"] == 0 and again["merged"] == 0
        assert service.environment.event_detail(open_event["id"])["version"] == detail["version"]


def test_repeated_evaluation_is_a_noop(service):
    with transaction(immediate=True):
        location, _, _, _ = _prepare_location(service)
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        _add_reading(service, location["id"], base)
        first = service.environment.detect_location(location["id"])
        event = service.environment.event_detail(first["events"][0])
        version_before = event["version"]
        again = service.environment.detect_location(location["id"])
        assert again["created"] == 0 and again["revised"] == 0 and again["merged"] == 0
        reloaded = service.environment.event_detail(event["id"])
        assert reloaded["version"] == version_before


def test_identical_reading_replay_does_not_duplicate(service):
    with transaction(immediate=True):
        location, _, _, _ = _prepare_location(service)
        payload = {
            "location_id": location["id"], "observed_at": datetime(2026, 9, 2, 0, 0, tzinfo=UTC),
            "temperature_c": -5, "humidity_percent": 31, "source_key": "same-sensor-key",
        }
        first = service.quality.add_reading(dict(payload))
        second = service.quality.add_reading(dict(payload))
        assert first["replayed"] is False and second["replayed"] is True
        result = service.environment.detect_location(location["id"])
        assert result["created"] == 1
        event = service.environment.event_detail(result["events"][0])
        assert event["reading_count"] == 1


def test_impact_assessment_uses_placement_history_and_thresholds(service):
    with transaction(immediate=True):
        location, lot, _, _ = _prepare_location(service)
        other = service.inventory.create_location({
            "location_code": "COLD-OTHER", "facility": "长期库", "room": "低温二室", "rack": "R2", "shelf": "S1",
            "capacity_grams": 5000, "temperature_c": -18, "humidity_percent": 30,
        })
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        # 暴露 30 分钟：warning 级别，retest=60，建议 monitor
        _add_reading(service, location["id"], base, temp=-12.0, key="short-1")  # 偏离 6，warning
        _add_reading(service, location["id"], base + timedelta(minutes=30), temp=-12.0, key="short-2")
        event_id = service.environment.detect_location(location["id"])["events"][0]
        event = service.environment.event_detail(event_id)
        assert event["severity"] == "warning"
        impact = event["impacts"][0]
        assert impact["lot_id"] == lot["id"]
        assert impact["container_code"] == f"BOX-{location['location_code']}"
        assert impact["exposure_minutes"] == 30
        assert impact["recommendation"] == "monitor"

        # 容器在异常结束后、下一窗口前移出本库位；之后的新异常窗口不应再把该批次计入
        service.environment.clock.advance(days=1, hours=2)
        service.inventory.move_placement(impact["placement_id"], {
            "target_location_id": other["id"], "expected_version": 1,
            "idempotency_key": "move-away-1", "actor": "保管员", "reason": "转移至二室",
        })
        later = datetime(2026, 9, 5, 0, 0, tzinfo=UTC)
        _add_reading(service, location["id"], later, key="later-1")
        _add_reading(service, location["id"], later + timedelta(minutes=30), key="later-2")
        result = service.environment.detect_location(location["id"])
        later_event = next(
            item for item in (service.environment.event_detail(ident) for ident in result["events"])
            if item["started_at"] == later.isoformat(timespec="seconds")
        )
        assert later_event["impacts"] == []


def test_confirm_hold_writes_hold_action_and_audit_atomically(service):
    with transaction(immediate=True):
        location, lot, _, _ = _prepare_location(service)
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        for i in range(5):  # 120 分钟，critical → hold
            _add_reading(service, location["id"], base + timedelta(minutes=30 * i))
        event_id = service.environment.detect_location(location["id"])["events"][0]
        event = service.environment.event_detail(event_id)
        impact = event["impacts"][0]
        assert impact["recommendation"] == "hold"

        confirmed = service.environment.confirm_event(event_id, {
            "expected_version": 1, "actor": "值班长",
            "decisions": [{"event_lot_id": impact["id"], "action": "hold", "reason": "按建议冻结"}],
        })
        assert confirmed["status"] == "confirmed"
        assert confirmed["impacts"][0]["disposition_status"] == "confirmed"
        action = confirmed["impacts"][0]["actions"][0]
        assert action["action_type"] == "hold" and action["hold_id"]
        lot_detail = service.repository.lot_detail(lot["id"])
        assert lot_detail["status"] == "held"
        assert any(hold["id"] == action["hold_id"] for hold in lot_detail["holds"])

        audits = get_connection().execute(
            "SELECT action,metadata_json FROM audit_events WHERE resource_type='environment_event' "
            "AND action='environment.event.hold'"
        ).fetchall()
        assert len(audits) == 1
        assert "EV-" in str(get_connection().execute(
            "SELECT resource_id FROM audit_events WHERE action='environment.event.hold'"
        ).fetchone()[0])


def test_confirm_early_retest_creates_review_test(service):
    with transaction(immediate=True):
        location, lot, _, _ = _prepare_location(service)
        rule = service.environment.list_rules(active_only=True)[0]
        assert rule["retest_minutes"] == 60
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        # 60 分钟、warning 级别：达到提前复检阈值 60，未达 warning 冻结阈值 240
        for i in range(3):
            _add_reading(service, location["id"], base + timedelta(minutes=30 * i), temp=-12.0, key=f"w-{i}")
        event_id = service.environment.detect_location(location["id"])["events"][0]
        event = service.environment.event_detail(event_id)
        assert event["severity"] == "warning"
        impact = event["impacts"][0]
        assert impact["recommendation"] == "early_retest"
        assert impact["exposure_minutes"] == 60
        confirmed = service.environment.confirm_event(event_id, {
            "expected_version": 1, "actor": "值班长",
            "decisions": [{"event_lot_id": impact["id"], "action": "early_retest", "sampled_grams": 5}],
        })
        action = confirmed["impacts"][0]["actions"][0]
        test = service.repository.require_test(action["test_id"])
        assert test["test_type"] == "异常复核"
        assert test["status"] == "scheduled"
        assert test["lot_id"] == lot["id"]
        assert service.repository.require_lot(lot["id"])["status"] == "stored"


def test_duplicate_confirmation_and_late_readings_do_not_reprocess(service):
    with transaction(immediate=True):
        location, _, _, _ = _prepare_location(service)
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        for i in range(5):
            _add_reading(service, location["id"], base + timedelta(minutes=30 * i), key=f"p1-{i}")
        event_id = service.environment.detect_location(location["id"])["events"][0]
        impact = service.environment.event_detail(event_id)["impacts"][0]
        confirmed = service.environment.confirm_event(event_id, {
            "expected_version": 1, "actor": "值班长",
            "decisions": [{"event_lot_id": impact["id"], "action": "hold"}],
        })
        holds_before = len(get_connection().execute(
            "SELECT id FROM lot_holds WHERE lot_id=?", (impact["lot_id"],)
        ).fetchall())

        # 重复提交同一处置
        service.environment.confirm_event(event_id, {
            "expected_version": confirmed["version"], "actor": "值班长",
            "decisions": [{"event_lot_id": impact["id"], "action": "hold"}],
        })
        holds_after = len(get_connection().execute(
            "SELECT id FROM lot_holds WHERE lot_id=?", (impact["lot_id"],)
        ).fetchall())
        assert holds_after == holds_before

        # 迟到读数挂到已确认事件上，但不产生新事件、不改写窗口
        _add_reading(service, location["id"], base - timedelta(minutes=10), key="after-close")
        result = service.environment.detect_location(location["id"])
        assert result["created"] == 0 and result["revised"] == 0
        detail = service.environment.event_detail(event_id)
        assert detail["status"] == "confirmed"
        assert detail["started_at"] == base.isoformat(timespec="seconds")
        assert len(get_connection().execute(
            "SELECT 1 FROM environment_event_readings WHERE event_id=? AND reading_id="
            "(SELECT id FROM environment_readings WHERE source_key='after-close')",
            (event_id,),
        ).fetchall()) == 1


def test_manual_override_is_recorded(service):
    with transaction(immediate=True):
        location, _, _, _ = _prepare_location(service)
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        for i in range(5):
            _add_reading(service, location["id"], base + timedelta(minutes=30 * i), key=f"o-{i}")
        event_id = service.environment.detect_location(location["id"])["events"][0]
        impact = service.environment.event_detail(event_id)["impacts"][0]
        assert impact["recommendation"] == "hold"
        # 人工改为提前复检
        confirmed = service.environment.confirm_event(event_id, {
            "expected_version": 1, "actor": "值班长",
            "decisions": [{"event_lot_id": impact["id"], "action": "early_retest", "reason": "种子包隔热良好"}],
        })
        row = confirmed["impacts"][0]
        assert row["override_action"] == "early_retest"
        assert row["override_by"] == "值班长"
        overrides = get_connection().execute(
            "SELECT COUNT(*) FROM audit_events WHERE action='environment.event.override'"
        ).fetchone()[0]
        assert overrides == 1


def test_stale_version_confirm_is_rejected(service):
    with transaction(immediate=True):
        location, _, _, _ = _prepare_location(service)
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        _add_reading(service, location["id"], base)
        event_id = service.environment.detect_location(location["id"])["events"][0]
        with pytest.raises(ConflictError):
            service.environment.confirm_event(event_id, {
                "expected_version": 99, "actor": "值班长",
                "decisions": [{"event_lot_id": 1, "action": "none"}],
            })


def test_confirmation_is_atomic_when_one_decision_fails(service):
    with transaction(immediate=True):
        location, lot, _, _ = _prepare_location(service)
        # 第二个批次，没有水稻以外的问题：让它的 early_retest 失败——删光规程
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        for i in range(5):
            _add_reading(service, location["id"], base + timedelta(minutes=30 * i), key=f"atom-{i}")
        event_id = service.environment.detect_location(location["id"])["events"][0]
        impact = service.environment.event_detail(event_id)["impacts"][0]
        get_connection().execute("UPDATE viability_protocols SET active=0")
        with pytest.raises(ConflictError):
            service.environment.confirm_event(event_id, {
                "expected_version": 1, "actor": "值班长",
                "decisions": [{"event_lot_id": impact["id"], "action": "early_retest"}],
            })
        # 事务已回滚（savepoint 外层是测试事务；此处验证函数自身不留半截数据）
        assert get_connection().execute(
            "SELECT COUNT(*) FROM environment_event_actions WHERE event_id=?", (event_id,)
        ).fetchone()[0] == 0
        assert service.repository.require_lot(lot["id"])["status"] == "stored"


def test_rule_version_is_kept_on_historical_events(service):
    with transaction(immediate=True):
        location, _, _, _ = _prepare_location(service)
        base = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
        _add_reading(service, location["id"], base, temp=-7.0, key="v1-reading")  # 偏离 11，v1 下 critical
        old_event_id = service.environment.detect_location(location["id"])["events"][0]
        old = service.environment.event_detail(old_event_id)
        assert old["rule"]["version"] == 1
        assert old["severity"] == "critical"

        # 发布更严的 v2：温度阈值 12，偏离 8 不再越界
        service.environment.clock.advance(days=30)
        service.environment.create_rule({
            "rule_code": "cold-storage-default", "temperature_delta_c": 12, "humidity_delta_percent": 15,
            "critical_temperature_delta_c": 15, "critical_humidity_delta_percent": 30,
            "max_gap_minutes": 60, "hold_minutes": 120, "critical_hold_minutes": 60,
            "retest_minutes": 360, "retest_due_days": 30, "created_by": "质量主管",
        })
        result = service.environment.detect_location(location["id"])
        # v1 的历史事件原样保留；v2 下该读数不构成事件
        old_reloaded = service.environment.event_detail(old_event_id)
        assert old_reloaded["rule"]["version"] == 1
        assert old_reloaded["severity"] == "critical"
        assert result["created"] == 0


def test_detection_jobs_dedupe_and_run_after_restart(client, admin, monkeypatch):
    # 读数入库自动入队检测作业；补传洪峰期间同库位只保留一个待执行作业
    headers = admin["headers"]
    location_id = None
    with transaction(immediate=True) as connection:
        clock = FrozenClock(datetime(2026, 9, 1, 0, 0, tzinfo=UTC))
        service = GermplasmService(connection, clock)
        location, _, _, _ = _prepare_location(service)
        location_id = location["id"]

    from app.database import close_connection

    for i, stamp in enumerate(["2026-09-02T00:00:00Z", "2026-09-02T00:30:00Z", "2026-09-02T01:00:00Z"]):
        response = client.post("/api/germplasm/readings", headers=headers, json={
            "location_id": location_id,
            "observed_at": stamp,
            "temperature_c": -5, "humidity_percent": 31, "source_key": f"http-reading-{i}",
        })
        assert response.status_code == 201, response.text

    pending = get_connection().execute(
        "SELECT COUNT(*) FROM background_jobs WHERE job_type='environment.detect' AND status IN ('pending','running')"
    ).fetchone()[0]
    assert pending == 1

    # 模拟进程崩溃：作业被领取但锁过期，重启后 run-pending 依靠租约回收继续完成
    get_connection().execute(
        "UPDATE background_jobs SET status='running',locked_by='dead-worker',"
        "locked_at='2026-09-01T00:00:00+00:00',attempts=1"
    )
    get_connection().commit()
    run = client.post("/api/germplasm/environment/jobs/run-pending?limit=5", headers=headers)
    assert run.status_code == 200, run.text
    assert run.json()["count"] == 1
    assert run.json()["processed"][0]["status"] == "completed"

    events = client.get(
        f"/api/germplasm/environment/events?location_id={location_id}", headers=headers
    ).json()
    assert events["total"] == 1
    event_id = events["items"][0]["id"]

    # 通过 API 从一次事故追到读数、容器、批次与处置结果
    detail = client.get(f"/api/germplasm/environment/events/{event_id}", headers=headers)
    assert detail.status_code == 200
    body = detail.json()
    assert len(body["readings"]) == 3
    impact = body["impacts"][0]
    assert impact["container_code"].startswith("BOX-")
    assert impact["lot_no"].startswith("LOT-")
    assert impact["accession_no"].startswith("ACC-")

    confirmed = client.post(f"/api/germplasm/environment/events/{event_id}/confirm", headers=headers, json={
        "expected_version": body["version"], "actor": "值班长",
        "decisions": [{"event_lot_id": impact["id"], "action": "hold"}],
    })
    assert confirmed.status_code == 200, confirmed.text
    action = confirmed.json()["impacts"][0]["actions"][0]
    lot = client.get(f"/api/germplasm/lots/{impact['lot_id']}", headers=headers).json()
    assert lot["status"] == "held"
    assert any(hold["id"] == action["hold_id"] for hold in lot["holds"])


def test_environment_endpoints_require_permissions(client, admin):
    headers = admin["headers"]
    assert client.get("/api/germplasm/environment/events").status_code == 401
    listing = client.get("/api/germplasm/environment/events", headers=headers)
    assert listing.status_code == 200
