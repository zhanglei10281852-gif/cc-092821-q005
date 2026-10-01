from __future__ import annotations

from datetime import UTC, datetime, timedelta


def _seed_event(client, headers) -> int:
    location = client.post("/api/germplasm/locations", headers=headers, json={
        "location_code": "ENV-API-1", "facility": "长期库", "room": "低温一室", "rack": "R1", "shelf": "S1",
        "capacity_grams": 1000, "temperature_c": -18, "humidity_percent": 30,
    }).json()
    source = client.post("/api/germplasm/sources", headers=headers, json={
        "source_code": "ENV-API-S1", "provider_name": "省级采集队", "country_code": "CN",
        "locality": "试验站",
    }).json()
    accession = client.post("/api/germplasm/accessions", headers=headers, json={
        "accession_no": "ENV-API-A1", "scientific_name": "Oryza sativa", "crop_name": "水稻",
        "source_id": source["id"], "acquisition_type": "采集",
        "received_on": "2026-09-01", "created_by": "登记员",
    }).json()
    client.post(f"/api/germplasm/accessions/{accession['id']}/transition", headers=headers, json={
        "target_status": "accepted", "reason": "齐全", "expected_version": 1, "actor": "审核员",
    })
    lot = client.post("/api/germplasm/lots", headers=headers, json={
        "lot_no": "ENV-API-L1", "accession_id": accession["id"], "harvest_year": 2025,
        "initial_weight_grams": 500, "created_by": "登记员",
    }).json()
    client.post("/api/germplasm/placements", headers=headers, json={
        "lot_id": lot["id"], "location_id": location["id"], "weight_grams": 500,
        "container_code": "ENV-API-B1", "idempotency_key": "env-api-place-1", "actor": "保管员",
    })
    # 读数时间安排在摆放之后（API 使用真实系统时钟）
    t0 = datetime.now(UTC) + timedelta(minutes=5)
    for minutes in (0, 60, 120, 180, 240):
        response = client.post("/api/germplasm/readings", headers=headers, json={
            "location_id": location["id"],
            "observed_at": (t0 + timedelta(minutes=minutes)).isoformat(),
            "temperature_c": -5, "humidity_percent": 31,
            "source_key": f"env-api-{minutes}",
        })
        assert response.status_code == 201, response.text
    client.post("/api/germplasm/readings", headers=headers, json={
        "location_id": location["id"], "observed_at": (t0 + timedelta(minutes=300)).isoformat(),
        "temperature_c": -18, "humidity_percent": 31, "source_key": "env-api-normal",
    })
    events = client.get("/api/germplasm/environment/events", headers=headers).json()
    assert events["total"] == 1
    return events["items"][0]["id"]


def test_environment_incident_to_disposition_api(client, admin):
    headers = admin["headers"]
    event_id = _seed_event(client, headers)

    detail = client.get(f"/api/germplasm/environment/events/{event_id}", headers=headers)
    assert detail.status_code == 200
    body = detail.json()
    assert len(body["readings"]) == 5
    assert body["affected_lots"][0]["lot_no"] == "ENV-API-L1"

    created = client.post(f"/api/germplasm/environment/events/{event_id}/assessments", headers=headers,
                          json={"requested_by": "值班员"})
    assert created.status_code == 201, created.text
    assessment_id = created.json()["id"]
    assert created.json()["status"] == "pending"

    run = client.post("/api/germplasm/environment/jobs/run-pending", headers=headers)
    assert run.status_code == 200, run.text
    assert run.json()["processed"] == 1

    pending = client.get(f"/api/germplasm/environment/assessments/{assessment_id}", headers=headers).json()
    assert pending["status"] == "awaiting_confirmation"
    assert pending["recommendation"] == "freeze"

    confirmed = client.post(f"/api/germplasm/environment/assessments/{assessment_id}/confirm", headers=headers,
                            json={"actor": "审核员"})
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["applied_result"]["holds_created"]

    # 再次驱动作业不应重复处理，再次确认不产生重复处置
    assert client.post("/api/germplasm/environment/jobs/run-pending", headers=headers).json()["processed"] == 0
    repeat = client.post(f"/api/germplasm/environment/assessments/{assessment_id}/confirm", headers=headers,
                         json={"actor": "审核员"})
    assert repeat.status_code == 200

    due = client.get("/api/germplasm/environment/retest-orders/due?due_before=2026-12-31", headers=headers).json()
    assert len(due) == 1 and due[0]["lot_no"] == "ENV-API-L1"


def test_environment_endpoints_require_authentication(client):
    assert client.get("/api/germplasm/environment/events").status_code == 401


def test_rule_management_api(client, admin):
    headers = admin["headers"]
    rules = client.get("/api/germplasm/environment/rules", headers=headers)
    assert rules.status_code == 200
    assert any(rule["version"] == "env-rule-v1" for rule in rules.json())

    created = client.post("/api/germplasm/environment/rules", headers=headers, json={
        "version": "env-rule-v2", "temperature_tolerance_c": 4, "temperature_critical_c": 8,
        "humidity_tolerance_percent": 10, "humidity_critical_percent": 20, "max_gap_minutes": 90,
        "freeze_minutes": 180, "retest_minutes": 60, "retest_due_days": 21, "created_by": "管理员",
    })
    assert created.status_code == 201, created.text
    assert created.json()["active"] == 1

    # 阈值矛盾被拒绝
    invalid = client.post("/api/germplasm/environment/rules", headers=headers, json={
        "version": "env-rule-bad", "temperature_tolerance_c": 9, "temperature_critical_c": 8,
        "humidity_tolerance_percent": 10, "humidity_critical_percent": 20, "max_gap_minutes": 90,
        "freeze_minutes": 180, "retest_minutes": 60, "retest_due_days": 21, "created_by": "管理员",
    })
    assert invalid.status_code == 422
