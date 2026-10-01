from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.germplasm.schemas import (
    AccessionCreate,
    AccessionPatch,
    AccessionTransition,
    AlertDecision,
    CountCreate,
    DistributionCreate,
    DistributionDecision,
    EventConfirm,
    EventDismiss,
    EventRuleCreate,
    HoldCreate,
    HoldRelease,
    LocationCreate,
    LotCreate,
    MovePlacement,
    PlacementCreate,
    PolicyCreate,
    ProtocolCreate,
    ReadingCreate,
    SourceCreate,
    TestComplete,
    TestCreate,
    TestInvalidate,
    TestStart,
    WithdrawalCreate,
)
from app.germplasm.service import GermplasmService


router = APIRouter(prefix="/api/germplasm", tags=["种质资源"])


def _service() -> GermplasmService:
    return GermplasmService(get_connection())


@router.get("/dashboard")
def dashboard(principal: Principal = Depends(current_principal)) -> dict:
    principal.require("accessions.read")
    return _service().dashboard()


@router.post("/sources", status_code=201)
def create_source(data: SourceCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("accessions.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).accessions.create_source(data.model_dump(mode="json"))


@router.put("/sources/{source_id}/restrictions")
def update_source_restrictions(
    source_id: int,
    restrictions: dict[str, Any],
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("accessions.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).accessions.update_source_restrictions(source_id, restrictions)


@router.post("/accessions", status_code=201)
def create_accession(data: AccessionCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("accessions.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).accessions.create_accession(data.model_dump(mode="json"))


@router.get("/accessions")
def list_accessions(
    status: str | None = None,
    crop: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("accessions.read")
    items, total = _service().repository.list_accessions(status=status, crop=crop, limit=limit, offset=offset)
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.get("/accessions/{accession_id}")
def accession_detail(accession_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("accessions.read")
    return _service().repository.accession_detail(accession_id)


@router.patch("/accessions/{accession_id}")
def update_accession(
    accession_id: int,
    data: AccessionPatch,
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("accessions.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).accessions.update_accession(
            accession_id, data.model_dump(mode="json", exclude_unset=True)
        )


@router.post("/accessions/{accession_id}/transition")
def transition_accession(
    accession_id: int,
    data: AccessionTransition,
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("accessions.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).accessions.transition(accession_id, data.model_dump(mode="json"))


@router.get("/accessions/{accession_id}/restrictions")
def accession_restrictions(accession_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("accessions.read")
    return _service().accessions.restrictions_for(accession_id)


@router.post("/locations", status_code=201)
def create_location(data: LocationCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("inventory.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).inventory.create_location(data.model_dump(mode="json"))


@router.get("/locations/{location_id}")
def location_detail(location_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("inventory.read")
    return _service().repository.location_detail(location_id)


@router.post("/locations/{location_id}/status/{status}")
def change_location_status(
    location_id: int,
    status: str,
    expected_version: int,
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("inventory.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).inventory.change_location_status(location_id, status, expected_version)


@router.post("/lots", status_code=201)
def create_lot(data: LotCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("inventory.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).inventory.create_lot(data.model_dump(mode="json"))


@router.get("/lots/{lot_id}")
def lot_detail(lot_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("inventory.read")
    return _service().repository.lot_detail(lot_id)


@router.get("/lots/{lot_id}/reconcile")
def reconcile_lot(lot_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("inventory.read")
    return _service().inventory.reconcile(lot_id)


@router.post("/placements", status_code=201)
def place_lot(data: PlacementCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("inventory.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).inventory.place_lot(data.model_dump(mode="json"))


@router.post("/placements/{placement_id}/move")
def move_placement(
    placement_id: int,
    data: MovePlacement,
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("inventory.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).inventory.move_placement(placement_id, data.model_dump(mode="json"))


@router.post("/withdrawals", status_code=201)
def withdraw(data: WithdrawalCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("inventory.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).inventory.withdraw(data.model_dump(mode="json"))


@router.post("/holds", status_code=201)
def impose_hold(data: HoldCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("quality.review")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).inventory.impose_hold(data.model_dump(mode="json"))


@router.post("/holds/{hold_id}/release")
def release_hold(
    hold_id: int,
    data: HoldRelease,
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("quality.review")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).inventory.release_hold(hold_id, data.actor, data.reason)


@router.post("/protocols", status_code=201)
def create_protocol(data: ProtocolCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("viability.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).viability.create_protocol(data.model_dump(mode="json"))


@router.post("/tests", status_code=201)
def schedule_test(data: TestCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("viability.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).viability.schedule_test(data.model_dump(mode="json"))


@router.get("/tests/{test_id}")
def test_detail(test_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("viability.read")
    return _service().repository.test_detail(test_id)


@router.post("/tests/{test_id}/start")
def start_test(test_id: int, data: TestStart, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("viability.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).viability.start_test(test_id, data.model_dump(mode="json"))


@router.post("/tests/{test_id}/counts", status_code=201)
def add_count(test_id: int, data: CountCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("viability.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).viability.add_count(test_id, data.model_dump(mode="json"))


@router.put("/counts/{count_id}")
def replace_count(count_id: int, data: CountCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("viability.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).viability.replace_count(count_id, data.model_dump(mode="json"))


@router.post("/tests/{test_id}/complete")
def complete_test(test_id: int, data: TestComplete, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("viability.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).viability.complete_test(test_id, data.model_dump(mode="json"))


@router.post("/tests/{test_id}/invalidate")
def invalidate_test(test_id: int, data: TestInvalidate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("quality.review")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).viability.invalidate_test(test_id, data.model_dump(mode="json"))


@router.post("/policies", status_code=201)
def create_policy(data: PolicyCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("quality.review")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).viability.create_policy(data.model_dump(mode="json"))


@router.get("/retest-schedules/due")
def due_schedules(
    before: date,
    limit: int = Query(default=100, ge=1, le=500),
    principal: Principal = Depends(current_principal),
) -> list[dict]:
    principal.require("viability.read")
    return _service().viability.due_schedules(before, limit)


@router.post("/readings", status_code=201)
def add_reading(data: ReadingCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("inventory.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).quality.add_reading(data.model_dump(mode="python"))


@router.post("/readings/import")
def import_readings(rows: list[ReadingCreate], principal: Principal = Depends(current_principal)) -> dict:
    principal.require("inventory.write")
    payload = [item.model_dump(mode="python") for item in rows]
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).quality.import_readings(payload)


@router.get("/locations/{location_id}/environment")
def environment_summary(
    location_id: int,
    hours: int = Query(default=24, ge=1, le=24 * 90),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("inventory.read")
    return _service().quality.location_summary(location_id, hours)


@router.get("/alerts")
def open_alerts(severity: str | None = None, principal: Principal = Depends(current_principal)) -> list[dict]:
    principal.require("quality.review")
    return _service().quality.open_alerts(severity)


@router.post("/alerts/{alert_id}/decision")
def decide_alert(alert_id: int, data: AlertDecision, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("quality.review")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).quality.decide_alert(alert_id, data.model_dump(mode="json"))


@router.get("/environment/rules")
def list_event_rules(principal: Principal = Depends(current_principal)) -> list[dict]:
    principal.require("environment.read")
    return _service().environment.list_rules()


@router.post("/environment/rules", status_code=201)
def create_event_rule(data: EventRuleCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("environment.review")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).environment.create_rule(data.model_dump(mode="json"))


@router.get("/environment/events")
def list_environment_events(
    status: str | None = Query(default=None, pattern="^(open|confirmed|dismissed|merged)$"),
    location_id: int | None = Query(default=None, gt=0),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("environment.read")
    items, total = _service().environment.list_events(
        status=status, location_id=location_id, limit=limit, offset=offset
    )
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.get("/environment/events/{event_id}")
def environment_event_detail(event_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("environment.read")
    return _service().environment.event_detail(event_id)


@router.post("/locations/{location_id}/environment/detect")
def detect_environment_events(location_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("environment.review")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).environment.detect_location(location_id)


@router.post("/environment/events/{event_id}/confirm")
def confirm_environment_event(
    event_id: int, data: EventConfirm, principal: Principal = Depends(current_principal)
) -> dict:
    principal.require("environment.review")
    payload = data.model_dump(mode="json")
    payload["actor_user_id"] = principal.user_id
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).environment.confirm_event(event_id, payload)


@router.post("/environment/events/{event_id}/dismiss")
def dismiss_environment_event(
    event_id: int, data: EventDismiss, principal: Principal = Depends(current_principal)
) -> dict:
    principal.require("environment.review")
    payload = data.model_dump(mode="json")
    payload["actor_user_id"] = principal.user_id
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).environment.dismiss_event(event_id, payload)


@router.post("/environment/jobs/run-pending")
def run_pending_environment_jobs(
    limit: int = Query(default=10, ge=1, le=100),
    principal: Principal = Depends(current_principal),
) -> dict:
    """驱动后台检测作业：每个作业独立事务提交，进程重启后继续领取未完成作业。"""
    principal.require("jobs.run")
    from app.core.errors import DomainError
    from app.services.jobs import JobService

    from app.germplasm.environment import JOB_TYPE_DETECT

    results: list[dict] = []
    for _ in range(limit):
        with transaction(immediate=True) as connection:
            jobs = JobService(connection)
            job = jobs.claim(principal.username, lease_seconds=120, job_type=JOB_TYPE_DETECT)
            if job is None:
                break
            try:
                result = GermplasmService(connection).environment.execute_job(job, principal.username)
            except DomainError as exc:
                jobs.fail(int(job["id"]), principal.username, f"{exc.code}: {exc.message}", retry_seconds=300)
                results.append({"job_id": int(job["id"]), "status": "retry", "error": exc.message})
                continue
        results.append({"job_id": int(job["id"]), "status": "completed", "result": result})
    return {"processed": results, "count": len(results)}


@router.post("/distributions", status_code=201)
def create_distribution(data: DistributionCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("accessions.read")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).distribution.create_request(data.model_dump(mode="json"))


@router.post("/distributions/{request_id}/submit")
def submit_distribution(
    request_id: int,
    expected_version: int,
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("accessions.read")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).distribution.submit(request_id, expected_version)


@router.post("/distributions/{request_id}/decision")
def decide_distribution(
    request_id: int,
    data: DistributionDecision,
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("distribution.approve")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).distribution.decide(request_id, data.model_dump(mode="json"))


@router.get("/distributions/{request_id}")
def distribution_detail(request_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("accessions.read")
    return _service().repository.distribution_detail(request_id)
