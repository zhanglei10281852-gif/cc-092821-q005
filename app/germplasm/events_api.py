from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.germplasm.schemas import (
    AssessmentConfirm,
    AssessmentRequest,
    EnvironmentRuleCreate,
    RetestOrderConsume,
)
from app.germplasm.service import GermplasmService

router = APIRouter(prefix="/api/germplasm/environment", tags=["环境异常事件"])


def _service() -> GermplasmService:
    return GermplasmService(get_connection())


@router.get("/events")
def list_events(
    status: str | None = None,
    severity: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("environment.read")
    items, total = _service().environment.list_events(status=status, severity=severity, limit=limit, offset=offset)
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.get("/events/{event_id}")
def event_detail(event_id: int, principal: Principal = Depends(current_principal)) -> dict:
    """一次事故的完整追踪：读数、容器摆放、受影响批次、评估与处置结果。"""
    principal.require("environment.read")
    return _service().environment.event_detail(event_id)


@router.post("/events/{event_id}/assessments", status_code=201)
def request_assessment(event_id: int, data: AssessmentRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("environment.assess")
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection)
        service.environment._require_event(event_id)
        return service.environment.request_assessment(event_id, data.requested_by)


@router.get("/assessments/{assessment_id}")
def assessment_detail(assessment_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("environment.read")
    return _service().environment._require_assessment(assessment_id)


@router.post("/assessments/{assessment_id}/confirm")
def confirm_assessment(assessment_id: int, data: AssessmentConfirm, principal: Principal = Depends(current_principal)) -> dict:
    """人工确认后原子写入质量冻结、提前复检单与审计；重复提交不产生重复处置。"""
    principal.require("environment.confirm")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).environment.confirm_assessment(
            assessment_id, data.actor, decision=data.decision, reason=data.reason
        )


@router.post("/jobs/run-pending")
def run_pending_jobs(principal: Principal = Depends(current_principal), max_jobs: int = Query(default=20, ge=1, le=200)) -> dict:
    """驱动尚未完成的评估作业（也可由 CLI 在进程重启后继续执行）。"""
    principal.require("environment.assess")
    processed = _service().environment.pump_assessment_jobs(max_jobs=max_jobs)
    return {"processed": len(processed), "assessments": processed}


@router.get("/retest-orders/due")
def due_retest_orders(
    due_before: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    principal: Principal = Depends(current_principal),
) -> list[dict]:
    principal.require("environment.read")
    return _service().environment.due_retest_orders(due_before, limit)


@router.post("/retest-orders/{order_id}/consume")
def consume_retest_order(order_id: int, data: RetestOrderConsume, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("viability.write")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).environment.consume_retest_order(order_id, data.test_id, data.actor)


@router.get("/rules")
def list_rules(principal: Principal = Depends(current_principal)) -> list[dict]:
    principal.require("environment.read")
    return _service().environment.list_rules()


@router.post("/rules", status_code=201)
def create_rule(data: EnvironmentRuleCreate, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("environment.confirm")
    with transaction(immediate=True) as connection:
        return GermplasmService(connection).environment.create_rule(data.model_dump(mode="json"))
