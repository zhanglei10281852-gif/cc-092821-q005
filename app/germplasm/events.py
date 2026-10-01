"""环境异常事件归并、影响评估与人工处置。

设计要点：
- 事件归并是对某库位全部读数的*确定性重算*：按规则版本把连续越界读数切段，
  再按“读数集合重叠”把切段对应到既有事件，迟到补传只会修订/合并同一事件，
  相同输入重复执行不产生额外事件。
- 影响评估以后台作业方式运行（pending → awaiting_confirmation），进程重启后
  未完成作业仍可被领取继续。
- 人工确认后的冻结、提前复检单与审计在同一个即时事务内原子写入，且可重复提交。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.germplasm.repository import GermplasmRepository, record, records

ASSESSMENT_JOB_TYPE = "environment.assess"
METRIC_LABELS = {"temperature": "温度", "humidity": "湿度"}


def _rule_defaults() -> dict[str, Any]:
    return {
        "version": "env-rule-v1",
        "temperature_tolerance_c": 5.0,
        "temperature_critical_c": 10.0,
        "humidity_tolerance_percent": 15.0,
        "humidity_critical_percent": 30.0,
        "max_gap_minutes": 120,
        "freeze_minutes": 240,
        "retest_minutes": 120,
        "retest_due_days": 30,
    }


def classify_reading(reading: dict[str, Any], location: dict[str, Any], rule: dict[str, Any]) -> set[str]:
    """返回读数越界的度量集合；空集合表示该读数处于正常范围。"""
    metrics: set[str] = set()
    if abs(float(reading["temperature_c"]) - float(location["temperature_c"])) > float(rule["temperature_tolerance_c"]):
        metrics.add("temperature")
    if abs(float(reading["humidity_percent"]) - float(location["humidity_percent"])) > float(rule["humidity_tolerance_percent"]):
        metrics.add("humidity")
    return metrics


def segment_readings(
    readings: list[dict[str, Any]], location: dict[str, Any], rule: dict[str, Any], now_iso: str
) -> list[dict[str, Any]]:
    """把按 observed_at 排序的读数切成连续越界段（纯函数，便于确定性重算与测试）。

    窗口在三种情况下结束：出现紧邻正常读数（结束时刻取该读数时间）、与下一条越界
    读数间隔超过最大允许间隔（结束时刻取最后一条越界读数时间）、或数据静默超过最大
    间隔（结束时刻取当前时间）。暴露时长只统计首末越界读数之间的分钟数。
    """
    max_gap = timedelta(minutes=int(rule["max_gap_minutes"]))
    segments: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for reading in readings:
        violating = bool(classify_reading(reading, location, rule))
        if violating and current and previous is not None:
            if from_storage(reading["observed_at"]) - from_storage(previous["observed_at"]) > max_gap:
                segments.append(_build_segment(current, location, rule, previous["observed_at"]))
                current = []
        if not violating and current:
            segments.append(_build_segment(current, location, rule, reading["observed_at"]))
            current = []
        if violating:
            current.append(reading)
        previous = reading
    if current:
        last_observed = from_storage(current[-1]["observed_at"])
        ended_at = now_iso if from_storage(now_iso) - last_observed > max_gap else None
        segments.append(_build_segment(current, location, rule, ended_at))
    return segments


def _build_segment(
    members: list[dict[str, Any]],
    location: dict[str, Any],
    rule: dict[str, Any],
    ended_at: str | None,
) -> dict[str, Any]:
    first = members[0]
    last = members[-1]
    started = from_storage(first["observed_at"])
    last_violating_at = from_storage(last["observed_at"])
    duration = int((last_violating_at - started).total_seconds() // 60)

    metrics: set[str] = set()
    max_temperature = min_temperature = max_humidity = min_humidity = None
    temperature_peak = humidity_peak = None
    temperature_peak_at = humidity_peak_at = None
    temperature_score = humidity_score = 0.0
    for item in members:
        item_metrics = classify_reading(item, location, rule)
        metrics |= item_metrics
        temperature = float(item["temperature_c"])
        humidity = float(item["humidity_percent"])
        max_temperature = temperature if max_temperature is None else max(max_temperature, temperature)
        min_temperature = temperature if min_temperature is None else min(min_temperature, temperature)
        max_humidity = humidity if max_humidity is None else max(max_humidity, humidity)
        min_humidity = humidity if min_humidity is None else min(min_humidity, humidity)
        if "temperature" in item_metrics:
            score = abs(temperature - float(location["temperature_c"]))
            if score > temperature_score:
                temperature_score, temperature_peak, temperature_peak_at = score, temperature, item["observed_at"]
        if "humidity" in item_metrics:
            score = abs(humidity - float(location["humidity_percent"]))
            if score > humidity_score:
                humidity_score, humidity_peak, humidity_peak_at = score, humidity, item["observed_at"]

    critical = (
        temperature_score > float(rule["temperature_critical_c"])
        or humidity_score > float(rule["humidity_critical_percent"])
    )
    peak_score = max(temperature_score, humidity_score)
    peak_at = temperature_peak_at if temperature_score >= humidity_score and temperature_peak_at else humidity_peak_at
    return {
        "reading_ids": [int(item["id"]) for item in members],
        "first_reading_id": int(first["id"]),
        "last_reading_id": int(last["id"]),
        "started_at": first["observed_at"],
        "ended_at": ended_at,
        "duration_minutes": duration,
        "peak_at": peak_at,
        "max_temperature_c": max_temperature,
        "min_temperature_c": min_temperature,
        "max_humidity_percent": max_humidity,
        "min_humidity_percent": min_humidity,
        "temperature_peak_c": temperature_peak,
        "temperature_peak_at": temperature_peak_at,
        "humidity_peak_percent": humidity_peak,
        "humidity_peak_at": humidity_peak_at,
        "peak_score": round(peak_score, 4),
        "severity": "critical" if critical else "warning",
        "metrics": "".join(metric for metric in ("temperature", "humidity") if metric in metrics),
    }


class EnvironmentService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        self.repository = GermplasmRepository(connection)

    # ----- 规则版本 -----
    def active_rule(self) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM environment_rule_versions WHERE active=1 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else _rule_defaults()

    # ----- 事件归并 -----
    def sync_location(self, location_id: int) -> list[dict[str, Any]]:
        """重算某库位的异常事件。幂等：同一批读数重复执行结果不变。"""
        location = self.repository.require_location(location_id)
        rule = self.active_rule()
        now_iso = to_storage(self.clock.now())
        timestamp = now_iso
        readings = records(self.connection.execute(
            "SELECT * FROM environment_readings WHERE location_id=? ORDER BY observed_at,id", (location_id,)
        ).fetchall())
        segments = segment_readings(readings, location, rule, now_iso)

        existing_rows = self.connection.execute(
            "SELECT * FROM environment_events WHERE location_id=? AND status!='superseded' ORDER BY id", (location_id,)
        ).fetchall()
        existing = [dict(row) for row in existing_rows]
        event_reading_sets = {
            int(item["id"]): set(json.loads(item["reading_ids_json"])) for item in existing
        }

        synced: list[dict[str, Any]] = []
        assigned: set[int] = set()
        for segment in segments:
            segment_set = set(segment["reading_ids"])
            overlaps = sorted(
                event_id for event_id, reading_set in event_reading_sets.items() if segment_set & reading_set
            )
            anchor = next((event_id for event_id in overlaps if event_id not in assigned), None)
            if anchor is None:
                # 没有任何存活事件与该段共享读数：新异常，或迟到的正常读数把旧窗口切成的后续段。
                synced.append(self._create_event(location, rule, segment, timestamp))
                continue
            assigned.add(anchor)
            for merged_id in overlaps:
                if merged_id != anchor and merged_id not in assigned:
                    assigned.add(merged_id)
                    self._supersede_event(merged_id, anchor, timestamp)
            synced.append(self._revise_event(anchor, location, rule, segment, timestamp))
        return self._events_by_ids([int(item["id"]) for item in synced])

    def _segment_key(self, location_id: int, first_reading_id: int, rule_version: str) -> str:
        # 键由库位与窗口首读数派生：同一窗口被迟到读数修订时首读数不变，重放得到相同事件编号；
        # 被正常读数切断出的后续窗口首读数不同，必然得到不同的键。
        digest = hashlib.sha256(f"{location_id}:{first_reading_id}:{rule_version}".encode()).hexdigest()[:16]
        return f"seg-{digest}"

    def _event_no(self, started_at: str, segment_key: str) -> str:
        day = started_at[:10].replace("-", "")
        return f"EV-{day}-{segment_key[4:].upper()}"

    def _create_event(self, location: dict[str, Any], rule: dict[str, Any], segment: dict[str, Any], timestamp: str) -> dict[str, Any]:
        key = self._segment_key(int(location["id"]), segment["first_reading_id"], rule["version"])
        event_no = self._event_no(segment["started_at"], key)
        status = "open" if segment["ended_at"] is None else "closed"
        cursor = self.connection.execute(
            "INSERT INTO environment_events(event_no,location_id,segment_key,status,rule_version,first_reading_id,"
            "last_reading_id,started_at,peak_at,ended_at,duration_minutes,max_temperature_c,min_temperature_c,"
            "max_humidity_percent,min_humidity_percent,temperature_peak_c,temperature_peak_at,humidity_peak_percent,"
            "humidity_peak_at,peak_score,severity,metrics,reading_ids_json,revision,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)",
            (
                event_no, location["id"], key, status, rule["version"], segment["first_reading_id"],
                segment["last_reading_id"], segment["started_at"], segment["peak_at"], segment["ended_at"],
                segment["duration_minutes"], segment["max_temperature_c"], segment["min_temperature_c"],
                segment["max_humidity_percent"], segment["min_humidity_percent"], segment["temperature_peak_c"],
                segment["temperature_peak_at"], segment["humidity_peak_percent"], segment["humidity_peak_at"],
                segment["peak_score"], segment["severity"], segment["metrics"],
                json.dumps(segment["reading_ids"]), timestamp, timestamp,
            ),
        )
        event_id = int(cursor.lastrowid)
        self._replace_links(event_id, segment["reading_ids"])
        self._upsert_event_alert(event_id, location, segment, status, timestamp)
        self._append_audit(
            "system", "environment.event.detected", event_id,
            after={"event_no": event_no, "severity": segment["severity"], "reading_count": len(segment["reading_ids"])},
            metadata={"rule_version": rule["version"]},
        )
        return self._require_event(event_id)

    def _revise_event(
        self, event_id: int, location: dict[str, Any], rule: dict[str, Any], segment: dict[str, Any], timestamp: str
    ) -> dict[str, Any]:
        before = self._require_event(event_id)
        new_status = "open" if segment["ended_at"] is None else "closed"
        if before["status"] == "closed" and new_status == "open":
            new_status = "reopened"
        elif before["status"] == "reopened" and segment["ended_at"] is not None:
            new_status = "closed"
        if self._window_unchanged(before, segment, new_status):
            # 相同读数重复归并：窗口无变化，不产生新修订号，也不使既有评估失效。
            return before
        segment_key = self._segment_key(int(location["id"]), segment["first_reading_id"], rule["version"])
        self.connection.execute(
            "UPDATE environment_events SET segment_key=?,status=?,rule_version=?,first_reading_id=?,last_reading_id=?,"
            "started_at=?,peak_at=?,ended_at=?,duration_minutes=?,max_temperature_c=?,min_temperature_c=?,"
            "max_humidity_percent=?,min_humidity_percent=?,temperature_peak_c=?,temperature_peak_at=?,humidity_peak_percent=?,"
            "humidity_peak_at=?,peak_score=?,severity=?,metrics=?,reading_ids_json=?,revision=revision+1,updated_at=? "
            "WHERE id=?",
            (
                segment_key,
                new_status, rule["version"], segment["first_reading_id"], segment["last_reading_id"],
                segment["started_at"], segment["peak_at"], segment["ended_at"], segment["duration_minutes"],
                segment["max_temperature_c"], segment["min_temperature_c"], segment["max_humidity_percent"],
                segment["min_humidity_percent"], segment["temperature_peak_c"], segment["temperature_peak_at"],
                segment["humidity_peak_percent"], segment["humidity_peak_at"], segment["peak_score"],
                segment["severity"], segment["metrics"], json.dumps(segment["reading_ids"]), timestamp, event_id,
            ),
        )
        self._replace_links(event_id, segment["reading_ids"])
        self._upsert_event_alert(event_id, location, segment, new_status, timestamp)
        # 迟到读数修订窗口后，尚未确认的评估依据已过期，确定性地作废并等待重新评估。
        stale = self.connection.execute(
            "UPDATE event_assessments SET status='superseded',updated_at=? WHERE event_id=? "
            "AND event_revision=? AND status IN ('pending','awaiting_confirmation')",
            (timestamp, event_id, before["revision"]),
        ).rowcount
        self._append_audit(
            "system", "environment.event.revised", event_id,
            before={"revision": before["revision"], "status": before["status"], "ended_at": before["ended_at"]},
            after={"revision": before["revision"] + 1, "status": new_status, "ended_at": segment["ended_at"],
                   "superseded_assessments": stale},
            metadata={"rule_version": rule["version"]},
        )
        return self._require_event(event_id)

    @staticmethod
    def _window_unchanged(before: dict[str, Any], segment: dict[str, Any], new_status: str) -> bool:
        return (
            before["status"] == new_status
            and int(before["first_reading_id"]) == int(segment["first_reading_id"])
            and int(before["last_reading_id"]) == int(segment["last_reading_id"])
            and before["started_at"] == segment["started_at"]
            and before["ended_at"] == segment["ended_at"]
            and (before["duration_minutes"] is None) == (segment["duration_minutes"] is None)
            and (before["duration_minutes"] is None or int(before["duration_minutes"]) == int(segment["duration_minutes"]))
            and float(before["peak_score"]) == float(segment["peak_score"])
            and before["severity"] == segment["severity"]
            and before["metrics"] == segment["metrics"]
            and set(before["reading_ids"]) == set(segment["reading_ids"])
        )

    def _supersede_event(self, event_id: int, absorbed_by: int, timestamp: str) -> None:
        before = self._require_event(event_id)
        # 释放原段键：迟到读数切断出的后续窗口可能复用相同首读数，需要重新占用该键。
        self.connection.execute(
            "UPDATE environment_events SET status='superseded',superseded_by=?,segment_key=segment_key||':m'||id,"
            "revision=revision+1,updated_at=? WHERE id=?",
            (absorbed_by, timestamp, event_id),
        )
        self.connection.execute(
            "UPDATE quality_alerts SET status='resolved',resolved_at=?,updated_at=? "
            "WHERE alert_key=? AND status IN ('open','acknowledged')",
            (timestamp, timestamp, f"environment-event-{event_id}"),
        )
        self.connection.execute(
            "UPDATE event_assessments SET status='superseded',updated_at=? WHERE event_id=? "
            "AND status IN ('pending','awaiting_confirmation')",
            (timestamp, event_id),
        )
        self.connection.execute(
            "UPDATE background_jobs SET status='cancelled',updated_at=? WHERE status IN ('pending','running') "
            "AND id IN (SELECT job_id FROM event_assessments WHERE event_id=? AND status='superseded' AND job_id IS NOT NULL)",
            (timestamp, event_id),
        )
        self._append_audit(
            "system", "environment.event.merged", event_id,
            before={"event_no": before["event_no"]}, after={"superseded_by": absorbed_by},
        )

    def _replace_links(self, event_id: int, reading_ids: list[int]) -> None:
        self.connection.execute("DELETE FROM event_reading_links WHERE event_id=?", (event_id,))
        self.connection.executemany(
            "INSERT OR IGNORE INTO event_reading_links(event_id,reading_id) VALUES(?,?)",
            [(event_id, reading_id) for reading_id in reading_ids],
        )

    def _upsert_event_alert(
        self, event_id: int, location: dict[str, Any], segment: dict[str, Any], status: str, timestamp: str
    ) -> None:
        event = self._require_event(event_id)
        labels = "、".join(METRIC_LABELS[metric] for metric in ("temperature", "humidity") if metric in segment["metrics"])
        message = f"库位 {location['location_code']} 发生连续{labels}异常事件 {event['event_no']}"
        detail = json.dumps({"event_id": event_id, "reading_count": len(segment["reading_ids"]),
                             "duration_minutes": segment["duration_minutes"]}, ensure_ascii=False)
        key = f"environment-event-{event_id}"
        existing = self.connection.execute("SELECT id,status FROM quality_alerts WHERE alert_key=?", (key,)).fetchone()
        if existing:
            if status == "closed":
                self.connection.execute(
                    "UPDATE quality_alerts SET severity=?,message=?,detail_json=?,status='resolved',resolved_at=?,"
                    "updated_at=? WHERE alert_key=?",
                    (segment["severity"], message, detail, timestamp, timestamp, key),
                )
            else:
                self.connection.execute(
                    "UPDATE quality_alerts SET severity=?,message=?,detail_json=?,"
                    "status=CASE WHEN status='resolved' THEN 'open' ELSE status END,resolved_at=NULL,updated_at=? "
                    "WHERE alert_key=?",
                    (segment["severity"], message, detail, timestamp, key),
                )
        else:
            self.connection.execute(
                "INSERT INTO quality_alerts(alert_key,alert_type,severity,location_id,message,detail_json,status,"
                "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (key, "environment_event", segment["severity"], location["id"], message, detail,
                 "resolved" if status == "closed" else "open", timestamp, timestamp),
            )

    # ----- 影响评估 -----
    def affected_lots(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        """结合事件窗口内有效的容器摆放历史，列出受影响批次与容器。"""
        now_iso = to_storage(self.clock.now())
        horizon = event["ended_at"] or now_iso
        rows = self.connection.execute(
            "SELECT p.id AS placement_id,p.container_code,p.weight_grams,p.placed_at,p.removed_at,"
            "l.id AS lot_id,l.lot_no,l.status AS lot_status,a.id AS accession_id,a.accession_no,a.crop_name "
            "FROM lot_placements p JOIN seed_lots l ON l.id=p.lot_id "
            "JOIN accessions a ON a.id=l.accession_id "
            "WHERE p.location_id=? AND p.placed_at<=? AND (p.removed_at IS NULL OR p.removed_at>=?) "
            "ORDER BY p.placed_at,p.id",
            (event["location_id"], horizon, event["started_at"]),
        ).fetchall()
        affected = []
        for row in rows:
            item = dict(row)
            overlap_start = max(from_storage(item["placed_at"]), from_storage(event["started_at"]))
            overlap_end = min(from_storage(item["removed_at"] or horizon), from_storage(horizon))
            item["exposed_minutes"] = max(0, int((overlap_end - overlap_start).total_seconds() // 60))
            affected.append(item)
        return affected

    def effective_duration(self, event: dict[str, Any]) -> int:
        """开放事件按已持续时长评估，已关闭事件使用实际窗口时长。"""
        if event["duration_minutes"] is not None:
            return int(event["duration_minutes"])
        return max(0, int((from_storage(to_storage(self.clock.now())) - from_storage(event["started_at"])).total_seconds() // 60))

    def recommend(self, event: dict[str, Any], rule: dict[str, Any]) -> tuple[str, str]:
        duration = self.effective_duration(event)
        severity = event["severity"]
        if duration >= int(rule["freeze_minutes"]) or (
            severity == "critical" and duration >= int(rule["retest_minutes"])
        ):
            reason = (
                f"暴露 {duration} 分钟，{'达到质量冻结阈值' if duration >= int(rule['freeze_minutes']) else '严重偏离且达到提前复检阈值'}"
                f"（冻结 {rule['freeze_minutes']} 分钟 / 提前复检 {rule['retest_minutes']} 分钟），建议冻结并安排提前复检"
            )
            return "freeze", reason
        if duration >= int(rule["retest_minutes"]) or severity == "critical":
            reason = (
                f"暴露 {duration} 分钟，达到提前复检阈值（{rule['retest_minutes']} 分钟）"
                if duration >= int(rule["retest_minutes"])
                else f"出现严重偏离（峰值评分 {event['peak_score']}），即使暴露较短也建议提前复检"
            )
            return "retest", reason
        return "none", f"暴露 {duration} 分钟，未达到处置阈值，建议仅观察"

    def request_assessment(self, event_id: int, requested_by: str) -> dict[str, Any]:
        event = self._require_event(event_id)
        if event["status"] == "superseded":
            raise ConflictError("事件已经被合并作废，请评估合并后的事件",
                                context={"superseded_by": event.get("superseded_by")})
        timestamp = to_storage(self.clock.now())
        assessment = self.connection.execute(
            "SELECT * FROM event_assessments WHERE event_id=? AND event_revision=? AND status!='superseded'",
            (event_id, event["revision"]),
        ).fetchone()
        if assessment:
            return self._require_assessment(int(assessment["id"]))
        cursor = self.connection.execute(
            "INSERT INTO event_assessments(event_id,event_revision,rule_version,status,requested_by,created_at,updated_at) "
            "VALUES(?,?,?,'pending',?,?,?)",
            (event_id, event["revision"], event["rule_version"], requested_by, timestamp, timestamp),
        )
        assessment_id = int(cursor.lastrowid)
        from app.services.jobs import JobService

        job = JobService(self.connection, self.clock).enqueue(
            ASSESSMENT_JOB_TYPE,
            f"env-assess:{assessment_id}",
            {"assessment_id": assessment_id, "event_id": event_id},
        )
        self.connection.execute(
            "UPDATE event_assessments SET job_id=?,updated_at=? WHERE id=?", (job["id"], timestamp, assessment_id)
        )
        self._append_audit(
            requested_by, "environment.assessment.requested", event_id,
            after={"assessment_id": assessment_id, "revision": event["revision"]},
        )
        return self._require_assessment(assessment_id)

    def run_due_assessment_job(self, worker: str = "environment-worker") -> dict[str, Any] | None:
        """领取并执行一个到期的评估作业（须在即时事务内调用，进程重启后可继续）。"""
        from app.services.jobs import JobService

        jobs = JobService(self.connection, self.clock)
        job = jobs.claim(worker, job_type=ASSESSMENT_JOB_TYPE)
        if job is None:
            return None
        payload = json.loads(job["payload_json"])
        result = self._compute_assessment(int(payload["assessment_id"]))
        jobs.complete(
            int(job["id"]), worker,
            {"assessment_id": int(payload["assessment_id"]), "affected": len(result["affected"])},
        )
        return result

    def _compute_assessment(self, assessment_id: int) -> dict[str, Any]:
        assessment = self._require_assessment(assessment_id)
        if assessment["status"] in ("awaiting_confirmation", "confirmed"):
            return assessment
        event = self._require_event(int(assessment["event_id"]))
        rule_row = self.connection.execute(
            "SELECT * FROM environment_rule_versions WHERE version=?", (assessment["rule_version"],)
        ).fetchone()
        rule = dict(rule_row) if rule_row else self.active_rule()
        affected = self.affected_lots(event)
        recommendation, reason = self.recommend(event, rule)
        timestamp = to_storage(self.clock.now())
        self.connection.execute(
            "UPDATE event_assessments SET status='awaiting_confirmation',affected_json=?,recommendation=?,"
            "recommendation_reason=?,updated_at=? WHERE id=? AND status='pending'",
            (json.dumps(affected, ensure_ascii=False), recommendation, reason, timestamp, assessment_id),
        )
        return self._require_assessment(assessment_id)

    def confirm_assessment(
        self, assessment_id: int, actor: str, *, decision: str | None = None, reason: str = ""
    ) -> dict[str, Any]:
        """人工确认后原子写入冻结、提前复检单与审计；重复提交不产生重复处置。"""
        assessment = self._require_assessment(assessment_id)
        event = self._require_event(int(assessment["event_id"]))
        if assessment["status"] == "confirmed":
            return assessment
        if assessment["status"] != "awaiting_confirmation":
            raise ConflictError("评估尚未完成，暂时不能确认", context={"status": assessment["status"]})
        final_decision = decision or assessment["recommendation"]
        if final_decision not in {"freeze", "retest", "none"}:
            raise ValidationError("处置决定必须是 freeze、retest 或 none")
        if decision is not None and decision != assessment["recommendation"] and not reason.strip():
            raise ValidationError("人工覆盖系统建议时必须填写原因")

        affected = assessment["affected"]
        timestamp = to_storage(self.clock.now())
        before = {"status": assessment["status"], "recommendation": assessment["recommendation"]}
        created_holds: list[int] = []
        created_orders: list[int] = []

        if final_decision in {"freeze", "retest"}:
            rule_row = self.connection.execute(
                "SELECT * FROM environment_rule_versions WHERE version=?", (assessment["rule_version"],)
            ).fetchone()
            due_days = int(rule_row["retest_due_days"]) if rule_row else int(_rule_defaults()["retest_due_days"])
            due_on = (self.clock.now() + timedelta(days=due_days)).date().isoformat()
            for lot in affected:
                lot_id = int(lot["lot_id"])
                if final_decision == "freeze":
                    hold = self.connection.execute(
                        "SELECT id FROM lot_holds WHERE lot_id=? AND hold_type='质量' AND released_at IS NULL", (lot_id,)
                    ).fetchone()
                    if hold is None:
                        hold_reason = f"环境异常事件 {event['event_no']} 质量冻结"
                        cursor = self.connection.execute(
                            "INSERT INTO lot_holds(lot_id,hold_type,reason,imposed_by,imposed_at) VALUES(?,?,?,?,?)",
                            (lot_id, "质量", hold_reason, actor, timestamp),
                        )
                        hold_id = int(cursor.lastrowid)
                        created_holds.append(hold_id)
                        self.connection.execute(
                            "INSERT INTO event_disposition_links(event_id,hold_id,created_at) VALUES(?,?,?)",
                            (event["id"], hold_id, timestamp),
                        )
                        self.connection.execute(
                            "UPDATE seed_lots SET status='held',version=version+1,updated_at=? WHERE id=? "
                            "AND status NOT IN ('depleted','disposed')",
                            (timestamp, lot_id),
                        )
                order = self.connection.execute(
                    "SELECT id FROM event_retest_orders WHERE event_id=? AND lot_id=?", (event["id"], lot_id)
                ).fetchone()
                if order is None:
                    cursor = self.connection.execute(
                        "INSERT INTO event_retest_orders(event_id,lot_id,assessment_id,due_on,created_at) "
                        "VALUES(?,?,?,?,?)",
                        (event["id"], lot_id, assessment_id, due_on, timestamp),
                    )
                    created_orders.append(int(cursor.lastrowid))

        if decision is not None and decision != assessment["recommendation"]:
            self.connection.execute(
                "INSERT INTO event_overrides(event_id,assessment_id,actor,system_recommendation,decision,reason,"
                "rule_version,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (event["id"], assessment_id, actor, assessment["recommendation"], final_decision, reason,
                 assessment["rule_version"], timestamp),
            )

        applied = {
            "decision": final_decision,
            "override": decision is not None and decision != assessment["recommendation"],
            "holds_created": created_holds,
            "retest_orders_created": created_orders,
            "affected_lot_count": len(affected),
        }
        self.connection.execute(
            "UPDATE event_assessments SET status='confirmed',override_json=?,confirmed_by=?,confirmed_at=?,"
            "applied_result_json=?,updated_at=? WHERE id=?",
            (json.dumps({"decision": decision, "reason": reason}, ensure_ascii=False), actor, timestamp,
             json.dumps(applied, ensure_ascii=False), timestamp, assessment_id),
        )
        self._append_audit(
            actor, "environment.disposition.apply", event["id"],
            before=before, after=applied,
            metadata={"assessment_id": assessment_id, "event_no": event["event_no"], "rule_version": assessment["rule_version"]},
        )
        return self._require_assessment(assessment_id)

    # ----- 查询：一次事故追到读数、容器、批次与处置 -----
    def list_events(self, *, status: str | None = None, severity: str | None = None, limit: int = 50, offset: int = 0) -> tuple[list[dict[str, Any]], int]:
        where: list[str] = []
        params: list[Any] = []
        if status:
            where.append("e.status=?")
            params.append(status)
        else:
            # 默认不展示已被合并作废的事件；需要时显式 status=superseded 查询。
            where.append("e.status!='superseded'")
        if severity:
            where.append("e.severity=?")
            params.append(severity)
        clause = " WHERE " + " AND ".join(where) if where else ""
        total = int(self.connection.execute(f"SELECT COUNT(*) FROM environment_events e{clause}", params).fetchone()[0])
        params.extend([limit, offset])
        rows = self.connection.execute(
            f"SELECT e.*,s.location_code FROM environment_events e JOIN storage_locations s ON s.id=e.location_id{clause} "
            "ORDER BY e.started_at DESC,e.id DESC LIMIT ? OFFSET ?",
            params,
        ).fetchall()
        items = []
        for row in rows:
            item = self._require_event(int(row["id"]))
            item["location_code"] = row["location_code"]
            items.append(item)
        return items, total

    def event_detail(self, event_id: int) -> dict[str, Any]:
        event = self._require_event(event_id)
        location = self.repository.require_location(int(event["location_id"]))
        event["location"] = location
        event["readings"] = records(self.connection.execute(
            "SELECT r.* FROM environment_readings r JOIN event_reading_links l ON l.reading_id=r.id "
            "WHERE l.event_id=? ORDER BY r.observed_at,r.id", (event_id,)
        ).fetchall())
        event["affected_lots"] = self.affected_lots(event)
        event["assessments"] = [self._assessment_payload(int(row["id"])) for row in self.connection.execute(
            "SELECT id FROM event_assessments WHERE event_id=? ORDER BY id", (event_id,)
        ).fetchall()]
        event["holds"] = records(self.connection.execute(
            "SELECT h.* FROM lot_holds h WHERE h.reason LIKE ? "
            "OR h.id IN (SELECT hold_id FROM event_disposition_links WHERE event_id=?) ORDER BY h.id",
            (f"%{event['event_no']}%", event_id),
        ).fetchall())
        event["retest_orders"] = records(self.connection.execute(
            "SELECT o.*,l.lot_no,a.accession_no,a.crop_name FROM event_retest_orders o "
            "JOIN seed_lots l ON l.id=o.lot_id JOIN accessions a ON a.id=l.accession_id "
            "WHERE o.event_id=? ORDER BY o.due_on,l.lot_no", (event_id,)
        ).fetchall())
        if event.get("superseded_by"):
            event["superseded_by_event"] = self._require_event(int(event["superseded_by"]))
        return event

    def due_retest_orders(self, due_before: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        cutoff = due_before or self.clock.now().date().isoformat()
        return records(self.connection.execute(
            "SELECT o.*,l.lot_no,a.accession_no,a.crop_name,e.event_no FROM event_retest_orders o "
            "JOIN seed_lots l ON l.id=o.lot_id JOIN accessions a ON a.id=l.accession_id "
            "JOIN environment_events e ON e.id=o.event_id "
            "WHERE o.status='scheduled' AND o.due_on<=? ORDER BY o.due_on,l.lot_no LIMIT ?",
            (cutoff, limit),
        ).fetchall())

    # ----- 规则版本管理 -----
    def create_rule(self, data: dict[str, Any]) -> dict[str, Any]:
        timestamp = to_storage(self.clock.now())
        exists = self.connection.execute(
            "SELECT id FROM environment_rule_versions WHERE version=?", (data["version"],)
        ).fetchone()
        if exists:
            raise ConflictError("环境事件规则版本号已经存在")
        self.connection.execute("UPDATE environment_rule_versions SET active=0 WHERE active=1")
        cursor = self.connection.execute(
            "INSERT INTO environment_rule_versions(version,temperature_tolerance_c,temperature_critical_c,"
            "humidity_tolerance_percent,humidity_critical_percent,max_gap_minutes,freeze_minutes,retest_minutes,"
            "retest_due_days,active,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,1,?,?)",
            (
                data["version"], data["temperature_tolerance_c"], data["temperature_critical_c"],
                data["humidity_tolerance_percent"], data["humidity_critical_percent"], data["max_gap_minutes"],
                data["freeze_minutes"], data["retest_minutes"], data["retest_due_days"],
                data["created_by"], timestamp,
            ),
        )
        rule = dict(self.connection.execute(
            "SELECT * FROM environment_rule_versions WHERE id=?", (cursor.lastrowid,)
        ).fetchone())
        self._append_audit(
            data["created_by"], "environment.rule.activated", 0,
            after={"version": data["version"]},
        )
        return rule

    def list_rules(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM environment_rule_versions ORDER BY id DESC"
        ).fetchall()]

    def pump_assessment_jobs(self, worker: str = "environment-worker", max_jobs: int = 100) -> list[dict[str, Any]]:
        """连续执行到期评估作业，供 CLI/轮询调用；每个作业在独立即时事务中提交。

        单个作业失败时回滚其事务并把作业标记为稍后重试，不阻塞后续作业。
        """
        from app.database import transaction
        from app.services.jobs import JobService

        results: list[dict[str, Any]] = []
        for _ in range(max_jobs):
            try:
                with transaction(immediate=True):
                    result = self.run_due_assessment_job(worker)
            except Exception as exc:
                with transaction(immediate=True):
                    jobs = JobService(self.connection, self.clock)
                    stale = jobs.claim(worker, job_type=ASSESSMENT_JOB_TYPE)
                    if stale is not None:
                        jobs.fail(int(stale["id"]), worker, str(exc)[:500], retry_seconds=30)
                continue
            if result is None:
                break
            results.append(result)
        return results

    def consume_retest_order(self, order_id: int, test_id: int, actor: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM event_retest_orders WHERE id=?", (order_id,)).fetchone()
        if row is None:
            raise NotFoundError("提前复检单不存在")
        order = dict(row)
        if order["status"] != "scheduled":
            raise ConflictError("该提前复检单已经核销或取消")
        test = self.repository.require_test(test_id)
        if int(test["lot_id"]) != int(order["lot_id"]):
            raise ValidationError("检测任务与复检单批次不一致")
        timestamp = to_storage(self.clock.now())
        self.connection.execute(
            "UPDATE event_retest_orders SET status='consumed',test_id=? WHERE id=?", (test_id, timestamp)
        )
        self._append_audit(
            actor, "environment.retest.consumed", int(order["event_id"]),
            after={"order_id": order_id, "test_id": test_id, "lot_id": order["lot_id"]},
        )
        return dict(self.connection.execute("SELECT * FROM event_retest_orders WHERE id=?", (order_id,)).fetchone())

    # ----- 内部 -----
    def _require_event(self, event_id: int) -> dict[str, Any]:
        item = record(self.connection.execute("SELECT * FROM environment_events WHERE id=?", (event_id,)).fetchone())
        if item is None:
            raise NotFoundError("环境异常事件不存在")
        item["reading_ids"] = json.loads(item.pop("reading_ids_json"))
        return item

    def _events_by_ids(self, event_ids: list[int]) -> list[dict[str, Any]]:
        if not event_ids:
            return []
        placeholders = ",".join("?" for _ in event_ids)
        rows = self.connection.execute(
            f"SELECT * FROM environment_events WHERE id IN ({placeholders}) ORDER BY started_at,id", event_ids
        ).fetchall()
        return [self._require_event(int(row["id"])) for row in rows]

    def _require_assessment(self, assessment_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM event_assessments WHERE id=?", (assessment_id,)).fetchone()
        if row is None:
            raise NotFoundError("环境影响评估不存在")
        return self._assessment_payload(assessment_id, row)

    def _assessment_payload(self, assessment_id: int, row: sqlite3.Row | None = None) -> dict[str, Any]:
        row = row or self.connection.execute("SELECT * FROM event_assessments WHERE id=?", (assessment_id,)).fetchone()
        item = dict(row)
        item["affected"] = json.loads(item.pop("affected_json"))
        for key in ("override_json", "applied_result_json"):
            raw = item.pop(key)
            item[key.removesuffix("_json")] = json.loads(raw) if raw else None
        item["overrides"] = records(self.connection.execute(
            "SELECT * FROM event_overrides WHERE assessment_id=? ORDER BY id", (assessment_id,)
        ).fetchall())
        return item

    def _append_audit(
        self, actor: str, action: str, event_id: int, *, before: dict[str, Any] | None = None,
        after: dict[str, Any] | None = None, metadata: dict[str, Any] | None = None,
    ) -> None:
        from app.services.audit import AuditContext, AuditService

        AuditService(self.connection, self.clock).record(
            AuditContext(actor_user_id=None, actor_name=actor),
            action=action, resource_type="environment_event", resource_id=event_id,
            before=before, after=after, metadata=metadata,
        )
