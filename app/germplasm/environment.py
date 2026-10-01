"""环境异常事件检测、影响评估与人工处置。

检测阶段按库位把连续越界读数归并为带开始、峰值、结束的事件；读数乱序补传时
重新扫描全量读数即可确定性修正窗口（扩展、更新峰值或合并事件），指纹不变则
零写入。评估阶段依据摆放历史区间重叠列出受影响批次与暴露时长并给出处置建议。
处置（质量冻结、提前复检）必须经人工确认，并在单事务内原子写入冻结、检测日程、
事件动作与审计。检测作业持久化在 background_jobs 中，进程重启后可以继续。
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

JOB_TYPE_DETECT = "environment.detect"


def _minutes(start: str, end: str) -> int:
    delta = from_storage(end) - from_storage(start)
    return max(0, int(round(delta.total_seconds() / 60.0)))


class EnvironmentEventService:
    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        self.repository = GermplasmRepository(connection)

    # ------------------------------------------------------------------ 规则

    def create_rule(self, data: dict[str, Any]) -> dict[str, Any]:
        latest = self.connection.execute(
            "SELECT version FROM environment_event_rules WHERE rule_code=? ORDER BY version DESC LIMIT 1",
            (data["rule_code"],),
        ).fetchone()
        version = int(latest[0]) + 1 if latest else 1
        timestamp = to_storage(self.clock.now())
        # 全局只保留一条生效规则线：发布即生效，旧规则（无论规则编码）全部停用，
        # 已产生的历史事件仍绑定原规则版本。
        self.connection.execute("UPDATE environment_event_rules SET active=0 WHERE active=1")
        cursor = self.connection.execute(
            "INSERT INTO environment_event_rules(rule_code,version,active,temperature_delta_c,humidity_delta_percent,"
            "critical_temperature_delta_c,critical_humidity_delta_percent,max_gap_minutes,hold_minutes,"
            "critical_hold_minutes,retest_minutes,retest_due_days,effective_from,created_by,created_at) "
            "VALUES(?,?,1,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                data["rule_code"], version, data["temperature_delta_c"], data["humidity_delta_percent"],
                data["critical_temperature_delta_c"], data["critical_humidity_delta_percent"],
                data["max_gap_minutes"], data["hold_minutes"], data["critical_hold_minutes"],
                data["retest_minutes"], data["retest_due_days"], timestamp, data["created_by"], timestamp,
            ),
        )
        return self._require_rule(int(cursor.lastrowid))

    def list_rules(self, *, active_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM environment_event_rules"
        if active_only:
            sql += " WHERE active=1"
        return records(self.connection.execute(sql + " ORDER BY created_at DESC,version DESC").fetchall())

    def _active_rule(self) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM environment_event_rules WHERE active=1 ORDER BY created_at DESC,id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise ConflictError("尚未配置生效的环境异常事件规则")
        return dict(row)

    def _rule_effective_windows(self) -> dict[int, tuple[str, str | None]]:
        """当前生效规则版本线各版本相对下一版本的时间窗口（左闭右开）。"""
        active = self._active_rule()
        versions = records(self.connection.execute(
            "SELECT * FROM environment_event_rules WHERE rule_code=? ORDER BY effective_from,id",
            (active["rule_code"],),
        ).fetchall())
        windows: dict[int, tuple[str, str | None]] = {}
        for index, rule in enumerate(versions):
            next_from = versions[index + 1]["effective_from"] if index + 1 < len(versions) else None
            windows[int(rule["id"])] = (rule["effective_from"], next_from)
        return windows

    def _require_rule(self, rule_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM environment_event_rules WHERE id=?", (rule_id,)).fetchone()
        if row is None:
            raise NotFoundError("环境异常事件规则不存在")
        return dict(row)

    # ------------------------------------------------------------ 事件检测归并

    def enqueue_detection(self, location_id: int) -> dict[str, Any] | None:
        """登记库位检测作业；已有待执行/执行中的同库位作业时直接复用，避免补传洪峰。"""
        from app.services.jobs import JobService

        self.repository.require_location(location_id)
        existing = self.connection.execute(
            "SELECT * FROM background_jobs WHERE job_type=? AND status IN ('pending','running') "
            "AND json_extract(payload_json,'$.location_id')=? ORDER BY id LIMIT 1",
            (JOB_TYPE_DETECT, location_id),
        ).fetchone()
        if existing is not None:
            return dict(existing)
        wave = self.connection.execute(
            "SELECT MAX(id) FROM environment_readings WHERE location_id=?", (location_id,)
        ).fetchone()[0]
        if wave is None:
            return None
        return JobService(self.connection, self.clock).enqueue(
            JOB_TYPE_DETECT,
            f"{JOB_TYPE_DETECT}:{location_id}:{wave}",
            {"location_id": location_id},
        )

    def detect_location(self, location_id: int) -> dict[str, Any]:
        """重扫库位全部读数，确定性地修正事件窗口并刷新影响评估。"""
        location = self.repository.require_location(location_id)
        target_t = float(location["temperature_c"])
        target_h = float(location["humidity_percent"])
        readings = records(self.connection.execute(
            "SELECT * FROM environment_readings WHERE location_id=? ORDER BY observed_at,id", (location_id,)
        ).fetchall())
        if not readings:
            return {"location_id": location_id, "events": [], "created": 0, "revised": 0, "merged": 0}

        events = records(self.connection.execute(
            "SELECT * FROM environment_events WHERE location_id=?", (location_id,)
        ).fetchall())
        members_by_event: dict[int, set[int]] = {}
        for row in self.connection.execute(
            "SELECT m.event_id,m.reading_id FROM environment_event_readings m "
            "JOIN environment_events e ON e.id=m.event_id WHERE e.location_id=?",
            (location_id,),
        ).fetchall():
            members_by_event.setdefault(int(row["event_id"]), set()).add(int(row["reading_id"]))

        # 已人工确认/驳回（含部分批次已处置）的事件窗口冻结：迟到读数仅补充挂接，
        # 不改写窗口、不参与合并、不触发重复处置。
        frozen_event_ids = {
            int(event["id"]) for event in events
            if event["status"] in {"confirmed", "dismissed"}
        }
        frozen_event_ids.update(int(row[0]) for row in self.connection.execute(
            "SELECT DISTINCT event_id FROM environment_event_lots WHERE disposition_status <> 'pending'"
        ).fetchall())
        frozen_claim: set[int] = set()
        for event in events:
            if int(event["id"]) not in frozen_event_ids:
                continue
            frozen_claim |= members_by_event.get(int(event["id"]), set())
            rule = self._require_rule(int(event["rule_id"]))
            gap = timedelta(minutes=int(rule["max_gap_minutes"]))
            started_at = from_storage(event["started_at"])
            ended_at = from_storage(event["ended_at"] or event["started_at"])
            start_bound = (started_at - gap).isoformat()
            end_bound = (ended_at + gap).isoformat()
            for reading in readings:
                reading_id = int(reading["id"])
                if reading_id in frozen_claim:
                    continue
                if not (start_bound <= reading["observed_at"] <= end_bound):
                    continue
                if (
                    abs(float(reading["temperature_c"]) - float(event["target_temperature_c"]))
                    > float(rule["temperature_delta_c"])
                    or abs(float(reading["humidity_percent"]) - float(event["target_humidity_percent"]))
                    > float(rule["humidity_delta_percent"])
                ):
                    self.connection.execute(
                        "INSERT OR IGNORE INTO environment_event_readings(event_id,reading_id) VALUES(?,?)",
                        (event["id"], reading_id),
                    )
                    frozen_claim.add(reading_id)
                    self._relink_alerts(int(event["id"]), int(event["location_id"]), {reading_id})

        # 规则按 rule_code 形成版本线，读数按观测时间落入当时有效的版本窗口；
        # 事件永久保留其检测时使用的 rule_id。
        rule_windows = self._rule_effective_windows()
        rule_ids = set(rule_windows)
        rule_ids.update(
            int(event["rule_id"]) for event in events if event["status"] in {"open", "merged"}
        )

        touched: list[int] = []
        created = revised = merged = 0
        active_from = self._active_rule()["effective_from"]
        for rule_id in sorted(rule_ids):
            rule = self._require_rule(rule_id)
            window = rule_windows.get(rule_id)
            if window is None:
                # 已停用的规则线只处理新规则线生效前的历史读数。
                window = (rule["effective_from"], active_from)
            effective_from, effective_to = window
            other_rule_claim: set[int] = set()
            for event in events:
                if int(event["rule_id"]) != rule_id:
                    other_rule_claim |= members_by_event.get(int(event["id"]), set())
            runs = self._build_runs(
                readings, rule, target_t, target_h,
                frozen_claim | other_rule_claim, effective_from, effective_to,
            )
            open_events = [
                event for event in events
                if int(event["rule_id"]) == rule_id and event["status"] in {"open", "merged"}
            ]

            for run in runs:
                run_ids = {int(item["id"]) for item in run}
                candidates = [
                    event for event in open_events
                    if members_by_event.get(int(event["id"]), set()) & run_ids
                ]
                if candidates:
                    canonical = min(candidates, key=lambda item: int(item["id"]))
                    canonical_id = int(canonical["id"])
                    absorbed = [
                        int(item["id"]) for item in candidates if int(item["id"]) != canonical_id
                    ]
                    changed = self._replace_event(canonical_id, run, rule, location, absorbed_ids=absorbed)
                    merged += len(absorbed)
                    if changed:
                        revised += 1
                    for absorbed_id in absorbed:
                        open_events = [event for event in open_events if int(event["id"]) != absorbed_id]
                        members_by_event.pop(absorbed_id, None)
                else:
                    canonical_id = self._insert_event(run, rule, location)
                    created += 1
                members_by_event[canonical_id] = run_ids
                self._relink_alerts(canonical_id, location_id, run_ids)
                self._assess_event(canonical_id)
                touched.append(canonical_id)

        return {
            "location_id": location_id,
            "events": touched,
            "created": created,
            "revised": revised,
            "merged": merged,
        }

    def _build_runs(
        self,
        readings: list[dict[str, Any]],
        rule: dict[str, Any],
        target_t: float,
        target_h: float,
        excluded_reading_ids: set[int],
        effective_from: str,
        effective_to: str | None,
    ) -> list[list[dict[str, Any]]]:
        max_gap = timedelta(minutes=int(rule["max_gap_minutes"]))
        runs: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        previous: dict[str, Any] | None = None
        for reading in readings:
            in_window = reading["observed_at"] >= effective_from and (
                effective_to is None or reading["observed_at"] < effective_to
            )
            if (
                reading["id"] in excluded_reading_ids
                or not in_window
                or not self._is_excursion(reading, rule, target_t, target_h)
            ):
                if current:
                    runs.append(current)
                    current = []
                previous = None
                continue
            if previous is not None:
                gap = from_storage(reading["observed_at"]) - from_storage(previous["observed_at"])
                if gap > max_gap:
                    runs.append(current)
                    current = []
            current.append(reading)
            previous = reading
        if current:
            runs.append(current)
        return runs

    @staticmethod
    def _is_excursion(
        reading: dict[str, Any], rule: dict[str, Any], target_t: float, target_h: float
    ) -> bool:
        return (
            abs(float(reading["temperature_c"]) - target_t) > float(rule["temperature_delta_c"])
            or abs(float(reading["humidity_percent"]) - target_h) > float(rule["humidity_delta_percent"])
        )

    def _event_values(
        self, run: list[dict[str, Any]], rule: dict[str, Any], location: dict[str, Any]
    ) -> dict[str, Any]:
        target_t = float(location["temperature_c"])
        target_h = float(location["humidity_percent"])
        peak_t = max(run, key=lambda item: abs(float(item["temperature_c"]) - target_t))
        peak_h = max(run, key=lambda item: abs(float(item["humidity_percent"]) - target_h))
        max_t_delta = max(abs(float(item["temperature_c"]) - target_t) for item in run)
        max_h_delta = max(abs(float(item["humidity_percent"]) - target_h) for item in run)
        metric_names: list[str] = []
        if max_t_delta > float(rule["temperature_delta_c"]):
            metric_names.append("temperature")
        if max_h_delta > float(rule["humidity_delta_percent"]):
            metric_names.append("humidity")
        severity = "critical" if (
            max_t_delta > float(rule["critical_temperature_delta_c"])
            or max_h_delta > float(rule["critical_humidity_delta_percent"])
        ) else "warning"
        identity = "|".join(
            [
                str(rule["id"]), str(target_t), str(target_h),
                ",".join(str(int(item["id"])) for item in sorted(run, key=lambda item: int(item["id"]))),
            ]
        )
        return {
            "started_at": min(item["observed_at"] for item in run),
            "ended_at": max(item["observed_at"] for item in run),
            "peak_temperature_c": float(peak_t["temperature_c"]),
            "peak_temperature_at": peak_t["observed_at"],
            "peak_humidity_percent": float(peak_h["humidity_percent"]),
            "peak_humidity_at": peak_h["observed_at"],
            "max_temperature_delta_c": round(max_t_delta, 6),
            "max_humidity_delta_percent": round(max_h_delta, 6),
            "severity": severity,
            "metrics": ",".join(metric_names),
            "reading_count": len(run),
            "fingerprint": hashlib.sha256(identity.encode()).hexdigest(),
        }

    def _insert_event(
        self, run: list[dict[str, Any]], rule: dict[str, Any], location: dict[str, Any]
    ) -> int:
        values = self._event_values(run, rule, location)
        timestamp = to_storage(self.clock.now())
        cursor = self.connection.execute(
            "INSERT INTO environment_events(event_no,location_id,rule_id,status,started_at,ended_at,"
            "target_temperature_c,target_humidity_percent,peak_temperature_c,peak_temperature_at,"
            "peak_humidity_percent,peak_humidity_at,max_temperature_delta_c,max_humidity_delta_percent,"
            "severity,metrics,reading_count,fingerprint,created_at,updated_at) "
            "VALUES('',? ,?,'open',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                location["id"], rule["id"], values["started_at"], values["ended_at"],
                location["temperature_c"], location["humidity_percent"], values["peak_temperature_c"],
                values["peak_temperature_at"], values["peak_humidity_percent"], values["peak_humidity_at"],
                values["max_temperature_delta_c"], values["max_humidity_delta_percent"], values["severity"],
                values["metrics"], values["reading_count"], values["fingerprint"], timestamp, timestamp,
            ),
        )
        event_id = int(cursor.lastrowid)
        self.connection.execute(
            "UPDATE environment_events SET event_no=? WHERE id=?", (f"EV-{event_id:08d}", event_id)
        )
        self._replace_members(event_id, [int(item["id"]) for item in run])
        return event_id

    def _replace_event(
        self, event_id: int, run: list[dict[str, Any]], rule: dict[str, Any],
        location: dict[str, Any], *, absorbed_ids: list[int],
    ) -> bool:
        existing = self._require_event(event_id)
        values = self._event_values(run, rule, location)
        if (
            existing["fingerprint"] == values["fingerprint"]
            and float(existing["peak_temperature_c"]) == values["peak_temperature_c"]
            and float(existing["peak_humidity_percent"]) == values["peak_humidity_percent"]
        ):
            # 读数集合与峰值完全相同：重复评估不产生任何写入。
            return False
        window_revised = bool(existing["window_revised"]) or (
            existing["started_at"] != values["started_at"] or existing["ended_at"] != values["ended_at"]
        )
        self.connection.execute(
            "UPDATE environment_events SET started_at=?,ended_at=?,peak_temperature_c=?,peak_temperature_at=?,"
            "peak_humidity_percent=?,peak_humidity_at=?,max_temperature_delta_c=?,max_humidity_delta_percent=?,"
            "severity=?,metrics=?,reading_count=?,fingerprint=?,window_revised=?,version=version+1,updated_at=? "
            "WHERE id=?",
            (
                values["started_at"], values["ended_at"], values["peak_temperature_c"],
                values["peak_temperature_at"], values["peak_humidity_percent"], values["peak_humidity_at"],
                values["max_temperature_delta_c"], values["max_humidity_delta_percent"], values["severity"],
                values["metrics"], values["reading_count"], values["fingerprint"], 1 if window_revised else 0,
                to_storage(self.clock.now()), event_id,
            ),
        )
        self._replace_members(event_id, [int(item["id"]) for item in run])
        timestamp = to_storage(self.clock.now())
        for absorbed_id in absorbed_ids:
            self.connection.execute(
                "UPDATE environment_events SET status='merged',merged_into_event_id=?,version=version+1,updated_at=? "
                "WHERE id=? AND status='open'",
                (event_id, timestamp, absorbed_id),
            )
            self.connection.execute(
                "DELETE FROM environment_event_lots WHERE event_id=? AND disposition_status='pending'",
                (absorbed_id,),
            )
            self.connection.execute("DELETE FROM environment_event_readings WHERE event_id=?", (absorbed_id,))
        return True

    def _replace_members(self, event_id: int, reading_ids: list[int]) -> None:
        self.connection.execute("DELETE FROM environment_event_readings WHERE event_id=?", (event_id,))
        self.connection.executemany(
            "INSERT OR IGNORE INTO environment_event_readings(event_id,reading_id) VALUES(?,?)",
            [(event_id, reading_id) for reading_id in reading_ids],
        )

    def _relink_alerts(self, event_id: int, location_id: int, reading_ids: set[int]) -> None:
        if not reading_ids:
            return
        alerts = records(self.connection.execute(
            "SELECT * FROM quality_alerts WHERE alert_type='environment_excursion' AND location_id=?",
            (location_id,),
        ).fetchall())
        for alert in alerts:
            detail = alert.get("detail") or {}
            if int(detail.get("reading_id", 0)) in reading_ids and int(detail.get("event_id", 0) or 0) != event_id:
                detail["event_id"] = event_id
                self.connection.execute(
                    "UPDATE quality_alerts SET detail_json=? WHERE id=?",
                    (json.dumps(detail, ensure_ascii=False), alert["id"]),
                )

    # ------------------------------------------------------------- 影响评估

    def _assess_event(self, event_id: int) -> None:
        event = self._require_event(event_id)
        rule = self._require_rule(int(event["rule_id"]))
        overlaps = records(self.connection.execute(
            "SELECT p.*,l.lot_no,l.status AS lot_status,a.accession_no,a.crop_name "
            "FROM lot_placements p JOIN seed_lots l ON l.id=p.lot_id "
            "JOIN accessions a ON a.id=l.accession_id "
            "WHERE p.location_id=? AND p.placed_at<=? AND (p.removed_at IS NULL OR p.removed_at>=?) "
            "ORDER BY p.placed_at,p.id",
            (event["location_id"], event["ended_at"], event["started_at"]),
        ).fetchall())
        active_keys: set[tuple[int, int]] = set()
        for placement in overlaps:
            overlap_start = max(placement["placed_at"], event["started_at"])
            overlap_end = min(placement["removed_at"] or event["ended_at"], event["ended_at"])
            exposure = _minutes(overlap_start, overlap_end)
            recommendation, reason = self._recommend(event, rule, exposure, placement["lot_status"])
            key = (int(placement["lot_id"]), int(placement["id"]))
            active_keys.add(key)
            self.connection.execute(
                "INSERT INTO environment_event_lots(event_id,lot_id,placement_id,container_code,"
                "overlap_started_at,overlap_ended_at,exposure_minutes,recommendation,recommendation_reason) "
                "VALUES(?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(event_id,lot_id,placement_id) DO UPDATE SET "
                "container_code=excluded.container_code,overlap_started_at=excluded.overlap_started_at,"
                "overlap_ended_at=excluded.overlap_ended_at,exposure_minutes=excluded.exposure_minutes,"
                "recommendation=excluded.recommendation,recommendation_reason=excluded.recommendation_reason "
                "WHERE environment_event_lots.disposition_status='pending'",
                (
                    event_id, placement["lot_id"], placement["id"], placement["container_code"],
                    overlap_start, overlap_end, exposure, recommendation, reason,
                ),
            )
        # 窗口收窄后不再重叠的待处理影响行删除；已人工处置的行保留作为审计证据。
        if active_keys:
            keys_values: list[Any] = []
            clauses: list[str] = []
            for lot_id, placement_id in active_keys:
                clauses.append("(lot_id=? AND placement_id=?)")
                keys_values.extend([lot_id, placement_id])
            self.connection.execute(
                f"DELETE FROM environment_event_lots WHERE event_id=? AND disposition_status='pending' "
                f"AND NOT ({' OR '.join(clauses)})",
                [event_id, *keys_values],
            )

    @staticmethod
    def _recommend(
        event: dict[str, Any], rule: dict[str, Any], exposure_minutes: int, lot_status: str
    ) -> tuple[str, str]:
        if lot_status in {"depleted", "disposed"}:
            return "monitor", "批次已耗尽或报废，仅保留暴露记录"
        hold_threshold = int(
            rule["critical_hold_minutes"] if event["severity"] == "critical" else rule["hold_minutes"]
        )
        if exposure_minutes >= hold_threshold:
            return "hold", (
                f"{event['severity']} 级别异常暴露 {exposure_minutes} 分钟，达到冻结阈值 {hold_threshold} 分钟"
            )
        if exposure_minutes >= int(rule["retest_minutes"]):
            return "early_retest", (
                f"异常暴露 {exposure_minutes} 分钟，达到提前复检阈值 {int(rule['retest_minutes'])} 分钟"
            )
        return "monitor", f"异常暴露 {exposure_minutes} 分钟，未达处置阈值，持续观察"

    # --------------------------------------------------------------- 查询追溯

    def _require_event(self, event_id: int) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM environment_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFoundError("环境异常事件不存在")
        return dict(row)

    def list_events(
        self, *, status: str | None = None, location_id: int | None = None, limit: int = 50, offset: int = 0
    ) -> tuple[list[dict[str, Any]], int]:
        where: list[str] = []
        params: list[Any] = []
        if status:
            where.append("e.status=?")
            params.append(status)
        if location_id:
            where.append("e.location_id=?")
            params.append(location_id)
        clause = " WHERE " + " AND ".join(where) if where else ""
        total = int(self.connection.execute(
            f"SELECT COUNT(*) FROM environment_events e{clause}", params
        ).fetchone()[0])
        params.extend([limit, offset])
        rows = self.connection.execute(
            "SELECT e.*,s.location_code FROM environment_events e "
            "JOIN storage_locations s ON s.id=e.location_id" + clause
            + " ORDER BY e.started_at DESC,e.id DESC LIMIT ? OFFSET ?",
            params,
        ).fetchall()
        return records(rows), total

    def event_detail(self, event_id: int) -> dict[str, Any]:
        event = self._require_event(event_id)
        event["location"] = self.repository.require_location(int(event["location_id"]))
        event["rule"] = self._require_rule(int(event["rule_id"]))
        event["readings"] = records(self.connection.execute(
            "SELECT r.* FROM environment_readings r "
            "JOIN environment_event_readings m ON m.reading_id=r.id WHERE m.event_id=? "
            "ORDER BY r.observed_at,r.id",
            (event_id,),
        ).fetchall())
        event["alerts"] = records(self.connection.execute(
            "SELECT a.id,a.alert_key,a.severity,a.message,a.status,a.created_at,a.acknowledged_by,a.resolved_at "
            "FROM quality_alerts a JOIN environment_event_readings m "
            "ON json_extract(a.detail_json,'$.reading_id')=m.reading_id "
            "WHERE m.event_id=? AND a.alert_type='environment_excursion' ORDER BY a.id",
            (event_id,),
        ).fetchall())
        impacts = records(self.connection.execute(
            "SELECT x.*,l.lot_no,l.status AS lot_status,a.id AS accession_id,a.accession_no,a.crop_name "
            "FROM environment_event_lots x JOIN seed_lots l ON l.id=x.lot_id "
            "JOIN accessions a ON a.id=l.accession_id WHERE x.event_id=? ORDER BY x.id",
            (event_id,),
        ).fetchall())
        for impact in impacts:
            impact["actions"] = records(self.connection.execute(
                "SELECT * FROM environment_event_actions WHERE event_lot_id=? ORDER BY id", (impact["id"],)
            ).fetchall())
        event["impacts"] = impacts
        event["pending_count"] = sum(1 for item in impacts if item["disposition_status"] == "pending")
        event["confirmed_count"] = sum(1 for item in impacts if item["disposition_status"] == "confirmed")
        event["dismissed_count"] = sum(1 for item in impacts if item["disposition_status"] == "dismissed")
        if event.get("merged_into_event_id"):
            event["merged_into"] = self._require_event(int(event["merged_into_event_id"]))["event_no"]
        return event

    # ----------------------------------------------------------- 人工确认处置

    def confirm_event(self, event_id: int, data: dict[str, Any]) -> dict[str, Any]:
        event = self._require_event(event_id)
        if int(event["version"]) != int(data["expected_version"]):
            raise ConflictError("环境异常事件版本冲突", context={"current_version": event["version"]})
        if event["status"] not in {"open", "confirmed"}:
            raise ConflictError("只有进行中或部分确认的事件可以继续处置", context={"status": event["status"]})
        decisions = {int(item["event_lot_id"]): item for item in data.get("decisions", [])}
        impacts = records(self.connection.execute(
            "SELECT * FROM environment_event_lots WHERE event_id=?", (event_id,)
        ).fetchall())
        unknown = set(decisions) - {int(item["id"]) for item in impacts}
        if unknown:
            raise ValidationError("存在不属于该事件的受影响记录", context={"event_lot_ids": sorted(unknown)})

        actor_id = data.get("actor_user_id")
        actor = data["actor"]
        timestamp = to_storage(self.clock.now())
        applied = 0
        for impact in impacts:
            decision = decisions.get(int(impact["id"]))
            if decision is not None:
                applied += self._apply_decision(event, impact, decision, actor, actor_id, timestamp)

        if applied == 0:
            # 全部为重复提交：不改状态、不递增版本、不产生任何处置。
            return self.event_detail(event_id)
        remaining = int(self.connection.execute(
            "SELECT COUNT(*) FROM environment_event_lots WHERE event_id=? AND disposition_status='pending'",
            (event_id,),
        ).fetchone()[0])
        new_status = "confirmed" if remaining == 0 else "open"
        if remaining == 0:
            self.connection.execute(
                "UPDATE environment_events SET status=?,closed_at=?,version=version+1,updated_at=? WHERE id=?",
                (new_status, timestamp, timestamp, event_id),
            )
        else:
            self.connection.execute(
                "UPDATE environment_events SET status=?,version=version+1,updated_at=? WHERE id=?",
                (new_status, timestamp, event_id),
            )
        return self.event_detail(event_id)

    def dismiss_event(self, event_id: int, data: dict[str, Any]) -> dict[str, Any]:
        event = self._require_event(event_id)
        if int(event["version"]) != int(data["expected_version"]):
            raise ConflictError("环境异常事件版本冲突", context={"current_version": event["version"]})
        if event["status"] != "open":
            raise ConflictError("只有进行中的事件可以驳回")
        if not data.get("reason"):
            raise ValidationError("驳回事件时必须填写原因")
        timestamp = to_storage(self.clock.now())
        self.connection.execute(
            "UPDATE environment_event_lots SET disposition_status='dismissed',override_action='none',"
            "override_by=?,override_at=?,override_reason=? WHERE event_id=? AND disposition_status='pending'",
            (data["actor"], timestamp, data["reason"], event_id),
        )
        self.connection.execute(
            "UPDATE environment_events SET status='dismissed',closed_at=?,version=version+1,updated_at=? WHERE id=?",
            (timestamp, timestamp, event_id),
        )
        self._audit(
            data.get("actor_user_id"), data["actor"], "environment.event.dismiss", event,
            metadata={"reason": data["reason"]}, created_at=timestamp,
        )
        return self.event_detail(event_id)

    def _apply_decision(
        self,
        event: dict[str, Any],
        impact: dict[str, Any],
        decision: dict[str, Any],
        actor: str,
        actor_id: int | None,
        timestamp: str,
    ) -> int:
        action = decision["action"]
        if action not in {"hold", "early_retest", "none"}:
            raise ValidationError("处置动作必须是 hold、early_retest 或 none")
        if impact["disposition_status"] != "pending":
            return 0  # 重复提交同一影响行直接跳过，绝不重复冻结或重复建复检

        if action == "none":
            self.connection.execute(
                "UPDATE environment_event_lots SET disposition_status='dismissed',override_action='none',"
                "override_by=?,override_at=?,override_reason=? WHERE id=?",
                (actor, timestamp, decision.get("reason", "人工确认无需处置"), impact["id"]),
            )
            self._audit(
                actor_id, actor, "environment.event.override", event,
                metadata={"event_lot_id": impact["id"], "lot_id": impact["lot_id"], "action": "none",
                          "recommendation": impact["recommendation"], "reason": decision.get("reason", "")},
                created_at=timestamp,
            )
            return 1

        if action == "hold":
            hold_id = self._impose_quality_hold(event, impact, actor, timestamp)
            action_id = self._insert_action(
                event, impact, "hold", reference_id=hold_id, actor=actor, timestamp=timestamp
            )
            self._audit(
                actor_id, actor, "environment.event.hold", event,
                metadata={"event_lot_id": impact["id"], "lot_id": impact["lot_id"], "hold_id": hold_id,
                          "action_id": action_id, "recommendation": impact["recommendation"],
                          "overridden": action != impact["recommendation"], "reason": decision.get("reason", "")},
                created_at=timestamp,
            )
        else:
            test_id = self._schedule_early_retest(event, impact, decision, actor, timestamp)
            action_id = self._insert_action(
                event, impact, "early_retest", reference_id=test_id, actor=actor, timestamp=timestamp
            )
            self._audit(
                actor_id, actor, "environment.event.early_retest", event,
                metadata={"event_lot_id": impact["id"], "lot_id": impact["lot_id"], "test_id": test_id,
                          "action_id": action_id, "recommendation": impact["recommendation"],
                          "overridden": action != impact["recommendation"], "reason": decision.get("reason", "")},
                created_at=timestamp,
            )
        # 人工选择与系统建议不一致时保留覆盖痕迹。
        if action != impact["recommendation"] and impact["recommendation"] in {"hold", "early_retest"}:
            self.connection.execute(
                "UPDATE environment_event_lots SET override_action=?,override_by=?,override_at=?,"
                "override_reason=? WHERE id=?",
                (action, actor, timestamp, decision.get("reason", "人工覆盖系统建议"), impact["id"]),
            )
            self._audit(
                actor_id, actor, "environment.event.override", event,
                metadata={"event_lot_id": impact["id"], "lot_id": impact["lot_id"],
                          "recommendation": impact["recommendation"], "chosen_action": action,
                          "reason": decision.get("reason", "")},
                created_at=timestamp,
            )
        self.connection.execute(
            "UPDATE environment_event_lots SET disposition_status='confirmed' WHERE id=? "
            "AND disposition_status='pending'",
            (impact["id"],),
        )
        return 1

    def _impose_quality_hold(
        self, event: dict[str, Any], impact: dict[str, Any], actor: str, timestamp: str
    ) -> int:
        existing = self.connection.execute(
            "SELECT id FROM lot_holds WHERE lot_id=? AND hold_type='质量' AND released_at IS NULL",
            (impact["lot_id"],),
        ).fetchone()
        if existing:
            return int(existing[0])
        cursor = self.connection.execute(
            "INSERT INTO lot_holds(lot_id,hold_type,reason,imposed_by,imposed_at) VALUES(?, '质量', ?, ?, ?)",
            (impact["lot_id"], f"环境异常事件 {event['event_no']} 确认质量冻结", actor, timestamp),
        )
        hold_id = int(cursor.lastrowid)
        self.connection.execute(
            "UPDATE seed_lots SET status='held',version=version+1,updated_at=? WHERE id=? "
            "AND status NOT IN ('depleted','disposed')",
            (timestamp, impact["lot_id"]),
        )
        return hold_id

    def _schedule_early_retest(
        self,
        event: dict[str, Any],
        impact: dict[str, Any],
        decision: dict[str, Any],
        actor: str,
        timestamp: str,
    ) -> int:
        rule = self._require_rule(int(event["rule_id"]))
        lot = self.repository.require_lot(int(impact["lot_id"]))
        accession = self.repository.require_accession(int(lot["accession_id"]))
        protocol = record(self.connection.execute(
            "SELECT * FROM viability_protocols WHERE crop_name=? AND active=1 ORDER BY version DESC,id DESC LIMIT 1",
            (accession["crop_name"],),
        ).fetchone())
        if protocol is None:
            raise ConflictError(
                "该作物尚无生效的活力检测规程，无法安排提前复检",
                context={"lot_id": impact["lot_id"], "crop_name": accession["crop_name"]},
            )
        sampled_grams = float(decision.get("sampled_grams") or 5.0)
        if sampled_grams > float(lot["available_weight_grams"]) + 1e-9:
            raise ConflictError("提前复检取样重量超过批次可用重量", context={"event_lot_id": impact["id"]})
        due_on = (self.clock.now() + timedelta(days=int(rule["retest_due_days"]))).date().isoformat()
        test_no = f"EVT-{event['event_no']}-L{int(impact['lot_id'])}"
        self.connection.execute(
            "INSERT OR IGNORE INTO viability_tests(test_no,lot_id,protocol_id,test_type,sampled_grams,"
            "scheduled_for,status,requested_by,created_at,updated_at) "
            "VALUES(?,?,?, '异常复核', ?,?, 'scheduled',?,?,?)",
            (test_no, lot["id"], protocol["id"], sampled_grams, due_on, actor, timestamp, timestamp),
        )
        test = record(self.connection.execute(
            "SELECT * FROM viability_tests WHERE test_no=?", (test_no,)
        ).fetchone())
        self.connection.execute(
            "UPDATE retest_schedules SET status='superseded',updated_at=? WHERE lot_id=? "
            "AND status IN ('pending','notified')",
            (timestamp, lot["id"]),
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO retest_schedules(lot_id,source_test_id,policy_id,due_on,status,reason,"
            "created_at,updated_at) VALUES(?,?,NULL,?,'scheduled',?,?,?)",
            (
                lot["id"], test["id"], due_on,
                f"环境异常事件 {event['event_no']} 要求提前复检", timestamp, timestamp,
            ),
        )
        return int(test["id"])

    def _insert_action(
        self,
        event: dict[str, Any],
        impact: dict[str, Any],
        action_type: str,
        *,
        reference_id: int,
        actor: str,
        timestamp: str,
    ) -> int:
        idempotency_key = f"env-{action_type}-{int(impact['id'])}"
        existing = self.connection.execute(
            "SELECT id FROM environment_event_actions WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        if existing:
            return int(existing[0])
        hold_column = "hold_id" if action_type == "hold" else "test_id"
        cursor = self.connection.execute(
            f"INSERT INTO environment_event_actions(event_id,event_lot_id,lot_id,action_type,{hold_column},"
            "idempotency_key,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (event["id"], impact["id"], impact["lot_id"], action_type, reference_id,
             idempotency_key, actor, timestamp),
        )
        return int(cursor.lastrowid)

    def _audit(
        self,
        actor_id: int | None,
        actor: str,
        action: str,
        event: dict[str, Any],
        *,
        metadata: dict[str, Any],
        created_at: str,
    ) -> None:
        from app.services.audit import AuditContext, AuditService

        AuditService(self.connection, self.clock).record(
            AuditContext(actor_id, actor),
            action=action,
            resource_type="environment_event",
            resource_id=event["event_no"],
            after={"event_id": event["id"], "status": event["status"]},
            metadata=metadata,
        )

    # ----------------------------------------------------------- 作业恢复执行

    def execute_job(self, job: dict[str, Any], worker: str) -> dict[str, Any]:
        """执行单个已领取作业；调用方负责事务提交，异常向外抛出由调用方回滚。"""
        from app.services.jobs import JobService

        jobs = JobService(self.connection, self.clock)
        payload = json.loads(job["payload_json"])
        result = self.detect_location(int(payload["location_id"]))
        jobs.complete(int(job["id"]), worker, result)
        return result
