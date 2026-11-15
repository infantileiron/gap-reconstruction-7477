"""逐站读数接收与连续缺测预警。

在线语义（与离线 RTS 严格区分）
--------------------------------
* 只看「已经到达并被受理」的读数：补值与区间只由该读数时刻之前（含该时刻）的
  读数给出。离线 RTS 会用未来观测，本模块完全不用。
* 首条实测之前没有任何训练观测，所有补值与区间标为 ``unestimable``；首个实测
  到达后也不回填更早时点，历史永远保持不可估标记。
* 同一批里按实际时刻顺序逐条推进；同一站点的并发请求由站点锁串行化，先到的
  批次先推进。读数时刻不晚于该站已受理游标的批次属于迟到冲突，整批拒绝且
  不写入任何状态。
* 缺测时长按读数时刻的实际时间差累计；空值读数是「已确认没有观测」，仍然推进
  时间。时长达到阈值产生一次 ``gap_alert``，该次缺测结束回到实测时产生一次
  ``gap_recovered``（仅针对已预警的缺口，二者成对）。
* 每条通知都带产生它时的完整依据快照：阈值、缺口起止、期间逐条读数与补值、
  触发前后的滤波状态、所属批次摘要。快照不随后续读数改写。
* 批处理先纯函数计算再单事务落库；失败时不会留下半成品，重试得到与一次处理
  相同的滤波状态、缺测时长和通知（通知 ID 由站点/缺口/触发时刻确定）。
"""
import copy
import csv
import hashlib
import io
import json
import math

from fastapi import HTTPException

from src.constants.domain import (
    DEFAULT_GAP_ALERT_HOURS,
    INTERVAL_Z,
    MAX_BATCH_READINGS,
    MAX_STATIONS,
    MIN_TIME_STEP_HOURS,
)
from src.utils import repository as repo
from src.utils.identity import now

DEFAULT_CONFIG = {
    "q_per_hour": 0.03,
    "observation_r": 0.09,
    "initial_variance": 10000.0,
    "initial_level": None,
    "gap_threshold_hours": DEFAULT_GAP_ALERT_HOURS,
}
CONFIG_FIELDS = tuple(DEFAULT_CONFIG)
MISSING_MARKERS = ("", "na", "n/a", "null", "none", "-")


# --------------------------------------------------------------------------- 解析

def parse_value(text):
    """把读数文本解析成数值；缺失标记返回 None，非法数值抛 ValueError。"""
    raw = (text or "").strip()
    if raw.lower() in MISSING_MARKERS:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"读数“{raw}”不是数值，也不是空/NA/null缺失标记")
    if not math.isfinite(value) or abs(value) > 1000000:
        raise ValueError(f"读数“{raw}”必须是有限且绝对值不超过1e6的数值")
    return value


def parse_text(text):
    """解析 station,time_hour,value 文本；首行可为表头。"""
    reader = csv.reader(io.StringIO((text or "").lstrip("﻿")))
    readings = []
    for line_number, row in enumerate(reader, 1):
        cells = [c.strip() for c in row]
        if not any(cells):
            continue
        if line_number == 1 and [c.lower() for c in cells[:3]] == ["station", "time_hour", "value"]:
            continue
        if len(cells) < 3:
            raise ValueError(f"第{line_number}行需要 station,time_hour,value 三列")
        station, raw_time, raw_value = cells[0], cells[1], cells[2]
        try:
            time_hour = float(raw_time)
        except ValueError:
            raise ValueError(f"第{line_number}行时间“{raw_time}”不是数值")
        if not math.isfinite(time_hour) or not 0 <= time_hour <= 10000000:
            raise ValueError(f"第{line_number}行时间必须是[0,1e7]小时内的有限值")
        readings.append({"station": station, "time_hour": time_hour, "value": parse_value(raw_value)})
    if not readings:
        raise ValueError("没有可接收的读数")
    return readings


def _normalize(readings):
    """校验并规范化读数，返回按实际时刻排序的列表与站点集合。"""
    if not readings:
        raise ValueError("至少一条读数")
    if len(readings) > MAX_BATCH_READINGS:
        raise ValueError(f"单批最多{MAX_BATCH_READINGS}条读数")
    normalized = []
    for item in readings:
        station = (item["station"] or "").strip()
        if not 1 <= len(station) <= 30:
            raise ValueError("测站编号长度必须在1—30字符")
        time_hour = float(item["time_hour"])
        value = item.get("value")
        if not math.isfinite(time_hour) or not 0 <= time_hour <= 10000000:
            raise ValueError("读数时间必须是[0,1e7]小时内的有限值")
        if value is not None:
            value = float(value)
            if not math.isfinite(value) or abs(value) > 1000000:
                raise ValueError("读数必须是有限且绝对值不超过1e6的数值，缺失用空/NA/null")
        normalized.append({"station": station, "time_hour": time_hour, "value": value})
    normalized.sort(key=lambda r: (r["time_hour"], r["station"]))
    by_station = {}
    for reading in normalized:
        by_station.setdefault(reading["station"], []).append(reading["time_hour"])
    if any(
        b - a < MIN_TIME_STEP_HOURS
        for times in by_station.values()
        for a, b in zip(times, times[1:])
    ):
        raise ValueError("同一测站的读数须按实际时刻严格递增且步长至少1e-6小时；迟到或重复读数请分开提交")
    stations = sorted(by_station)
    if len(stations) > MAX_STATIONS:
        raise ValueError(f"最多{MAX_STATIONS}个测站")
    return normalized, stations


def _digest(normalized, config):
    payload = {"readings": normalized, "config": config}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()


# --------------------------------------------------------------------------- 状态

def empty_stream(station, config):
    return {
        "id": station,
        "station": station,
        "config": dict(config),
        "cursor": None,
        "filter": None,
        "readings": [],
        "notifications": [],
        "batches": [],
        "missing": {
            "active": False,
            "started_at": None,
            "completed_hours": 0.0,
            "current_hours": 0.0,
            "total_hours": 0.0,
            "alerted": False,
            "alert_id": None,
            "gap_index": 0,
            "readings": [],
        },
        "counters": {"readings": 0, "estimable": 0, "unestimable": 0, "alerts": 0, "recoveries": 0},
        "created_at": now(),
        "updated_at": now(),
    }


def _notification_id(station, kind, gap_index, anchor):
    raw = f"{station}|{kind}|{gap_index}|{anchor:.9f}".encode()
    return "ntf-" + hashlib.sha256(raw).hexdigest()[:12]


def _estimable(time_hour, mean, variance, observation_r, value, received_at, digest):
    state_sd = math.sqrt(max(0.0, variance))
    observation_sd = math.sqrt(max(0.0, variance + observation_r))
    is_observation = value is not None
    return {
        "time_hour": time_hour,
        "value": value,
        "is_observation": is_observation,
        "estimate_status": "estimable",
        "mean": mean,
        "state_variance": variance,
        "observation_variance": variance + observation_r,
        "state_low": mean - INTERVAL_Z * state_sd,
        "state_high": mean + INTERVAL_Z * state_sd,
        "observation_low": mean - INTERVAL_Z * observation_sd,
        "observation_high": mean + INTERVAL_Z * observation_sd,
        "fill_value": None if is_observation else mean,
        "received_at": received_at,
        "batch_digest": digest,
    }


def _unestimable(time_hour, received_at, digest):
    return {
        "time_hour": time_hour,
        "value": None,
        "is_observation": False,
        "estimate_status": "unestimable",
        "mean": None,
        "state_variance": None,
        "observation_variance": None,
        "state_low": None,
        "state_high": None,
        "observation_low": None,
        "observation_high": None,
        "fill_value": None,
        "reason": "首条实测尚未到达，补值与区间不可估",
        "received_at": received_at,
        "batch_digest": digest,
    }


def _basis_point(point):
    return {
        "time_hour": point["time_hour"],
        "value": point["value"],
        "estimate_status": point["estimate_status"],
        "mean": point["mean"],
        "state_low": point["state_low"],
        "state_high": point["state_high"],
        "observation_low": point["observation_low"],
        "observation_high": point["observation_high"],
    }


def _filter_snapshot(filt):
    if filt is None:
        return None
    return {"time_hour": filt["time_hour"], "mean": filt["mean"], "variance": filt["variance"]}


def _gap_basis(state, trigger, duration, filter_before):
    missing = state["missing"]
    config = state["config"]
    readings = missing["readings"] or []
    return {
        "station": state["station"],
        "threshold_hours": config["gap_threshold_hours"],
        "duration_hours": duration,
        "gap_started_at": missing["started_at"],
        "triggered_at": trigger,
        "gap_readings": [_basis_point(p) for p in readings],
        "filter_before": filter_before,
        "config": dict(config),
        "explanation": (
            "缺测时长按该站实际读数时刻之差累计；触发判断只使用触发时刻及之前已到达的读数。"
            "空值读数表示确认没有观测，期间补值为单侧Kalman预测，不使用任何未来读数。"
        ),
    }


def _alert(state, trigger, duration, digest, received_at, value=None, point=None):
    missing = state["missing"]
    basis = _gap_basis(state, trigger, duration, _filter_snapshot(state["filter"])) | {
        "trigger_reading": {"time_hour": trigger, "value": value},
        "trigger_estimate": _basis_point(point) if point is not None else None,
        "filter_at_trigger": _filter_snapshot(state["filter"]),
        "batch_digest": digest,
        "batch_received_at": received_at,
        "explanation": (
            "缺测时长按该站实际读数时刻之差累计，达到阈值即产生一次超限预警。"
            "触发判断只使用触发时刻及之前已到达的读数；空值读数表示确认没有观测，"
            "期间补值为单侧Kalman预测，不使用任何未来读数。"
        ),
    }
    notification = {
        "id": _notification_id(state["station"], "gap_alert", missing["gap_index"], trigger),
        "kind": "gap_alert",
        "station": state["station"],
        "duration_hours": duration,
        "created_at": received_at,
        "basis": basis,
    }
    missing["alerted"] = True
    missing["alert_id"] = notification["id"]
    state["notifications"].append(notification)
    state["counters"]["alerts"] += 1
    return notification


def _recovery(state, trigger, duration, digest, received_at, filter_before, filter_after):
    missing = state["missing"]
    basis = _gap_basis(state, trigger, duration, filter_before) | {
        "alert_id": missing["alert_id"],
        "filter_after": filter_after,
        "batch_digest": digest,
        "batch_received_at": received_at,
        "explanation": (
            "该次缺测此前已触发超限预警，本次实测到达后缺口结束并按实际时间结算；"
            "恢复通知与该次预警成对，不重复产生。"
        ),
    }
    notification = {
        "id": _notification_id(state["station"], "gap_recovered", missing["gap_index"], trigger),
        "kind": "gap_recovered",
        "station": state["station"],
        "duration_hours": duration,
        "created_at": received_at,
        "basis": basis,
    }
    state["notifications"].append(notification)
    state["counters"]["recoveries"] += 1
    return notification


def _apply_reading(state, reading, digest, received_at):
    """把一条已受理读数推进到站点状态；返回本条新产生的通知。"""
    config = state["config"]
    q, observation_r = config["q_per_hour"], config["observation_r"]
    threshold = config["gap_threshold_hours"]
    time_hour, value = reading["time_hour"], reading["value"]
    filt = state["filter"]
    missing = state["missing"]
    fresh = []

    if value is None:
        if not missing["active"]:
            missing["active"] = True
            missing["started_at"] = filt["time_hour"] if filt is not None else time_hour
            missing["gap_index"] += 1
            missing["alerted"] = False
            missing["alert_id"] = None
            missing["readings"] = []
        if filt is None:
            point = _unestimable(time_hour, received_at, digest)
        else:
            mean = filt["mean"]
            variance = filt["variance"] + q * (time_hour - filt["time_hour"])
            point = _estimable(time_hour, mean, variance, observation_r, None, received_at, digest)
        missing["readings"].append(point)
        missing["current_hours"] = time_hour - missing["started_at"]
        missing["total_hours"] = missing["completed_hours"] + missing["current_hours"]
        state["readings"].append(point)
        state["counters"]["readings"] += 1
        state["counters"]["estimable" if point["estimate_status"] == "estimable" else "unestimable"] += 1
        if missing["current_hours"] >= threshold and not missing["alerted"]:
            fresh.append(_alert(state, time_hour, missing["current_hours"], digest, received_at, point=point))
    else:
        duration = time_hour - missing["started_at"] if missing["active"] else None
        if missing["active"] and not missing["alerted"] and duration >= threshold:
            fresh.append(_alert(state, time_hour, duration, digest, received_at, value, point=missing["readings"][-1] if missing["readings"] else None))
        if filt is None:
            level = config["initial_level"]
            mean = value if level is None else level
            variance = config["initial_variance"]
        else:
            mean = filt["mean"]
            variance = filt["variance"] + q * (time_hour - filt["time_hour"])
        gain = variance / (variance + observation_r)
        mean = mean + gain * (value - mean)
        variance = observation_r * variance / (variance + observation_r)
        point = _estimable(time_hour, mean, variance, observation_r, value, received_at, digest)
        state["filter"] = {"time_hour": time_hour, "mean": mean, "variance": variance}
        state["readings"].append(point)
        state["counters"]["readings"] += 1
        state["counters"]["estimable"] += 1
        if missing["active"]:
            if missing["alerted"]:
                fresh.append(_recovery(state, time_hour, duration, digest, received_at, _filter_snapshot(filt), _filter_snapshot(state["filter"])))
            missing["completed_hours"] += duration
            missing["total_hours"] = missing["completed_hours"]
            missing["current_hours"] = 0.0
            missing["active"] = False
            missing["alerted"] = False
            missing["alert_id"] = None
            missing["started_at"] = None
            missing["readings"] = []
    state["cursor"] = time_hour
    state["updated_at"] = received_at
    return fresh


# --------------------------------------------------------------------------- 接收

def _summary(state):
    missing = state["missing"]
    return {
        "station": state["station"],
        "config": dict(state["config"]),
        "cursor": state["cursor"],
        "gap_active": missing["active"],
        "gap_started_at": missing["started_at"],
        "current_gap_hours": missing["current_hours"],
        "missing_hours_total": missing["total_hours"],
        "alerted": missing["alerted"],
        "counters": dict(state["counters"]),
        "filter": state["filter"],
        "updated_at": state["updated_at"],
    }


def _station_receipt(state, digest, added, new_notifications):
    return {
        "station": state["station"],
        "cursor": state["cursor"],
        "points_added": len(added),
        "estimable_added": sum(p["estimate_status"] == "estimable" for p in added),
        "unestimable_added": sum(p["estimate_status"] == "unestimable" for p in added),
        "notifications": [
            {"id": n["id"], "kind": n["kind"], "duration_hours": n["duration_hours"]}
            for n in new_notifications
        ],
        "gap_active": state["missing"]["active"],
        "missing_hours_total": state["missing"]["total_hours"],
        "batch_digest": digest,
    }


def _ingest_locked(readings, config, digest, read_at, commit=True):
    stations = sorted({r["station"] for r in readings})
    batch_times = {s: [r["time_hour"] for r in readings if r["station"] == s] for s in stations}
    existing = {s: repo.find("streams", s) for s in stations}

    # Replay: the exact same readings and parameters already accepted -> return the stored receipt.
    for state in existing.values():
        if state is None:
            continue
        stored = next((b for b in state["batches"] if b["batch_digest"] == digest), None)
        if stored is not None:
            return {
                "batch_digest": digest,
                "replayed": True,
                "received_at": stored["received_at"],
                "window": {"first_time": stored["first_time"], "last_time": stored["last_time"]},
                "count": stored["count"],
                "stations": stored["stations"],
            }

    for station in stations:
        current = existing[station]
        if current is None:
            continue
        for field in ("q_per_hour", "observation_r", "initial_variance", "initial_level"):
            if config[field] != current["config"][field]:
                raise HTTPException(409, f"{station} 的滤波参数已在首条读数时固定，不能中途修改；请新建站点流")
        if current["config"]["gap_threshold_hours"] != config["gap_threshold_hours"]:
            raise HTTPException(409, f"{station} 的缺测预警阈值已在首条读数时固定，不能中途修改")
        late = [t for t in batch_times[station] if current["cursor"] is not None and t <= current["cursor"]]
        if late:
            raise HTTPException(
                409,
                f"{station} 收到迟到读数 {late[0]}h（已受理至 {current['cursor']}h）：整批拒绝，原状态保持不变",
            )

    working = {
        station: empty_stream(station, config) if existing[station] is None else copy.deepcopy(existing[station])
        for station in stations
    }
    before = {station: len(working[station]["readings"]) for station in stations}
    new_by_station = {station: [] for station in stations}
    for reading in readings:
        state = working[reading["station"]]
        new_by_station[reading["station"]].extend(_apply_reading(state, reading, digest, read_at))

    receipts = {}
    for station in stations:
        state = working[station]
        added = state["readings"][before[station]:]
        receipts[station] = _station_receipt(state, digest, added, new_by_station[station])
    ordered_receipts = [receipts[station] for station in stations]
    for station in stations:
        state = working[station]
        state["batches"].append({
            "batch_digest": digest,
            "received_at": read_at,
            "count": len(batch_times[station]),
            "first_time": batch_times[station][0],
            "last_time": batch_times[station][-1],
            "stations": ordered_receipts,
        })
    if commit:
        repo.put_many("streams", [working[station] for station in stations])
    return {
        "batch_digest": digest,
        "replayed": False,
        "received_at": read_at,
        "window": {"first_time": readings[0]["time_hour"], "last_time": readings[-1]["time_hour"]},
        "count": len(readings),
        "stations": ordered_receipts,
    }


def _resolve_config(payload, sample):
    base = dict(sample["config"]) if sample else dict(DEFAULT_CONFIG)
    for field in CONFIG_FIELDS:
        value = getattr(payload, field, None)
        if value is not None:
            base[field] = float(value)
    if base["gap_threshold_hours"] <= 0:
        raise ValueError("缺测预警阈值必须为正")
    if min(base["q_per_hour"], base["observation_r"], base["initial_variance"]) <= 0:
        raise ValueError("q/r/初始方差必须为正")
    return base


def _raw_readings(payload):
    raw = getattr(payload, "readings", None)
    if raw is None:
        return parse_text(getattr(payload, "text", "") or "")
    return [r.model_dump() if hasattr(r, "model_dump") else dict(r) for r in raw]


def ingest(payload, commit=True):
    readings = _raw_readings(payload)
    normalized, stations = _normalize(readings)
    sample = next((state for state in (repo.find("streams", s) for s in stations) if state is not None), None)
    config = _resolve_config(payload, sample)
    digest = _digest(normalized, config)
    read_at = now()

    with repo.guard_stations(stations):
        return _ingest_locked(normalized, config, digest, read_at, commit=commit)


def preview(payload):
    """Same pipeline as ingest, computed in memory only: nothing is written or notified."""
    return ingest(payload, commit=False)


def list_streams():
    return [_summary(state) for state in repo.all_rows("streams")]


def detail(station):
    state = repo.get("streams", station)
    return {
        **_summary(state),
        "missing": state["missing"],
        "readings": state["readings"],
        "notifications": state["notifications"],
        "batches": state["batches"],
    }


def notifications(station):
    return repo.get("streams", station)["notifications"]


def notification(station, notification_id):
    for item in notifications(station):
        if item["id"] == notification_id:
            return item
    raise HTTPException(404, "通知记录不存在")
