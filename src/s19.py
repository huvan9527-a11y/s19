#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""S19 candidate selection, market data adapters, and audit helpers."""
from __future__ import annotations

import json
import math
import os
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import requests

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
BJ = ZoneInfo(CFG.get("timezone", "Asia/Shanghai"))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
THS_FIELD = "199112,10,9001,330323,330324,330325,9002,330329,133971,133970,1968584,3475914,9003,9004"


def now_bj() -> datetime:
    return datetime.now(BJ)


@lru_cache(maxsize=1)
def calendar():
    return xcals.get_calendar("XSHG")


def is_session(day: date) -> bool:
    return calendar().is_session(str(day))


def previous_session(day: date) -> date:
    for offset in range(1, 15):
        candidate = day - timedelta(days=offset)
        if is_session(candidate):
            return candidate
    raise RuntimeError("could not find previous XSHG session within 14 days")


def atomic_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def journal(record: dict, dt: datetime | None = None) -> None:
    stamp = dt or now_bj()
    path = ROOT / "logs" / "signals" / f"{stamp:%Y-%m-%d}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"time": stamp.isoformat(), **record}, ensure_ascii=False) + "\n")


def fetch_pool(day: date, timeout: int = 12) -> list[dict]:
    """Fetch the prior completed session's THS limit-up pool; errors are not empty data."""
    url = "https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool"
    result, seen, total = [], set(), None
    for page in range(1, 51):
        params = {
            "page": page, "limit": 200, "field": THS_FIELD,
            "filter": "HS,GEM2STAR", "order_field": "330324",
            "order_type": "0", "date": day.strftime("%Y%m%d"),
        }
        response = requests.get(
            url, params=params,
            headers={"User-Agent": UA, "Referer": "https://data.10jqka.com.cn/market/longhu/"},
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("status_code") not in (None, 0):
            raise ValueError("invalid THS response/status")
        block = payload.get("data")
        if not isinstance(block, dict) or not isinstance(block.get("info"), list):
            raise ValueError("missing THS data.info")
        rows = block["info"]
        declared = (block.get("page") or {}).get("total")
        if declared is not None:
            declared = int(declared)
            if declared < 0 or (total is not None and total != declared):
                raise ValueError("pool total changed during pagination")
            total = declared
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("invalid THS row")
            code = str(row.get("code") or row.get("stock_code") or "").zfill(6)
            if not re.fullmatch(r"\d{6}", code) or code == "000000" or code in seen:
                raise ValueError("invalid or duplicate THS code")
            seen.add(code)
            result.append(row)
        if total is not None and len(result) >= total:
            if len(result) != total:
                raise ValueError("THS pool total mismatch")
            return result
        if not rows or (total is None and len(rows) < 200):
            if total is not None and len(result) != total:
                raise ValueError("incomplete THS pagination")
            return result
    raise ValueError("THS pagination limit exceeded")


def number(value, default=0.0) -> float:
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def integer(value, default=0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def split_themes(raw: str) -> list[str]:
    themes = []
    for item in re.split(r"[+＋]", raw or ""):
        item = re.sub(r"[（(].*?[)）]", "", item).strip()
        if item and len(item) <= 12 and item not in themes:
            themes.append(item)
    return themes


def parse_preview(raw) -> list[float]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if not isinstance(raw, list):
        return []
    output = []
    for item in raw:
        value = item.get("price") if isinstance(item, dict) and "price" in item else (
            item.get("value", item.get("rate")) if isinstance(item, dict) else item
        )
        try:
            value = float(value)
            level = value if isinstance(item, dict) and "price" in item else 1 + value / 100
            if math.isfinite(level) and level > 0:
                output.append(level)
        except (TypeError, ValueError):
            continue
    return output


def normalize(row: dict) -> dict:
    code = str(row.get("code") or row.get("stock_code") or "").zfill(6)
    name = str(row.get("name") or row.get("stock_name") or "")
    reason = row.get("reason_type") or row.get("reason") or ""
    streak = integer(row.get("high_days") or row.get("continue_num") or row.get("limit_up_days") or 1, 1)
    match = re.search(r"(\d+)\s*板", str(row.get("high_days") or ""))
    if match:
        streak = integer(match.group(1), streak)
    price = number(row.get("latest") or row.get("price") or row.get("current_price"))
    upper = number(row.get("limit_up_price") or row.get("ztp"))
    change = number(row.get("change_rate") or row.get("zf"))
    first = str(row.get("first_limit_up_time") or row.get("first_time") or row.get("first_limit_up") or "")
    preview = parse_preview(row.get("time_preview"))
    if upper > 0 and price > 0:
        sealed = price >= upper - 0.005
    else:
        threshold = 4.85 if "ST" in name.upper() else (
            19.7 if code.startswith(("300", "301", "688", "689")) else (
                29.7 if code.startswith(("4", "8", "92")) else 9.85
            )
        )
        sealed = change >= threshold
    return {
        "code": code, "name": name, "themes": split_themes(reason),
        "streak": streak, "price": price, "upper": upper,
        "change_pct": change, "first": first, "preview": preview, "sealed": sealed,
    }


def acceptance(stock: dict) -> tuple[bool, str]:
    """Legacy S19 prior-session support rule; does not evaluate live VWAP."""
    preview = stock["preview"]
    if len(preview) < 4:
        return False, "time_preview_points_lt_4"
    current, high, average = preview[-1], max(preview), sum(preview) / len(preview)
    if current < high * (1 - float(CFG["max_give"])):
        return False, "time_preview_retracement_over_limit"
    if current < average:
        return False, "time_preview_below_average"
    return True, "accepted"


def select_s19(rows: list[dict]) -> tuple[list[dict], dict]:
    normalized = [normalize(row) for row in rows]
    themes = defaultdict(list)
    for stock in normalized:
        for theme in stock["themes"]:
            themes[theme].append(stock)

    theme_stats = {
        theme: {
            "members": len(members),
            "sealed": sum(stock["sealed"] for stock in members),
        }
        for theme, members in themes.items()
    }
    strong = {
        theme: members for theme, members in themes.items()
        if theme_stats[theme]["sealed"] >= int(CFG["min_theme_sealed"])
    }
    candidates, audit = [], {
        "normalized_rows": len(normalized),
        "themes_seen": len(themes),
        "theme_stats": theme_stats,
        "strong_themes": len(strong),
        "candidate_roles_before_acceptance": 0,
        "streak_rejected": [],
        "acceptance_rejected": [],
        "selected": [],
    }
    ranked = []
    for theme, members in strong.items():
        members = sorted(members, key=lambda stock: (-stock["streak"], stock["first"] or "999999"))
        sealed_count = theme_stats[theme]["sealed"]
        if members and members[0]["streak"] >= int(CFG["min_streak"]):
            ranked.append((theme, members[0], "龙头", sealed_count))
        elif members:
            audit["streak_rejected"].append({
                "code": members[0]["code"], "theme": theme,
                "streak": members[0]["streak"], "required": int(CFG["min_streak"]),
            })
        if CFG["allow_second"] and len(members) >= 2:
            second = members[1]
            required = max(2, int(CFG["min_streak"]) - 1)
            if second["streak"] >= required:
                ranked.append((theme, second, "二龙头", sealed_count))
            else:
                audit["streak_rejected"].append({
                    "code": second["code"], "theme": theme,
                    "streak": second["streak"], "required": required,
                })

    ranked.sort(key=lambda item: (-item[3], -item[1]["streak"], item[1]["first"] or "999999"))
    audit["candidate_roles_before_acceptance"] = len(ranked)
    used = set()
    for theme, stock, role, sealed_count in ranked:
        if len(candidates) >= int(CFG["top_n"]):
            break
        if stock["code"] in used:
            continue
        accepted, reason = acceptance(stock)
        if not accepted:
            audit["acceptance_rejected"].append({
                "code": stock["code"], "theme": theme, "role": role, "reason": reason,
            })
            continue
        item = {
            "theme": theme, "role": role, "theme_sealed": sealed_count,
            **stock,
        }
        candidates.append(item)
        audit["selected"].append(stock["code"])
        used.add(stock["code"])
    audit["candidate_count"] = len(candidates)
    audit["weak_themes"] = len(themes) - len(strong)
    return candidates, audit

def quote(code: str):
    market = "sh" if code.startswith(("6", "9")) else "sz"
    response = requests.get(
        f"https://qt.gtimg.cn/q={market}{code}",
        headers={"User-Agent": UA, "Referer": "https://gu.qq.com/"},
        timeout=6,
    )
    response.raise_for_status()
    fields = response.content.decode("gbk", "strict").split("~")
    if len(fields) < 49 or fields[2] != code:
        return None
    stamp = datetime.strptime(fields[30], "%Y%m%d%H%M%S").replace(tzinfo=BJ)
    price, previous_close = number(fields[3]), number(fields[4])
    volume_hands, amount_wan = number(fields[6]), number(fields[37])
    volume_shares = volume_hands * 100
    vwap = amount_wan * 10000 / volume_shares if volume_shares > 0 and amount_wan > 0 else None
    return {
        "code": code, "name": fields[1], "price": price, "prev_close": previous_close,
        "open": number(fields[5]), "volume_hands": volume_hands,
        "amount_wan": amount_wan, "vwap": vwap,
        "upper": number(fields[47]), "lower": number(fields[48]), "timestamp": stamp,
    }


def valid_quote(item: dict | None, now: datetime, max_age: int) -> bool:
    if not item or not isinstance(item.get("timestamp"), datetime):
        return False
    age = (now - item["timestamp"]).total_seconds()
    return (
        item["timestamp"].date() == now.date()
        and -5 <= age <= max_age
        and item.get("price", 0) > 0
        and item.get("prev_close", 0) > 0
        and item.get("upper", 0) > 0
        and item.get("lower", 0) > 0
        and item["lower"] <= item["price"] <= item["upper"]
    )


def load_state(path: Path, today: str) -> dict:
    if path.exists():
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
            if state.get("date") == today:
                return state
        except (OSError, json.JSONDecodeError):
            pass
    return {"date": today, "sent": [], "last_quotes": {}, "fired": []}


def save_state(path: Path, state: dict) -> None:
    atomic_json(path, state)


def send_push(title: str, body: str) -> None:
    key = os.getenv("SERVERCHAN_SENDKEY", "").strip()
    if not key:
        raise RuntimeError("SERVERCHAN_SENDKEY missing")
    response = requests.post(
        f"https://sctapi.ftqq.com/{key}.send",
        data={"title": title[:32], "desp": body},
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise RuntimeError("Server酱 rejected the notification")
