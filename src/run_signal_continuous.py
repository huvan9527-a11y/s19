#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prepare a prior-session S19 watchlist and monitor live VWAP reclaim signals."""
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import s19

POOL_PATH = ROOT / "state" / "premarket_candidates.json"
STATE_PATH = ROOT / "state" / "signal_state.json"


def clock(dt):
    return dt.strftime("%H:%M")


def push_once(state, key, title, body):
    sent = set(state.get("sent", []))
    if key in sent:
        return False
    s19.send_push(title, body)
    sent.add(key)
    state["sent"] = sorted(sent)
    s19.save_state(STATE_PATH, state)
    print(f"PUSH_OK {key}", flush=True)
    return True


def prepare_pool(now):
    source_day = s19.previous_session(now.date())
    last_error = None
    rows = None
    for attempt in range(3):
        try:
            rows = s19.fetch_pool(source_day, timeout=int(s19.CFG["request_timeout_seconds"]))
            break
        except Exception as exc:
            last_error = exc
            print(f"POOL_FETCH_RETRY attempt={attempt + 1} error={type(exc).__name__}", flush=True)
            if attempt < 2:
                time.sleep(attempt + 1)
    if rows is None:
        raise RuntimeError(f"prior-session pool unavailable: {type(last_error).__name__}")

    candidates, audit = s19.select_s19(rows)
    snapshot = {
        "session_date": now.date().isoformat(),
        "source_session_date": source_day.isoformat(),
        "prepared_at": now.isoformat(),
        "source": "THS_previous_session_limit_up_pool",
        "raw_count": len(rows),
        "audit": audit,
        "candidates": candidates,
    }
    s19.atomic_json(POOL_PATH, snapshot)
    s19.journal({"status": "PREMARKET_POOL", **snapshot}, now)
    print(
        f"PREMARKET_POOL_READY session={snapshot['session_date']} "
        f"source={snapshot['source_session_date']} raw={len(rows)} "
        f"strong_themes={audit['strong_themes']} candidates={len(candidates)}",
        flush=True,
    )
    return snapshot


def watchlist_push(snapshot, state, now):
    candidates = snapshot["candidates"]
    key = "WATCHLIST_READY"
    if not candidates:
        audit = snapshot["audit"]
        body = (
            f"交易日：{snapshot['session_date']}\n"
            f"前一交易日：{snapshot['source_session_date']}\n"
            f"原始涨停池：{snapshot['raw_count']} 只\n"
            f"识别题材：{audit['themes_seen']} 个；达宽度条件：{audit['strong_themes']} 个\n"
            f"连板/龙头候选：{audit['candidate_roles_before_acceptance']} 只\n"
            f"承接筛选淘汰：{audit['acceptance_rejected']}\n"
            "最终候选：0。数据请求成功、日期有效；本次为空由以上筛选结果产生。"
        )
        push_once(state, key, "S19 盘前候选池｜暂无候选", body)
        return
    entries = [
        f"{item['name']} {item['code']}｜{item['theme']}｜{item['role']}｜{item['streak']}板"
        for item in candidates
    ]
    body = (
        f"交易日：{snapshot['session_date']}\n前一交易日数据：{snapshot['source_session_date']}\n"
        "候选来自前一交易日收盘涨停池，盘中固定跟踪。\n\n"
        + "\n".join(entries)
        + "\n\n盘中触发：连续竞价实时价从成交均价（VWAP）下方上穿。"
    )
    push_once(state, key, "S19 盘前候选池已就绪", body)


def scan(snapshot, state, now):
    fired = set(state.get("fired", []))
    last_quotes = state.setdefault("last_quotes", {})
    events, pushed = [], []
    for candidate in snapshot["candidates"]:
        code = candidate["code"]
        try:
            quote = s19.quote(code)
        except Exception as exc:
            events.append({"code": code, "status": "QUOTE_ERROR", "error": type(exc).__name__})
            continue
        if not s19.valid_quote(quote, now, int(s19.CFG["max_quote_age_seconds"])):
            events.append({"code": code, "status": "QUOTE_STALE_OR_INVALID"})
            continue

        stamp = quote["timestamp"]
        if clock(now) < s19.CFG["entry_start"]:
            events.append({"code": code, "status": "AUCTION_OBSERVATION", "quote_time": stamp.isoformat()})
            continue
        if quote["vwap"] is None:
            events.append({"code": code, "status": "VWAP_UNAVAILABLE", "quote_time": stamp.isoformat()})
            continue

        previous = last_quotes.get(code)
        crossed = (
            code not in fired
            and previous is not None
            and previous.get("vwap") is not None
            and float(previous["price"]) <= float(previous["vwap"])
            and quote["price"] > quote["vwap"]
        )
        last_quotes[code] = {
            "price": quote["price"], "vwap": quote["vwap"],
            "quote_time": stamp.isoformat(),
        }
        status = "VWAP_CROSS"
        if crossed:
            sealed = quote["price"] >= quote["upper"] - float(s19.CFG["limit_price_epsilon"])
            stage = "UNFILLED" if sealed else "SIGNAL"
            key = f"{code}|VWAP_CROSS"
            body = (
                f"发现时间：{now:%Y-%m-%d %H:%M:%S}（北京时间）\n"
                f"行情时间：{stamp:%H:%M:%S}；行情年龄：{max(0, (now-stamp).total_seconds()):.1f} 秒\n"
                f"股票：{candidate['name']} {code}\n题材：{candidate['theme']}；"
                f"角色：{candidate['role']}；前一日连板：{candidate['streak']}板\n"
                f"现价：{quote['price']:.2f}；成交均价（VWAP）：{quote['vwap']:.2f}；"
                f"涨停价：{quote['upper']:.2f}\n"
                f"状态：{stage}。此提醒不是下单指令，也不保证成交。"
            )
            if push_once(state, key, f"S19 均价上穿｜{candidate['name']}", body):
                pushed.append(code)
                fired.add(code)
            status = "VWAP_CROSS_UNFILLED" if sealed else "VWAP_CROSS_SIGNAL"
        events.append({
            "code": code, "status": status,
            "price": quote["price"], "vwap": quote["vwap"],
            "upper": quote["upper"], "quote_time": stamp.isoformat(),
        })

    state["last_quotes"] = last_quotes
    state["fired"] = sorted(fired)
    s19.save_state(STATE_PATH, state)
    s19.journal({
        "status": "LIVE_SCAN", "source_session_date": snapshot["source_session_date"],
        "candidate_count": len(snapshot["candidates"]), "evaluated": events, "pushed": pushed,
    }, now)
    print(f"LIVE_SCAN_OK at={now:%H:%M:%S} watch={len(snapshot['candidates'])} pushes={len(pushed)}", flush=True)


def main():
    if not os.getenv("SERVERCHAN_SENDKEY", "").strip():
        raise RuntimeError("SERVERCHAN_SENDKEY missing")
    now = s19.now_bj()
    if not s19.is_session(now.date()):
        print("NON_TRADING_DAY_SKIP", flush=True)
        return 0

    snapshot = prepare_pool(now)
    state = s19.load_state(STATE_PATH, now.date().isoformat())
    watchlist_push(snapshot, state, now)
    print(
        f"SIGNAL_START {now.isoformat()} window={s19.CFG['monitor_start']}–"
        f"{s19.CFG['scan_end']} interval={s19.CFG['scan_interval_seconds']}s",
        flush=True,
    )
    while clock(s19.now_bj()) < s19.CFG["monitor_start"]:
        time.sleep(5)

    while clock(s19.now_bj()) <= s19.CFG["scan_end"]:
        now = s19.now_bj()
        if snapshot["candidates"]:
            scan(snapshot, state, now)
        time.sleep(int(s19.CFG["scan_interval_seconds"]))
    print("SCAN_WINDOW_CLOSED", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
