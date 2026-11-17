#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""商品期货主连全周期增量更新（GitHub Actions 版）
=====================================
覆盖：新浪全部商品期货主连 76 只（排除股指国债，与本地 futures_kline 库同口径）
周期：5m / 15m / 30m / 60m / 1d，新浪免费源上限（5m 约 3~5 周）
口径：分钟级只保留已走完 K 线（ts <= now）；夜盘跨午夜保留原始时间戳
产出：data/futures/YYYYMMDD-HH.tsv.gz（ts >= 今-15天 全量行，列与本地
      fut_kline 完全一致：symbol,interval,ts,open,high,low,close,volume,oi,settle）
      + futures_summary.json
合并：灵光每日拉回本地库，delete >= min(delta ts) + insert，幂等

说明：不含天勤补充（天勤需账号）。新浪 1d 当日根若未发布，夜盘那轮
（北京 03:10）会带出，本地库次日自动补齐，不丢数据。

并行：2026-11-18 起 380 组请求改小规模线程池并发（FUT_WORKERS 可调，
默认 5），单线程串行约 10+ 分钟，压缩到 3~5 分钟；并发度刻意压低，
避免触发新浪免费源限频。
"""
import gzip
import json
import os
import re
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

CN_TZ = timezone(timedelta(hours=8))
NOW = datetime.now(CN_TZ)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"
SINA_REF = "https://finance.sina.com.cn"
API_MIN = ("https://stock2.finance.sina.com.cn/futures/api/jsonp.php"
           "/var%20t=/InnerFuturesNewService.getFewMinLine")
API_DAY = ("https://stock2.finance.sina.com.cn/futures/api/jsonp.php"
           "/var%20t=/InnerFuturesNewService.getDailyKLine")
OUT_DIR = os.environ.get("OUT_DIR", "data/futures")
LOOKBACK_DAYS = 15
MIN_OK_RATIO = 0.85
WORKERS = max(1, int(os.environ.get("FUT_WORKERS", "5")))
PER_CALL_DELAY = float(os.environ.get("FUT_CALL_DELAY", "0.05"))

SYMBOLS = sorted([
    "A0", "AD0", "AG0", "AL0", "AO0", "AP0", "AU0", "B0", "BC0", "BR0",
    "BU0", "BZ0", "C0", "CF0", "CJ0", "CS0", "CU0", "CY0", "EB0", "EC0",
    "EG0", "FB0", "FG0", "FU0", "HC0", "I0", "J0", "JD0", "JM0", "JR0",
    "L0", "LC0", "LG0", "LH0", "LR0", "LU0", "M0", "MA0", "NI0", "NR0",
    "OI0", "OP0", "P0", "PB0", "PD0", "PF0", "PG0", "PK0", "PL0", "PP0",
    "PR0", "PS0", "PT0", "PX0", "RB0", "RI0", "RM0", "RR0", "RS0", "RU0",
    "SA0", "SC0", "SF0", "SH0", "SI0", "SM0", "SN0", "SP0", "SR0", "SS0",
    "TA0", "UR0", "V0", "WH0", "Y0", "ZN0",
])
TIMEFRAMES = {"5m": "5", "15m": "15", "30m": "30", "60m": "60", "1d": "daily"}


def log(msg):
    print(f"{datetime.now(CN_TZ):%H:%M:%S} {msg}", flush=True)


def http_json(api, params):
    qs = urllib.parse.urlencode(params)
    req = urllib.request.Request(api + "?" + qs, headers={
        "User-Agent": UA, "Referer": SINA_REF, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=25) as r:
        txt = r.read().decode("utf-8", errors="replace")
    m = re.search(r"\((\[.*\])\)", txt, re.S)
    if not m:
        raise RuntimeError("响应不含 JSON")
    return json.loads(m.group(1))


def is_active_session(today: str, yesterday: str) -> bool:
    """交易日守卫：腾讯 sh000001 快照日期在今昨两天窗口内即有效
    （日盘跑 -> 快照=今天；凌晨跑 -> 快照=昨天）"""
    try:
        import codecs
        req = urllib.request.Request(
            "https://qt.gtimg.cn/q=sh000001",
            headers={"User-Agent": UA, "Referer": "https://gu.qq.com/"})
        with urllib.request.urlopen(req, timeout=10) as r:
            txt = codecs.decode(r.read(4096), "gbk", errors="replace")
        snap = txt.split('="')[1].split("~")[30][:8]   # YYYYMMDD
        return snap in (today, yesterday)
    except Exception:
        log(f"交易日探测失败，按有效会话继续: {traceback.format_exc(limit=1)[:120]}")
        return True


def fetch_bars(sym, tf, now_str, today):
    if tf == "1d":
        rows = http_json(API_DAY, {"symbol": sym})
        return [(it["d"], float(it["o"]), float(it["h"]), float(it["l"]),
                 float(it["c"]), float(it["v"]), float(it.get("p") or 0),
                 float(it.get("s") or 0) or None)
                for it in rows if it["d"] <= today]
    rows = http_json(API_MIN, {"symbol": sym, "type": TIMEFRAMES[tf]})
    return [(it["d"], float(it["o"]), float(it["h"]), float(it["l"]),
             float(it["c"]), float(it["v"]), float(it.get("p") or 0), None)
            for it in rows if it["d"] <= now_str]


def fetch_all_parallel(now_str, today):
    """线程池并发抓 76 品种 x 5 周期；失败不中断，汇总错误清单。"""
    tasks = [(sym, tf) for sym in SYMBOLS for tf in TIMEFRAMES]
    total = len(tasks)
    results = []
    errors = []
    counter = {"done": 0, "ok": 0, "fail": 0}
    lock = threading.Lock()

    def work(item):
        sym, tf = item
        try:
            time.sleep(PER_CALL_DELAY)
            bars = fetch_bars(sym, tf, now_str, today)
            return sym, tf, bars, None
        except Exception as e:
            return sym, tf, None, e

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for sym, tf, bars, err in pool.map(work, tasks):
            with lock:
                counter["done"] += 1
                if err is None:
                    results.extend((sym, tf, *b) for b in bars)
                    counter["ok"] += 1
                else:
                    errors.append(f"{sym}.{tf}: {err}")
                    counter["fail"] += 1
                if counter["done"] % 76 == 0 or counter["done"] == total:
                    log(f"进度 {counter['done']}/{total} 成功{counter['ok']} 失败{counter['fail']}")
    return counter["ok"], counter["fail"], results, errors


def main():
    today = NOW.strftime("%Y-%m-%d")
    yesterday = (NOW - timedelta(days=1)).strftime("%Y-%m-%d")
    now_str = NOW.strftime("%Y-%m-%d %H:%M:%S")
    if not is_active_session(today.replace("-", ""), yesterday.replace("-", "")):
        log(f"{now_str} 非期货交易时段，退出")
        json.dump({"skipped": True, "date": today},
                  open("futures_summary.json", "w"), ensure_ascii=False)
        return 0

    t0 = time.time()
    cutoff_ts = (NOW - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    ok, fail, all_rows, errors = fetch_all_parallel(now_str, today)
    log(f"抓取完成: 成功 {ok} 组, 失败 {fail}, 耗时 {time.time() - t0:.0f}s")
    if ok < len(SYMBOLS) * len(TIMEFRAMES) * MIN_OK_RATIO:
        log("成功率过低，判失败")
        if errors:
            log(f"失败清单(前30): {errors[:30]}")
        return 1

    delta = [r for r in all_rows if r[2] >= cutoff_ts]
    delta.sort(key=lambda r: (r[0], r[1], r[2]))
    os.makedirs(OUT_DIR, exist_ok=True)
    out_gz = f"{OUT_DIR}/{NOW:%Y%m%d-%H}.tsv.gz"
    with gzip.open(out_gz, "wt", encoding="utf-8") as f:
        for r in delta:
            f.write("\t".join("" if v is None else str(v) for v in r) + "\n")
    summary = {"date": now_str, "groups_ok": ok, "groups_fail": fail,
               "rows": len(delta), "cutoff": cutoff_ts, "file": out_gz,
               "elapsed_sec": round(time.time() - t0)}
    json.dump(summary, open("futures_summary.json", "w"), ensure_ascii=False, indent=1)
    log(f"写出 {out_gz}: {len(delta):,} 行 (ts>={cutoff_ts})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
