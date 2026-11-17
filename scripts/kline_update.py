#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股全市场日线增量更新（GitHub Actions 版）
=====================================
职责：
 1. 交易日守卫：腾讯 sh000001 快照日期 != 今天 -> 非交易日，退出 0；
 2. 全市场清单：东财分页（沪深主板/创业板/科创板）；
 3. 增量下载：最近 15 个自然日（覆盖复权修正），东财主源 + 腾讯兜底；
 4. 产出 data/YYYYMMDD.tsv.gz（date >= 今日-15天 全量行，TSV 列与
    本地库 daily_kline 完全一致：symbol,date,open,high,low,close,
    volume,amount,turnover,adjust）+ update_summary.json；
 5. 由 workflow 提交到仓库 data/ 分支目录并上传 artifact，
    灵光侧每日拉取合并进 2GB 本地库（delete >= min(delta date) + insert）。

注意：Actions 运行器在海外，若东财接口被墙/限流自动落到腾讯源，
两源全败超过 20% 时以退出码 1 让 run 失败（灵光每日核对 run 状态）。
"""
import gzip
import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"
ADJUST = "qfq"
WORKERS = 8
BATCH = 300
LOOKBACK_DAYS = 15
MIN_OK_RATIO = 0.80          # 成功只数低于清单 80% 判失败
OUT_DIR = os.environ.get("OUT_DIR", "data")
EM_HOSTS = ["https://push2his.eastmoney.com",
            "https://92.push2his.eastmoney.com",
            "http://push2his.eastmoney.com"]
EM_SECID = {"sh": "1.", "sz": "0."}
TX_URL = "http://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
SINA_LIST_URL = ("https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
                 "Market_Center.getHQNodeData")
SINA_REF = "https://finance.sina.com.cn"
EM_DISABLED = False   # 探测东财不可达时置 True，全部直落腾讯源

session = requests.Session()
session.headers.update({"User-Agent": UA})


def log(msg):
    print(f"{datetime.now():%H:%M:%S} {msg}", flush=True)


def is_trading_day(today: str) -> bool:
    try:
        r = session.get("https://qt.gtimg.cn/q=sh000001", timeout=10,
                        headers={"Referer": "https://gu.qq.com/"})
        r.encoding = "gbk"
        f = r.text.split('="')[1].split("~")
        return f[30].startswith(today.replace("-", ""))
    except Exception:
        # 探测失败时不拦：按交易日继续，后续下载若全部为空会自然退出
        log(f"交易日探测失败，按交易日继续: {traceback.format_exc(limit=1)[:120]}")
        return True


def get_stock_list():
    syms, total, page = [], None, 1
    while True:
        rows = []
        for attempt in range(3):
            try:
                r = session.get(
                    "https://push2.eastmoney.com/api/qt/clist/get",
                    params={"pn": page, "pz": 200, "po": 1, "np": 1, "fltt": 2,
                            "invt": 2, "fid": "f3",
                            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
                            "fields": "f12"}, timeout=15)
                data = r.json().get("data") or {}
                rows = data.get("diff") or []
                if total is None:
                    total = data.get("total", 0)
                break
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(3)
        codes = [d["f12"] for d in rows if not d["f12"].startswith(("4", "8", "92"))]
        if not codes:
            break
        syms.extend(codes)
        if total and len(syms) >= total:
            break
        page += 1
        time.sleep(0.2)
    syms = sorted(set(syms))
    if len(syms) >= 4000:
        return syms
    log(f"东财清单不可用（仅 {len(syms)} 只），改用新浪 hs_a 备源")
    syms2 = get_stock_list_sina()
    if len(syms2) < 4000:
        raise RuntimeError(f"股票清单异常（东财 {len(syms)} / 新浪 {len(syms2)} 只）")
    return syms2


def get_stock_list_sina():
    """新浪 hs_a 节点分页拉沪深A股（沙箱扫描同源；东财 clist 被拦时兜底）。"""
    syms, page = [], 1
    while page <= 120:
        try:
            r = session.get(SINA_LIST_URL,
                            params={"page": page, "num": 100, "sort": "code",
                                    "asc": 1, "node": "hs_a"},
                            headers={"Referer": SINA_REF}, timeout=15)
            arr = r.json()
        except Exception:
            break
        if not arr:
            break
        for it in arr:
            code = str(it.get("code", ""))
            if len(code) == 6 and code.isdigit() and code[:2] in ("60", "68", "00", "30"):
                syms.append(code)
        page += 1
        time.sleep(0.15)
    syms = sorted(set(syms))
    log(f"新浪清单 {len(syms)} 只")
    return syms


def sina_symbol(sym):
    return f"{'sh' if sym.startswith('6') else 'sz'}{sym}"


def _em_get(params, attempts=5):
    last = None
    for i in range(attempts):
        host = EM_HOSTS[i % len(EM_HOSTS)]
        try:
            r = session.get(host + "/api/qt/stock/kline/get", params=params, timeout=10)
            j = r.json()
            if isinstance(j, dict) and j.get("data"):
                return j
            last = RuntimeError(f"bad payload: {str(j)[:80]}")
        except Exception as e:
            last = e
        time.sleep(0.4 + i * 0.4)
    raise last


def download_one_em(sym, start, end):
    if EM_DISABLED:
        raise RuntimeError("EM disabled")
    s = sina_symbol(sym)
    params = {"secid": EM_SECID.get(s[:2], "1.") + s[2:], "klt": "101", "fqt": "1",
              "fields1": "f1,f2,f3,f4,f5,f6",
              "fields2": "f51,f52,f53,f54,f55,f56,f57", "beg": start, "end": end}
    kl = ((_em_get(params).get("data") or {}).get("klines")) or []
    rows = []
    for line in kl:
        p = line.split(",")
        if len(p) < 7:
            continue
        try:
            o, c, h, l = float(p[1]), float(p[2]), float(p[3]), float(p[4])
            v = float(p[5]) * 100      # 东财 volume 单位手
            a = float(p[6])
        except (ValueError, TypeError, IndexError):
            continue
        rows.append((sym, p[0], o, h, l, c, v, a, 0.0, ADJUST))
    return rows


def download_one_tx(sym, start, end):
    s = sina_symbol(sym)
    dashed = lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}" if len(x) == 8 else x
    r = session.get(TX_URL, params={"param": f"{s},day,{dashed(start)},{dashed(end)},320,qfq"},
                    timeout=10, headers={"Referer": "https://gu.qq.com/"})
    node = (r.json().get("data") or {}).get(s) or {}
    kl = node.get("qfqday") or node.get("day") or []
    rows = []
    for line in kl:
        if len(line) < 6:
            continue
        try:
            o, c, h, l = float(line[1]), float(line[2]), float(line[3]), float(line[4])
            v = float(line[5]) * 100   # 腾讯 volume 单位手
        except (ValueError, TypeError, IndexError):
            continue
        rows.append((sym, str(line[0]), o, h, l, c, v, 0.0, 0.0, ADJUST))
    return rows


def download_one(sym, start, end):
    for fn in (download_one_em, download_one_tx):
        try:
            rows = fn(sym, start, end)
            if rows:
                return rows
        except Exception:
            continue
    return []


def get_exrights_codes(today):
    """近15个自然日内除权除息的股票（东财名单），这些股票整段全历史重拉
    修正前复权基准；接口失败返回空集（本地合并按普通窗口处理）。"""
    codes = set()
    try:
        page = 1
        floor = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        while page <= 30:
            r = session.get(
                "https://datacenter-web.eastmoney.com/api/data/v1/get",
                params={"reportName": "RPT_SHAREBONUS_DET", "columns": "SECURITY_CODE,EX_DIVIDEND_DATE",
                        "pageSize": 500, "pageNumber": page,
                        "sortColumns": "EX_DIVIDEND_DATE", "sortTypes": "-1"},
                timeout=15)
            rows = ((r.json().get("result") or {}).get("data")) or []
            if not rows:
                break
            for x in rows:
                d = (x.get("EX_DIVIDEND_DATE") or "")[:10]
                if d and floor <= d <= today:
                    codes.add(x["SECURITY_CODE"])
            page += 1
    except Exception:
        log(f"除权名单获取失败，按空集处理: {traceback.format_exc(limit=1)[:120]}")
    return codes


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    if not is_trading_day(today):
        log(f"{today} 非A股交易日，退出")
        json.dump({"skipped": True, "date": today}, open("update_summary.json", "w"), ensure_ascii=False)
        return 0
    global EM_DISABLED
    try:
        _em_get({"secid": "1.000001", "klt": "101", "fqt": "1",
                 "fields1": "f1,f2,f3", "fields2": "f51,f53",
                 "beg": "20260101", "end": "20260110"}, attempts=2)
        log("东财接口探测可用")
    except Exception as e:
        EM_DISABLED = True
        log(f"东财接口探测失败（{type(e).__name__}），本次全部直落腾讯源")
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")
    syms = get_stock_list()
    exr = get_exrights_codes(today)
    log(f"清单 {len(syms)} 只，窗口 {start}~{end}，近15日除权 {len(exr)} 只整段重拉")
    all_rows = {}
    fails = 0
    for i in range(0, len(syms), BATCH):
        batch = syms[i:i + BATCH]
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(download_one, s, "19901219" if s in exr else start, end): s for s in batch}
            for fu in as_completed(futures):
                try:
                    rows = fu.result()
                except Exception:
                    rows = []
                if rows:
                    all_rows[futures[fu]] = rows
                else:
                    fails += 1
        log(f"进度 {i + len(batch)}/{len(syms)}")
    ok = len(all_rows)
    log(f"下载完成: 成功 {ok}, 双源皆败 {fails}")
    if ok < len(syms) * MIN_OK_RATIO:
        log(f"成功率 {ok/len(syms):.1%} 低于阈值 {MIN_OK_RATIO:.0%}，判失败")
        return 1
    rows = [r for sym_rows in all_rows.values() for r in sym_rows]
    rows.sort(key=lambda r: (r[0], r[1]))
    os.makedirs(OUT_DIR, exist_ok=True)
    out_gz = f"{OUT_DIR}/{datetime.now():%Y%m%d}.tsv.gz"
    with gzip.open(out_gz, "wt", encoding="utf-8") as f:
        for r in rows:
            f.write("\t".join("" if v is None else str(v) for v in r) + "\n")
    dates = sorted({r[1] for r in rows})
    summary = {"date": today, "symbols_ok": ok, "symbols_fail": fails,
               "rows": len(rows), "min_date": dates[0] if dates else None,
               "max_date": dates[-1] if dates else None, "file": out_gz,
               "replace_full": sorted(exr)}
    json.dump(summary, open("update_summary.json", "w"), ensure_ascii=False, indent=1)
    log(f"写出 {out_gz}: {len(rows)} 行, 日期 {dates[0]}~{dates[-1]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
