#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""近一年全量K线窗口打包（2026-11-17 起，方法一：公共仓库每日发布 Release 附件）
=================================================================
职责：
 1. 非交易日守卫：增量步骤 update_summary.json 标记 skipped -> 本脚本同样跳过；
 2. 第二趟下载：全市场每只股票取近 LOOKBACK_CAL_DAYS 自然日窗口（约340根日K，
    覆盖扫描公式所需的 300 根窗口），东财主源 + 腾讯兜底（复用 kline_update 同源）；
 3. 产出 data/full_year.tsv.gz（列与增量 tsv 完全一致：symbol,date,open,high,low,
    close,volume,amount,turnover,adjust）+ full_pack_summary.json；
 4. 由 workflow 上传到 Release 附件（固定名 kline-full-year.tsv.gz 每日覆盖，
    另留最近 3 个交易日的带日期包），不进 git 历史。
口径：qfq 前复权，与增量包同源同口径；除权股无需整段重拉——500日窗口当日
    整体取数时已按最新除权基准一致复权，私有仓库扫描直接整包使用。

用法（仅由 kline.yml workflow 调用）：
  python scripts/full_year_pack.py
退出码：0=成功或正常跳过；2=打包失败（workflow 中 continue-on-error，
    不影响增量提交；私有仓库扫描侧对陈旧包自动回退联网源）。
"""
import gzip
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from kline_update import get_stock_list, download_one, log, BATCH, WORKERS  # noqa: E402

LOOKBACK_CAL_DAYS = 500      # 约 340 根日K，留足扫描 300 根窗口 + 节假日余量
MIN_OK_RATIO = 0.90          # 成功只数低于清单 90% 判失败
OUT_DIR = os.environ.get("OUT_DIR", "data")
OUT_GZ = os.path.join(OUT_DIR, "full_year.tsv.gz")
SUMMARY = "full_pack_summary.json"


def load_delta_summary():
    try:
        with open("update_summary.json", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def main():
    delta = load_delta_summary()
    if delta and delta.get("skipped"):
        log("非A股交易日（增量已跳过），全量包不生成")
        json.dump({"skipped": True, "date": datetime.now().strftime("%Y-%m-%d")},
                  open(SUMMARY, "w", encoding="utf-8"), ensure_ascii=False)
        return 0
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=LOOKBACK_CAL_DAYS)).strftime("%Y%m%d")
    syms = get_stock_list()
    log(f"全量窗口 {start}~{end}，共 {len(syms)} 只")
    all_rows, fails = [], 0
    for i in range(0, len(syms), BATCH):
        batch = syms[i:i + BATCH]
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futs = {pool.submit(download_one, s, start, end): s for s in batch}
            for fu in as_completed(futs):
                try:
                    rows = fu.result()
                except Exception:
                    rows = []
                if rows:
                    all_rows.extend(rows)
                else:
                    fails += 1
        log(f"全量进度 {i + len(batch)}/{len(syms)}")
    ok = len(syms) - fails
    log(f"全量下载完成: 成功 {ok}, 双源皆败 {fails}")
    if ok < len(syms) * MIN_OK_RATIO:
        log(f"成功率 {ok/len(syms):.1%} 低于阈值 {MIN_OK_RATIO:.0%}，全量包判失败")
        json.dump({"ok": False, "date": datetime.now().strftime("%Y-%m-%d"),
                   "symbols_ok": ok, "symbols_fail": fails},
                  open(SUMMARY, "w", encoding="utf-8"), ensure_ascii=False)
        return 2
    all_rows.sort(key=lambda r: (r[0], r[1]))
    os.makedirs(OUT_DIR, exist_ok=True)
    with gzip.open(OUT_GZ, "wt", encoding="utf-8") as f:
        for r in all_rows:
            f.write("\t".join("" if v is None else str(v) for v in r) + "\n")
    dates = sorted({r[1] for r in all_rows})
    summary = {"ok": True, "date": datetime.now().strftime("%Y-%m-%d"),
               "symbols_ok": ok, "symbols_fail": fails, "rows": len(all_rows),
               "min_date": dates[0] if dates else None,
               "max_date": dates[-1] if dates else None, "file": OUT_GZ}
    json.dump(summary, open(SUMMARY, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    log(f"写出 {OUT_GZ}: {len(all_rows)} 行, 日期 {dates[0]}~{dates[-1]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
