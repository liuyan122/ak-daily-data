#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""期货注册仓单日频采集（GitHub Actions 版，纯 requests）
=====================================
覆盖：上期所/能源中心/大商所/郑商所/广期所 每日注册仓单小计
接口（自 akshare futures/receipt.py 移植，2026-11-19）：
  SHFE/INE  GET  https://www.shfe.com.cn/data/tradedata/future/dailydata/{ymd}dailystock.dat
            JSON o_cursor：VARNAME / WRTWGHTS(仓单重量) / WRTCHANGE；BC 单列
  DCE       POST http://www.dce.com.cn/dcereport/publicweb/dailystat/wbillWeeklyQuotes
            json {"tradeDate":ymd,"varietyId":"all"} → data.entityList 取"小计"行
  GFEX      POST http://www.gfex.com.cn/u/interfacesWebTdWbillWeeklyQuotes/loadList
            form {"gen_date":ymd} → data 取"小计"行
  CZCE      GET  http://www.czce.com.cn/cn/DFSStaticFiles/Future/{yyyy}/{ymd}/FutureDataWhsheet.htm
            HTML 表格解析（品种行含"合计"列）
口径：receipt 单位各所不同——SHFE/INE 为吨（WRTWGHTS），DCE/CZCE/GFEX 为手。
产出：data/futures_receipt/{ymd}.tsv.gz（market,var,receipt,receipt_chg,date）
      + futures_receipt_summary.json（单所失败不阻塞，summary 标注）
说明：周末/节假日接口自然无数据 → 全所空时写 skipped:true，不算失败。
"""
import gzip
import json
import os
import re
import sys
import time
import html as html_mod
import urllib.request
from datetime import date, datetime, timedelta, timezone

CN_TZ = timezone(timedelta(hours=8))
NOW = datetime.now(CN_TZ)
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
OUT_DIR = os.environ.get("RECEIPT_OUT_DIR", "data/futures_receipt")
RETRY = int(os.environ.get("RECEIPT_RETRY", "2"))

# 中文品种 → 品种代码（大商所/广期所/上期所 VARNAME 均返回中文名）
CN2CODE = {
    "豆一": "A", "豆二": "B", "玉米": "C", "玉米淀粉": "CS", "生猪": "LH",
    "原木": "LG", "纯苯": "BZ", "苯乙烯": "EB", "乙二醇": "EG", "鸡蛋": "JD",
    "铁矿石": "I", "焦炭": "J", "焦煤": "JM", "塑料": "L", "聚乙烯": "L",
    "豆粕": "M", "豆油": "Y", "棕榈油": "P", "聚氯乙烯": "V", "PVC": "V",
    "聚丙烯": "PP", "粳米": "RR", "纤维板": "FB", "胶合板": "BB",
    "液化石油气": "PG", "工业硅": "SI", "碳酸锂": "LC", "多晶硅": "PS",
    "铜": "CU", "铝": "AL", "锌": "ZN", "铅": "PB", "镍": "NI", "锡": "SN",
    "黄金": "AU", "白银": "AG", "螺纹钢": "RB", "热轧卷板": "HC", "热轧卷": "HC",
    "石油沥青": "BU", "燃料油": "FU", "纸浆": "SP", "不锈钢": "SS",
    "天然橡胶": "RU", "橡胶": "RU", "合成橡胶": "BR", "丁二烯橡胶": "BR",
    "氧化铝": "AO", "线材": "WR", "20号胶": "NR", "低硫燃料油": "LU",
    "国际铜": "BC", "原油": "SC", "中质含硫原油": "SC", "白砂糖": "SR", "白糖": "SR",
    "铸造铝合金": "AD", "胶版印刷纸": "OP", "印刷纸": "OP",
}


def _http_json(url, payload=None, headers=None, timeout=30):
    last = None
    for i in range(RETRY + 1):
        try:
            body = json.dumps(payload).encode() if isinstance(payload, (dict, list)) else \
                (payload.encode() if payload is not None else None)
            req = urllib.request.Request(url, data=body, headers=headers or {"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "ignore"))
        except Exception as e:  # noqa
            last = e
            time.sleep(2 * (i + 1))
    print(f"  [warn] {url} 全部重试失败: {last}")
    return None


def _http_text(url, headers=None, timeout=30):
    try:
        req = urllib.request.Request(url, headers=headers or {"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "ignore")
    except Exception as e:  # noqa
        print(f"  [warn] {url} 下载失败: {e}")
        return None


def code_of(name):
    name = re.sub(r"小计|合计|（.*?）|\(.*?\)", "", str(name)).strip()
    if name in CN2CODE:
        return CN2CODE[name]
    up = name.upper()
    return up if re.fullmatch(r"[A-Z]{2}", up) else None


def fetch_shfe_ine(ymd):
    """上期所+能源中心，单位：吨。页面结构：品种头行['铜','单位：吨'] +
    仓库行 → 各仓库群'合计' → 品种'总计'行；总计行即品种当日仓单与增减。"""
    text = _http_text(
        f"https://www.shfe.com.cn/data/tradedata/future/stockdata/dailystock_{ymd}/ZH/all.html",
        headers={"User-Agent": UA, "Referer": "https://www.shfe.com.cn/"})
    if not text:
        return []
    trs = re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.S)
    out, cur, seen = [], None, set()
    for tr in trs:
        cells = [html_mod.unescape(re.sub(r"<[^>]+>", "", c)).replace("\xa0", " ").strip()
                 for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if len(cells) == 2 and "单位：" in cells[1] and cells[0]:
            cur = code_of(cells[0]) or re.sub(r"\W|[a-zA-Z]", "", cells[0])
        elif len(cells) == 3 and cells[0] == "总计" and cur:
            try:
                if cur not in seen:  # 页面同品种可能重复分段，保留首个（官方口径）
                    seen.add(cur)
                    out.append(("SHFE", cur, int(float(cells[1])), int(float(cells[2]))))
            except ValueError:
                pass
            cur = None
    return out


def fetch_dce(ymd):
    """大商所，单位：手"""
    ctx = _http_json(
        "http://www.dce.com.cn/dcereport/publicweb/dailystat/wbillWeeklyQuotes",
        payload={"tradeDate": ymd, "varietyId": "all"})
    if not ctx or "data" not in (ctx or {}):
        return []
    rows = {}
    for r in (ctx["data"].get("entityList") or []):
        vn = str(r.get("variety", ""))
        if not vn.endswith("小计"):
            continue
        var = code_of(vn)
        if var is None:
            print(f"  [info] DCE 未映射品种，跳过: {vn}")
            continue
        try:
            rows[var] = (int(float(r["wbillQty"])), int(float(r.get("diff") or 0)))
        except (TypeError, ValueError, KeyError):
            continue
    return [("DCE", v, q, c) for v, (q, c) in sorted(rows.items())]


def fetch_gfex(ymd):
    """广期所，单位：手。接口只收表单编码。"""
    ctx = _http_json(
        "http://www.gfex.com.cn/u/interfacesWebTdWbillWeeklyQuotes/loadList",
        payload=f"gen_date={ymd}",
        headers={"User-Agent": UA, "Content-Type": "application/x-www-form-urlencoded",
                 "Referer": "http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml",
                 "X-Requested-With": "XMLHttpRequest"})
    if not ctx or not isinstance(ctx.get("data"), list):
        return []
    rows = {}
    for r in ctx["data"]:
        vn = str(r.get("variety", ""))
        if "小计" not in vn:
            continue
        var = code_of(vn)
        if var is None:
            print(f"  [info] GFEX 未映射品种，跳过: {vn}")
            continue
        try:
            rows[var] = (int(float(r["wbillQty"])), int(float(r.get("diff") or 0)))
        except (TypeError, ValueError, KeyError):
            continue
    return [("GFEX", v, q, c) for v, (q, c) in sorted(rows.items()) if v]


def _czce_cells(line):
    return [html_mod.unescape(re.sub(r"<[^>]+>", "", c)).strip()
            for c in re.findall(r"<td[^>]*>(.*?)</td>", line, re.S)]


def fetch_czce(ymd):
    """郑商所，单位：张。HTML 表格解析，品种行取"当日仓单量/增减"（含小计行）。"""
    text = _http_text(
        f"http://www.czce.com.cn/cn/DFSStaticFiles/Future/{ymd[:4]}/{ymd}/FutureDataWhsheet.htm")
    if not text:
        return []
    trs = re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.S)
    out = []
    for tr in trs:
        cells = _czce_cells(tr)
        if len(cells) < 2 or not cells[0]:
            continue
        # 郑商所行结构：品种/合约 | 今日仓单量 | 增减 …（小计行已含当日合计）
        if len(cells) < 3:
            continue
        qty_raw, chg_raw = cells[1], cells[2]
        m_qty = re.sub(r"[^\d\-]", "", qty_raw)
        m_chg = re.sub(r"[^\d\-]", "", chg_raw) if chg_raw else ""
        if not m_qty or len(cells[0]) > 6:
            continue  # 只取品种合计行（合约行首列长、无小计语义则跳过）
        var = code_of(cells[0])
        if not var:
            continue
        try:
            out.append(("CZCE", var, int(m_qty), int(m_chg) if m_chg else 0))
        except ValueError:
            continue
    # 同品种去重（保留首次出现的合计行）
    seen, dedup = set(), []
    for r in out:
        if r[1] not in seen:
            seen.add(r[1]); dedup.append(r)
    return dedup


def main():
    ymd = sys.argv[1] if len(sys.argv) > 1 else NOW.strftime("%Y%m%d")
    os.makedirs(OUT_DIR, exist_ok=True)
    results, rows_all, summary = {}, [], {"date": ymd, "markets": {}, "skipped": False}
    for name, fn in (("SHFE", fetch_shfe_ine), ("DCE", fetch_dce),
                     ("GFEX", fetch_gfex), ("CZCE", fetch_czce)):
        t0 = time.time()
        try:
            rows = fn(ymd)
        except Exception as e:  # noqa
            print(f"  [warn] {name} 采集异常: {e}")
            rows = []
        results[name] = rows
        summary["markets"][name] = {"rows": len(rows), "ok": len(rows) > 0,
                                    "elapsed_sec": round(time.time() - t0, 1)}
        print(f"{name}: {len(rows)} 只品种, {summary['markets'][name]['elapsed_sec']}s")
        rows_all.extend(rows)
        time.sleep(1)
    if not rows_all:
        summary["skipped"] = True  # 非交易时段：各所均无数据
    summary["rows"] = len(rows_all)
    out_gz = f"{OUT_DIR}/{ymd}.tsv.gz"
    with gzip.open(out_gz, "wt", encoding="utf-8") as f:
        f.write("market\tvar\treceipt\treceipt_chg\tdate\n")
        for m, v, q, c in rows_all:
            f.write(f"{m}\t{v}\t{q}\t{c}\t{ymd}\n")
    with open("futures_receipt_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1)
    print(f"产出 {out_gz}: {len(rows_all)} 行，skipped={summary['skipped']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
