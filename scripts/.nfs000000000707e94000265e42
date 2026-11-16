#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多源热榜/快讯聚合（GitHub Actions 每小时跑）
=====================================
来源（单源失败自动降级，不影响其余）：
 - 微博热搜      weibo.com/ajax/statuses/hot_band
 - 知乎热榜      api.zhihu.com/topstory/hot-list
 - 百度热搜      top.baidu.com/api/board (realtime)
 - 新浪财经7×24  zhibo.sina.com.cn zhibo_id=152
 - 东财快讯      np-listapi.eastmoney.com getFastNewsList

产出（由 workflow 推 gh-pages）：
 - docs/latest.json               最新快照（每小时覆盖）
 - docs/index.html                静态页（读 latest.json 渲染）
 - docs/data/YYYYMMDD-HH.json     当日逐小时快照
 - docs/data/manifest.json        最近 48 个快照索引
 - docs/history/YYYYMMDD.json     每日归档（每源前 20）
"""
import json
import os
import time
import traceback
from datetime import datetime, timezone, timedelta

import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"
CN_TZ = timezone(timedelta(hours=8))
NOW = datetime.now(CN_TZ)
DOCS = os.environ.get("DOCS_DIR", "docs")
KEEP_HOURS = 48


def fetch(name, fn):
    try:
        items = fn()
        print(f"[{name}] {len(items)} 条")
        return {"name": name, "ok": True, "items": items}
    except Exception:
        print(f"[{name}] 失败: {traceback.format_exc(limit=1)[:160]}")
        return {"name": name, "ok": False, "items": []}


def src_weibo():
    r = requests.get("https://weibo.com/ajax/statuses/hot_band",
                     headers={"User-Agent": UA, "Referer": "https://s.weibo.com"}, timeout=12)
    lst = r.json()["data"]["band_list"]
    return [{"title": x.get("note") or x.get("word"), "hot": x.get("raw_hot"),
             "url": "https://s.weibo.com/weibo?q=%23" + requests.utils.quote(str(x.get("word") or "")) + "%23"}
            for x in lst if x.get("note")][:50]


def src_zhihu():
    r = requests.get("https://api.zhihu.com/topstory/hot-list?limit=50",
                     headers={"User-Agent": "osee2unitedRelease/7.33.0"}, timeout=12)
    return [{"title": (x.get("target") or {}).get("title"), "hot": x.get("detail_text"),
             "url": "https://www.zhihu.com/question/" + str((x.get("target") or {}).get("id"))}
            for x in (r.json().get("data") or [])][:50]


def src_baidu():
    r = requests.get("https://top.baidu.com/api/board?platform=wise&tab=realtime",
                     headers={"User-Agent": UA, "Referer": "https://top.baidu.com/board"}, timeout=12)
    out = []
    for card in r.json()["data"]["cards"]:
        for group in card.get("content", []):
            for x in group.get("content", []):
                if x.get("word"):
                    out.append({"title": x["word"], "hot": x.get("hotScore"), "url": x.get("url")})
    return out[:50]


def src_sina724():
    r = requests.get("https://zhibo.sina.com.cn/api/zhibo/feed",
                     params={"page": 1, "page_size": 100, "zhibo_id": 152, "tag_id": 0,
                             "dire": "f", "dpc": 1},
                     headers={"User-Agent": UA, "Referer": "https://finance.sina.com.cn/7x24"}, timeout=12)
    lst = r.json()["result"]["data"]["feed"]["list"]
    return [{"title": (x.get("rich_text") or "")[:80],
             "url": (x.get("docurl") or x.get("ext2") or "https://finance.sina.com.cn/7x24")}
            for x in lst if x.get("rich_text")][:100]


def src_em_fast():
    r = requests.get("https://np-listapi.eastmoney.com/comm/web/getFastNewsList",
                     params={"client": "web", "biz": "web_724", "fastColumn": "102",
                             "sortEnd": "", "pageSize": 100,
                             "req_trace": str(int(time.time() * 1000))},
                     headers={"User-Agent": UA, "Referer": "https://kuaixun.eastmoney.com"}, timeout=12)
    lst = r.json()["data"]["fastNewsList"]
    return [{"title": (x.get("summary") or x.get("title") or "")[:80], "url": x.get("code", "")}
            for x in lst][:100]


def main():
    sources = [
        fetch("微博热搜", src_weibo),
        fetch("知乎热榜", src_zhihu),
        fetch("百度热搜", src_baidu),
        fetch("新浪财经7x24", src_sina724),
        fetch("东财快讯", src_em_fast),
    ]
    stamp_hour = NOW.strftime("%Y%m%d-%H")
    stamp_day = NOW.strftime("%Y%m%d")
    snapshot = {"updated_at": NOW.strftime("%Y-%m-%d %H:%M:%S"), "sources": sources}

    os.makedirs(f"{DOCS}/data", exist_ok=True)
    os.makedirs(f"{DOCS}/history", exist_ok=True)
    json.dump(snapshot, open(f"{DOCS}/latest.json", "w", encoding="utf-8"), ensure_ascii=False)
    json.dump(snapshot, open(f"{DOCS}/data/{stamp_hour}.json", "w", encoding="utf-8"), ensure_ascii=False)

    ok_sources = [s for s in sources if s["ok"]]
    if not ok_sources:
        print("全部来源失败，不发布")
        return 1
    # manifest：最近 48 个快照
    files = sorted(f for f in os.listdir(f"{DOCS}/data") if f.endswith(".json") and f != "manifest.json")
    if len(files) > KEEP_HOURS:
        for f in files[:-KEEP_HOURS]:
            os.remove(f"{DOCS}/data/{f}")
        files = files[-KEEP_HOURS:]
    json.dump({"snapshots": [f[:-5] for f in files]},
              open(f"{DOCS}/data/manifest.json", "w", encoding="utf-8"), ensure_ascii=False)
    # 每日归档（每源前 20）
    hist = {"date": NOW.strftime("%Y-%m-%d"), "updated_at": snapshot["updated_at"],
            "sources": [{"name": s["name"], "items": s["items"][:20]} for s in ok_sources]}
    json.dump(hist, open(f"{DOCS}/history/{stamp_day}.json", "w", encoding="utf-8"), ensure_ascii=False)
    print(f"发布完成 {stamp_hour}，成功来源 {len(ok_sources)}/{len(sources)}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
