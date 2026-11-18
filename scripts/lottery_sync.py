#!/usr/bin/env python3
"""lottery/ 自动同步：跟踪上游仓库更新，静默同步，不发通知。

规则（用户 2026-11-18 拍板）：
- 上游有更新：整目录替换为新版（剔除 .git 与 .github 定时工作流），提交并推送；
- 上游清库/不可达：本地存档原地保留，只在状态文件里记 upstream_gone，绝不跟随删除；
- 全程不发送任何提醒。
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOTTERY = os.path.join(REPO_ROOT, "lottery")
STATE_FILE = os.path.join(LOTTERY, "upstream_state.json")

UPSTREAMS = {
    "DoubleColorBall": "wangdaodao/DoubleColorBall",
    "Double-Color-Ball-AI": "sinyu1012/Double-Color-Ball-AI",
    "predict_Lottery_ticket": "KittenCN/predict_Lottery_ticket",
}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def check_upstream(repo):
    """返回 (status, sha)：ok / gone / error。"""
    headers = {"User-Agent": "ak-daily-data-lottery-sync", "Accept": "application/vnd.github+json"}
    try:
        req = urllib.request.Request(f"https://api.github.com/repos/{repo}", headers=headers)
        with urllib.request.urlopen(req, timeout=30) as r:
            meta = json.load(r)
        branch = meta.get("default_branch", "main")
        req2 = urllib.request.Request(f"https://api.github.com/repos/{repo}/commits/{branch}", headers=headers)
        with urllib.request.urlopen(req2, timeout=30) as r2:
            commit = json.load(r2)
        return "ok", commit["sha"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return "gone", None
        return "error", None
    except Exception:
        return "error", None


def clone_and_replace(name, repo):
    tmp = tempfile.mkdtemp(prefix="lottery-sync-")
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", f"https://github.com/{repo}.git", tmp],
            check=True, timeout=600,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        dest = os.path.join(LOTTERY, name)
        if os.path.exists(dest):
            shutil.rmtree(dest)
        shutil.copytree(tmp, dest, ignore=shutil.ignore_patterns(".git", ".github"))
        return True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def git(*args, check=True):
    return subprocess.run(["git", *args], cwd=REPO_ROOT, check=check,
                          capture_output=True, text=True)


def main():
    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)

    changed = False
    for name, repo in UPSTREAMS.items():
        status, sha = check_upstream(repo)
        entry = state.get(name, {})

        if status == "ok":
            if sha and sha != entry.get("sha"):
                if entry.get("sha"):  # 已有存档且上游变化：整目录替换
                    clone_and_replace(name, repo)
                else:  # 首次建档：目录已在仓库中，仅记录基准
                    pass
                state[name] = {"sha": sha, "status": "synced", "synced_at": now_iso()}
                changed = True
            # 无变化：不动状态文件，避免每天空提交
        elif status == "gone":
            # 上游清库：保留本地存档，只记录状态，不删任何文件，不通知
            if entry.get("status") != "upstream_gone":
                entry.update({"status": "upstream_gone", "checked_at": now_iso()})
                state[name] = entry
        else:  # error：网络等临时问题，跳过本轮，保持原状
            if entry.get("status") != "upstream_error":
                entry.update({"status": "upstream_error", "checked_at": now_iso()})
                state[name] = entry

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)

    if not changed:
        print("no upstream changes")
        return

    git("add", "lottery")
    dirty = git("status", "--porcelain", "lottery").stdout.strip()
    if not dirty:
        print("nothing to commit")
        return

    names = sorted({line.split()[1].split("/")[1] for line in dirty.splitlines()
                    if len(line.split()) > 1})
    msg = "lottery-sync: 上游更新，静默同步 " + ", ".join(names)
    for _ in range(2):
        git("pull", "--rebase", check=False)
        c = git("commit", "-qm", msg, check=False)
        if c.returncode == 0:
            break
        git("add", "lottery")
    p = git("push", check=False)
    if p.returncode != 0:
        git("pull", "--rebase", check=False)
        git("push")
    print("synced:", ", ".join(names))


if __name__ == "__main__":
    sys.exit(main())
