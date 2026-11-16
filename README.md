# ak-daily-data

GitHub Actions 免费托管的两条自动化数据管线（公共仓库）：

| 工作流 | 时间（北京） | 产物 |
|---|---|---|
| A股日线增量更新 | 每交易日 17:15 | `data/YYYYMMDD.tsv.gz`（最近15天全行，列同 daily_kline 表）+ Actions artifact |
| 热榜资讯聚合 | 每小时整点 | GitHub Pages 静态页（微博/知乎/百度/新浪7×24/东财快讯）+ 每日归档 `archive/` |

- 日线增量同时提交到仓库 `data/` 目录，可直接 raw 下载合并进本地库。
- 热榜页面入口：仓库 Settings → Pages（`gh-pages` 分支）。
