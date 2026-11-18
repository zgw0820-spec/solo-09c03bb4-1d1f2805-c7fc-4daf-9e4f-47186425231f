# 供电柜告警检修抑制台

单设备（供电柜 #1）过热告警的检修抑制与可靠送达演示。FastAPI + SQLite，
两个容器：告警服务（网页 + API）与本地通知端（可模拟「已受理但丢回执」等故障）。

## 启动

```bash
docker compose up --build
```

- 操作页：<http://localhost:8080>
- 通知端（仅观测/注入故障）：<http://localhost:8081>

本地不装 Docker 时也可直接运行（需先 `pip install -r requirements-dev.txt`）：

```bash
NOTIFIER_DB=/tmp/notifier.db uvicorn app.notifier.main:app --port 8081
ALARM_DB=/tmp/alarm.db NOTIFIER_URL=http://127.0.0.1:8081 uvicorn app.alarm.main:app --port 8080
pytest -q
```

SQLite 文件分别落在两个具名卷 `alarm-data`、`notifier-data` 的 `/data` 下，
容器重启/重建后数据仍在。

## 业务规则与实现位置

| 需求 | 实现 |
| --- | --- |
| 设置检修截止时刻（可随时调整） | `POST /api/maintenance`，截止前再设置即顺延同一窗口 |
| 检修内新触发告警先不通知 | 触发时落在 `suppressed`，不产生任何投递 |
| 到期前恢复的不补送 | `recover()` 置 `skipped`，之后截止事件也不会再捞起它 |
| 到期仍活跃的按本轮固定通知号送达 | 截止时 `suppressed → pending`，通知号在建轮时即固定为 `ALM-000001` 形式 |
| 检修外触发的正常通知 | 触发即 `pending`，工作线程立即投递 |
| 恢复前已受理的通知保留记录 | `sent` 状态在恢复后不变，只追加说明；`accepted_at` 保留 |
| 恢复后尚未开始的投递取消 | `recover()` 见 `delivery_started_at IS NULL` 即 `cancelled` |
| 回执丢失先按通知号核实 | 任何非确认回执都 `GET /notifications/{no}` 核实，核实到受理即 `sent` |
| 未受理不能误报已送 | 只有通知端明确 `accepted:true` 才写 `accepted_at`；拒绝则退避重试 |
| 故障或重启后接续 | 启动时把残留 `delivering` 行改为「先核实」；通知端按通知号幂等 |
| 不重复实际通知 | 同一轮固定一个通知号，通知端对同号只记一条（`deliveries` 计数可观测） |
| 交接核查：逐轮比对两端 | `POST /api/handover/check`；对照告警库状态与通知端按通知号保存的轮次、触发时间、受理时间 |
| 一段记录都没有/查不了不能判定 | 通知端查无记录而该轮仍在投递或 `needs_verify` 时，结论为「暂无法判定」 |
| 核查期间状态变化须重查 | 核查前后做指纹比对，变化即整份报告 `changed_during_check`；已存报告随时间变 `stale` |
| 通知端慢/不可达不影响告警 | 核查的通知端查询不持告警锁且受超时约束，触发/恢复不受影响 |

### 状态说明

| 状态 | 含义 |
| --- | --- |
| `suppressed` 抑制中 | 检修内触发，暂缓通知 |
| `pending` 待送 | 已安排投递，含退避等待或待按号核实（`needs_verify`） |
| `delivering` 待送（投递中） | 工作线程正在投递 |
| `sent` 已送 | 通知端已确认受理 |
| `skipped` 未补送 | 检修截止/结束前已恢复，按规则不补送 |
| `cancelled` 已取消 | 恢复时投递尚未开始，取消本轮 |

## 交接核查

值班员在交接班时核对“已结束检修窗口”内的告警轮次是否真的送到了通知端。
操作页的“交接核查”卡片或 API：

```bash
# 核查全部已结束检修窗口；可加 ?maintenance_id=N 只查某个窗口
curl -X POST localhost:8080/api/handover/check
curl localhost:8080/api/handover/reports/<check_id>
curl -X POST localhost:8080/api/handover/reports/<check_id>/confirm
```

判定规则（逐轮）：

| 结论 | 条件 |
| --- | --- |
| 一致 | 告警库 `sent`，通知端同一通知号的轮次、触发时间、受理时间均一致；或告警库 `skipped`/`cancelled`，通知端无记录 |
| 两端冲突 | 一方记已受理、另一方记未送达；或通知号的轮次/触发时间/受理时间不一致 |
| 暂无法判定 | 通知端查询失败或不可达；通知端查无记录但该轮仍在投递/待按号核实；通知端缺轮次或触发时间；两端都还没完成核实 |

- 核查结果带 `check_id`，只保存在告警服务内存中（最近 20 份）。
- 核查前后各取一次告警库指纹；期间任何轮次状态、尝试次数、受理/恢复时间变化，
  整份报告标为 `changed_during_check`，`can_handover=false`，提示重新核查。
- 已存报告在读取/确认时重新比对指纹；状态变过即为 `stale`，确认接口返回 409。
- 只有全部轮次“一致”且结论未过期时才允许 `confirm`；存在冲突或暂无法判定一律 409。
- 通知端查询并发执行且在服务锁之外，单次受 `NOTIFIER_TIMEOUT`（默认 2s）约束，
  因此通知端缓慢或不可达时，告警触发、恢复仍照常工作。
- 两端分别重启后不缓存旧结论：核查每次都重新读取各自当前记录。

## 通知端故障注入

```bash
# normal | drop_receipt（已受理但回 504，回执丢失）| reject（未受理，回 503）
curl -X POST localhost:8081/api/fault -H 'content-type: application/json' \
     -d '{"mode":"drop_receipt"}'
curl localhost:8081/api/notifications      # 已受理的固定通知号
```

`NOTIFIER_MODE` 环境变量可设置初始模式。

## 自测覆盖

`pytest -q`（45 项）：

- 检修外触发立即送达，重复 tick 不重复通知；
- 检修内触发抑制、截止前恢复 `skipped` 且不补送；
- 检修到期后活跃告警按固定通知号送达，恢复后保留记录；
- 手动结束检修释放抑制告警；
- 通知端 503 时保持 `pending`、无 `accepted_at`，恢复后仅送达一次；
- 丢回执（504）按号核实为已受理，通知端 `deliveries` 仍为 1；
- 核实也不可达时保持待核实，不误报已送；
- 重启后残留投递先核实、不重发（通知端计数仍为 1）；
- 恢复后未开始投递取消；已开始投递保留并只核实；
- 恢复后停止对明确未受理轮次的退避重试；
- 调整检修截止时刻后，抑制轮次按新截止送达；
- 服务停机跨过截止时刻，重启后仍按时送达；
- 手动结束检修时已恢复轮次不补送；
- 通知端全程不可达时不误报已送，恢复后按号核实未受理则取消；
- API 参数校验、409 冲突、页面与静态资源、通知端故障模式与幂等；
- 交接核查：逐轮一致 / 两端冲突 / 暂无法判定；通知端查无记录但仍在投递；
  通知端不可达；核查期间状态交错导致结论失效；报告变 stale 后拒绝确认；
  通知端慢查询不阻塞触发与恢复；两端重启后按当前记录重新核查。

## 已知限制

- 单设备、单实例假设：用进程内锁 + SQLite 串行化，未做多副本协调。
  交接核查结论也只存在单个告警进程内存中，重启后需重新核查。
- 「投递已开始」以发出 HTTP 请求前写入 `delivery_started_at` 为准；
  通知端慢响应期间恢复会走「只核实」路径，而不是直接取消。
- 通知端是演示替身，受理记录与故障模式同样存 SQLite；生产替换时需保留
  按通知号查询的幂等语义。
- 交接判定以「轮次 ID + 触发时间 + 受理时间」比对，且时间容差为 1ms；
  两端时钟若不同步会显示为冲突，这是有意的保守行为。
- 单次核查最多查询 50 轮（`HANDOVER_MAX_CHECKS`），超出部分标 `truncated`
  并判为暂无法判定。
- 未做鉴权、HTTPS、告警分级与真实设备采集；页面按 1 秒轮询 `/api/state`。
