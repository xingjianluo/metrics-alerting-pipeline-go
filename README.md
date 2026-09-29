# Metrics Aggregation & Alerting Pipeline (in-memory, Go)

一套**本地可运行、零外部依赖**的指标聚合与告警评估流水线。用内存数据源模拟采集，
重点保证在以下干扰下结果**始终一致且可解释**：

- 乱序 / 延迟到达的样本仍归入其时间戳所属窗口；
- 重复上报（重试）绝不被二次计数；
- 超出保留期的样本被拒绝，且拒绝原因可区分；
- 查询跨越已淘汰窗口时**显式报错并返回缺口列表**，不静默返回残缺结果；
- 告警在 `resolved → pending → firing → resolved` 间的每次转换都附带判定依据；
- 规则热更新保留存活规则的状态，不丢告警、不重复触发；
- 求值在互斥锁内串行完成，并发调用结论不串扰；
- **规则批量变更管理**：整批先校验（一条不合格整批不生效）、原子前后切换；
  受影响的 pending/firing 告警明确分为「继续跟踪」和「显式收尾」，每批改动可查询可追溯。

## 1. 快速开始

```bash
go run ./cmd/demo            # 打印确定性场景的完整输入与判定
go test ./... -v             # 单测日志会打印输入样本与判定依据
go test -race -count=1 ./... # 竞态检测（已通过）
```

## 2. 目录结构

| 包 | 职责 |
|---|---|
| `model` | 数据模型：`Labels/Sample/Aggregate/Rule/AlertState/AlertEvent`、标签指纹 |
| `storage` | 内存窗口存储：对齐聚合、幂等去重、保留期淘汰、缺口感知查询 |
| `alert` | 规则求值与告警状态机、规则热更新、**整批校验与原子切换 / 告警收尾** |
| `change` | **规则变更管理层：批量提交（校验→应用）、审计历史、幂等重试** |
| `pipeline` | 门面接线 + 确定性内存数据源脚本（乱序/重复/延迟） |
| `cmd/demo` | 端到端可运行演示 |

## 3. 数据模型

```
Sample { Labels, Ts(unix 秒), Value, SourceID, DedupKey }
```

- **序列身份**：`Labels` 的指纹（键排序后 `k=v|k=v`，对 `| = \` 转义），
  与 map 迭代顺序无关，因此可复现、可作为 map 键。选择器匹配是「子集包含」。
- **上报身份（幂等键）**：`(SourceID, DedupKey)`。同一身份的第二次及以后写入
  返回 `status=deduped`，**携带的样本值即使不同也不会污染聚合**。
- 时间戳采用 `int64` unix 秒（而非 `time.Time`），使测试结果与输出完全确定。

每个序列每个窗口维护一个 `Aggregate`：

```
Count, Sum, Min, Max, Latest, LatestTs
```

`Latest` 按**样本时间戳**而非到达顺序决定，所以乱序到达不会让较旧的值覆盖最新值。

## 4. 窗口与保留策略

- **基础窗口**大小 `WindowSeconds`（默认 60）。样本归入
  `windowStart = Ts - Ts % WindowSeconds`（半开区间 `[start, start+w)`）。
  乱序、延迟样本与正常样本走同一路径，合并进同一个 `Aggregate`。
- **保留期**：`RetentionWindows` 个窗口（含当前窗口）。时钟为 `now` 时，
  最低保留窗口起点为

  ```
  boundary = floor(now, w) - (RetentionWindows - 1) * w
  ```

  每次写入前以及 `Tick()` 时淘汰 `windowStart < boundary` 的窗口，并单调推进
  `Horizon()`。去重索引随其窗口一起淘汰，避免无界增长。
- **拒绝原因（`model.RejectReason`，可区分）**：
  - `sample_too_old`：窗口早于保留边界；
  - `sample_in_future`：`Ts > now + MaxFutureSeconds`；
  - `validation_error`：负时间戳、缺少 `SourceID/DedupKey`；
  - `duplicate`：仅作为 `status=deduped` 的信息性原因，**不是错误**，样本不会被计数两次。
- 时钟通过 `Config.Now` 注入，测试中可手动推进。

## 5. 聚合查询语义

```go
Query(start, end, granularitySeconds, selector) (*QueryResult, error)
```

- `[start, end)` 半开区间，二者必须按 `granularitySeconds` 对齐；
- `granularitySeconds` 必须是基础窗口的整数倍——粗粒度桶由基础窗口**合并**而来，
  因此同一序列在不同边界粒度上的 `count/sum/min/max/latest` 严格自洽，
  相邻桶无缝拼接（测试 `TestWindowBoundarySelfConsistency` 验证）；
- 没有样本的基础窗口不产生点（不补零，避免伪造「观测到 0」）；
- **淘汰不静默**：只要查询范围与 `Horizon` 之前的区间相交，就返回
  `*storage.GapError`，其中 `Gaps []QueryGap` 逐桶标出被淘汰的区间；
  同时仍返回可用部分的结果，但调用方不得把它当作完整结果。

## 6. 告警状态机

每个 `(规则 ID, 序列指纹)` 维护一个独立状态：

```
                 违规(第 1 个窗口)            连续违规达 PendingWindows
  resolved ───────────────────────▶ pending ───────────────────────▶ firing
     ▲                                  │                             │
     │ 恢复(未达触发)                    │ 恢复                        │ 持续恢复超过
     └──────────────────────────────────┘                             │ KeepFiringWindows
     ▲                                                                │
     └────────────────────────────────────────────────────────────────┘
```

- `PendingWindows`：进入 `firing` 所需的**连续违规窗口数**。`0` 表示违规即触发。
- `KeepFiringWindows`：恢复后仍**保持 firing 的恢复窗口数**；`0` 立即恢复。
  即第 `KeepFiringWindows+1` 个连续恢复窗口才转 `resolved`（与 pending 对称）。
- 比较运算符为字面语义：阈值边界精确可控——`v == threshold` 时 `>` 为假、`>=` 为真
  （测试 `TestThresholdBoundary` 专门钉住该行为）。
- 连续计数用带符号整数：正数=连续违规窗口数，负数=连续恢复窗口数，信号翻转时重置，
  保证 pending 期间短暂恢复后，旧计数不会残留导致误触发。
- 每次转换产出一条不可变 `AlertEvent`，`Reason` 记录字段、观测值、运算符、
  阈值、窗口与样本数，例如：

  ```
  firing: violating streak 2 reached PendingWindows=1:
          count=4 >= 4 over [10,20) (count=4)
  ```

### 求值的时间窗口与「缺数据不动作」

`Eval(evalTs)` 求值**最后一个完整对齐窗口**
`[floor(evalTs,w)-w, floor(evalTs,w))`。

- 窗口内没有任何数据点：跳过，**状态不动**；
- 窗口已被淘汰（存储返回 `GapError`）：规则进入 `Report.Skipped`，**状态不动**。

缺数据既不会误触发，也不会静默恢复——这对延迟到达和保留期边界尤其重要。

## 7. 规则热更新与并发

- `ReplaceRules` 与 `Eval` 在同一把互斥锁内串行；整个求值 pass 持锁完成，
  并发 `Eval` 结论不会交错错乱。
- 按**规则 ID**对齐：ID 保留 ⇒ 状态（含 pending 连续计数、firing 状态）原样保留，
  只替换规则参数（如阈值、pending 设置）；ID 删除 ⇒ 其状态被清理；ID 新增 ⇒
  从 `resolved` 冷启动。因此热更新**不会重放 firing 事件，也不会丢掉 pending 进度**。
- 规则按 ID 排序、序列按指纹排序后求值，`Report.States` 快照顺序确定，可复现。

## 8. 规则批量变更管理（新增）

运维通常一次攒一批规则改动再推。`change.Manager`（门面暴露为
`pipeline.SubmitRules / CheckRules / RuleChangeHistory`）在原有求值器之上加了
一层「变更管理」，把推规则变成**可控、可追溯**的操作。

### 8.1 整批先校验，一条不合格整批不生效

- `CheckRules(rules)`：**只检查不改动**，一次性列出整批的全部问题，不写历史、
  不动规则和状态。
- `SubmitRules(rules, key)`：先对整批做校验，**任一规则不合格则整批拒绝**，
  现有规则和正在跟踪的告警状态与提交前**完全一致**。
- 校验项（关键配置）：
  - 规则名非空，且**同一批内不允许重名**；
  - `WindowSeconds > 0`；
  - `AggField ∈ {count,sum,avg,min,max,latest}`；
  - `Op ∈ {>,>=,<,<=,==,!=}`；
  - `Threshold` 不为 `NaN/Inf`；`PendingWindows/KeepFiringWindows >= 0`。
- 校验结果聚合成 `*alert.ValidationErrors`（多条问题全部返回），错误定位到
  `规则 + 字段 + 原因`。

### 8.2 合法批次原子前后切换，并发求值看不到半成品

- 校验通过后，在求值器的**同一把互斥锁**内一次性整体替换规则切片；每次 `Eval` 也持同一把锁，
  并在 `Report.Rules` 中返回该 pass 实际使用的完整规则快照。因此并发 `Eval` 只能看到
  「整批旧」或「整批新」，**绝不会看到一部分新、一部分旧的撕裂规则集**。
  `change.TestConcurrentEvalSeesWholeRuleSet` 会对当次 pass 的规则 ID、选择器和全部关键配置
  生成完整签名，而不是只看规则数量；该用例在 `-race` 下钉住此性质。
- 单个规则的问题在提交前就被整批拦下，不会出现「某条规则失败把整批规则与
  存活状态一起带走」的中间态。
- 切换与状态对齐仍按**规则 ID + 选择器**进行（见 8.3）。

### 8.3 受影响告警的完整口径：继续跟踪与显式收尾分开记录

「受影响告警」只统计**批次提交时仍处于活跃状态**的告警，即
`pending`（待触发）和 `firing`（已触发）。`resolved` 是惰性状态：删除规则或选择器不再匹配
时只清理状态，不计入受影响告警，也不产生 `closed` 事件。

切换时对每条活跃告警按**规则 ID 与新选择器**分类，并写入两个互不混杂的列表：

| 提交前情况 / 本批变化 | 记录结局 | 状态处理与原因 |
|---|---|---|
| 规则 ID 保留；关键配置变化（阈值、窗口、口径、运算符、pending/keep 等），**新选择器仍覆盖该序列** | `ContinuedAlerts[]`，`Outcome=continued` | 保留原 `pending/firing` 状态和 streak；原因：`rule critical configuration updated and new selector still matches this series`。提交本身不会重新触发 |
| 规则 ID 保留；选择器收窄/改写后仍覆盖该序列 | `ContinuedAlerts[]`，`Outcome=continued` | 同上；只是 `ChangedFields` 中包含 `Selector` |
| 规则 ID 保留；新选择器不再覆盖该序列 | `ClosedAlerts[]`，`Outcome=closed`，并产生一条 `ClosedEvents[]` | 状态移除；结论：`closed`；原因：`rule updated and no longer matches this series` |
| 规则 ID 被删除 | `ClosedAlerts[]`，`Outcome=closed`，并产生一条 `ClosedEvents[]` | 状态移除；结论：`closed`；原因：`rule deleted by batch change` |
| 规则 ID 为新增 | 不进入受影响告警列表 | 新规则没有旧状态，下次求值从干净的 `resolved` 状态开始 |
| 规则 ID 保留且规则完全未变 | 不进入受影响告警列表 | 状态本来就持续跟踪；该批没有改变其生命周期 |

- `ContinuedAlerts[]` 与 `ClosedAlerts[]` 是两个独立字段：继续跟踪的告警不会带 `closed`
  结论；被收尾的告警不会出现在继续跟踪列表中。两类都可能为空，但不会被合并成一个语义不清的列表。
- 每条记录都包含 `RuleID / Labels / PreviousStatus / Outcome / Reason`，因此事后可回答：
  这批碰到了**哪条规则下的哪个序列**、提交时它是 `pending` 还是 `firing`、最后是继续跟踪还是收尾以及原因。
- `ClosedEvents[]` 与 `ClosedAlerts[]` 一一对应；事件的 `Reason` 还包含之前状态、streak 和
  最后观测值，是管理性收尾结论（非数据驱动状态机迁移）。
- 仍然在覆盖范围内的告警原样保留，切换后对同一窗口重复求值**不会重复触发**；选择器不再覆盖
  的状态立即删除，不能在记录中无声消失。
- 规则之后被加回来或改回来时，由于旧状态已移除，**从干净状态重新开始**，不会继承已收尾的旧进度
  （`alert.TestBatchReaddStartsClean`）。

### 8.4 每批改动留下可查询记录

`SubmitRules` 返回并保存一条 `alert.BatchReport`：

```
BatchID, Ts
Changes[]         // 每条规则：kind=added|modified|deleted|unchanged
                  // modified 含改动前/后关键配置与 ChangedFields
ContinuedAlerts[] // 本批后仍在覆盖范围内、继续跟踪的提交时 pending/firing 告警
ClosedEvents[]    // 本批收尾掉的活跃告警（显式 closed 结论与详细原因）
ClosedAlerts[]    // 与 ClosedEvents 一一对应的结构化视图（规则/序列/前状态/原因）
ValidationErrors  // 被拒批次的全部问题（被拒同样留痕，Applied()==false）
```

`ContinuedAlerts[]` 和 `ClosedAlerts[]` 的元素结构相同：

```go
type AffectedAlert struct {
    RuleID         string
    Labels         model.Labels // 哪条序列
    PreviousStatus model.AlertStatus // 提交时：pending 或 firing
    Outcome        AlertOutcome // continued 或 closed
    Reason         string
}
```

- `RuleChangeHistory()` 按**最新在前**返回所有记录（含被拒批次）；
  `RuleChangeBatch(id)` 按批次 ID 精确查询。
- `SubmitRules` 的第二个参数是**幂等键**：相同 key 的重试返回首次的批次记录，
  不会把规则集切换两次（适合运维重试推送）。不需要可传空串。

### 8.5 与原单条热更新方式的兼容范围

- 原有 `Evaluator.ReplaceRules` / `pipeline.ReplaceRules` **保留**：仍是一次性
  原子换规则、存活 ID 保状态；但它是**未校验、不产生收尾事件、不写审计**的
  旧路径（删除/选择器收窄时静默丢状态）。新代码建议使用
  `CheckRules + SubmitRules`。
- 两条路径操作同一套求值器与状态，可并存；`New(...)` 现在自带一个
  `*change.Manager`（`pipeline.Changes`）。

## 9. 验证方法与测试覆盖

```bash
go vet ./...
go test -race -count=1 ./...          # 竞态检测一遍（要求项）
go test ./... -v                      # 查看带输入样本与判定依据的日志
go run ./cmd/demo                     # 端到端演示（第 6 节即批量变更管理）
```

只验证本次追溯边界时，可按场景运行：

```bash
# 只改关键配置（阈值），选择器仍覆盖：pending/firing 进入 continued，提交不重复触发
go test ./alert -run TestBatchConfigOnlyChangeRecordsContinuedAlerts -count=1 -v

# 选择器收窄：同一批中一个 continued、一个 closed，二者分区且原因明确
go test ./alert -run TestBatchSelectorNarrow -count=1 -v

# 整批删除：pending/firing 显式 closed；resolved 惰性状态不进入受影响告警
go test ./alert -run TestBatchDeleteClosesActiveAlerts -count=1 -v

# 一批混合新增、配置修改、删除、收窄：规则 diff 与受影响告警对账
go test ./alert -run TestBatchMixedAddModifyDeleteNarrowReconcilesAlerts -count=1 -v

# 并发求值必须看到完整旧批或完整新批；使用每次 Eval 捕获的 Report.Rules 签名判定
go test ./change -run TestConcurrentEvalSeesWholeRuleSet -race -count=1 -v
```

`go run ./cmd/demo` 的第 6 节会先输出一次配置只改但告警继续跟踪的记录：

```text
CONTINUED ... previous=firing: rule critical configuration updated and new selector still matches this series
affected active alerts: continued=1 closed=0
```

随后整批删除同一条 firing 告警：

```text
CLOSED ... previous=firing: rule deleted by batch change
```

本地最小验证批量变更管理的调用方式：

```go
p := pipeline.New(store, rules, now)

// 1) 只检查、不改动：一次性拿到整批问题
if verrs := p.CheckRules(candidate); verrs != nil {
    log.Print(verrs.Error())
    return
}

// 2) 校验通过才原子切换；幂等键避免重试重复应用
rec, verrs := p.SubmitRules(candidate, "change-ticket-123")
if verrs != nil { /* 整批未生效，规则/状态原样 */ }

// 3) 回看这批动了什么、哪些活跃告警继续跟踪、哪些被收尾
for _, ch := range rec.Changes { /* added/modified/deleted/unchanged + 前后配置 */ }
for _, a := range rec.ContinuedAlerts { /* outcome=continued；pending/firing 状态保留 */ }
for _, a := range rec.ClosedAlerts { /* outcome=closed；规则删除或新选择器不再覆盖 */ }
for _, ev := range rec.ClosedEvents { /* closed 结论、streak、最后观测值、详细原因 */ }

// 4) 事后查询任意批次
history := p.RuleChangeHistory()   // 最新在前，含被拒批次
one, ok := p.RuleChangeBatch(rec.BatchID)
```

| 测试 | 覆盖点 |
|---|---|
| `storage.TestOutOfOrderAndDuplicates` | 乱序归窗、latest 按 Ts、重复上报不二次计数（不同值也被忽略） |
| `storage.TestWindowBoundarySelfConsistency` | 基础/2× 粒度合并自洽、桶边界无缝拼接 |
| `storage.TestRetentionRejection` | `sample_too_old` / `sample_in_future` 区分、Horizon、`GapError` |
| `storage.TestConcurrentWritesRace` | 并发重复写入下计数精确（配合 `-race`） |
| `alert.TestThresholdBoundary` | `>` 与 `>=` 在恰好等于阈值时的边界差异 |
| `alert.TestStateMachineTransitions` | pending→firing→grace→resolved 全转换、不重复触发 |
| `alert.TestPendingRecoversBeforeFiring` | pending 中恢复后计数从零重启 |
| `alert.TestHotReloadPreservesState` | 热更新保状态、删规则清状态、回退后只触发一次 |
| `alert.TestMissingDataDoesNotResolve` | 空窗口与淘汰缺口都不改变状态 |
| `alert.TestConcurrentEvalRace` | 并发求值 + 并发热更新结论稳定（配合 `-race`） |
| `alert.TestValidateBatchListsAllProblems` | 只检查入口一次列清整批问题（重名/窗口/口径/运算符/阈值） |
| `alert.TestInvalidBatchChangesNothing` | **整批非法时不生效**：规则与 firing 状态提交前后一致 |
| `alert.TestBatchDiffRecordsAddModifyDelete` | 批次记录：新增/修改/删除/未变 + 改动前后关键配置 |
| `alert.TestBatchConfigOnlyChangeRecordsContinuedAlerts` | **只改关键配置、选择器仍覆盖**：pending/firing 均进入 continued，提交不重复触发 |
| `alert.TestBatchDeleteClosesActiveAlerts` | **整批删除**：pending/firing 显式收尾，resolved 惰性状态不进入受影响告警 |
| `alert.TestBatchSelectorNarrow` | **选择器收窄**：仍匹配序列 continued、失配序列 closed；分区不混类、不重复触发 |
| `alert.TestBatchMixedAddModifyDeleteNarrowReconcilesAlerts` | **一批混合新增/配置修改/删除/收窄**：规则 diff、continued/closed 告警和提交后状态完整对账 |
| `alert.TestBatchReaddStartsClean` | 规则加回来从干净状态重启，不继承已收尾进度 |
| `change.TestCheckDoesNotMutate` | 只检查不改规则、不写历史 |
| `change.TestSubmitAuditRecordAndIdempotency` | 被拒/成功留痕、按 ID 查询、幂等重试不重复应用 |
| `change.TestDeleteBatchRecordsAffectedAlerts` | 管理层批次记录列出删除规则导致的 closed 告警 |
| `change.TestConcurrentEvalSeesWholeRuleSet` | **并发求值中整批切换**：对每次 `Eval` 的 `Report.Rules` 完整签名做 A/B 判定，无撕裂规则集、firing 仅一次（`-race`） |
| `pipeline.TestBatchRuleChangeEndToEnd` | 门面端到端：dry-run→拒绝→原子应用→删除收尾→历史→旧接口兼容 |
| `pipeline.TestEndToEnd` | 确定性脚本端到端：11 接受 / 2 去重、聚合值、告警生命周期、淘汰查询报错 |

所有测试日志均通过 `t.Logf` 打印**输入样本**（labels/ts/value/dedup key）与
**判定依据**（窗口、计数、比较式、streak、缺口原因），时钟全部手工注入，
结论与运行次数、机器时间无关，稳定可复现。

## 10. 设计取舍说明

- 时间用整型 unix 秒而非 `time.Time`：为了让日志、断言与指纹在任何机器上字节级可复现。
- 去重要求显式 `SourceID + DedupKey`：没有幂等键的写入直接 `validation_error`，
  避免在「重试」语义不明时悄悄误计或误去重。
- 空窗口不补零：指标缺失与指标为 0 语义不同；告警侧也因此坚持「缺数据不动作」。
- 淘汰查询采取「报错 + 部分结果 + 缺口清单」而非静默截断，调用方可显式决定如何展示。
