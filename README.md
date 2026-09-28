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
- **规则变更支持整批提交**：整批先校验（重名、窗口、聚合口径、比较方式、阈值、
  pending/keep 配置、删除不存在的规则），任一条不合格则整批不生效，规则与告警
  状态保持提交前一致；另有只检查不落地的预览入口；
- **合法批次原子切换**：并发求值不会看到半旧半新的规则集；每批留下可查询记录
  （增/改/删清单、改动前后关键配置、影响到的在跟踪告警）；
- 规则被删或选择器收窄后，仍处于 pending/firing 的告警会以 `closed` 事件**显式收尾
  并附原因**，不会无声消失；之后规则加回来/改回来从干净状态重新跟踪。

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
| `alert` | 规则求值与告警状态机、规则热更新、**原子规则集切换 + 告警收尾** |
| `change` | **整批规则变更管理：预校验、只检查预览、原子提交、收尾与变更记录** |
| `pipeline` | 门面接线 + 确定性内存数据源脚本（乱序/重复/延迟） |
| `cmd/demo` | 端到端可运行演示（含整批规则变更演示段） |

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

## 7.5 整批规则变更管理（`change` 包）

线上通常攒一批改动一起推。`change.Manager` 在求值器之上加了一层「先整批检查、
再一次性切换、全程留痕」的变更管理。

### 7.5.1 批次模型与整批校验

一次提交是一批 `change.Item`，每条要么 `upsert`（新增或修改），要么 `delete`：

```go
batch := []change.Item{
    {Op: change.OpUpsert, Rule: ruleA},
    {Op: change.OpDelete, Rule: model.Rule{ID: "old-rule"}},
}
pv := p.PreviewRuleChanges(batch) // 只检查，不落地
rec, errs, err := p.CommitRuleChanges(batch)
```

提交前对**整批**做检查，问题一次性列全（`[]ValidationError`，按规则名/字段排序）：

- 同一批内规则名重复（即使是 `delete` 与 `upsert` 混用）——批次意图必须无歧义；
- `ID` 非空；`WindowSeconds > 0`；
- `AggField ∈ {count,sum,avg,min,max,latest}`；
- `Op ∈ {>,>=,<,<=,==,!=}`；`Threshold` 必须是有限数；
- `PendingWindows / KeepFiringWindows >= 0`；
- `delete` 一个当前规则集里不存在的规则（陈旧批次）也会被拒绝；
- `op` 必须是 `upsert|delete`。

**任一条不合格，整批不生效**：校验是纯只读的，提交前不会改任何东西；被拒绝时规则、
告警状态、规则集版本号、变更历史与提交前完全一致。`Validate`（门面
`PreviewRuleChanges`）是只检查入口：它在一次性模拟求值器上预演切换，因此还能提前
列出这批会增改删哪些规则、会影响/收尾哪些在跟踪告警，而对线上求值器零影响。

### 7.5.2 合法批次原子切换 + 可查询记录

合法批次通过 `Evaluator.ApplyRuleset` **在求值器同一把互斥锁内**完成「规则集替换 +
状态对账 + 版本号自增」，因此并发 `Eval` 不可能看到一半新一半旧的规则集；规则集带
单调版本号（`Evaluator.Version()`，求值 `Report.Version` 即该 pass 看到的版本），
单条规则处理失败也无法把整批规则和存活状态一起带走（切换中无外部 I/O，不会部分失败）。

每批成功提交产生一条 `change.Record`，可按 ID 查询、也可列全量历史：

```
Record { ID, CommittedAt, RulesetVersion,
         Changes []RuleChange{ added|updated|deleted, Before*, After* }, // 改动前后关键配置
         Affected []AffectedAlert{ RuleID, Labels, Status(切换前),
                                   preserved|closed_by_rule_delete|
                                   closed_by_selector_narrowed, CloseReason } }
```

- `Changes` 只含本批动到的规则：新增带 `After`，删除带 `Before`，修改前后都带；
- `Affected` 列出本批动到的规则下、处于 pending/firing 的在跟踪告警：是被保留
  （`preserved`，继续跟踪、不重复触发）还是被收尾、收尾原因是什么。

### 7.5.3 告警收尾与干净重启

- 规则被**删除**：其 pending/firing 告警各产生一条 `EventClosed` 事件，原因形如
  `rule "x" deleted ...: firing alert closed explicitly instead of vanishing`，
  随后状态被移除；
- 规则**选择器收窄**（改后不再匹配某序列）：对应 pending/firing 告警同样以
  `closed_by_selector_narrowed` 显式收尾。注意：仅改阈值/窗口/算子等参数、选择器
  仍覆盖的告警**保留原状态**，更新不会把仍有效的告警重复触发，也不会丢掉本该继续
  跟踪的进度（求值语义不变：下一个窗口按新参数自然推进）；
- resolved 的休眠条目不产生收尾事件（本就没有进行中的告警）；
- 规则之后被**加回来或改回来**时，由于旧状态已在切换时移除，求值器会以全新的
  resolved 起点重新走 pending/firing，而不会继承已收尾的旧进度或旧 streak。

### 7.5.4 与原有单条/整体替换方式的兼容范围

- 原 `Evaluator.ReplaceRules` / `Pipeline.ReplaceRules` **保留不变**：按规则 ID 对齐、
  存活规则状态保留、不重放 firing；区别是它对被删规则的状态是**静默清理**（无
  closed 事件、无记录、不做批次校验）。适合原本就接受该语义的调用方；
- 新链路 `PreviewRuleChanges` / `CommitRuleChanges`（底层 `change.Manager` +
  `Evaluator.ApplyRuleset`）提供校验、原子整批切换、显式收尾与审计记录，推荐用于
  运维批量推规则；
- 两条链路共用同一把求值锁与同一套状态机，可以并存；规则集版本号在两种切换下都会
  单调递增。

## 8. 验证方法与测试覆盖

本地验证新增能力：

```bash
go vet ./...
go test -race -count=1 ./change ./alert ./pipeline  # 新行为 + 并发整批切换竞态检测
go test ./... -v                                    # 查看输入与判定依据日志
go run ./cmd/demo                                   # 第 6 段演示预览/拒批/原子提交/收尾/记录
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
| `change.TestBatchInvalidRejectsWholeBatch` | 整批非法时一次列全所有问题，提交后规则/在跟踪告警/版本/历史均不变 |
| `change.TestPreviewDoesNotMutate` | 只检查预览给出增删改与预演收尾，但线上规则集与状态零改动 |
| `change.TestCommitClosesAndRecords` | 删除→`closed_by_rule_delete`、选择器收窄→`closed_by_selector_narrowed`、存活更新→preserved；记录含改动前后配置；加回来从干净状态重新触发 |
| `change.TestCommitMixedAddUpdateDeleteNoPartialFailure` | 批中一条非法，新增/修改/删除的兄弟条目全部不生效，版本号不动 |
| `change.TestConcurrentEvalAndBatchSwitchRace` | 并发求值过程中规则在两版间整批翻转：永不同时观测到新旧两版特征（无半切换），`-race` 通过 |
| `pipeline.TestPipelineBatchChangesEndToEnd` | 门面链路：预览→非法整批被拒→合法原子提交→firing 告警显式收尾→记录可查 |
| `pipeline.TestEndToEnd` | 确定性脚本端到端：11 接受 / 2 去重、聚合值、告警生命周期、淘汰查询报错 |

所有测试日志均通过 `t.Logf` 打印**输入样本**（labels/ts/value/dedup key）与
**判定依据**（窗口、计数、比较式、streak、缺口原因），时钟全部手工注入，
结论与运行次数、机器时间无关，稳定可复现。

## 9. 设计取舍说明

- 时间用整型 unix 秒而非 `time.Time`：为了让日志、断言与指纹在任何机器上字节级可复现。
- 去重要求显式 `SourceID + DedupKey`：没有幂等键的写入直接 `validation_error`，
  避免在「重试」语义不明时悄悄误计或误去重。
- 空窗口不补零：指标缺失与指标为 0 语义不同；告警侧也因此坚持「缺数据不动作」。
- 淘汰查询采取「报错 + 部分结果 + 缺口清单」而非静默截断，调用方可显式决定如何展示。
