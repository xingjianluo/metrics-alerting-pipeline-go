# Metrics Aggregation & Alerting Pipeline (in-process, Go)

一个完全本地运行、无外部依赖的指标聚合与告警评估流水线。用内存存储模拟采集端，
重点保证三件事在**乱序到达、重复上报、规则热更新**下始终一致且可解释：

1. 样本被归入它真正所属的时间窗口，重复上报幂等；
2. 同一序列在不同窗口粒度/边界上的聚合自洽，查询不会静默返回残缺数据；
3. 告警状态机的每次转换都有证据，热更新与并发求值不丢告警、不重复触发。

## 目录结构

```
pkg/model/    数据模型：Sample / SeriesID / WindowAggregate / 拒绝原因
pkg/storage/  内存存储：窗口分桶、去重索引、保留期淘汰、区间查询
pkg/alert/    告警规则、阈值比较、三态状态机、原子热更新
cmd/demo/     可运行的端到端演示（虚拟时钟，输出完整证据链）
```

## 1. 数据模型

| 类型 | 说明 |
| --- | --- |
| `model.Sample` | `{Name, Labels, Value, Timestamp(ms)}`，一条上报数据点 |
| `model.SeriesID` | 序列的规范标识：`name{k="v",...}`，标签先排序，标签书写顺序不影响身份 |
| `model.WindowAggregate` | 一个窗口内的 `count/sum/min/max/last/avg`，窗口为半开区间 `[start,end)` |
| `model.WriteError` | 携带机器可区分的 `RejectReason` |

**去重键**为 `(序列, 时间戳, 值)`：

| 拒绝原因 | 含义 |
| --- | --- |
| `sample_too_old` | 样本所属窗口已低于保留期地平线，不能再写入（拒绝复活已淘汰状态） |
| `sample_from_future` | 时间戳超过当前时钟 + 60s 时钟偏移容忍 |
| `duplicate_sample` | 完全相同的 `(序列,ts,值)` 已存在，不重复计数 |
| `duplicate_timestamp_value_mismatch` | 同 `(序列,ts)` 上报了不同的值，显式冲突错误 |

## 2. 窗口与保留策略

- **固定窗口，按 epoch 对齐**：窗口大小 `windowSizeMs`，`start = ts - ts%size`。
  乱序/延迟样本按时间戳归桶，与到达顺序无关。聚合中的 `last` 也以**样本时间**
  而非到达顺序为准，因此同批数据无论按什么顺序到达，聚合结果逐字节一致
  （见 `TestOutOfOrderAndDuplicate` 中的顺序互换对比）。
- **保留期** `retentionMs >= windowSizeMs`。保留地平线
  `horizon = align(now - retention, window)`；`Evict()` 删除 `start < horizon`
  的窗口。落在地平线之前的写入直接以 `sample_too_old` 拒绝。
- **查询** `QueryRange(series, start, end, step)`：
  - `[start,end)` 与 `step` 必须对齐基准窗口，`step` 必须是基准窗口的整数倍；
  - 粗粒度结果是其覆盖的基准窗口的**确定性合并**（sum/count 相加、min/max 取
    极值、last 取时间戳最大者），跨边界自洽（`TestGranularityConsistency`）；
  - 没有数据的槽位返回 `Count=0` 的聚合（空槽与"数据被淘汰"语义不同）；
  - **一旦查询起点低于保留地平线，返回 `storage.ErrPartialData` 错误**，
    绝不静默返回残缺结果（`TestEvictAndPartialData`）。

## 3. 告警状态机

三态（初始 `resolved`），每次求值只看"当前已闭合的最近一个规则窗口"：

```
                 cond=true
   ┌──────────────────────────────┐
   │                              ▼
resolved                     ┌─────────┐  cond 持续 >= forMs   ┌────────┐
   ▲                         │ pending │ ───────────────────▶ │ firing │
   │  cond=false             └─────────┘                      └────────┘
   │  (任一活跃态)                │ cond=false                     │ cond=false
   └─────────────────────────────┴────────────────────────────────┘
```

- `for=0` 时 `resolved` 直接跳 `firing`。
- `trueSince` 锚定在**首个为真窗口的起点**，与求值节拍频率无关：条件连续保持
  `windowEnd - trueSince >= forMs` 才触发；中途断档回到 `resolved` 并重新计时。
- **未知 ≠ 恢复**：窗口内无样本，或查询命中已淘汰数据（`ErrPartialData`）时
  状态保持不变——数据缺失绝不产生假恢复或丢告警（`TestEvictedDataKeepsAlert`）。
- 每次转换生成一条 `Transition{RuleID, Series, AtMs, From, To, Condition, Value, Reason}`，
  `Reason` 内含规则、窗口、聚合值、比较式，判定依据可逐条复述；引擎保留有序的
  完整转换日志（`Transitions()`）。

### 热更新语义（`SetRules`）

- 一次调用原子替换整个规则集，校验失败（id 重复、窗口不对齐、未知聚合/算符）
  时整批拒绝，旧规则集继续生效。
- 状态按 **rule ID + 序列** 键控：同 ID 规则改阈值/聚合不丢状态、不重复触发。
- 被删除的规则若处于 `pending/firing`，热更新时**显式**记录一条
  `-> resolved`（原因 `rule removed during hot reload...`），不允许告警悬挂。

并发上，引擎用互斥锁把"求值节拍"串行化；存储用读写锁保护。并发求值 + 并发
热更新的不丢不重由 `TestConcurrentEvaluation` 在 `-race` 下验证。

## 4. 验证方法

```bash
# 普通单测
go test ./...

# 竞态检测 + 重复运行验证可复现（要求测试使用注入的虚拟时钟）
go test -race -count=3 ./...

# 端到端演示：乱序/重复写入、两种粒度查询、告警生命周期、热更新保状态
go run ./cmd/demo
```

单测均使用注入的 `Clock`（虚拟时间），结果与墙钟无关、稳定可复现；每个测试
通过 `t.Logf` 打印**输入样本**与**判定依据**（拒绝原因、聚合值、状态转换与
理由），`go test -v` 可查看完整判定链。覆盖场景：

- `storage`：乱序与顺序到达聚合等价、四类拒绝原因、跨粒度自洽、淘汰后查询
  报错、并发重复写入只计一次；
- `alert`：`>=` 阈值边界、`resolved→pending→firing→resolved` 全链路、pending
  中断重计时、热更新保状态/删除显式恢复/非法原子回滚、数据缺失不动状态、
  并发求值与热更新恰好触发一次。
