# Legacy Notice

This document is the original product draft. It is kept for historical context only.

Use the formal documents below as the source of truth:

- `docs/PRODUCT_DESIGN.md`
- `docs/ARCHITECTURE.md`
- `docs/ROADMAP.md`

Do not use this legacy draft for future design or implementation decisions unless a task explicitly asks for legacy context.

# influx-cli
CLI + TUI + 数据分析一体化方案（Go 技术栈）

* 产品定位
* 总体架构
* 模块设计
* 关键技术选型
* chart / TUI / query engine 设计
* 插件体系
* roadmap（分阶段可交付）
* MVP边界（避免一开始做成怪物系统）

---

# 一、产品定义（你这个工具的本质）

> 一个面向 InfluxDB / openGemini 的 **终端数据探索与运维分析控制台**

一句话版本：

> **“k9s + pgcli + Grafana-lite + influx_inspect + query profiler 的 Go TUI 工具”**

---

## 核心目标

解决 4 类问题：

### 1️⃣ 查询体验差

* SQL / InfluxQL / Flux 写起来痛苦
* 结果不可视化

### 2️⃣ 运维信息分散

* db / rp / series / cardinality 要手动查

### 3️⃣ 性能问题不可感知

* slow query / series explosion 看不到

### 4️⃣ storage / internal 数据难分析

* `_internal` 只能看数字

---

# 二、整体架构设计（核心）

```text
┌────────────────────────────────────────────┐
│               TUI / CLI Layer              │
│  statusline | charts | query editor       │
├────────────────────────────────────────────┤
│            Query Orchestration             │
│  parser | router | AST | history          │
├────────────────────────────────────────────┤
│         Intelligence / Analytics          │
│  explain | profiling | schema graph       │
├────────────────────────────────────────────┤
│             Visualization Layer            │
│  sparkline | ascii chart | table render   │
├────────────────────────────────────────────┤
│              Storage Adapters             │
│ influxdb1.x | openGemini | file parser   │
└────────────────────────────────────────────┘
```

---

# 三、核心模块设计

---

# 1️⃣ CLI Core（入口层）

## 功能

* REPL
* 单次 query 执行
* pipeline 模式
* watch 模式

```bash
cli query "SELECT ..."
cli repl
cli watch "SELECT ..."
```

---

## 关键能力

* command router
* context（db/rp/precision）
* session state

---

# 2️⃣ Query Engine（核心）

## 负责：

### ✔ SQL / InfluxQL / Flux

* parse
* normalize
* route adapter

### ✔ AST layer（非常关键）

统一表达：

```go
type Query struct {
    Dialect string
    Database string
    RP string
    Raw string
    AST interface{}
}
```

---

## 进阶能力：

### 🔥 query explain

* scan series
* cost estimation
* index usage hint

---

# 3️⃣ Adapter Layer（关键扩展点）

```text
influxdb1.x adapter
openGemini adapter
file replay adapter（可选）
```

统一接口：

```go
type Adapter interface {
    Query(ctx context.Context, q Query) (Result, error)
    ShowDatabases()
    ShowSeries()
    GetSchema()
}
```

---

# 4️⃣ Visualization Layer（你最关键的差异点）

---

## ✔ 三种输出模式

### ① table（默认）

类似：

```
time                value
2026-07-05T10:00    12.3
2026-07-05T10:01    14.1
```

---

### ② sparkline（强烈推荐默认）

```
CPU usage:
▁▂▃▅▆▇█▇▆▅▄▂
```

👉 用于：

* _internal
* metrics
* latency

---

### ③ ASCII chart（Grafana-lite）

```
CPU
100 |        *
 80 |      *   *
 60 |   *
 40 |
     ----------------
     0   10  20  30
```

---

## ✔ 自动图表识别（核心体验）

| 数据结构              | 图       |
| ----------------- | ------- |
| time + value      | line    |
| category + value  | bar     |
| 2 numeric columns | scatter |
| small set ratio   | pie     |

---

# 5️⃣ Statusline（k9s 风格核心体验）

```text
db: metrics | rp: autogen | mode: influxql
series: 1.2M | latency: 12ms | qps: 3.2k
```

---

## 动态内容：

* 当前 db/rp
* series cardinality
* query latency
* write throughput（如果支持）
* last error
* adapter type

---

# 6️⃣ Schema Intelligence（杀手功能）

---

## 提供：

### ✔ schema explorer

```
db -> measurement -> field/tag tree
```

---

### ✔ cardinality profiler

```
top measurements by series
top tags causing explosion
```

---

### ✔ schema drift detection

```
tag type变化
field type变化
measurement爆炸
```

---

# 7️⃣ Storage File Analyzer（高级模块）

支持：

* tsm / tsi / wal
* openGemini tssp / mergeset

---

## 分层设计：

### L1（MVP）

* file metadata
* segment summary

### L2

* index analysis
* block stats

### L3

* queryable inspector

---

# 8️⃣ Dataset Generator（压测能力）

```bash
cli ingest demo-cpu --rate 10k/s
```

支持：

* 时间序列模拟
* tag组合爆炸
* 数据乱序模拟

👉 类似 k6 + influx load tool

---

# 9️⃣ Query Lab（高级体验）

---

## ✔ query history replay

```
replay query id=123
```

---

## ✔ query diff

```
diff query A vs B
```

---

## ✔ query template

```
top slow series
cardinality explosion detect
```

---

# 四、TUI 设计（k9s风格）

---

## layout

```text
┌─────────────── status ───────────────┐
│ db: metrics | series: 1.2M | 12ms    │
├───────────────────────────────────────┤
│ query editor                         │
├───────────────────────────────────────┤
│ result (chart / table / sparkline)   │
├───────────────────────────────────────┤
│ history / schema / hints             │
└───────────────────────────────────────┘
```

---

# 五、技术栈选择（Go）

---

## UI / TUI

* `bubbletea`（核心推荐）
* `lipgloss`（样式）
* `bubbles`（组件）

---

## chart

* `asciigraph`（line chart）
* 自研 sparkline renderer（推荐）

---

## CLI

* `cobra`
* `urfave/cli`（轻量备选）

---

## parser

* 自写 SQL / InfluxQL parser（建议简化）
* AST 可逐步增强

---

## storage client

* influxdb client v2 / 1.x HTTP API
* openGemini client（HTTP/gRPC）

---

# 六、插件体系设计（非常重要）

```go
type Plugin interface {
    Name() string
    Execute(ctx Context, input any) (any, error)
}
```

插件类别：

* chart plugin
* adapter plugin
* analyzer plugin
* ingest plugin

---

# 七、Roadmap（非常关键）

---

# 🧩 Phase 0（1~2周）：CLI MVP

### 目标：能用

* REPL
* influxql query
* table output
* basic statusline

✔ deliverable：

* cli query
* cli repl
* cli use db

---

# 🧩 Phase 1（2~4周）：体验升级

### 目标：像 pgcli

* history
* multiline editor
* autocomplete（db/rp/measurement）
* sparkline

✔ deliverable：

* interactive REPL
* schema fetch
* sparkline chart

---

# 🧩 Phase 2（1~2月）：分析能力

### 目标：开始“像 k9s + grafana-lite”

* ASCII chart
* cardinality analysis
* schema explorer
* query explain

✔ deliverable：

* chart module
* schema profiler
* explain system

---

# 🧩 Phase 3（2~3月）：杀手级能力

### 目标：从工具变平台

* query diff
* dataset generator
* replay system
* anomaly detection

✔ deliverable：

* query lab
* ingest simulator
* alert hints

---

# 🧩 Phase 4（长期）：TUI dashboard

* multi-panel
* live monitoring
* storage file inspector UI
* plugin ecosystem

---

# 八、你这个项目的“真正护城河”

如果做完整，你的工具会有 3 个别人很难复制的点：

---

## 1️⃣ TSDB-native UI（不是 SQL CLI）

不是 pgcli，而是：

> 专门理解 series / time / cardinality 的 CLI

---

## 2️⃣ storage + query 一体化

别人只做 query tool，你做：

* query
* storage
* file
* internal metrics

---

## 3️⃣ grafana-lite in terminal

👉 这是最有价值点：

> “不用开 Grafana，也能看趋势”

---

# 九、我给你的最终建议（很重要）

不要一开始做“大而全”

你应该先死磕一个点：

## 👉 MVP 必须只做 3 件事：

1. REPL + query
2. sparkline（趋势图）
3. statusline
