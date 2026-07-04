# Legacy Notice

This document is the original TUI draft. It is kept for historical context only.

Use the formal documents below as the source of truth:

- `docs/PRODUCT_DESIGN.md`
- `docs/ARCHITECTURE.md`
- `docs/ROADMAP.md`

Do not use this legacy draft for future design or implementation decisions unless a task explicitly asks for legacy context.

TUI 设计书

目标是：

> 做一个类似 **k9s + pgcli + grafana-lite + influx query console** 的 terminal UI

---

# 一、TUI 总体设计（核心理念）

这个 TUI 不是“终端界面”，而是：

> **TSDB Observability Console（时间序列数据库控制台）**

核心原则：

* 一屏完成“查 + 看 + 分析”
* 所有信息围绕：time / series / schema
* 默认减少输入成本（少打命令）
* 查询结果默认“可视化优先”

---

# 二、整体布局（核心 UI）

## 🧭 主界面 Layout（3 Pane + Status）

```text
┌──────────────────────────────────────────────────────────────┐
│ STATUS LINE (context / db / rp / mode / metrics)            │
├──────────────────────────────────────────────────────────────┤
│ QUERY EDITOR (multi-line / history / autocomplete)         │
│ > SELECT mean(value) FROM cpu WHERE time > now()-1h       │
│                                                            │
├───────────────────────────────┬──────────────────────────────┤
│ RESULT VIEW                   │ CONTEXT PANEL               │
│ (table / chart / sparkline)   │ schema / hints / stats      │
│                               │                              │
│  ▁▂▃▄▆▇█▇▆▅▄▂                │  db: metrics                │
│  cpu usage trend             │  series: 1.2M              │
│                               │  top measurement: cpu      │
│                               │                              │
├───────────────────────────────┴──────────────────────────────┤
│ FOOTER: shortcuts / mode / errors / hints                   │
└──────────────────────────────────────────────────────────────┘
```

---

# 三、区域设计拆解

# 1️⃣ STATUS LINE（最关键）

## 功能

全局 context + 系统状态

```text
db: metrics | rp: autogen | mode: influxql | adapter: influxdb1.x
series: 1.24M | cardinality: high | latency: 12ms | qps: 2.1k
last error: none
```

---

## 状态内容设计

| 类别          | 内容                    |
| ----------- | --------------------- |
| Context     | db / rp / precision   |
| Query mode  | influxql / flux       |
| Engine      | influxdb / openGemini |
| Performance | latency / qps         |
| Data scale  | series cardinality    |
| Health      | last error            |

---

## 交互

* click / hotkey → 展开详情
* `s` → schema view
* `p` → performance panel

---

# 2️⃣ QUERY EDITOR（核心输入区）

## 功能

* SQL / InfluxQL / Flux 输入
* multi-line
* history search
* autocomplete

---

## 特性设计

### ✔ 自动补全

```text
db → metrics
measurement → cpu / mem / disk
field → usage / value
tag → host / region
```

---

### ✔ 多行编辑

```sql
SELECT mean(value)
FROM cpu
WHERE time > now() - 1h
GROUP BY time(1m)
```

---

### ✔ 快捷键

| key        | action         |
| ---------- | -------------- |
| Ctrl+Enter | execute        |
| Ctrl+R     | history search |
| Tab        | autocomplete   |
| Ctrl+E     | expand editor  |

---

# 3️⃣ RESULT VIEW（核心体验区）

这是最重要的差异点。

---

## 支持 3 种渲染模式

---

## 🟢 1. Table mode（默认）

```text
time                 value
2026-07-05T10:00    12.3
2026-07-05T10:01    13.1
```

---

## 🔵 2. Sparkline mode（默认推荐）

```text
cpu usage:
▁▂▃▄▆▇█▇▆▅▄▂
min: 12%  max: 92%
```

---

## 🔴 3. ASCII Chart mode（Grafana-lite）

```text
CPU usage (%)

100 |        *
 80 |      *   *
 60 |    *
 40 |
     ---------------------
      0  10  20  30  40
```

---

## 自动切换规则（核心逻辑）

| 数据结构                 | UI         |
| -------------------- | ---------- |
| time + value         | line chart |
| category + value     | bar        |
| small set ratio      | pie        |
| single column series | sparkline  |

---

## 交互能力

| key | action        |
| --- | ------------- |
| 1   | table         |
| 2   | sparkline     |
| 3   | chart         |
| f   | fullscreen    |
| s   | save snapshot |

---

# 4️⃣ CONTEXT PANEL（右侧）

这是“增强理解层”，非常关键。

---

## 内容结构

### schema info

```text
measurement: cpu
fields:
  usage (float)
tags:
  host, region
```

---

### cardinality

```text
cpu: 120k series
mem: 80k series
disk: 30k series
```

---

### insights（很重要）

```text
⚠ cpu has high cardinality (host + pid)
⚠ possible series explosion detected
```

---

### query hints

```text
Suggested:
- group by host
- add time bucket 1m
```

---

# 5️⃣ FOOTER（辅助层）

```text
Ctrl+Enter execute | Ctrl+R history | Tab autocomplete | :q quit
```

---

# 四、核心交互模式设计（非常重要）

---

# 🧠 模式 1：REPL mode

```text
cli >
```

每条 query 即执行

---

# 🧠 模式 2：Explorer mode（类似 k9s）

```text
:use metrics
:list measurements
```

---

# 🧠 模式 3：Chart mode

自动进入：

```text
[GRAPH MODE]
```

---

# 🧠 模式 4：Watch mode（杀手功能）

```bash
watch SELECT mean(cpu) FROM ...
```

UI 自动刷新：

```text
live chart updating...
```

---

# 五、关键 UI 交互流程（很重要）

---

## 场景：分析 `_internal`

### Step 1

用户输入：

```sql
SELECT mean(write_points) FROM _internal...
```

---

### Step 2（自动识别）

系统判断：

```text
✔ time-series query detected
✔ switching to sparkline mode
```

---

### Step 3（UI）

```text
▁▂▃▄▆▇█▇▆▅▄▂
write QPS trend
```

---

### Step 4（context panel）

```text
⚠ spike detected at 10:32
```

---

# 六、快捷键体系（必须设计好）

| key        | 功能                    |
| ---------- | --------------------- |
| Enter      | run query             |
| Ctrl+Enter | force run             |
| Tab        | autocomplete          |
| Ctrl+R     | history               |
| 1/2/3      | table/chart/sparkline |
| S          | schema panel          |
| P          | performance           |
| W          | watch mode            |
| F          | fullscreen            |
| Q          | quit                  |

---

# 七、技术实现建议（Go + bubbletea）

---

## UI框架

* `bubbletea`（核心）
* `lipgloss`（布局）
* `bubbles`（input/list/pager）

---

## chart

* sparkline：自研（推荐）
* ASCII chart：自研 renderer
* optional：asciigraph

---

## layout model

```go
type Model struct {
    Query string
    Result Result
    Mode ViewMode
    Schema Schema
    Status Status
}
```

---

## view switch

```go
type ViewMode int

const (
    TableMode ViewMode = iota
    SparklineMode
    ChartMode
)
```

---

# 八、UI核心创新点（你这个工具的护城河）

---

## 1️⃣ “Query → Visualization 自动映射”

用户不需要选图类型

---

## 2️⃣ “Schema-aware CLI”

右侧 panel 永远知道数据结构

---

## 3️⃣ “TSDB-first UI”

不是 SQL CLI，是：

> time-series exploration console

---

## 4️⃣ “Zero-switch analytics”

* 查
* 看
* 分析

在一屏完成

---

# 九、MVP版本（建议你交给 Codex 的第一版）

---

## 必须实现：

### CLI

* REPL
* query execute

### UI

* statusline
* query editor
* table result
* sparkline result（必须）

### schema

* db list
* measurement list

---

## 不做（先别做）：

* TUI多窗口拖拽
* plugin system
* storage file parser
* query optimizer

---

# 十、最终产品形态（一句话）

你的工具最终会变成：

> **“在终端里完成 Grafana + pgcli + k9s + influx_inspect 的事情”**

---

# 如果你下一步要继续，我可以帮你补三样“Codex 直接能用”的东西：

1. 👉 bubbletea 项目骨架（main.go + model.go + view.go）
2. 👉 sparkline / ascii chart renderer（Go实现）
3. 👉 query → chart 自动识别算法
