# influx-cli 产品设计书

## 1. 产品定位

`influx-cli` 是一个面向 InfluxDB 1.x、openGemini 以及后续兼容 TSDB 的终端数据探索与运维分析控制台。

一句话定位：

> 在终端里完成 `pgcli` 的查询效率、`k9s` 的上下文导航、Grafana-lite 的趋势观察，以及 `influx_inspect`/query profiler 的诊断分析。

它不是通用 SQL CLI，而是 TSDB-native console：默认理解 time、series、measurement、tag、field、retention policy、cardinality、storage block 和 query cost。

## 2. 设计来源

本设计整理自当前仓库素材和本地 Claude 多轮讨论：

| 来源 | 用途 |
| --- | --- |
| `README.md` | 产品定位、模块边界、Go 技术栈、初始 roadmap |
| `TUIDesign.md` | TUI 布局、交互模式、快捷键、渲染策略 |
| 本地 Claude InfluxDB 会话 `479cffe1-f6b9-40bf-96ec-1dbe5a34db28` | InfluxDB TSM 窄时间范围查询优化评审，提炼 query profiler、storage analyzer、TSM block decode 指标、复现场景和高级诊断能力 |
| 本地 Claude paste-cache `d2dd308886abac68`、`b1bfdb2c8cf553ac` | TSM KeyCursor 范围过滤方案，作为 storage/query analysis 的能力参考 |

## 3. 目标用户

| 用户 | 典型问题 | 产品价值 |
| --- | --- | --- |
| TSDB 使用者 | 临时查询、确认趋势、排查数据缺失 | 在终端里快速查询和看趋势 |
| SRE/运维 | 确认库、RP、series、cardinality、写入/查询延迟 | 一屏查看上下文和健康状态 |
| 数据平台工程师 | 分析 schema 爆炸、tag 设计问题、慢查询 | schema-aware hints 和 profiler |
| 数据库内核/存储工程师 | 复现 TSM/TSI/WAL/tssp 问题，观察 block 解码和查询路径 | storage analyzer 与 query explain |
| openGemini/InfluxDB 开发者 | 同时需要 query、storage、internal 指标、复现数据生成 | 集成式实验台 |

## 4. 要解决的问题

### 4.1 查询体验差

InfluxQL、Flux、SQL-like 方言都存在输入成本高、历史复用弱、补全不足和结果观察困难的问题。传统 CLI 只输出表格，用户还要切换 Grafana 或脚本才能看趋势。

### 4.2 运维上下文分散

db、retention policy、measurement、field、tag、series cardinality、internal metrics 往往分散在不同命令和不同系统里。排查时需要手动拼命令。

### 4.3 性能问题不可见

慢查询、series explosion、tag 高基数、窄时间范围查询放大、storage block 过度解码等问题，传统 query console 很难直接感知。

### 4.4 存储和查询割裂

TSDB 问题常常同时横跨查询语句、索引、文件布局和内部指标。现有工具通常只覆盖 query 或 storage 其中一端。

## 5. 产品原则

1. 一屏完成“查、看、分析”。
2. 查询结果默认可视化优先，表格是基础能力但不是唯一输出。
3. 所有信息围绕 time、series、schema、cardinality 和 query cost 组织。
4. 先把 MVP 做薄、做准，不在第一阶段引入复杂插件、storage parser 和 dashboard 系统。
5. CLI 和 TUI 共用同一套 query orchestration、adapter、result model 和 renderer。
6. 所有高级分析都必须能降级：没有 profiler 权限时仍能完成基础查询。

## 6. 核心价值主张

### 6.1 TSDB-native UI

工具天然理解 TSDB 的数据模型，而不是把所有内容当成普通二维表。

核心对象：

| 对象 | 含义 |
| --- | --- |
| database | InfluxDB/openGemini database |
| retention policy | 存储策略和默认查询上下文 |
| measurement | 时间序列集合 |
| field | 数值或字符串值列 |
| tag | 维度标签 |
| series | measurement + tagset |
| shard | 时间分片 |
| block | 存储层读取单元 |
| internal metric | `_internal` 或系统观测数据 |

### 6.2 Query to Visualization

用户执行查询后，系统根据结果结构自动选择渲染方式：

| 数据结构 | 默认渲染 |
| --- | --- |
| time + numeric value | line chart 或 sparkline |
| time + 多个 numeric series | multi-line chart |
| category + numeric value | bar chart |
| 少量分类占比 | ratio view |
| 大型二维结果 | table + pager |
| 单列 numeric series | sparkline |
| explain/profiler 数据 | summary + table + hints |

### 6.3 Schema-aware Console

右侧 context panel 始终围绕当前查询对象展示 schema、cardinality、字段类型、tag 分布和建议。

### 6.4 Zero-switch Analytics

用户不用在终端、Grafana、脚本、数据库日志和 storage 工具之间反复切换。MVP 先覆盖 query + table/sparkline + statusline，后续逐步加入 profiler 和 storage analyzer。

## 7. 主要使用场景

### 7.1 临时查询并看趋势

用户输入：

```sql
SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1h GROUP BY time(1m)
```

系统行为：

1. 识别 time + numeric value。
2. 默认以 table 渲染；用户可用 `--format auto`、`--format sparkline` 或 REPL `:format` 切换趋势渲染。
3. statusline 展示 db、rp、latency、result points。
4. context panel 展示 measurement schema 和字段类型。

### 7.2 分析 `_internal`

用户查询 `_internal` 写入、查询或 shard 指标。

系统行为：

1. 默认保留 table 输出，便于检查原始点。
2. 显式切换到 auto 或 sparkline 后，以 sparkline 展示变化。
3. 标出 spike、空洞、突增。
4. 给出“可能与 series cardinality 或写入峰值有关”的 hints。

### 7.3 查看 schema 和 cardinality

用户使用 explorer 模式：

```text
:use metrics
:schema cpu
:cardinality top
```

系统行为：

1. 展示 measurement、field、tag tree。
2. 展示 top measurement by series。
3. 展示高基数 tag 组合。
4. 对疑似 series explosion 给出提示。

### 7.4 Watch 实时观察

用户执行：

```bash
influx-cli watch "SELECT mean(value) FROM cpu WHERE time > now() - 10m GROUP BY time(10s)"
```

系统行为：

1. 按 interval 自动刷新。
2. 保留趋势窗口。
3. statusline 展示刷新间隔、最近延迟、last error。
4. 异常时 footer 给出错误摘要。

### 7.5 慢查询和 TSM block 解码诊断

来自本地 Claude TSM 评审的高级场景：

用户遇到 `WHERE time = x` 或窄时间范围查询在乱序覆盖 TSM 文件下异常慢。诊断模块需要能解释：

1. 查询时间范围 `[StartTime, EndTime]`。
2. cursor seek time 与 scan range。
3. TSM file/block 是否与查询范围相交。
4. block decode 数量，例如 `float_blocks_decoded`。
5. 是否存在覆盖 block 导致 merge/dedup 窗口扩大。
6. 优化建议：在 storage cursor 的 locations 阶段过滤与查询范围不相交的 block。

该场景不进入 MVP，但进入 Phase 3/4 的 Query Profiler 和 Storage Analyzer。

## 8. 产品形态

### 8.1 CLI 模式

面向脚本、单次查询和 pipeline。

```bash
influx-cli query "SELECT * FROM cpu LIMIT 10"
influx-cli query --format table "SHOW MEASUREMENTS"
influx-cli query --format sparkline "SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(1m)"
influx-cli watch "SELECT mean(value) FROM cpu WHERE time > now() - 10m GROUP BY time(10s)"
influx-cli repl
```

### 8.2 REPL 模式

面向连续查询和上下文切换。

```text
influx> :use metrics
influx> :use metrics.autogen
influx> :db metrics
influx[metrics/autogen]> :format sparkline
influx[metrics/autogen]> SELECT mean(value) FROM cpu WHERE time > now() - 1h
```

REPL 必备能力：

| 能力 | MVP | 说明 |
| --- | --- | --- |
| 单行 query | yes | 基础执行 |
| 多行 query | phase 1 | 支持复杂 InfluxQL/Flux |
| history | phase 1 | 本地持久化，可用 `:history`/`:hist` 检索 |
| autocomplete | phase 1 | db/rp/measurement/field/tag |
| meta command | MVP/Phase 1 | `:use`、`:db`、`:dbs`、`:rps`、`:schema`、`:format`、`:history` |

### 8.3 TUI 模式

面向交互式探索。

```text
┌──────────────────────────────────────────────────────────────┐
│ db: metrics | rp: autogen | mode: influxql | adapter: influx │
│ series: 1.24M | latency: 12ms | result: 360 points | ok       │
├──────────────────────────────────────────────────────────────┤
│ > SELECT mean(value) FROM cpu WHERE time > now() - 1h         │
├───────────────────────────────┬──────────────────────────────┤
│ RESULT VIEW                   │ CONTEXT PANEL                │
│ table / sparkline / chart     │ schema / hints / stats       │
│                               │                              │
│ cpu usage                     │ measurement: cpu             │
│ ▁▂▃▄▆▇█▇▆▅▄▂                │ fields: usage,value          │
│ min 12.1 max 92.4 avg 43.0    │ tags: host,region,pid        │
├───────────────────────────────┴──────────────────────────────┤
│ Ctrl+Enter run | Ctrl+R history | Tab complete | q quit       │
└──────────────────────────────────────────────────────────────┘
```

## 9. TUI 区域设计

### 9.1 Statusline

职责：全局上下文和运行状态。

| 分类 | 内容 |
| --- | --- |
| context | db、rp、precision |
| query mode | influxql、flux、sql |
| adapter | influxdb1.x、openGemini、file replay |
| performance | latency、rows/points、qps |
| data scale | series cardinality、result size |
| health | last error、connection status |

MVP 展示：

```text
db: metrics | rp: autogen | mode: influxql | latency: 12ms | ok
```

### 9.2 Query Editor

职责：输入 query、meta command 和快捷操作。

核心能力：

| 能力 | 说明 |
| --- | --- |
| InfluxQL 输入 | MVP 必须支持 |
| meta command | `:use`、`:db`、`:dbs`、`:rps`、`:measurements`、`:msts`、`:schema`、`:format`、`:history` |
| history search | Phase 1 |
| autocomplete | Phase 1 |
| multiline | Phase 1 |
| syntax hint | Phase 2 |

### 9.3 Result View

职责：展示 query result。

渲染模式：

| 模式 | 用途 |
| --- | --- |
| table | 默认渲染，适合 schema、SHOW 查询和原始点检查 |
| sparkline | 趋势数据的显式渲染模式 |
| ASCII chart | 多点趋势和对比 |
| summary | explain、profiler、analysis result |

### 9.4 Context Panel

职责：帮助用户理解当前查询对象。

内容：

| 内容 | MVP | 说明 |
| --- | --- | --- |
| 当前 db/rp | yes | 状态上下文 |
| measurement schema | phase 1 | field/tag/type |
| cardinality summary | phase 2 | top measurement/tag |
| query hints | phase 2 | time bucket、group by、limit |
| profiler hints | phase 3 | scan/block decode/cost |

### 9.5 Footer

职责：显示快捷键、错误和下一步提示。

MVP：

```text
Enter run | Ctrl+C cancel | :q quit | :help commands
```

TUI：

```text
Ctrl+Enter run | Ctrl+R history | Tab complete | 1 table | 2 spark | 3 chart | q quit
```

## 10. 交互模式

| 模式 | 目标 | 入口 |
| --- | --- | --- |
| query mode | 单次查询和脚本输出 | `influx-cli query` |
| repl mode | 连续查询和上下文切换 | `influx-cli repl` |
| explorer mode | 类 k9s 的资源浏览 | `:dbs`、`:rps`、`:schema`、`:measurements`、`:msts` |
| chart mode | 自动或手动可视化结果 | `1/2/3` |
| watch mode | 实时刷新趋势 | `influx-cli watch` 或 `W` |
| profiler mode | query explain 和诊断 | `:explain` |
| storage mode | 文件/块/索引分析 | 后续 `storage` 子命令 |

## 11. 快捷键

| key | 动作 | 阶段 |
| --- | --- | --- |
| Enter | 执行当前 query | MVP |
| Ctrl+C | 取消当前执行或退出 | MVP |
| Ctrl+R | history search | Phase 1 |
| Tab | autocomplete | Phase 1 |
| 1 | table mode | MVP/TUI |
| 2 | sparkline mode | MVP/TUI |
| 3 | chart mode | Phase 2 |
| S | schema panel | Phase 1 |
| P | performance/profiler | Phase 3 |
| W | watch mode | Phase 2 |
| F | result fullscreen | Phase 2 |
| Q | quit | MVP |

## 12. MVP 边界

MVP 只做三件事：

1. REPL + query execution。
2. table 默认输出和 sparkline 趋势展示。
3. statusline 上下文状态。

MVP 必须包含：

| 模块 | 能力 |
| --- | --- |
| CLI | `query`、`repl` |
| adapter | InfluxDB 1.x HTTP API；openGemini 走兼容查询接口 |
| result | table model、series model |
| render | table、sparkline |
| session | db/rp/context |
| statusline | db/rp/mode/latency/error |
| config | host/token/user/password/db/rp |

MVP 明确不做：

| 不做 | 原因 |
| --- | --- |
| 完整 TUI dashboard | 容易膨胀，先稳定 query loop |
| 插件系统 | 先把内部接口设计好 |
| storage file parser | 高价值但复杂，放 Phase 4 |
| query optimizer | 先做 explain/hints，不改写 query |
| 多窗口拖拽 | 非核心 |
| 完整 Flux parser | 可先透传执行，后续增强 |

## 13. 成功指标

### 13.1 MVP 成功指标

| 指标 | 目标 |
| --- | --- |
| 首次连接到执行 query | 1 分钟内 |
| 单次 query 输出 | 支持 table/sparkline |
| REPL 上下文切换 | `:use db` 自动选择默认 RP，`:use db.rp` 可直接指定 RP，`:db db` 自动选择默认 RP |
| 错误可理解 | 网络、认证、语法错误有明确提示 |
| 趋势观察 | 通过 `--format auto`、`--format sparkline` 或 REPL `:format` 展示 sparkline |

### 13.2 中期指标

| 指标 | 目标 |
| --- | --- |
| schema autocomplete 覆盖 | db/rp/measurement/field/tag |
| cardinality profiling | top measurement/tag 可见 |
| watch 稳定性 | 长时间运行不刷屏、不泄漏 |
| explain 可用性 | 能解释 scan range、series count、latency |

### 13.3 长期指标

| 指标 | 目标 |
| --- | --- |
| storage analyzer | 可解释 TSM/TSI/WAL/tssp 结构和热点 |
| query profiler | 可定位慢查询、block decode 放大、series explosion |
| dataset generator | 可复现高基数、乱序、覆盖写入 |
| plugin ecosystem | adapter/analyzer/renderer 可扩展 |
