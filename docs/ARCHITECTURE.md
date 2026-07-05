# influx-cli 架构说明

## 1. 架构目标

`influx-cli` 的架构目标是让 CLI、REPL、TUI、watch、profiler 和后续 storage analyzer 共用同一套核心能力。

设计重点：

1. Adapter 与 UI 解耦，便于同时支持 InfluxDB 1.x、openGemini 和文件分析。
2. Query orchestration 与渲染解耦，便于同一查询结果输出 table、sparkline、chart、JSON。
3. Session/context 是一等对象，db/rp/precision/mode 不散落在命令参数里。
4. Result model 统一承载 table、series、schema、profiler summary。
5. Advanced analytics 作为可选层，不阻塞 MVP 查询链路。

## 2. 总体架构

```text
┌─────────────────────────────────────────────────────────────┐
│                         Entry Points                         │
│        cobra CLI | REPL | Bubble Tea TUI | watch loop         │
├─────────────────────────────────────────────────────────────┤
│                       Application Core                       │
│      command router | session | config | history | state      │
├─────────────────────────────────────────────────────────────┤
│                    Query Orchestration                       │
│    dialect detect | parser-lite | query model | execution     │
├─────────────────────────────────────────────────────────────┤
│                    Intelligence Layer                        │
│      schema cache | cardinality | explain | query profiler    │
├─────────────────────────────────────────────────────────────┤
│                    Visualization Layer                       │
│      table renderer | sparkline | ascii chart | summaries     │
├─────────────────────────────────────────────────────────────┤
│                       Adapter Layer                          │
│        influxdb1.x HTTP | openGemini | file replay/analyzer   │
├─────────────────────────────────────────────────────────────┤
│                    External Systems                          │
│             InfluxDB | openGemini | local storage files       │
└─────────────────────────────────────────────────────────────┘
```

## 3. 建议目录结构

```text
.
├── cmd/
│   └── influx-cli/
│       └── main.go
├── internal/
│   ├── app/
│   │   ├── app.go
│   │   ├── command.go
│   │   └── session.go
│   ├── config/
│   │   └── config.go
│   ├── adapter/
│   │   ├── adapter.go
│   │   ├── influxdb/
│   │   ├── opengemini/
│   │   └── file/
│   ├── query/
│   │   ├── query.go
│   │   ├── dialect.go
│   │   ├── orchestrator.go
│   │   └── explain.go
│   ├── result/
│   │   ├── result.go
│   │   ├── table.go
│   │   └── series.go
│   ├── render/
│   │   ├── table.go
│   │   ├── sparkline.go
│   │   └── chart.go
│   ├── schema/
│   │   ├── schema.go
│   │   └── cache.go
│   ├── analysis/
│   │   ├── cardinality.go
│   │   ├── profiler.go
│   │   └── storage.go
│   ├── repl/
│   ├── tui/
│   ├── watch/
│   └── history/
├── docs/
└── README.md
```

MVP 可以先建立较少目录，但 package 边界应提前按上述方向收敛。

## 4. 核心数据模型

### 4.1 Session

```go
type Session struct {
    AdapterName string
    Database    string
    RP          string
    Precision   string
    Dialect     Dialect
    LastLatency time.Duration
    LastError   error
}
```

职责：

| 字段 | 用途 |
| --- | --- |
| AdapterName | 当前连接的后端类型 |
| Database | 默认 database |
| RP | 默认 retention policy |
| Precision | 查询/输出时间精度 |
| Dialect | influxql、flux、sql |
| LastLatency | statusline 展示 |
| LastError | footer/statusline 展示 |

### 4.2 Query

```go
type Query struct {
    Raw       string
    Dialect   Dialect
    Database  string
    RP        string
    Precision string
    Kind      QueryKind
    Range     TimeRange
}
```

`Kind` 用于标识：

| Kind | 示例 |
| --- | --- |
| Select | `SELECT mean(value) FROM cpu` |
| Show | `SHOW MEASUREMENTS` |
| Meta | `:use metrics`、`:use metrics.autogen` |
| Explain | `:explain SELECT ...` |
| Schema | `:schema cpu` |
| Watch | watch loop 内部 query |

### 4.3 Result

```go
type Result struct {
    Kind      ResultKind
    Table     *Table
    Series    []Series
    Schema    *SchemaSnapshot
    Profile   *QueryProfile
    Metadata  ResultMetadata
}
```

统一 result model 可以让同一份数据被不同 UI 消费。

### 4.4 Series

```go
type Series struct {
    Name   string
    Tags   map[string]string
    Points []Point
}

type Point struct {
    Time  time.Time
    Value float64
}
```

Sparkline 和 ASCII chart 优先消费 `[]Series`。

### 4.5 SchemaSnapshot

```go
type SchemaSnapshot struct {
    Database     string
    RP           string
    Measurements []Measurement
}

type Measurement struct {
    Name   string
    Fields []Field
    Tags   []Tag
}
```

## 5. 核心接口

### 5.1 Adapter

```go
type Adapter interface {
    Name() string
    Ping(ctx context.Context) error
    Query(ctx context.Context, q query.Query) (result.Result, error)
    ShowDatabases(ctx context.Context) ([]string, error)
    ShowRetentionPolicies(ctx context.Context, db string) ([]RetentionPolicy, error)
    GetSchema(ctx context.Context, scope SchemaScope) (schema.SchemaSnapshot, error)
}

type RetentionPolicy struct {
    Name               string
    Duration           string
    ShardGroupDuration string
    ReplicaN           string
    Default            bool
}
```

MVP 先保证 `Query` 和 `ShowDatabases`，其余能力可以返回 `ErrNotSupported`。

### 5.2 Analyzer

```go
type Analyzer interface {
    Name() string
    Analyze(ctx context.Context, input AnalysisInput) (AnalysisResult, error)
}
```

Analyzer 不是 MVP 必需，但接口应为 cardinality、query profiler、storage analyzer 留扩展空间。

### 5.3 Renderer

```go
type Renderer interface {
    Name() string
    CanRender(result.Result) bool
    Render(result.Result, RenderOptions) (string, error)
}
```

Renderer 类型：

| Renderer | 用途 |
| --- | --- |
| table | 兜底 |
| sparkline | 时间序列趋势 |
| chart | ASCII chart |
| summary | explain/profile |
| json | 脚本输出 |

## 6. Query 执行流程

```text
user input
  -> command router
  -> meta command? yes -> app/session/schema action
  -> query normalize
  -> dialect detect
  -> session context injection
  -> adapter.Query()
  -> normalize adapter response into Result
  -> render mode selection
  -> renderer.Render()
  -> status/session update
```

### 6.1 单次 query

```text
CLI args
  -> build Query
  -> execute
  -> render stdout
  -> exit code
```

### 6.2 REPL

```text
read input
  -> assemble multiline statement
  -> complete from schema cache on Tab
  -> detect meta/query
  -> execute
  -> render
  -> persist history
  -> continue
```

REPL history 属于 application core 的本地状态，默认写入 `~/.local/state/influx-cli/history.jsonl`，也支持 `XDG_STATE_HOME`。MVP/Phase 1 的 REPL 使用 `:history [limit] [filter]` 或 `:hist [limit] [filter]` 检索已持久化的 query；交互式终端启动时会把已持久化 query 加载进 readline 内存历史，支持 Up/Down 导航，readline 不单独写历史文件。

REPL multiline 属于 UI 输入组装层：普通单行 query 保持 Enter 即执行；显式续行 `\`、未闭合引号/括号、Flux pipeline continuation 或进入多行后的终止分号会控制 pending buffer。pending query 中的 meta command 不会执行，可用 `:cancel` 或 `:clear` 清空。

Autocomplete 由 application core 提供候选，REPL line editor 只负责调用。候选来源为 `ShowDatabases`、`ShowRetentionPolicies`、`ShowMeasurements` 和 `GetSchema`。当 query 已能识别 `FROM <measurement>` 时补全使用单 measurement schema；当用户从左到右输入 `SELECT ...` 尚未写出 `FROM` 时，补全使用当前 db/rp 的 DB 级 schema。schema/measurement cache 默认 TTL 60s，并可通过 `:refresh schema` 手动清空。

### 6.3 TUI

```text
Bubble Tea update loop
  -> editor command
  -> async query command
  -> result message
  -> model update
  -> view render
```

## 7. Adapter Layer

### 7.1 InfluxDB 1.x Adapter

使用 HTTP query API。

基础能力：

| 能力 | Query |
| --- | --- |
| query | `/query?q=...&db=...&rp=...` |
| show databases | `SHOW DATABASES` |
| show measurements | `SHOW MEASUREMENTS` |
| show field keys | `SHOW FIELD KEYS` |
| show tag keys | `SHOW TAG KEYS` |
| cardinality | `SHOW SERIES CARDINALITY` 或近似查询 |

认证：

| 方式 | 阶段 |
| --- | --- |
| username/password | MVP |
| token/header | Phase 1 |
| config profile | MVP |

### 7.2 openGemini Adapter

openGemini 初期可走 InfluxDB 兼容查询接口。

差异点：

| 项 | 说明 |
| --- | --- |
| 连接信息 | 可复用 InfluxDB style config |
| schema 查询 | 先用兼容 SHOW 查询 |
| storage/internal | 后续增加 openGemini 特有分析 |
| 集群状态 | Phase 3 之后支持 meta/store/sql 维度 |

### 7.3 File Replay/Storage Adapter

长期能力，用于分析本地文件或复现数据。

支持范围：

| 类型 | 阶段 |
| --- | --- |
| query result replay | Phase 3 |
| TSM metadata | Phase 4 |
| TSI summary | Phase 4 |
| WAL summary | Phase 4 |
| openGemini tssp/mergeset | Phase 4+ |

## 8. Visualization Layer

### 8.1 Render Mode Selection

MVP 默认 render mode 为 `table`。`auto` 是显式选择的模式，用于根据 result shape 自动选择 sparkline、chart 或 table。

```text
Result
  -> inspect columns/types
  -> time + numeric? sparkline/chart
  -> category + numeric? bar
  -> schema/meta? table
  -> profiler? summary
  -> fallback table
```

选择规则：

| 条件 | 渲染 |
| --- | --- |
| `time` + 1 numeric column | sparkline |
| `time` + 多 numeric series | chart |
| SHOW/metadata | table |
| 行数过大 | paged table + summary |
| profile result | summary + hotspots table |

### 8.2 Sparkline

MVP 推荐自研，避免引入复杂依赖。

要求：

1. 支持空值。
2. 支持 min/max/avg summary。
3. 宽度随终端调整。
4. 小数据量不崩溃。
5. 多 series 初期可先选第一条或显示 top N。

### 8.3 ASCII Chart

Phase 2 引入，可先用 `asciigraph`，也可以逐步自研。

要求：

1. 支持 line chart。
2. 支持固定高度。
3. 支持 axis label。
4. 支持多 series 时的可读降级。

## 9. TUI 架构

建议使用：

| 组件 | 作用 |
| --- | --- |
| Bubble Tea | event/update/view |
| Lip Gloss | layout/style |
| Bubbles | textarea/list/pager |

### 9.1 TUI Model

```go
type Model struct {
    Session      app.Session
    Editor       EditorModel
    Result       result.Result
    RenderMode   RenderMode
    Schema       schema.SchemaSnapshot
    Status       Status
    Focus        FocusArea
    Width        int
    Height       int
    Err          error
}
```

### 9.2 Update Loop

```text
key press
  -> editor/update mode
  -> if execute: run async query command
  -> query result message
  -> normalize result
  -> choose renderer from user-selected/default render mode
  -> update context/status
```

### 9.3 Layout

布局不应依赖固定 terminal size。需要处理：

| 终端尺寸 | 行为 |
| --- | --- |
| 窄屏 | 隐藏 context panel，结果全宽 |
| 中等 | 结果 + context 双列 |
| 高度不足 | editor 单行，footer 简化 |
| watch mode | result 优先，editor 压缩 |

## 10. Intelligence Layer

### 10.1 Schema Cache

缓存 db/rp/measurement/field/tag。

策略：

| 项 | 说明 |
| --- | --- |
| TTL | 默认 60s，可配置 |
| 手动刷新 | `:refresh schema` |
| 背景刷新 | Phase 2 |
| cache key | adapter + host + db + rp |

### 10.2 Cardinality Profiler

能力：

1. top measurement by series。
2. top tag keys by cardinality。
3. tag combination explosion hint。
4. measurement drift detection。

MVP 不做，但接口预留。

### 10.3 Query Explain

Explain 初期是 advisory，不要求数据库内核支持真实 explain。

可分析：

| 项 | 来源 |
| --- | --- |
| 时间范围 | query parser-lite |
| 是否缺少 time filter | query text + heuristic |
| group by time bucket | query text |
| measurement count | schema cache |
| series cardinality | adapter metadata |
| result point count | result metadata |

### 10.4 Query Profiler

来自 Claude TSM 评审的关键能力：

| 能力 | 说明 |
| --- | --- |
| narrow range detection | `WHERE time = x` 或非常小的 time range |
| scan range summary | StartTime、EndTime、SeekTime |
| block overlap analysis | file/block 是否与 query range 相交 |
| decode counter | `float_blocks_decoded` 等指标 |
| merge window explanation | 覆盖 block 导致 dedup/merge 窗口扩张 |
| recommendation | 在 storage cursor locations 阶段过滤不相交 block |

这些能力需要数据库内核指标、debug 输出或离线 storage analyzer 支持，放在后期实现。

## 11. Storage Analyzer 设计

Storage Analyzer 分三层：

| 层级 | 能力 | 阶段 |
| --- | --- | --- |
| L1 metadata | 文件数量、时间范围、measurement/key summary | Phase 4 |
| L2 index/block stats | block count、min/max time、overlap、tombstone | Phase 4 |
| L3 queryable inspector | 模拟 cursor、解释读取路径、decode count | Phase 5 |

TSM 窄范围查询诊断所需字段：

| 字段 | 说明 |
| --- | --- |
| file min/max time | 文件级过滤 |
| index entry min/max time | block 级过滤 |
| key/series | 查询目标 |
| block type | float/int/string/bool |
| overlap with query range | 是否应进入 cursor |
| decoded blocks | 实际解码数量 |

InfluxDB WAL analyzer 覆盖 TSM1 WAL segment 的本地 entry 解码：entry type、snappy payload size、key samples、write point time range、delete/delete-range target，以及基于 query range/key 的 replay candidate filtering。WAL analyzer 不直接调用 engine cache 或 WAL replay runtime。

openGemini TSSP analyzer 先覆盖 attached 文件 trailer/meta-index/chunk metadata，以及 detached `segment.idx` meta-index sidecar。detached sidecar 只做本地结构解析、CRC 校验和 query-range candidate filtering；完整 detached data/chunk reader 执行路径仍属于后续 Phase 5。

openGemini mergeset analyzer 先覆盖 part directory metadata、metaindex row summary、index block-header summary、item payload summary、file-set item search/TableSearch seek/heap/merge-dedup simulation：解析 `items_blocks_suffix` part 名、`metadata.json`、`metaindex.bin` zstd row、`index.bin` zstd block header、以及 `items.bin`/`lens.bin` plain/zstd item payload，校验 header range、decoded item count、first/last item 和排序。`--storage-format mergeset --key <item>` 可模拟 sorted block candidate lookup、exact item match、跨已分析 part 的 file-set 汇总、TableSearch seek/heap candidate accounting，以及重复 item candidate 的 merge/dedup window。完整 storage-engine-backed table cursor execution 仍属于后续 Phase 5。

## 12. Plugin System

插件系统不进入 MVP，但内部接口按插件化方向设计。

```go
type Plugin interface {
    Name() string
    Version() string
    Register(registry Registry) error
}
```

插件类型：

| 类型 | 示例 |
| --- | --- |
| adapter plugin | openGemini、InfluxDB 2.x |
| renderer plugin | heatmap、histogram |
| analyzer plugin | TSM profiler、cardinality analyzer |
| ingest plugin | dataset generator |

初期不要动态加载外部二进制，先使用内部 registry。

## 13. 错误处理

错误需要区分：

| 类型 | UI 表现 |
| --- | --- |
| config error | 启动失败或明确提示缺少 profile |
| connection error | statusline 显示 disconnected |
| auth error | 提示认证失败，不打印敏感信息 |
| query syntax error | footer/result 显示数据库返回错误 |
| timeout/cancel | 明确显示 cancelled/timeout |
| unsupported | 显示 adapter 不支持 |

错误对象建议带 code：

```go
type AppError struct {
    Code    string
    Message string
    Cause   error
}
```

## 14. 配置设计

默认配置路径：

```text
~/.config/influx-cli/config.yaml
```

示例：

```yaml
profiles:
  local:
    adapter: influxdb
    url: http://127.0.0.1:8086
    username: ""
    password: ""
    database: metrics
    retention_policy: autogen
    precision: ns
defaults:
  profile: local
  render: table
  timeout: 10s
```

环境变量覆盖：

| 变量 | 用途 |
| --- | --- |
| `INFLUX_CLI_URL` | server URL |
| `INFLUX_CLI_USERNAME` | username |
| `INFLUX_CLI_PASSWORD` | password |
| `INFLUX_CLI_TOKEN` | token |
| `INFLUX_CLI_PROFILE` | profile |

## 15. 并发与取消

要求：

1. 所有 query 接受 `context.Context`。
2. Ctrl+C 能取消当前 query。
3. watch mode 下一轮执行前必须等待上一轮结束或明确取消。
4. TUI 异步执行 query，不阻塞 UI event loop。
5. 长任务后续进入 run/task model，避免 UI 假死。

## 16. 测试策略

### 16.1 单元测试

| 模块 | 测试重点 |
| --- | --- |
| render/sparkline | 空值、负数、固定宽度、极值 |
| result normalize | InfluxDB JSON 到 Result |
| query dialect | meta command、dialect detection |
| session | db/rp 切换 |
| config | profile/env merge |

### 16.2 集成测试

| 场景 | 说明 |
| --- | --- |
| fake adapter | 稳定测试 CLI/REPL/TUI |
| InfluxDB container | 基础 query |
| openGemini compatible endpoint | 兼容 query |
| watch loop | cancel 和 refresh |

### 16.3 手动验证

1. `influx-cli query "SHOW DATABASES"`。
2. `influx-cli repl` 后 `:use metrics` 或 `:use metrics.autogen`。
3. time series query 默认输出 table；使用 `--format auto`、`--format sparkline` 或 REPL `:format sparkline` 时输出 sparkline。
4. 错误 query 返回明确错误。
5. TUI 尺寸变化不崩溃。

## 17. 演进约束

1. 不让 UI 直接调用数据库 client，必须经过 app/query/adapter。
2. 不让 adapter 返回 UI 专用结构，必须返回统一 `Result`。
3. 不在 MVP 引入复杂 parser，先用 dialect detect 和轻量 heuristic。
4. 不把 profiler 逻辑混入 renderer。
5. 不让 watch mode 复制 query 执行逻辑，应复用 orchestrator。
6. 不让 openGemini adapter fork 大量 InfluxDB adapter 代码，先抽公共 HTTP query client。
