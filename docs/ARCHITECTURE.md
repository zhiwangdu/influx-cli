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

Storage Analyzer 只解析调用方传入的本地文件或目录。它可以在本进程内复用或移植 InfluxDB/openGemini 的文件格式、索引结构和 codec 逻辑，但不得连接数据库、调用 HTTP query API、依赖 engine/runtime service，或把 analyzer 结果建立在在线查询返回值之上。

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

InfluxDB TSM analyzer 覆盖本地 TSM file metadata、index entry、tombstone range/impact、基于 query range/key 的 KeyCursor/FileStore cursor window/merge-window simulation、optimized/baseline value-output comparison、本地 KeyCursor block read/merge-window execution step sample，以及本地 optimized cursor final output sample；FileStore 汇总的 execution/final output sample 会记录被本地 cursor merge/dedup 后保留的 TSM 文件来源。它只读取调用方传入的本地 `.tsm`/`.tombstone` 文件，不调用 tsdb engine、shard runtime、HTTP query API 或数据库服务。

InfluxDB WAL analyzer 覆盖 TSM1 WAL segment 的本地 entry 解码：entry type、snappy payload size、key samples、write point time range/value sample、delete/delete-range target，以及基于 query range/key 的 replay candidate filtering 和本地 write/delete replay candidate output sample。WAL analyzer 不直接调用 engine cache 或 WAL replay runtime。

InfluxDB series file analyzer 覆盖本地 `_series` 目录、partition directory 和 `SSEG` segment 文件：解析 segment header、insert/tombstone entry、series key、live/tombstone series-id、measurement/tag summary 和 series-id filter sample。`series-file` 的 `--series-id` 只做本地 ID 检索，不需要 query range，也不调用 tsdb runtime、series index mmap runtime、HTTP query API 或数据库服务。

InfluxDB fields index analyzer 覆盖本地 shard `fields.idx` 和 sibling `fields.idxl` change log：解析 fields index magic、protobuf measurement/field/type snapshot、8-byte length-prefixed field change sets、add-field/delete-measurement changes，并把 change log 应用到本地摘要。`fields-index` 不调用 `tsdb.NewMeasurementFieldSet`、不启动 writer goroutine、不连接 shard engine runtime。

openGemini TSSP analyzer 先覆盖 attached 文件 trailer/meta-index/chunk metadata，以及 detached `segment.idx` meta-index sidecar。attached 文件在 chunk metadata expanded 且 trailer 声明 data section 时，会按 query 命中且被 `--column` 投影保留的 column segment 做 data block header probe、可证明 header 类型的 row-count materialization、`Block*One` value sample extraction、含 null bitmap 的普通 block sample extraction、`BlockFloatFull` raw/old-gorilla/snappy/gorilla/same/RLE/MLF float sample extraction、`BlockIntegerFull` uncompressed/const-delta/simple8b/zstd integer sample extraction、`BlockBooleanFull` bitpack boolean sample extraction、`BlockStringFull` uncompressed/snappy/zstd/LZ4 string sample extraction、已解码多列的跨列 record sample materialization 和 record execution row samples，以及 decoded time 可用时的 row-level query-range filtering input/match/reject accounting/range execution row samples、`--field` required-AND、`--field-any` OR 和 `--field-none` NOT 简单、existence、有限集合、range、regex、like/not-like wildcard、contains/not-contains substring 与 starts/ends prefix/suffix、decoded time 和字符串有序字段谓词过滤后的本地 record row output/materialization 与 input/match/reject row accounting、predicate operator accounting、per-clause result evaluation accounting、predicate short-circuit skip accounting 和带已评估谓词 decision 的 filter execution row samples；file-set 汇总会保留 decoded output sample、range execution sample、record execution sample 与 filter execution sample 的本地文件来源，并对完全相同 key/time/type/value 的 decoded output 生成 final exact dedup sample，同时采样本地 LocationCursor metadata skip/read execution step。detached sidecar 做本地结构解析、CRC 校验、query-range candidate filtering、`ChunkMetaReadNum` 风格的 chunk metadata batch planning 和 local batch execution step sample，并在同目录存在 `segment.meta` 时按 openGemini detached chunk-meta record CRC 解码 chunk metadata，进一步用 chunk metadata 规划 `--column` 投影后的 detached segment-level data ReadAt 范围并采样 detached LocationCursor metadata skip/read execution step；若同目录存在 `segment.bin`，还会校验 data file header、column segment offset/size 是否落在文件内，并对 query 命中且被投影保留的 detached data block 做 ReadAt CRC/header probe、可证明 header 类型的 row-count materialization、`Block*One` value sample extraction、含 null bitmap 的普通 block sample extraction、`BlockFloatFull` raw/old-gorilla/snappy/gorilla/same/RLE/MLF float sample extraction、`BlockIntegerFull` uncompressed/const-delta/simple8b/zstd integer sample extraction、`BlockBooleanFull` bitpack boolean sample extraction、`BlockStringFull` uncompressed/snappy/zstd/LZ4 string sample extraction、已解码多列的跨列 record sample materialization 和 record execution row samples，以及 decoded time 可用时的 row-level query-range filtering input/match/reject accounting/range execution row samples、`--field` required-AND、`--field-any` OR 和 `--field-none` NOT 简单、existence、有限集合、range、regex、like/not-like wildcard、contains/not-contains substring 与 starts/ends prefix/suffix、decoded time 和字符串有序字段谓词过滤后的本地 record row output/materialization 与 input/match/reject row accounting、predicate operator accounting、per-clause result evaluation accounting、predicate short-circuit skip accounting 和带已评估谓词 decision 的 filter execution row samples。更完整的本地 filter 表达式语法/record execution 仍属于后续 Phase 5。

Decode path JSON summaries expose `cursor_execution_action_counts`, `range_execution_action_counts`, `record_execution_action_counts`, and `filter_execution_action_counts` as sampled local execution action-count maps, so count-oriented table/report output can summarize local cursor, query-range, record materialization, and predicate decision steps without depending on raw sampled row values. TSSP summaries also expose `range_execution_total_action_counts`, `record_execution_total_action_counts`, `filter_execution_total_action_counts`, and `filter_clause_total_action_counts`, derived from full local data-probe row counters rather than sample-limited execution rows, for count-oriented diagnostics that need total query-range match/reject, record output/range-reject/filter-reject, predicate match/reject, and required/any/none clause match/miss/skip action counts. `range_execution_omitted_action_counts`, `record_execution_omitted_action_counts`, and `filter_execution_omitted_action_counts` report full row actions not represented by sampled execution steps after sample limits are applied. Filter row total and omitted actions intentionally use generic `filter_row_match`/`filter_row_reject` names because per-clause reject reasons are only available in sampled row decisions.

TSSP `--field`、`--field-any` 和 `--field-none` 只在本地已解码 data block value 上执行有限谓词；重复 `--field` 作为 required-AND 谓词，重复 `--field-any` 作为至少命中一个的 OR 谓词，重复 `--field-none` 作为不能命中任意一个的 NOT 谓词，三者组合时先满足 required-AND，再满足 OR，最后排除命中 NOT 的行：`=`/`==`/`!=`/`<>` 按 decoded value 类型比较，`is`/`is-not` 是同一等值/不等值路径的输入别名，`exists` 只命中本地 decoded 非 null 行，`not-exists`/`!exists` 命中 decoded null 行或缺失的本地 decoded column，`in`/`not-in`/`!in` 对逗号分隔集合执行同样的 typed membership 比较，`between`/`not-between`/`!between` 对两个边界值执行本地闭区间比较，边界括号可选，`null` 边界会在分析前拒绝，倒置边界按输入顺序执行且不会命中 ordered decoded value，boolean block 不参与 range 命中，单引号或双引号包裹的 string literal 可在本地集合拆分前保留逗号或括号字符，`null` 作为 decoded null row 的哨兵用于 `=null`、`==null`、`!=null`、`<>null`、`is null`、`is-not null` 和集合匹配，quoted `"null"` 仍保留 null sentinel 语义，`>`/`>=`/`<`/`<=` 对 decoded integer/float block 做数值比较、对 decoded string block 做字典序比较，`=~`/`!~` 使用 Go regexp 对本地 decoded value 的字符串表示做匹配且在文件分析前校验 regex，`like`/`not-like`/`!like` 使用 SQL 风格 `%` 与 `_` wildcard，`contains`/`not-contains`/`!contains` 与 `starts-with`/`not-starts-with`/`!starts-with`/`ends-with`/`not-ends-with`/`!ends-with` 只对非 null decoded string block 做本地 wildcard、substring、prefix 和 suffix 判断，`time` 可在 chunk metadata 暴露本地 decoded time block 时作为整数列参与同一套谓词；在 `--column` 投影下，filter 所需列和 sample timestamp 所需的 `time` 列会随投影保留，并输出本地 decoded row 的 query-range input/match/reject 计数、range execution row samples、field predicate input/match/reject 计数、record candidate/output/range-reject/filter-reject row 计数、sampled record materialization execution rows 和有限 filter execution row samples；range sample 会包含本地 decoded row time、query range 和 match/reject 决策，record sample 会包含本地 decoded row time、字段值和 output 决策，filter sample 会包含参与谓词的本地 decoded 字段值、per-clause 命中数、短路计数、用于解释该行决策的已评估 clause/key/operator/value/match-or-miss 和最终 match/reject 决策；它不调用 openGemini filter executor、record runtime、engine、HTTP API 或数据库服务。

TSSP 多词 field predicate operator alias 会把 hyphen、space 和 underscore 分隔符归一到同一个本地 operator，例如 `not-like`、`not like` 和 `not_like` 等价；`not-exists`、`not exists`、`not_exists` 和 `!exists` 会归一到同一个本地 existence operator；`matches`/`match`/`regex`/`regexp` 及其 `not`/`!` 否定别名会归一到本地 decoded-row regex operator `=~`/`!~`；bang-negated prefix/suffix alias 也接受 underscore 形式，例如 `!starts_with` 和 `!ends_with`。归一化只影响本地 decoded-row predicate 解释，不调用外部 parser 或查询执行器。

TSSP record execution diagnostics 会区分本地 materialized record candidate row、output row、query-range reject row、field-filter reject row 与受 sample limit 限制的 execution row samples；record execution sample 的采样额度独立于 record output sample，execution sample 会记录 chunk-local row、file-local `local_input` ordinal、query range context、decoded value-column count 和 output/range-reject/filter-reject result，rejected row 标记为 `local_output=none`，record output sample 与 output execution sample 会记录 local output ordinal，count-only decode path summary 会同时输出 full local range/record/filter row action counts、filter clause action counts、sample-omitted action counts 和 sampled cursor/range/record/filter execution action counts，便于区分单列 value sample、跨列 record materialization、查询窗口、filter 决策、全量本地输入/输出计数与采样顺序。

openGemini detached primary key meta analyzer 覆盖本地 `primary.meta` sidecar：解析 `COLX` public header、主键 schema、time-cluster location、meta block 的 block-id 范围、`primary.idx` offset/length、列 offset 和 IEEE CRC；若同目录存在 `primary.idx`，只做本地文件大小和 range 边界校验，不解码主键 record，不调用 `DetachedPKMetaReader`、OBS/fileops runtime、shard engine、HTTP API 或数据库服务。

openGemini attached primary key index analyzer 覆盖本地 colstore 主键 `.idx` 文件：解析 `COLX` header、attached meta size、row count、主键 schema、time-cluster location 和列数据绝对 offset，并校验列 offset 是否落在当前文件 data section 内，汇总完整落界的 column data byte；不解码 record，不调用 `PrimaryKeyReader`、fileops runtime、shard engine、HTTP API 或数据库服务。`primary.meta` 仍由 `opengemini-pk-meta` 解析，detached `primary.idx` data sidecar 不会被 auto-detect 当作 attached index。

openGemini bloom filter secondary index analyzer 覆盖本地 colstore skip-index sidecar：attached `.bf` 按连续 line filter block 解析，detached `bloomfilter_*.idx` 按 vertical filter group/piece 摘要，使用 openGemini 当前 logstore filter 尺寸和 Castagnoli CRC 校验完整 block/piece，并报告 field/full-text 推断、有效字节、尾随字节和 CRC mismatch；不调用 `NewFilterReader`、`NewVerticalFilterReader`、fileops/OBS runtime、shard engine、HTTP API 或数据库服务。

openGemini text secondary index analyzer 当前显式跳过：目录扫描不收集 `.ph`/`.bh`/`.pos` sidecar；直接分析这些文件或显式指定 `opengemini-text-index` 时只返回 skipped notice 和本地路径派生的 component/field 信息，不读取或解析 text sidecar 内容。它不解压 posting list，不执行文本查询语义，不调用 `TextIndexFilterReader`、fileops/OBS runtime、shard engine、HTTP API 或数据库服务。

openGemini mergeset analyzer 覆盖 part directory metadata、metaindex row summary、index block-header summary、plain/zstd item payload summary/read/decode/range-skip/success-failure accounting、openGemini TSI/tag namespace item summary、openGemini field-index namespace item summary、openGemini CLV text-index item summary、payload-backed table scan summary、file-set item search/TableSearch seek/heap/merge-dedup simulation：解析 `items_blocks_suffix` part 名、`metadata.json`、`metaindex.bin` zstd row、`index.bin` zstd block header、以及 `items.bin`/`lens.bin` plain/zstd item payload，校验 header range、metaindex row index.bin range out-of-bounds/overlap/gap/order 并输出有限 anomaly sample，校验 items/lens block header range out-of-bounds/overlap/gap/order 并输出有限 anomaly sample，校验 metaindex row first-item 与首个 block header first-item consistency、common-prefix/first-item consistency、index header metadata range 越界计数、decoded item count、first/last item、metadata range 越界 item/block 计数、plain/zstd payload decode 成功/失败计数和排序。若 decoded item 属于 openGemini TSI/tag namespace，会额外识别 `seriesKey->TSID`、`TSID->seriesKey`、`tag->TSID list`、`tagKey->tagValue` 四类本地 item；若 decoded item 属于 openGemini field index namespace，会额外识别 `tsid->field value`、`field value->pid`、`measurement->field key` 三类本地 item；若 decoded item 属于 openGemini CLV text-index namespace，会额外识别 document position row、term row、dictionary row、dictionary-version row，并输出计数与样例；不调用 `fieldIndex`、`clv.TokenIndex`、analyzer cache、`mergeset.TableSearch` runtime 或查询执行路径。无 `--key` 时可基于 decoded item payload 生成 part-level table scan block/window/output sample，并用 TableSearch-style heap cursor 汇总 file-set scan heap candidate output、记录每条 heap candidate 的 part 来源、记录本地 heap insert/pop 和 cursor advance/exhaustion execution accounting、采样 table-scan heap pop/advance/exhaust execution step、为 duplicate item 输出 merge window 和 dedup accounting，同时输出本地 dedup 后的 final cursor output sample；`--storage-format mergeset --key <item>` 可按 ascending/descending cursor order 模拟 sorted block candidate lookup、part-level seek/advance cursor window、exact item match、ascending block-gap cursor advance、exact-miss nearest item candidate window，以及 part-level exact match final cursor output sample，跨已分析 part 的 file-set 汇总、TableSearch seek/heap candidate accounting、本地 candidate heap pop execution step、本地去重后的精确命中 final cursor output sample，以及重复 item candidate 的 merge/dedup window。后续 Phase 5 可继续补充更多真实 mergeset part 编码样本，但 multi-part table cursor execution 已以本地 decoded payload 和 heap 模拟形式落地。

mergeset item payload byte accounting 只来自当前 part 的本地 `items.bin` 和 `lens.bin`：读入字节按 plain/zstd marshal type 累计，成功 decode 块的读入字节单独累计，未压缩 payload 字节只在完整 decode 成功后累计，并输出未压缩 payload 减成功 decode 读入字节的差值；decode 失败不推断部分未压缩字节。

mergeset cursor window、output 和 heap execution samples 会在 sampled item 含不可打印二进制字节时附带 hex 字段，保留原始 string 字段兼容性，同时让真实 openGemini index item 可读、可比对。

openGemini meta topology analyzer 只读取本地 meta snapshot/export 文件，格式名为 `opengemini-meta`。它用轻量 protobuf/JSON reader 提取 database、retention policy、meta/data/sql node、PT view 和 replica group summary，不连接 meta service、HTTP API、raft runtime 或 storage engine runtime。

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
