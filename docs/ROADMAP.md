# influx-cli Roadmap

## 1. Roadmap 原则

1. 先完成可用的 query loop，再做复杂 TUI 和分析系统。
2. 每个阶段都要有可演示交付物。
3. MVP 避免“大而全”，只验证核心价值：终端查询 + 趋势可视化 + 上下文状态。
4. 高级能力围绕真实 TSDB 痛点推进：schema、cardinality、query explain、storage/block profiler。
5. InfluxDB 1.x 和 openGemini 兼容路径优先，其他后端后续插件化。

## 2. 当前仓库状态

截至 2026-07-04，当前仓库已经包含 Phase 0 CLI MVP 基础实现：

| 文件 | 状态 |
| --- | --- |
| `go.mod`、`go.sum` | Go module 和依赖 |
| `cmd/influx-cli/main.go` | Cobra CLI 入口，包含 `query`、`repl`、`config` |
| `internal/config` | profile、环境变量和命令行覆盖合并 |
| `internal/adapter/influxdb` | InfluxDB 1.x/openGemini 兼容 HTTP query path |
| `internal/result` | table、series、schema result model |
| `internal/render` | table 和 sparkline renderer |
| `internal/app`、`internal/repl` | session、statusline、meta command 和 REPL loop |
| `internal/history` | REPL query history 本地持久化和检索 |
| `internal/analysis/storage` | Phase 5 初始 storage file analyzer：TSM/WAL/TSSP/TSI/series-file/fields-index/mergeset/opengemini-meta/opengemini-pk-meta/opengemini-pk-index/opengemini-bloom-filter 文件元数据（opengemini-text-index 显式请求仅跳过）、key/series 样例、block/meta-index/entry 统计、TSI index/log series-id set cardinality、InfluxDB `_series` live/tombstone series summary、`fields.idx`/`fields.idxl` field type summary、TSM tombstone range/impact、TSM/WAL decode path estimate、TSM KeyCursor execution step samples、TSM final optimized cursor output samples、WAL local write/delete replay candidate samples、TSSP chunk metadata、attached/detached TSSP local cursor execution samples、attached/detached TSSP column projection/required-AND、OR 与 NOT simple/quoted finite-set/regex/decoded-time/string-ordered field predicate filtering/data block probe/filter row、decoded-row query-range input/match/reject accounting、predicate operator accounting、per-clause result evaluation accounting 和 predicate short-circuit skip accounting、row-count materialization、one-row value samples、含 null bitmap 的普通 block samples、raw/old-gorilla/snappy/gorilla/same/RLE/MLF float full-block samples、uncompressed/const-delta/simple8b/zstd integer full-block samples、bitpack boolean full-block samples、uncompressed/snappy/zstd/LZ4 string full-block samples、跨列 record samples、detached TSSP meta-index sidecar/chunk metadata batch planning/local execution samples/`segment.meta` expansion/data ReadAt planning/`segment.bin` range validation、openGemini detached `primary.meta` 和 attached `.idx` 主键 schema/meta-block/CRC/data/valid-data range summary、openGemini bloom filter secondary index block/group/CRC summary、opengemini-text-index skipped notice、mergeset metaindex/index block-header/item payload/header-and-payload metadata-range anomaly accounting、openGemini TSI/tag namespace item summary、openGemini field-index namespace item summary、openGemini CLV text-index item summary、part/file-set table scan summary、part/file-set item search exact-miss seek window 和 block-gap cursor advance、TableSearch seek/heap cursor simulation、heap output part provenance、heap insert/pop/cursor-advance execution accounting、scan/search heap execution step samples、part-level exact search final output samples、final deduped scan/search output samples 和 duplicate item merge-window/dedup summary、openGemini meta topology snapshot summary、query range overlap |
| `docs/PRODUCT_DESIGN.md` | 产品设计书 |
| `docs/ARCHITECTURE.md` | 架构说明 |
| `docs/ROADMAP.md` | 本 roadmap |

Phase 0 仍应通过本地 InfluxDB/openGemini 兼容端点做人工验收。

Phase 5 已开始一个窄切面的本地文件分析命令：

```bash
influx-cli storage analyze <file-or-dir>...
```

当前覆盖 InfluxDB TSM attached file metadata、WAL write/delete/delete-range entry metadata、WAL local write/delete replay candidate samples、tombstone range/impact summary、基于 query range/key 的 TSM/WAL decode path estimate、per-file 和 FileStore-level ascending/descending cursor window/merge window simulation、优化前后 block/entry/byte/value decode path estimate、decoded timestamp output/dedup estimate、TSM value output comparison、local TSM KeyCursor-style execution stats/execution step samples/comparison output samples/final optimized cursor output samples、TSM FileStore.Cost-style file/block/byte estimate、TSI index file measurement/tag summary、TSI log entry summary、live/tombstone series-id set cardinality、InfluxDB `_series` series-file partition/segment entry/tombstone/key summary、`fields.idx`/`fields.idxl` measurement field-type summary 和 measurement/tag predicate inspection，以及 openGemini attached TSSP trailer/meta-index metadata、none/snappy/LZ4/self-compressed chunk metadata、per-file 和 file-set series-id filtered segment-level decode path estimate、TSSP ContainsValue/MetaIndex-style candidate cost estimate、ascending/descending TSSP LocationCursor-style segment window/local execution samples、TSSP ReadAt call estimate、`--column` projected optimized column-segment read ranges、file-set decoded output provenance/final exact dedup samples、`--field` required-AND、`--field-any` OR 与 `--field-none` NOT 简单、quoted finite-set、regex、decoded time 和字符串有序字段谓词过滤及 decoded-row query-range/input/match/reject/predicate short-circuit accounting、attached query-hit projected data block header probe、row-count materialization、one-row value samples、含 null bitmap 的普通 block samples、raw/old-gorilla/snappy/gorilla/same/RLE/MLF float full-block samples、uncompressed/const-delta/simple8b/zstd integer full-block samples、bitpack boolean full-block samples、uncompressed/snappy/zstd/LZ4 string full-block samples、跨列 record samples、detached `segment.idx` meta-index CRC validation、query-range candidate filtering、detached chunk metadata batch planning/local execution samples、可选 sibling `segment.meta` expansion、detached segment-level data ReadAt planning、`--column` projected detached data probe、`--field` required-AND、`--field-any` OR 与 `--field-none` NOT detached decoded-row simple/quoted finite-set/regex/decoded-time/string-ordered predicate filtering及 decoded-row query-range/input/match/reject/predicate short-circuit accounting、可选 sibling `segment.bin` header/range validation、query-hit projected data block CRC/header probe、row-count materialization、one-row value samples、含 null bitmap 的普通 block samples、raw/old-gorilla/snappy/gorilla/same/RLE/MLF float full-block samples、uncompressed/const-delta/simple8b/zstd integer full-block samples、bitpack boolean full-block samples、uncompressed/snappy/zstd/LZ4 string full-block samples、跨列 record samples、openGemini detached `primary.meta` 和 attached `.idx` 主键 schema/meta-block/CRC/data/valid-data range summary、openGemini bloom filter secondary index block/group/CRC summary、opengemini-text-index skipped notice、mergeset part metadata/metaindex/index block-header/item payload summary、openGemini TSI/tag namespace item summary、openGemini field-index namespace item summary、openGemini CLV text-index item summary、payload-backed part/file-set table scan summary、part/file-set item search exact-match/exact-miss window simulation 和 ascending block-gap cursor advance accounting、TableSearch seek/heap cursor simulation、heap output part provenance、heap insert/pop/cursor-advance execution accounting、scan/search heap execution step samples、part-level exact search final output samples、final deduped scan/search output samples、duplicate item merge-window/dedup summary、openGemini meta topology snapshot protobuf/JSON summary。完整本地 storage cursor simulation/execution 和更完整的 filter 表达式执行仍属于后续 Phase 5 工作。

## 3. Phase 0: CLI MVP Foundation

建议周期：1 到 2 周。

目标：能连接、能查询、能输出表格和 sparkline。

### 3.1 交付范围

| 模块 | 交付内容 |
| --- | --- |
| Go project | `go.mod`、`cmd/influx-cli/main.go` |
| CLI | `query`、`repl`、`config` 基础命令 |
| config | profile、URL、认证、db/rp |
| adapter | InfluxDB 1.x HTTP query adapter |
| result | InfluxDB JSON normalize 成 table/series |
| render | table renderer、sparkline renderer |
| session | db/rp/mode/latency/error |
| statusline | REPL 中展示上下文 |

### 3.2 必备命令

```bash
influx-cli query "SHOW DATABASES"
influx-cli query --db metrics "SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(1m)"
influx-cli repl
```

REPL meta command：

```text
:use <db>
:use <db>.<rp>
:db <db>
:rp <rp>
:dbs
:rps [db]     # show named/current DB RP details; without current DB, show all DBs
:measurements
:msts
:format [auto|table|sparkline|json]
:fmt [auto|table|sparkline|json]
:help
:q
```

### 3.3 验收标准

| 验收项 | 标准 |
| --- | --- |
| 连接 | 能连接本地 InfluxDB 兼容接口 |
| 查询 | `SHOW DATABASES` 和基础 `SELECT` 可执行 |
| 表格 | SHOW 查询以 table 输出 |
| 默认渲染 | 未指定 format 时以 table 输出 |
| sparkline | 通过 `--format auto`、`--format sparkline` 或 REPL `:format sparkline` 输出 time + numeric 趋势 |
| 状态 | 显示 db/rp/latency/last error |
| 错误 | 认证失败、连接失败、语法错误可读 |
| 测试 | render、result normalize、config 有单测 |

### 3.4 不做

| 不做 | 原因 |
| --- | --- |
| Bubble Tea 完整 TUI | Phase 0 先验证核心查询链路 |
| autocomplete | 依赖 schema cache，放 Phase 1 |
| Flux 深度支持 | 先透传或暂不承诺 |
| storage analyzer | 非 MVP |

## 4. Phase 1: REPL 体验升级

建议周期：2 到 4 周。

目标：接近 pgcli 的交互效率，同时保持 TSDB context。

### 4.1 交付范围

| 模块 | 交付内容 |
| --- | --- |
| history | query history 持久化 |
| multiline | 多行 query 输入，支持 `\` 续行、pending query 分号结束和 `:cancel`/`:clear` |
| autocomplete | Tab 补全 db/rp/measurement/field/tag |
| schema cache | TTL + 手动刷新 |
| openGemini | 兼容查询 adapter |
| render | sparkline 细化，支持多 series 降级 |

### 4.2 新命令

```text
:history [limit] [filter]
:hist [limit] [filter]
:measurements
:msts
:fields <measurement>
:tags <measurement>
:schema <measurement>
:refresh schema
:cancel
:clear
```

### 4.3 验收标准

| 验收项 | 标准 |
| --- | --- |
| history | Up/Down 可导航 query 历史，`:history`/`:hist` 可检索历史 |
| multiline | REPL 可组装多行 InfluxQL/Flux query 并作为一次查询执行 |
| autocomplete | Tab 能补全 db、rp、measurement、field/tag |
| schema | `:schema cpu` 可展示 field/tag |
| openGemini | 通过兼容接口执行基础 query |
| render | 多 series 不产生不可读输出 |

## 5. Phase 2: TUI 和 Grafana-lite

建议周期：1 到 2 个月。

目标：形成可持续使用的终端控制台。

### 5.1 交付范围

| 模块 | 交付内容 |
| --- | --- |
| Bubble Tea TUI | statusline、query editor、result view、context panel、footer |
| layout | 窄屏/宽屏自适应 |
| chart | ASCII line chart |
| watch | live refresh |
| renderer switch | table/sparkline/chart 切换 |
| schema panel | 右侧展示当前 measurement 信息 |

### 5.2 TUI 快捷键

| key | action |
| --- | --- |
| Ctrl+Enter | run query |
| Ctrl+R | history |
| Tab | autocomplete |
| 1 | table |
| 2 | sparkline |
| 3 | chart |
| S | schema panel |
| W | watch |
| F | fullscreen |
| Q | quit |

### 5.3 验收标准

| 验收项 | 标准 |
| --- | --- |
| TUI 启动 | `influx-cli tui` 可进入 |
| 查询 | TUI 内可执行 query |
| 结果 | table/sparkline/chart 可切换 |
| context | 显示 db/rp/schema summary |
| watch | 能定时刷新且可取消 |
| resize | 改变终端尺寸不崩溃、不严重重叠 |

## 6. Phase 3: Schema Intelligence 和 Query Explain

建议周期：1 到 2 个月。

目标：从“能查能看”升级为“能解释 TSDB 问题”。

### 6.1 交付范围

| 模块 | 交付内容 |
| --- | --- |
| cardinality | top measurement/tag cardinality |
| schema explorer | db -> measurement -> field/tag tree |
| query hints | 缺少 time filter、bucket 过细、limit 缺失 |
| explain | query 时间范围、measurement、series 估算 |
| context panel | hints 和风险提示 |

### 6.2 典型提示

```text
measurement cpu has high cardinality
tag pid may cause series explosion
query has no time predicate
GROUP BY time(1s) over 30d may return too many points
```

### 6.3 验收标准

| 验收项 | 标准 |
| --- | --- |
| cardinality | 可展示 top measurement/tag |
| explain | 可解释 query 时间范围和风险 |
| hints | 至少覆盖无 time filter、高基数、过大结果集 |
| UI | hints 不阻塞 query，不把 warning 当 fatal error |

## 7. Phase 4: Query Profiler、Dataset Generator、Query Lab

建议周期：2 到 3 个月。

目标：支持真实排障和复现实验。

### 7.1 Query Profiler

结合本地 Claude TSM 评审，优先支持以下场景：

| 场景 | 能力 |
| --- | --- |
| 窄时间范围慢查询 | 识别 `WHERE time = x` 或很小 range |
| block decode 放大 | 展示 decoded blocks 指标 |
| 覆盖 block 放大 | 解释乱序覆盖 block 导致 merge/dedup 窗口扩大 |
| scan range | 展示 StartTime、EndTime、SeekTime |
| 优化建议 | 提示 storage cursor 应过滤不相交 block |

### 7.2 Dataset Generator

命令示例：

```bash
influx-cli ingest demo-cpu --rate 10k/s --duration 5m
influx-cli ingest high-cardinality --hosts 1000 --pids 10000
influx-cli ingest out-of-order --ratio 0.1
```

支持：

| 数据模式 | 用途 |
| --- | --- |
| demo-cpu | 基础展示 |
| high-cardinality | series explosion 复现 |
| out-of-order | 乱序写入复现 |
| covering-block | TSM 覆盖 block 慢查询复现 |

### 7.3 Query Lab

能力：

| 能力 | 说明 |
| --- | --- |
| query history replay | 重放历史查询 |
| query diff | 对比两个查询结果/延迟 |
| query template | top slow series、cardinality detect |
| snapshot | 保存结果和图表文本 |

### 7.4 验收标准

| 验收项 | 标准 |
| --- | --- |
| profiler | 至少能输出 query latency、range、result count |
| TSM 场景 | 可记录/展示 block decode 相关指标或离线解释 |
| generator | 可生成基础 demo 和高基数数据 |
| lab | 可 replay 历史 query |

## 8. Phase 5: Storage File Analyzer

建议周期：长期。

目标：把 query 视角与 storage 视角打通。

边界：Storage Analyzer 只解析调用方传入的本地文件或目录；它可以复用/移植 InfluxDB 与 openGemini 的文件格式和 codec 逻辑，但不能连接数据库、调用 HTTP query API 或依赖 engine/runtime service。

### 8.1 L1: File Metadata

| 能力 | 说明 |
| --- | --- |
| file list | 文件、大小、时间范围 |
| series/key summary | key 数量和样例 |
| block summary | block 数量、类型 |

### 8.2 L2: Index 和 Block Stats

| 能力 | 说明 |
| --- | --- |
| TSM block stats | min/max time、type、count |
| overlap analysis | block/file 与 query range 关系 |
| tombstone summary | 删除范围和影响 |
| TSI summary | series index 概览 |
| series-file summary | 本地 `_series` partition/segment、live/tombstone series-id、measurement/tag summary |
| fields-index summary | 本地 `fields.idx`/`fields.idxl` measurement field-type summary |

### 8.3 L3: Queryable Inspector

| 能力 | 说明 |
| --- | --- |
| simulate cursor | 模拟 seek、locations、dedup |
| explain decode path | 哪些 block 被读、为什么 |
| compare optimization | 优化前后 block decode 数量 |
| report | 输出可复制到 issue/PR 的诊断报告 |

### 8.4 openGemini Storage

后续支持：

| 类型 | 说明 |
| --- | --- |
| series-file | 已有本地 InfluxDB `_series` 目录、partition directory 和 `SSEG` segment 解析，输出 insert/tombstone entry、live/tombstone series-id、series key sample、measurement/tag summary；`--series-id` 只做本地 ID 检索，不连接数据库或 tsdb runtime |
| fields-index | 已有本地 InfluxDB `fields.idx` snapshot 和 sibling `fields.idxl` change log 解析，输出 measurement、field、field type 和 add/delete change summary；不调用 shard engine 或 `MeasurementFieldSet` runtime |
| tssp | openGemini 文件元数据、chunk metadata expansion、local LocationCursor execution samples、`--column` projected ReadAt planning、file-set decoded output provenance/final exact dedup samples、decoded-time row-level query-range filtering/input-match-reject accounting、`--field` required-AND、`--field-any` OR 与 `--field-none` NOT simple/quoted finite-set/regex/decoded-time/string-ordered decoded-row predicate filtering/input-match-reject/operator accounting/per-clause result evaluation accounting/predicate short-circuit skip accounting、query-hit projected data block header probe、row-count materialization、one-row value samples、含 null bitmap 的普通 block samples、raw/old-gorilla/snappy/gorilla/same/RLE/MLF float full-block samples、uncompressed/const-delta/simple8b/zstd integer full-block samples、bitpack boolean full-block samples、uncompressed/snappy/zstd/LZ4 string full-block samples 和跨列 record samples |
| tssp-metaindex | openGemini detached `segment.idx` meta-index sidecar、chunk metadata batch planning/local execution samples、sibling `segment.meta` expansion、`--column` projected segment-level data ReadAt planning、decoded-time row-level query-range filtering/input-match-reject accounting、`--field` required-AND、`--field-any` OR 与 `--field-none` NOT simple/quoted finite-set/regex/decoded-time/string-ordered decoded-row predicate filtering/input-match-reject/operator accounting/per-clause result evaluation accounting/predicate short-circuit skip accounting、sibling `segment.bin` range validation、query-hit projected data block CRC/header probe、row-count materialization、one-row value samples、含 null bitmap 的普通 block samples、raw/old-gorilla/snappy/gorilla/same/RLE/MLF float full-block samples、uncompressed/const-delta/simple8b/zstd integer full-block samples、bitpack boolean full-block samples、uncompressed/snappy/zstd/LZ4 string full-block samples 和跨列 record samples |
| opengemini-pk-meta | 已有本地 openGemini detached `primary.meta` sidecar 解析，输出 `COLX` public header、主键 schema、time-cluster location、meta block block-id range、`primary.idx` offset/length、列 offset、IEEE CRC 和 sibling `primary.idx` range validation；不调用 `DetachedPKMetaReader`、OBS/fileops runtime、engine 或数据库服务 |
| opengemini-pk-index | 已有本地 openGemini attached colstore 主键 `.idx` 解析，输出 `COLX` header、attached meta size、row count、主键 schema、time-cluster location、inline column data offset/range/valid-byte summary；不解码 record，不调用 `PrimaryKeyReader`、fileops runtime、engine 或数据库服务 |
| opengemini-bloom-filter | 已有本地 openGemini bloom filter secondary index sidecar 解析，attached `.bf` 输出 line filter block count/CRC，detached `bloomfilter_*.idx` 输出 vertical group/piece count/CRC，报告 field/full-text 推断、有效字节和尾随字节；不调用 `NewFilterReader`、OBS/fileops runtime、engine 或数据库服务 |
| opengemini-text-index | 当前按需求跳过 text index 分析：目录扫描不收集 `.ph`/`.bh`/`.pos` sidecar，直接分析或显式指定该格式时只返回 skipped notice，不读取或解析 sidecar 内容；不解压 posting list，不调用 `TextIndexFilterReader`、fileops/OBS runtime、engine 或数据库服务 |
| mergeset | 已有 part metadata、metaindex row、index block-header、metaindex row index.bin range out-of-bounds/overlap/gap/order validation、items/lens block header range out-of-bounds/overlap/gap/order validation、metaindex row first-item 与首个 block header first-item consistency validation、common-prefix/first-item consistency validation、index header metadata range anomaly accounting、item payload summary/read/decode/range-skip accounting、metadata range before/after block-item anomaly accounting、openGemini TSI/tag namespace item summary、openGemini field-index namespace item summary、openGemini CLV text-index item summary、payload-backed part/file-set table scan summary、ascending/descending part/file-set item search exact-match/exact-miss seek window simulation、ascending block-gap cursor advance accounting、TableSearch seek/heap cursor simulation、heap output part provenance、heap insert/pop/cursor-advance execution accounting、scan/search heap execution step samples、multi-part table scan heap cursor execution、part-level exact search final output samples、final deduped scan/search output samples 和 duplicate item merge-window/dedup summary；后续补充更多真实 mergeset part 编码和异常边界样本 |
| meta/store/sql topology | 已有本地 openGemini meta topology snapshot protobuf/JSON 解析，输出 database、retention policy、meta/data/sql node、PT view 和 replica group summary；不连接 meta service、HTTP API 或 runtime |

## 9. Phase 6: Plugin Ecosystem 和 Dashboard

建议周期：长期。

目标：从单体工具变成可扩展的 TSDB 终端平台。

交付：

| 能力 | 说明 |
| --- | --- |
| internal plugin registry | adapter/renderer/analyzer 注册 |
| external plugin spec | 后续再考虑外部插件 |
| multi-panel dashboard | 多查询面板 |
| saved workspace | 保存 db、query、layout |
| live monitoring | 多指标 watch |
| report export | markdown/json 输出 |

## 10. 风险和应对

| 风险 | 影响 | 应对 |
| --- | --- | --- |
| 一开始做太大 | 项目难以交付 | Phase 0 只做 query/table/sparkline/statusline |
| TUI 复杂度过高 | 查询链路不稳定 | CLI/REPL 先稳定，TUI 复用核心 |
| 方言 parser 过重 | 实现周期拉长 | MVP 先轻量识别和透传 |
| InfluxDB/openGemini 差异 | adapter 分叉 | 先抽公共 HTTP query client |
| profiler 依赖内核指标 | 无法通用 | profiler 设计为可选能力 |
| storage parser 难度高 | 长期投入大 | Phase 5 才进入，先做 metadata |

## 11. 优先级清单

### P0

| 项 | 说明 |
| --- | --- |
| Go 工程骨架 | 可编译 |
| InfluxDB query adapter | 基础查询 |
| Result normalize | table/series |
| table renderer | SHOW 查询 |
| sparkline renderer | 趋势查询 |
| REPL | 连续查询 |
| statusline | 上下文状态 |

### P1

| 项 | 说明 |
| --- | --- |
| history | 查询复用 |
| schema cache | 补全和 context |
| autocomplete | 降低输入成本 |
| openGemini adapter | 兼容查询 |
| watch mode | 实时趋势 |

### P2

| 项 | 说明 |
| --- | --- |
| Bubble Tea TUI | 一屏交互 |
| ASCII chart | Grafana-lite |
| context panel | schema/hints |
| cardinality | 高基数分析 |
| query explain | 风险解释 |

### P3

| 项 | 说明 |
| --- | --- |
| query profiler | 慢查询诊断 |
| dataset generator | 复现数据 |
| query lab | replay/diff/template |
| storage metadata | 文件视角 |

## 12. 下一步执行建议

最小下一步：

1. 初始化 Go module 和 `cmd/influx-cli`。
2. 实现 config/profile 读取。
3. 实现 InfluxDB HTTP query adapter。
4. 实现 Result table normalize。
5. 实现 table 和 sparkline renderer。
6. 实现 `query` 和 `repl` 两个命令。
7. 用 fake adapter 写单测，再用本地 InfluxDB/openGemini 兼容接口手动验证。
