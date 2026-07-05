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
| `internal/analysis/storage` | Phase 5 初始 storage file analyzer：TSM/WAL/TSSP/TSI/mergeset 文件元数据、key/series 样例、block/meta-index/entry 统计、TSI index/log series-id set cardinality、TSM tombstone range/impact、TSM/WAL decode path estimate、TSSP chunk metadata、detached TSSP meta-index sidecar、mergeset metaindex/index block-header/item payload summary、file-set item search simulation 和 duplicate item merge/dedup summary、query range overlap |
| `docs/PRODUCT_DESIGN.md` | 产品设计书 |
| `docs/ARCHITECTURE.md` | 架构说明 |
| `docs/ROADMAP.md` | 本 roadmap |

Phase 0 仍应通过本地 InfluxDB/openGemini 兼容端点做人工验收。

Phase 5 已开始一个窄切面的本地文件分析命令：

```bash
influx-cli storage analyze <file-or-dir>...
```

当前覆盖 InfluxDB TSM attached file metadata、WAL write/delete/delete-range entry metadata、tombstone range/impact summary、基于 query range/key 的 TSM/WAL decode path estimate、per-file 和 FileStore-level ascending/descending cursor window/merge window simulation、优化前后 block/entry/byte/value decode path estimate、decoded timestamp output/dedup estimate、TSM value output comparison、local TSM KeyCursor-style execution stats/output samples、TSM FileStore.Cost-style file/block/byte estimate、TSI index file measurement/tag summary、TSI log entry summary、live/tombstone series-id set cardinality 和 measurement/tag predicate inspection，以及 openGemini attached TSSP trailer/meta-index metadata、none/snappy/LZ4/self-compressed chunk metadata、per-file 和 file-set series-id filtered segment-level decode path estimate、TSSP ContainsValue/MetaIndex-style candidate cost estimate、ascending/descending TSSP LocationCursor-style segment window samples、TSSP ReadAt call estimate、sampled optimized column-segment read ranges、detached `segment.idx` meta-index CRC validation、query-range candidate filtering 和 mergeset part metadata/metaindex/index block-header/item payload summary、file-set item search simulation、duplicate item merge/dedup summary。完整 storage-engine-backed cursor execution 仍属于后续 Phase 5 工作。

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
| tssp | openGemini 文件元数据 |
| tssp-metaindex | openGemini detached `segment.idx` meta-index sidecar |
| mergeset | 已有 part metadata、metaindex row、index block-header、item payload summary、file-set item search simulation 和 duplicate item merge/dedup summary；后续补充 storage-engine-backed table cursor simulation |
| meta/store/sql topology | 集群拓扑和节点状态 |

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
