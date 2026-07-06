# Phase 2 TUI 迭代路线

本文是 Phase 2：TUI 和 Grafana-lite 的详细迭代路线，用于细化 `docs/ROADMAP.md`。它保持 Phase 2 聚焦于可持续使用的终端查询控制台，不扩展为 dashboard、profiler 或 storage analyzer。

该路线已经结合当前 `internal/tui` 实现、正式产品/架构文档，并经过本地 Claude 设计审视。

## 1. 当前基线

当前 TUI 已具备：

| 区域 | 当前状态 |
| --- | --- |
| 入口 | `influx-cli tui` |
| 框架 | Bubble Tea model，包含 textarea query editor 和 viewport result view |
| 查询执行 | 通过 `app.Executor` 复用 shared adapter/result/render path |
| 结果渲染 | table、sparkline、chart 切换 |
| 状态展示 | TUI statusline 和 executor session status |
| 上下文 | db/rp/adapter/measurement、field/tag schema summary |
| schema 加载 | 查询后按推断 measurement 加载 schema |
| watch | 可开关的定时刷新 |
| history | 加载持久化历史并支持轮转回调 |
| completion | 调用应用层 completion 并应用第一个候选 |
| layout | 宽屏 context panel、窄屏堆叠、result fullscreen |

当前主要缺口：

| 缺口 | 影响 |
| --- | --- |
| mode/focus 模型隐式 | 后续加入 result scrolling、overlay 和复杂快捷键时容易互相干扰 |
| result viewport 键盘工作流弱 | 大结果不适合纯键盘检查 |
| watch 不能取消 in-flight query | Roadmap Phase 2 验收要求 watch 可取消 |
| context panel 仍是 schema summary | 有用，但还不是强 TSDB-native context surface |
| completion 没有候选选择器 | 多候选时只能应用第一个 |
| history 没有面板/搜索 UI | TUI history 只能轮转召回，不能浏览和筛选 |
| 测试主要覆盖 happy path | resize、view、watch lifecycle、overlay 需要更强覆盖 |

## 2. 范围边界

Phase 2 的核心交付是可靠的单查询 TUI，不进入 dashboard 或 profiler 阶段。

Phase 2 范围内：

| 能力 | 范围 |
| --- | --- |
| TUI layout | 宽屏、窄屏、fullscreen 响应式布局 |
| Query editor | 稳定的编辑、执行、取消循环 |
| Result workbench | 当前结果的键盘浏览和 renderer 切换 |
| Context panel | 当前 db/rp/adapter/measurement/schema/result metadata |
| Watch | 稳定刷新、取消、间隔控制和 last error 可见 |
| History panel | 搜索并加载持久化 query history |
| Completion menu | 可选择的补全候选 |
| Hardening | 测试、文档、本地 InfluxDB/openGemini smoke check |

延后到 Phase 3：

| 延后能力 | 原因 |
| --- | --- |
| Cardinality summary | 属于 Schema Intelligence，可能需要基础 schema 之外的 adapter 支持 |
| Query hints | 属于 Query Explain，需要 time predicate 和 range risk 分析 |
| Explain output | 需要 parser-lite、range extraction 和 risk model |
| Schema explorer tree | 超出当前 query-oriented context panel |
| Context panel risk tips | 依赖 Phase 3 analyzer/hints |

延后到更后阶段：

| 延后能力 | 目标阶段 |
| --- | --- |
| Query profiler 和 block decode metrics | Phase 4 |
| Storage file analyzer | Phase 5 |
| Multi-panel dashboard 和 saved workspace | Phase 6 |
| Plugin system | Phase 6+ |

## 3. 迭代原则

1. TUI 必须保持纯键盘可用。
2. 不复制 query execution 逻辑，继续使用 `app.Executor`。
3. Renderer 切换只作用于最后一次 normalized `result.Result`，不重新查询。
4. Schema/context 加载必须是可选能力；schema 失败不能阻塞 query。
5. 每个迭代内同步补测试，不把测试集中留到最后。
6. 抽象 overlay/list primitive 时要服务 history 和 completion 两类明确场景。
7. 增加信息密度前，先保证窄终端可用。

## 4. 迭代路线

### P2.1 State Machine、Input Routing 和 Layout Stability

目标：在增加复杂界面前，先让 TUI 行为可预测。

交付：

| 区域 | 工作 |
| --- | --- |
| Mode model | 引入显式模式：`edit`、`command`、`result`、`completion`、`history` |
| Key routing | 先按 mode 分发 key，再进入 editor/result/overlay |
| Layout | 稳定宽屏、窄屏、fullscreen 的尺寸计算 |
| Statusline | 尽量减少重复 status formatting，同时保留 TUI 特有状态 |
| Result viewport | 确保 result mode 下键盘事件能进入 result view |
| Tests | 固化 key behavior、resize behavior 和无重叠布局约束 |

验收：

| 检查项 | 标准 |
| --- | --- |
| Resize | 80x24、120x40、160x50 不 panic，核心区域不明显重叠 |
| Edit safety | edit mode 输入 `S`、`W`、`1`、`2`、`3` 时插入文本，不触发命令 |
| Command keys | renderer/schema/watch/fullscreen shortcut 在非 edit mode 生效 |
| Result keys | result mode 可滚动结果，不抢 editor 输入 |

风险和约束：

| 风险 | 处理方式 |
| --- | --- |
| 快捷键回归 | refactor 前先用测试锁住当前行为 |
| 重构过大 | 本迭代不改 query execution、renderer 和 schema loading |

### P2.2 Result Workbench

目标：让 result 区域适合日常检查大结果。

交付：

| 区域 | 工作 |
| --- | --- |
| Keyboard browsing | 增加 `j/k`、`PgUp/PgDn`、`g/G` 结果滚动 |
| Render modes | 支持 table、sparkline、chart、json、auto |
| Result title | 展示 renderer、rows/points、series count、query latency |
| Large result behavior | 暴露 renderer option 中的截断限制 |
| Empty/error states | 统一 `no rows`、render error、query error 展示 |
| Tests | renderer 切换、滚动、空结果、大结果提示 |

验收：

| 检查项 | 标准 |
| --- | --- |
| Pure keyboard | 大表格无需鼠标即可检查 |
| Renderer switch | 切换 renderer 不重新执行 query |
| Result metadata | 用户能看到当前 renderer 和数据规模 |
| Degradation | sparkline/chart 不支持当前结果形态时可读降级 |

### P2.3 Watch Lifecycle

目标：让 watch mode 安全、可控、可恢复。

交付：

| 区域 | 工作 |
| --- | --- |
| Cancellation | 跟踪每次 query 的 cancel function，支持取消 in-flight watch/query |
| Scheduling | 上一次刷新完成或取消前，不启动新刷新 |
| Interval control | 增加 `+`/`-` 调整 interval，并设置上下限 |
| Manual refresh | 在 command mode 增加 `R`，手动刷新当前/上一次 query |
| Status | 展示 watch on/off、interval、last refresh、last latency、last error |
| Error handling | watch 刷新失败时保留 last successful result |
| Tests | watch toggle、cancel、interval 调整、无并发刷新、错误保留 |

验收：

| 检查项 | 标准 |
| --- | --- |
| Cancel | 用户可以取消运行中的 watch refresh，而不退出 TUI |
| No overlap | watch 不发起并发 query execution |
| Failure | refresh 失败不清空 last good result |
| Interval | 调整后的 interval 体现在 status 和后续 tick 中 |

### P2.4 Query Editor Refinement

目标：改善编辑循环，但不把 TUI 做成完整 SQL IDE。

交付：

| 区域 | 工作 |
| --- | --- |
| Run/cancel | 保持 `Ctrl+J`/`Ctrl+Enter` run；`Ctrl+C` 优先 cancel，再退出 |
| Clear | 增加明确的 clear-editor 命令 |
| Multiline display | 固定 editor 高度内让多行 query 可读 |
| Syntax hint | 仅加入不依赖 Phase 3 analyzer 的轻量本地提示 |
| Tests | 空 query、running guard、cancel behavior、多行显示 |

验收：

| 检查项 | 标准 |
| --- | --- |
| Running guard | loading 时重复 run 有明确 status message |
| Cancel | in-flight query 可取消，不杀掉程序 |
| Recovery | error 或 cancel 后可继续编辑和重新执行 |

### P2.5 Context Panel Structure

目标：增强上下文信息，但不进入 Phase 3 智能分析。

交付：

| 区域 | 内容 |
| --- | --- |
| Connection | adapter、db、rp、precision |
| Query | inferred measurement 和 last query summary |
| Result | renderer、rows/points、series count、time range |
| Schema | 当前 measurement 的 field/tag count 和紧凑名称列表 |
| State | schema loading/error、watch status、last query error |
| Controls | context toggle、manual schema refresh |

明确排除：

| 排除项 | 阶段 |
| --- | --- |
| top measurement/tag cardinality | Phase 3 |
| no-time-filter 和 bucket-size hints | Phase 3 |
| explain/range/series estimate | Phase 3 |
| schema tree explorer | Phase 3 |

验收：

| 检查项 | 标准 |
| --- | --- |
| Useful context | 执行 `SELECT ... FROM cpu` 后，能展示 `cpu` schema summary |
| Non-blocking | schema load 失败可见，但不导致 query 失败 |
| Responsive | context panel 在宽屏和窄屏均可读 |
| Refresh | 用户可刷新当前 measurement schema |

### P2.6 Completion Menu

目标：让 TUI 中的多候选补全可用。

交付：

| 区域 | 工作 |
| --- | --- |
| Overlay | 增加可复用 overlay/list primitive |
| Candidate list | 展示 completion candidates 和当前选中行 |
| Navigation | `Tab`、`Shift+Tab`、`Up`、`Down` 切换候选 |
| Accept/cancel | `Enter` 接受，`Esc` 取消 |
| Sources | 复用 `executor.Complete` 的 db/rp/measurement/field/tag/meta completion |
| Tests | 单候选、多候选、取消、接受、无匹配 |

验收：

| 检查项 | 标准 |
| --- | --- |
| Multi-candidate | 用户可以选择任意候选，而不是只能用第一个 |
| Editor integrity | 接受补全只替换目标 prefix |
| Error handling | completion error 显示在 status，不破坏 edit mode |

### P2.7 History Panel

目标：让 TUI 能有效使用持久化 query history。

交付：

| 区域 | 工作 |
| --- | --- |
| Overlay reuse | 复用 completion menu 的 list overlay |
| Search | 按文本过滤 history entries |
| Navigation | up/down 选择，必要时支持分页 |
| Load | `Enter` 将 query 加载进 editor，但不执行 |
| Metadata | 空间允许时展示 db/rp 和 query timestamp |
| Tests | 打开、过滤、选择、加载、取消、空 history |

验收：

| 检查项 | 标准 |
| --- | --- |
| Browse | 用户可以浏览历史，而不是盲目轮转 |
| Search | filter 可以缩小可见 entries |
| Load | 加载 query 保留多行内容 |
| Safety | 加载 history 不自动执行 |

### P2.8 Hardening、Docs 和 Manual Acceptance

目标：把 Phase 2 收敛为稳定、可文档化的 TUI 能力。

交付：

| 区域 | 工作 |
| --- | --- |
| Docs | README/TUI usage、keymap、mode behavior、watch behavior |
| Tests | mode、layout、watch、overlay、context 的完整 model tests |
| Smoke checks | 本地 InfluxDB/openGemini-compatible query 和 watch 手工检查 |
| Cleanup | 清理 roadmap/state docs 中的过期假设 |
| Accessibility | 确认核心 workflow 不依赖鼠标 |

验收：

| 检查项 | 标准 |
| --- | --- |
| Test suite | `go test ./...`、`go vet ./...` 和相关 race tests 通过 |
| TUI launch | `influx-cli tui` 可正常启动 |
| Query | TUI 可执行基础 `SHOW` 和 `SELECT` query |
| Render | table/sparkline/chart/json/auto 可选择 |
| Context | db/rp/schema summary 可见且 non-blocking |
| Watch | refresh、cancel、interval adjustment、error persistence 可用 |
| History | searchable history panel 可用 |
| Completion | selectable completion menu 可用 |
| Resize | 常见终端尺寸保持可用 |

## 5. 跨迭代测试策略

每个 P2.x 迭代都必须同步增加聚焦测试。

| 测试层 | 覆盖 |
| --- | --- |
| Model update tests | key routing、mode transitions、watch lifecycle |
| View tests | status/footer/context 文本和 layout constraints |
| Fake adapter tests | query success/error、schema success/error、completion candidates |
| History tests | panel loading/filtering/selection |
| Completion tests | prefix replacement 和 candidate navigation |
| Smoke checks | 本地 InfluxDB/openGemini-compatible endpoint 的 query/watch |

## 6. 完成定义

Phase 2 完成时应满足：

1. 用户可以在 `influx-cli tui` 中完成常规 query exploration。
2. 所有核心 workflow 都支持纯键盘操作。
3. Result rendering 和 scrolling 稳定。
4. Context panel 提供当前 connection、result 和 schema summary。
5. Watch 可以 refresh、cancel，并从失败中恢复。
6. History 和 completion 都可选择，不只是 one-shot helper。
7. TUI 在窄屏和宽屏终端尺寸下都保持稳定。
8. Phase 3 智能分析可以在不重做 TUI 基础的前提下继续推进。
