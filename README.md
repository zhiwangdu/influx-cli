# influx-cli

`influx-cli` is a Go-based CLI and TUI for exploring, visualizing, and diagnosing InfluxDB/openGemini time-series data from the terminal.

The product direction is:

> A TSDB-native terminal console combining query execution, schema context, lightweight visualization, and later query/storage profiling.

## Documentation

The formal documents are the source of truth:

| Document | Purpose |
| --- | --- |
| [Product Design](docs/PRODUCT_DESIGN.md) | Product positioning, users, workflows, TUI/CLI behavior, MVP boundaries |
| [Architecture](docs/ARCHITECTURE.md) | Go package structure, core interfaces, data flow, adapter/render/analyzer design |
| [Roadmap](docs/ROADMAP.md) | Delivery phases, scope, acceptance criteria, risks |

Legacy drafts are kept under [docs/legacy](docs/legacy/) for historical context only. Do not use them for future decisions unless explicitly needed.

## Current Status

This repository now contains the Phase 0/1 CLI and REPL foundation, the first Phase 2 TUI surface, and the Phase 4 Dataset Generator foundation:

Current files:

```text
README.md
AGENTS.md
go.mod
go.sum
cmd/influx-cli/
internal/
docs/
  PRODUCT_DESIGN.md
  ARCHITECTURE.md
  ROADMAP.md
  legacy/
    README.legacy.md
    TUIDesign.legacy.md
```

Implemented query surface:

```bash
influx-cli query "SHOW DATABASES"
influx-cli query --db metrics "SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(1m)"
influx-cli repl
influx-cli tui
influx-cli config show
```

Implemented Dataset Generator surface:

```bash
influx-cli ingest demo-cpu --db metrics --rate 10k/s --duration 5m
influx-cli ingest high-cardinality --db metrics --hosts 1000 --pids 10000
influx-cli ingest out-of-order --db metrics --ratio 0.1
influx-cli ingest covering-block --db metrics
influx-cli ingest demo-cpu --rate 2/s --duration 1s --start 2026-07-05T00:00:00Z --dry-run
```

Use `--start` when the generated timestamp range must be reproducible. Without it, the range is relative to the current clock.

## MVP Scope

The first implementation phase should stay narrow:

1. CLI query execution.
2. REPL with session context, local query history, multiline input, and Tab autocomplete.
3. Table output by default.
4. Sparkline and ASCII chart output for time-series results when selected.
5. Statusline showing db/rp/mode/latency/error.
6. Full-screen TUI with query editor, result view, context panel, renderer switching, and watch refresh.

Do not start with a full dashboard, plugin system, storage parser, or query optimizer.

## Tech Stack

| Area | Choice |
| --- | --- |
| Language | Go |
| CLI | Cobra |
| TUI | Bubble Tea, Lip Gloss, Bubbles |
| Visualization | Table default, built-in sparkline selectable, ASCII chart selectable |
| Initial adapter | InfluxDB 1.x HTTP query API |
| Compatible target | openGemini via InfluxDB-compatible query path |

## Verification

```bash
go test ./...
go vet ./...
```

## Working Notes

Agent and contributor instructions live in [AGENTS.md](AGENTS.md).
