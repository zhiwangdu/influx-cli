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

This repository is currently in the product and architecture design stage. It does not yet contain a Go implementation.

Current files:

```text
README.md
AGENTS.md
docs/
  PRODUCT_DESIGN.md
  ARCHITECTURE.md
  ROADMAP.md
  legacy/
    README.legacy.md
    TUIDesign.legacy.md
```

## MVP Scope

The first implementation phase should stay narrow:

1. CLI query execution.
2. REPL with session context.
3. Table output.
4. Sparkline output for time-series results.
5. Statusline showing db/rp/mode/latency/error.

Do not start with a full dashboard, plugin system, storage parser, or query optimizer.

## Planned Tech Stack

| Area | Choice |
| --- | --- |
| Language | Go |
| CLI | Cobra |
| TUI | Bubble Tea, Lip Gloss, Bubbles |
| Visualization | Built-in sparkline first, ASCII chart later |
| Initial adapter | InfluxDB 1.x HTTP query API |
| Compatible target | openGemini via InfluxDB-compatible query path |

## Next Implementation Step

Follow [docs/ROADMAP.md](docs/ROADMAP.md), Phase 0:

1. Initialize `go.mod`.
2. Add `cmd/influx-cli/main.go`.
3. Implement config/profile loading.
4. Implement InfluxDB HTTP query adapter.
5. Normalize query results into table/series models.
6. Add table and sparkline renderers.
7. Add `query` and `repl` commands.

## Working Notes

Agent and contributor instructions live in [AGENTS.md](AGENTS.md).
