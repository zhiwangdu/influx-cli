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
| [Phase 2 TUI Iteration Plan](docs/PHASE2_TUI_ITERATION_PLAN.md) | Detailed TUI iteration route, scope boundaries, and acceptance criteria |

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
influx-cli --host influx.example.com --port 443 --ssl --unsafeSsl query "SHOW DATABASES"
influx-cli repl
influx-cli tui
influx-cli config show
```

Connection flags follow the InfluxDB 1.x CLI shape: use `--host`, `--port`, `--ssl`, and `--unsafeSsl`. The old URL-style connection option is not supported; use `--host localhost --port 8086` instead of `--url http://localhost:8086`.

## TUI Usage

Start the terminal UI with:

```bash
influx-cli tui
```

The TUI provides a query editor, result workbench, context panel, selectable completion menu, searchable history panel, and watch refresh loop.

Key bindings:

Single-letter shortcuts apply in command/result modes. In edit mode, printable keys are inserted into the query editor.

| Key | Action |
| --- | --- |
| `Ctrl+J` / `Ctrl+Enter` | Run the current query |
| `Ctrl+C` | Cancel a running query; press again to quit while cancellation is pending |
| `Ctrl+U` | Clear the current editor line |
| `Ctrl+L` | Clear the whole query editor |
| `Ctrl+R` | Open searchable query history |
| `Tab` | Open completion candidates |
| `Esc` | Switch edit/command modes or close overlays |
| `Enter` / `V` | Enter result mode from command mode |
| `0` / `1` / `2` / `3` / `4` | Switch auto/table/sparkline/chart/json rendering |
| `R` | Refresh the current editor query, falling back to the last query |
| `+` / `-` | Adjust watch interval |
| `S` | Toggle context panel |
| `L` | Refresh schema for the current measurement |
| `W` | Toggle watch refresh |
| `F` | Toggle fullscreen result view |
| `Q` | Quit |

Watch mode never starts a second query while one is still running. A failed watch refresh keeps the last successful result visible and reports the last error in status/context.
In the history panel, `Ctrl+U` clears the history filter instead of editing the query.

Implemented Dataset Generator surface:

```bash
influx-cli ingest demo-cpu --db metrics --rate 10k/s --duration 5m
influx-cli ingest high-cardinality --db metrics --hosts 1000 --pids 10000
influx-cli ingest out-of-order --db metrics --ratio 0.1
influx-cli ingest covering-block --db metrics
influx-cli ingest stress-basic --db stress --point-count 10 --series-count 1000 --tick 10s --start 2006-01-02T00:00:00Z
influx-cli ingest iql --file ./mock.iql --dry-run
influx-cli ingest demo-cpu --rate 2/s --duration 1s --start 2026-07-05T00:00:00Z --dry-run
```

Use `--start` when the generated timestamp range must be reproducible. Without it, the range is relative to the current clock.
The `stress-basic` dataset mirrors the basic `influx_stress` point generator shape: `point-count * series-count` points, `host=server-N` series tags, and timestamps advanced by `--tick`.
The `iql` dataset reads an `influx_stress`-style IQL file as a mock data generator input. It supports write-oriented `SET`, `INSERT`, `GO INSERT`, and `WAIT` statements; query and raw InfluxQL statements are reported as skipped rather than used for pressure testing.
For `ingest iql`, explicit CLI flags take precedence over IQL `SET` values, which take precedence over profile/config defaults. `GO` and `WAIT` are accepted for compatibility, but writes are executed through the existing synchronous Dataset Generator path.

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
