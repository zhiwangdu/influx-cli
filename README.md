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

This repository now contains the Phase 0 CLI MVP foundation:

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

Implemented MVP surface:

```bash
influx-cli query "SHOW DATABASES"
influx-cli query --db metrics "SELECT mean(value) FROM cpu WHERE time > now() - 1h GROUP BY time(1m)"
influx-cli repl
influx-cli config show
```

Initial Phase 5 storage analyzer surface:

```bash
influx-cli storage analyze /path/to/shard --recursive
influx-cli storage analyze /path/to/file.tsm --from 2026-07-05T00:00:00Z --to 2026-07-05T01:00:00Z
influx-cli --format json storage analyze /path/to/file.tsm --from 10 --to 20 --key "cpu,host=a value"
influx-cli --format json storage analyze /path/to/_00001.wal --from 10 --to 20 --key "cpu,host=a value"
influx-cli --format json storage analyze /path/to/file.tssp --from 10 --to 20 --series-id 7
influx-cli --format json storage analyze /path/to/segment.idx --storage-format tssp-metaindex --from 10 --to 20 --meta-index-id 7
influx-cli --format json storage analyze /path/to/L0-00000001.tsi --measurement cpu --tag host=a
influx-cli --format json storage analyze /path/to/L0-00000001.tsl --storage-format tsi-log
influx-cli --format json storage analyze /path/to/_series --storage-format series-file --series-id 42
influx-cli --format json storage analyze /path/to/fields.idx --storage-format fields-index
influx-cli --format json storage analyze /path/to/index/41_1_1847A3A45055EEF0 --storage-format mergeset
influx-cli --format json storage analyze /path/to/index --storage-format mergeset --key aa
influx-cli --format json storage analyze /path/to/primary.meta --storage-format opengemini-pk-meta
influx-cli --format json storage analyze /path/to/0000-0000-0001.idx --storage-format opengemini-pk-index
influx-cli --format json storage analyze /path/to/0000-0000-0001.content.bf --storage-format opengemini-bloom-filter
influx-cli --format json storage analyze /path/to/0000-0000-0001.content.ph --storage-format opengemini-text-index
```

Use `--storage-format tsm|wal|tssp|tssp-metaindex|tsi|tsi-log|series-file|fields-index|mergeset|opengemini-meta|opengemini-pk-meta|opengemini-pk-index|opengemini-bloom-filter|opengemini-text-index` to override auto-detection when needed. Repeat `--key` to scope TSM/WAL decode-path planning to specific TSM index keys or to simulate mergeset item lookup for raw item keys across analyzed parts. TSM/WAL key filtering requires `--from` and `--to`; explicit `--storage-format mergeset` supports key-only item search. Use `--cursor-order asc|desc` to model ascending or descending TSM/openGemini TSSP cursor planning. Repeat `--series-id` to scope attached openGemini TSSP decode-path planning to specific series IDs; with explicit `--storage-format series-file`, `--series-id` only inspects local `_series` live/tombstone IDs and does not require `--from`/`--to`. Repeat `--meta-index-id` to scope detached `segment.idx` planning to specific meta-index IDs. Repeat `--column` to project TSSP columns during local ReadAt planning and data block probes, and repeat `--field` with `key=value`, `key!=value`, `key>value`, `key>=value`, `key<value`, or `key<=value` to apply simple TSSP decoded-field predicates to local record rows; both require `--from` and `--to`. Equality and inequality compare decoded values by type, `null` matches decoded null rows for `=null` and `!=null`, while ordered `--field` predicates are numeric for decoded integer/float blocks and otherwise do not match. JSON output includes local TSM final optimized cursor output samples, WAL write/delete/delete-range entry summaries, TSI index/log and `_series` live/tombstone series-id set cardinality, InfluxDB `_series` partition/segment entry summaries, local `fields.idx`/`fields.idxl` measurement field-type summaries, openGemini mergeset part metadata plus metaindex/index block-header, item payload, TSI/tag namespace, field-index namespace, and CLV text-index item summaries, payload-backed part and file-set table scan summaries, file-set item search, TableSearch seek/heap cursor candidates with part provenance, final deduplicated mergeset scan/search cursor output samples, duplicate item merge-window/dedup summaries, local openGemini `primary.meta` detached primary-key schema/meta-block/CRC/data-range summaries, local openGemini attached primary-key `.idx` schema/row-count/column-offset summaries, local openGemini bloom filter secondary index block/group/CRC summaries for attached `.bf` and detached `bloomfilter_*.idx` files, local openGemini text secondary index sidecar summaries for attached `.pos`/`.bh`/`.ph` triplets, TSSP projected ReadAt call estimates, sampled optimized column-segment read ranges, file-set decoded output provenance/final exact dedup samples, simple decoded-field predicate filtering, and attached data block header/value probes when chunk metadata is expanded, plus detached meta-index candidate filtering, chunk metadata batch planning, optional sibling `segment.meta` expansion, detached projected segment-level data ReadAt planning, and optional `segment.bin` header/range/CRC block probes with row-count materialization and value samples for `tssp-metaindex`. Repeat `--measurement` and `--tag key=value` to inspect TSI measurement/tag predicates.

## MVP Scope

The first implementation phase should stay narrow:

1. CLI query execution.
2. REPL with session context, local query history, multiline input, and Tab autocomplete.
3. Table output by default.
4. Sparkline output for time-series results when selected.
5. Statusline showing db/rp/mode/latency/error.

Do not start with a full dashboard, plugin system, storage parser, or query optimizer.

## Tech Stack

| Area | Choice |
| --- | --- |
| Language | Go |
| CLI | Cobra |
| TUI | Bubble Tea, Lip Gloss, Bubbles planned for later phases |
| Visualization | Table default, built-in sparkline selectable, ASCII chart later |
| Initial adapter | InfluxDB 1.x HTTP query API |
| Compatible target | openGemini via InfluxDB-compatible query path |

## Verification

```bash
go test ./...
go vet ./...
```

## Working Notes

Agent and contributor instructions live in [AGENTS.md](AGENTS.md).
