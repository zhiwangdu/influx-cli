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
```

Use `--storage-format tsm|wal|tssp|tssp-metaindex|tsi|tsi-log|series-file|fields-index|mergeset|opengemini-meta|opengemini-pk-meta|opengemini-pk-index|opengemini-bloom-filter|opengemini-text-index` to override auto-detection when needed. Repeat `--key` to scope TSM/WAL decode-path planning to specific TSM index keys or to simulate mergeset item lookup for raw item keys across analyzed parts. TSM/WAL key filtering requires `--from` and `--to`; explicit `--storage-format mergeset` supports key-only item search. Use `--cursor-order asc|desc` to model ascending or descending TSM/openGemini TSSP cursor planning and mergeset local item search/table scan order. Repeat `--series-id` to scope attached openGemini TSSP decode-path planning to specific series IDs; with explicit `--storage-format series-file`, `--series-id` only inspects local `_series` live/tombstone IDs and does not require `--from`/`--to`. Repeat `--meta-index-id` to scope detached `segment.idx` planning to specific meta-index IDs. Repeat `--column` to project TSSP columns during local ReadAt planning and data block probes, repeat `--field` with `key=value`, `key==value`, `key!=value`, `key<>value`, `key=~regex`, `key!~regex`, `key>value`, `key>=value`, `key<value`, `key<=value`, `key is value`, `key is-not value`, `key is not value`, `key in (value1,value2)`, `key not-in (value1,value2)`, `key between (lower,upper)`, or `key not-between (lower,upper)` to apply required-AND simple, regex, finite-set, and range TSSP decoded-field predicates to local record rows, repeat `--field-any` with the same predicate syntax when at least one OR predicate should match, and repeat `--field-none` when no repeated NOT predicate may match; all TSSP field predicates require `--from` and `--to`. Field predicate keys may target decoded value columns or the decoded `time` column when chunk metadata exposes it; projected probes keep `time` available when it is required for filtering or sample timestamps. Single- or double-quote string literals when a decoded string value contains commas or parentheses, for example `key in ("red,primary","blue)")`. Equality, inequality, `is`/`is-not` aliases, `in`/`not-in` set membership, and `between`/`not-between` inclusive range membership compare decoded values by type; `key is between (...)` remains equality against the literal text `between (...)`, so use `key between ...` for range predicates. `null` matches decoded null rows for `=null`, `==null`, `!=null`, `<>null`, `is null`, `is-not null`, and set membership, including quoted `"null"`, while ordered `--field`, `--field-any`, and `--field-none` predicates are numeric for decoded integer/float blocks and lexicographic for decoded string blocks. Range predicate bounds may be wrapped in parentheses or provided as `lower,upper`; they reject `null` bounds, use the bound order as provided, and do not match unordered boolean blocks. Regex predicates `=~` and `!~` use Go regular expressions against the local decoded value string representation and are validated before any file analysis. Default table output includes per-file `details` summaries for local index/fields/primary-key/secondary-index analyses and a `<file-set>` aggregate row when local multi-file decode-path planning is available. JSON output includes local TSM final optimized cursor output samples, WAL write/delete/delete-range entry summaries and local write/delete replay candidate samples, TSI index/log and `_series` live/tombstone series-id set cardinality, InfluxDB `_series` partition/segment entry summaries, local `fields.idx`/`fields.idxl` measurement field-type summaries, openGemini mergeset part metadata plus metaindex/index block-header, item payload, TSI/tag namespace, field-index namespace, and CLV text-index item summaries, payload-backed part and file-set table scan summaries, part/file-set item search exact-match/exact-miss seek windows with local block-gap cursor advance accounting, TableSearch seek/heap cursor candidates with part provenance, local heap insert/pop/cursor-advance accounting and scan/search heap execution step samples, final deduplicated mergeset scan/search cursor output samples including part-level exact item-search output samples, duplicate item merge-window/dedup summaries, local openGemini `primary.meta` detached primary-key schema/meta-block/CRC/data-range summaries, local openGemini attached primary-key `.idx` schema/row-count/column-offset summaries, local openGemini bloom filter secondary index block/group/CRC summaries for attached `.bf` and detached `bloomfilter_*.idx` files, opengemini-text-index requests reported as skipped without parsing sidecars, TSSP projected ReadAt call estimates, sampled optimized column-segment read ranges, file-set decoded output provenance/final exact dedup samples, required-AND, OR, and NOT simple/finite-set/range/regex decoded-field predicate filtering with decoded-row input/match/reject counts, and attached data block header/value probes when chunk metadata is expanded, plus detached meta-index candidate filtering, chunk metadata batch planning, optional sibling `segment.meta` expansion, detached projected segment-level data ReadAt planning, and optional `segment.bin` header/range/CRC block probes with row-count materialization and value samples for `tssp-metaindex`. Repeat `--measurement` and `--tag key=value` to inspect TSI measurement/tag predicates.

Default table `details` cells also include sorted local `block_types` counts when an analyzer records per-block categories, plus local series/meta-index ID count/range summaries and local index/fields/primary-key/secondary-index count, byte, anomaly, and notice-count summaries when those structures are decoded; table-mode stderr warnings are notice-count only, while JSON output keeps full notice text. Index details include count-only measurement/tag query matched/missing summaries when local index filters are applied. `<file-set>` aggregate rows include the cross-file query-hit file count, tombstone file count, notice count, and block-type rollup. Default `decode_path` cells include local query range/seek context, target/match/missing counts, block filter skip counts, sorted local location/decode block-type rollups, block/read-segment/ReadAt saved counts, sampled optimized ReadAt range counts/bytes, decode byte/value/output count comparisons and value-output mismatch counts, cursor/window/execution sample counts, TSSP data probe checked/valid/failure and failure-breakdown counts, failure reason rollups, row-count/output materialization counts, probed block-type rollups, and unavailable-value reason rollups when those local planning/probe stages run.

`opengemini-text-index` is accepted only to return a skipped notice; automatic directory scans ignore `.ph`, `.bh`, and `.pos` text-index sidecars.

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
