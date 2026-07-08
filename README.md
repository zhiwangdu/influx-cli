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
influx-cli storage analyze --report /path/to/file.tsm --from 10 --to 20
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

Use `--storage-format tsm|wal|tssp|tssp-metaindex|tsi|tsi-log|series-file|fields-index|mergeset|opengemini-meta|opengemini-pk-meta|opengemini-pk-index|opengemini-bloom-filter|opengemini-text-index` to override auto-detection when needed. Repeat `--key` to scope TSM/WAL decode-path planning to specific TSM index keys or to simulate mergeset item lookup for raw item keys across analyzed parts. TSM/WAL key filtering requires `--from` and `--to`; explicit `--storage-format mergeset` supports key-only item search. Use `--cursor-order asc|desc` to model ascending or descending TSM/openGemini TSSP cursor planning and mergeset local item search/table scan order. Repeat `--series-id` to scope attached openGemini TSSP decode-path planning to specific series IDs; with explicit `--storage-format series-file`, `--series-id` only inspects local `_series` live/tombstone IDs and does not require `--from`/`--to`. Repeat `--meta-index-id` to scope detached `segment.idx` planning to specific meta-index IDs. Repeat `--column` to project TSSP columns during local ReadAt planning and data block probes, repeat `--field` with `key=value`, `key==value`, `key equals/equal value`, `key!=value`, `key<>value`, `key not-equals/not equals/not_equals value`, `key not = value`, `key not == value`, `key !equals/!equal value`, `key exists`, `key not-exists`, `key !exists`, `key=~<pattern>`, `key!~<pattern>`, `key matches/match/regex/regexp <pattern>` and `not`/`!` variants, `key>value`, `key>=value`, `key<value`, `key<=value`, `key !> value`, `key !>= value`, `key !< value`, `key !<= value`, `key not > value`, `key not >= value`, `key not < value`, `key not <= value`, `key is value`, `key is-not value`, `key is not value`, `key in (value1,value2)`, `key not-in (value1,value2)`, `key !in (value1,value2)`, `key between (lower,upper)`, `key not-between (lower,upper)`, `key !between (lower,upper)`, `key contains value`, `key not-contains value`, `key !contains value`, `key icontains value`, `key not-icontains value`, `key !icontains value`, `key like pattern`, `key not-like pattern`, `key !like pattern`, `key ilike pattern`, `key not-ilike pattern`, `key !ilike pattern`, `key starts-with value`, `key not-starts-with value`, `key !starts-with value`, `key istarts-with value`, `key not-istarts-with value`, `key !istarts-with value`, `key ends-with value`, `key not-ends-with value`, `key !ends-with value`, `key iends-with value`, `key not-iends-with value`, or `key !iends-with value` to apply required-AND simple, existence, regex, finite-set, range, case-sensitive and case-insensitive LIKE-wildcard, substring, prefix, and suffix TSSP decoded-field predicates to local record rows, repeat `--field-any` with the same predicate syntax when at least one OR predicate should match, and repeat `--field-none` when no repeated NOT predicate may match; all TSSP field predicates require `--from` and `--to`. Field predicate keys may target decoded value columns or the decoded `time` column when chunk metadata exposes it; projected probes keep `time` available when it is required for filtering or sample timestamps. Single- or double-quote string literals when a decoded string value contains commas or parentheses, for example `key in ("red,primary","blue)")`. Equality, `equals`/`equal`, inequality, negated equality aliases, `is`/`is-not` aliases, `in`/`not-in`/`!in` set membership, and `between`/`not-between`/`!between` inclusive range membership compare decoded values by type; `!>`/`not >`, `!>=`/`not >=`, `!<`/`not <`, and `!<=`/`not <=` normalize to inverse ordered comparisons. `key is between (...)` remains equality against the literal text `between (...)`, so use `key between ...` for range predicates. `null` matches decoded null rows for `=null`, `==null`, `!=null`, `<>null`, `is null`, `is-not null`, and set membership, including quoted `"null"`, while `exists` matches only locally decoded non-null rows and `not-exists`/`!exists` matches decoded null rows or missing local decoded columns. Ordered `--field`, `--field-any`, and `--field-none` predicates are numeric for decoded integer/float blocks and lexicographic for decoded string blocks. Range predicate bounds may be wrapped in parentheses or provided as `lower,upper`; they reject `null` bounds, use the bound order as provided, and do not match unordered boolean blocks. Regex predicates `=~`, `!~`, `matches`, `match`, `regex`, `regexp`, and their `not`/`!` aliases use Go regular expressions against the local decoded value string representation and are validated before any file analysis; `like`/`not-like`/`!like` and `ilike`/`not-ilike`/`!ilike` use SQL-style `%` and `_` wildcards, while `contains`/`not-contains`/`!contains`/`icontains`/`not-icontains`/`!icontains` and `starts-with`/`not-starts-with`/`!starts-with`/`istarts-with`/`not-istarts-with`/`!istarts-with`/`ends-with`/`not-ends-with`/`!ends-with`/`iends-with`/`not-iends-with`/`!iends-with` perform local substring, prefix, and suffix checks; all of these string-only predicates match only non-null decoded string blocks, so decoded null rows and non-string blocks do not match them. Default table output includes per-file `details` summaries for local index/fields/primary-key/secondary-index analyses and a `<file-set>` aggregate row when local multi-file decode-path planning is available. JSON output includes local TSM final optimized cursor output samples, WAL write/delete/delete-range entry summaries and local write/delete replay candidate samples, TSI index/log and `_series` live/tombstone series-id set cardinality, InfluxDB `_series` partition/segment entry summaries, local `fields.idx`/`fields.idxl` measurement field-type summaries, openGemini mergeset part metadata plus metaindex/index block-header, plain/zstd item payload decode success/failure counts, TSI/tag namespace, field-index namespace, and CLV text-index item summaries, payload-backed part and file-set table scan summaries, part/file-set item search exact-match/exact-miss seek windows with local block-gap cursor advance accounting, TableSearch seek/heap cursor candidates with part provenance, local heap insert/pop/cursor-advance accounting and scan/search heap execution step samples, final deduplicated mergeset scan/search cursor output samples including part-level exact item-search output samples, duplicate item merge-window/dedup summaries, local openGemini `primary.meta` detached primary-key schema/meta-block/CRC/data-range summaries, local openGemini attached primary-key `.idx` schema/row-count/column-offset summaries, local openGemini bloom filter secondary index block/group/CRC summaries for attached `.bf` and detached `bloomfilter_*.idx` files, opengemini-text-index requests reported as skipped without parsing sidecars, TSSP projected ReadAt call estimates, sampled optimized column-segment read ranges, file-set decoded output provenance/final exact dedup samples, required-AND, OR, and NOT simple/existence/finite-set/range/regex/like/ilike/substring/prefix/suffix decoded-field predicate filtering with decoded-row input/match/reject counts and row-level evaluated predicate decision details, local cursor/range/record/filter execution samples and sampled action count maps, and attached data block header/value probes when chunk metadata is expanded, plus detached meta-index candidate filtering, chunk metadata batch planning, optional sibling `segment.meta` expansion, detached projected segment-level data ReadAt planning, and optional `segment.bin` header/range/CRC block probes with row-count materialization and value samples for `tssp-metaindex`. Repeat `--measurement` and `--tag key=value` to inspect TSI measurement/tag predicates.

TSSP multi-word field predicate operator aliases accept hyphen, space, or underscore separators, for example `not-like`, `not like`, and `not_like` normalize to the same local decoded-row operator; `not-exists`, `not exists`, `not_exists`, and `!exists` normalize to the same existence operator; regex aliases such as `matches`, `match`, `not matches`, `regex`, `regexp`, and `!regexp` normalize to `=~` or `!~`; negated ordered comparison aliases `!>`/`not >`/`not->`/`not_>`, `!>=`/`not >=`/`not->=`/`not_>=`, `!<`/`not <`/`not-<`/`not_<`, and `!<=`/`not <=`/`not-<=`/`not_<=` normalize to `<=`, `<`, `>=`, and `>` respectively; case-insensitive string aliases such as `ilike`, `icontains`, `istarts-with`, and `iends-with` follow the same local string-only path, and bang-negated prefix/suffix aliases also accept underscores, such as `!starts_with`, `!istarts_with`, `!ends_with`, and `!iends_with`.

TSSP equality word aliases `equals`/`equal` normalize to local decoded-row equality, while `not-equals`/`not equals`/`not_equals`, `not-equal`/`not equal`/`not_equal`, `not =`/`not ==`, and `!equals`/`!equal` normalize to local decoded-row inequality.

Explicit `in`/`not-in` and `between`/`not-between` field predicates keep `=`, `<`, `>`, and `!` characters inside their local decoded-row value list or bound text instead of treating those characters as earlier scalar comparison operators.

For mergeset JSON output, item payload extras include plain/zstd `items.bin` plus `lens.bin` read bytes, successful decoded read bytes, successful uncompressed payload bytes, and uncompressed-minus-decoded-read byte deltas; failed decodes keep read-byte and failure counts without estimating partial uncompressed bytes. Mergeset cursor window, output, and heap execution samples add `key_hex`, `optimized_value_hex`, or `candidate_value_hex` when a sampled item contains non-printable binary bytes.

Default table `tombstone` cells include local tombstone range counts, query-overlap range counts, and affected-block counts when decoded. Default table `details` cells also include sorted local `block_types` counts when an analyzer records per-block categories, plus local series/meta-index ID count/range summaries and local index/fields/primary-key/secondary-index count, byte, anomaly, and notice-count summaries when those structures are decoded; table-mode stderr warnings are notice-count only, while JSON output keeps full notice text. `storage analyze --report` renders a count-oriented Markdown diagnostic report for issue or PR sharing, using stable file labels and notice counts instead of raw notice text. Index details include count-only measurement/tag query matched/missing summaries when local index filters are applied. `<file-set>` aggregate rows include the cross-file query-hit file count, tombstone file/byte/range/query-overlap/affected-block counts, notice count, and block-type rollup. Default `decode_path` cells include local query range/seek context, target/match/missing counts, block filter skip counts, sorted local location/decode block-type rollups, block/read-segment/ReadAt saved counts, sampled optimized ReadAt range counts/bytes, decode byte/value/output count comparisons and value-output mismatch counts, cursor/window/execution sample counts, full local range/record/filter row and filter-clause action counts, sample-omitted range/record/filter row action counts, sampled cursor/range/record/filter execution action counts, TSSP data probe checked/valid/failure and failure-breakdown counts, failure reason rollups, row-count/output/record-output materialization counts, record materialization row/output/range-reject/filter-reject counts, probed block-type rollups, unavailable-value reason rollups, query-range row execution samples, sampled record materialization execution rows, and filter execution row samples with local decoded field values and evaluated predicate decisions when those local planning/probe stages run.

TSSP execution diagnostics expose `cursor_execution_action_counts`, `range_execution_action_counts`, `record_execution_action_counts`, and `filter_execution_action_counts` for sampled local execution steps. JSON also exposes `range_execution_total_action_counts`, `record_execution_total_action_counts`, `filter_execution_total_action_counts`, and `filter_clause_total_action_counts` from full local data-probe row counters, so callers can distinguish total decoded-row match/reject/output and required/any/none predicate clause match/miss/skip counts from sample-limited execution steps; `range_execution_omitted_action_counts`, `record_execution_omitted_action_counts`, and `filter_execution_omitted_action_counts` report total row actions not represented by sampled execution steps. Filter row totals and omissions use generic `filter_row_match` and `filter_row_reject` action names, while reject reason details remain sample-limited. Filter execution sample `decisions` keep `:` between clause/key/operator/value/result fields and `;` between decisions, escaping literal `\`, whitespace, `:`, and `;` inside those decision fields; record/filter execution `values=` lists keep `=` between local decoded column/value pairs and `,` between pairs, escaping literal `\`, whitespace, `=`, and `,` inside list fields. Record execution diagnostics distinguish total local record candidate rows, output rows, range-rejected rows, filter-rejected rows, and sampled execution rows; execution samples are capped independently from record output samples and include the chunk-local row, file-local `local_input` ordinal, query range context, decoded value-column count, and output/range-reject/filter-reject result, with rejected rows marked `local_output=none`, while record output samples and output execution samples include the local output ordinal for each materialized local record row.

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
