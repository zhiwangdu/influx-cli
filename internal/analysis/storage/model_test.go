package storage

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReportResultIncludesTSSPDecodePathSummary(t *testing.T) {
	report := Report{
		Files: []FileReport{{
			Path:   "00000001-0001-00000000.tssp",
			Format: FormatTSSP,
			DecodePath: &DecodePathSummary{
				Mode:                        "tssp-location-cursor-ascending",
				BaselineDecodeBlocks:        3,
				OptimizedDecodeBlocks:       1,
				SavedDecodeBlocks:           2,
				LocationBlocksByType:        map[string]int{"chunk-meta": 2, "meta-index": 1},
				DecodeBlocksByType:          map[string]int{"chunk-meta": 1},
				BaselineDecodeBytes:         288,
				OptimizedDecodeBytes:        96,
				SavedDecodeBytes:            192,
				BaselineReadSegments:        3,
				OptimizedReadSegments:       1,
				SavedReadSegments:           2,
				BaselineCursorReadCalls:     3,
				OptimizedCursorReadCalls:    1,
				BaselineReadAtCalls:         6,
				OptimizedReadAtCalls:        2,
				IteratorCostFiles:           1,
				IteratorCostBlocks:          3,
				IteratorCostBytes:           273,
				DataBlockProbeFilterRows:    5,
				DataBlockProbeFilterMatches: 3,
				DataBlockProbeFilterRejects: 2,
				DataBlockProbeRangeRows:     7,
				DataBlockProbeRangeMatches:  5,
				DataBlockProbeRangeRejects:  2,
				DataBlockProbeFilterSkips:   4,
				DataBlockProbeAnySkips:      4,
				DataBlockProbeFilterOps: map[string]int{
					"between":     2,
					"not-between": 1,
					"=":           3,
				},
				Recommendations: []string{
					"read 1 overlapping TSSP segment(s) instead of 3 meta-index candidate segment(s)",
				},
			},
		}},
	}

	result := report.Result()
	row := result.Table.Rows[0]
	decodeText := row[tableColumnIndex(t, result.Table.Columns, "decode_path")].(string)
	for _, want := range []string{
		"tssp-location-cursor-ascending",
		"blocks 3->1",
		"location_block_types chunk-meta:2 meta-index:1",
		"decode_block_types chunk-meta:1",
		"saved_bytes 192",
		"segments 3->1",
		"cursor_reads 3->1",
		"read_at calls 6->2",
		"iterator_cost files=1 blocks=3 bytes=273",
		"field_filter rows=5 matches=3 rejects=2",
		"field_filter_ops =:3 between:2 not-between:1",
		"row_range rows=7 matches=5 rejects=2",
		"field_filter_short_circuit skips=4 required=0 any=4 none=0",
	} {
		if !strings.Contains(decodeText, want) {
			t.Fatalf("decode path summary %q does not contain %q", decodeText, want)
		}
	}
	advice := row[tableColumnIndex(t, result.Table.Columns, "advice")].(string)
	if !strings.Contains(advice, "read 1 overlapping TSSP segment") {
		t.Fatalf("advice = %q, want TSSP segment recommendation", advice)
	}
}

func TestDecodePathTextOmitsEmptyFilterOperatorCounts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		counts map[string]int
	}{
		{name: "nil"},
		{name: "empty", counts: map[string]int{}},
		{name: "zero", counts: map[string]int{"between": 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := decodePathText(&DecodePathSummary{DataBlockProbeFilterOps: tc.counts})
			if strings.Contains(text, "field_filter_ops") {
				t.Fatalf("decode path text = %q, want no field_filter_ops segment", text)
			}
		})
	}
}

func TestDecodePathTextOmitsEmptyBlockTypeCounts(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		LocationBlocksByType: map[string]int{"chunk-meta": 0},
		DecodeBlocksByType:   map[string]int{"chunk-meta": -1},
	})
	for _, notWant := range []string{"location_block_types", "decode_block_types"} {
		if strings.Contains(text, notWant) {
			t.Fatalf("decode path text = %q, want no %s segment", text, notWant)
		}
	}
}

func TestReportResultIncludesReportLevelDecodePathSummary(t *testing.T) {
	report := Report{
		Files: []FileReport{
			{
				Path:               "00000001-0001-00000000.tssp",
				Format:             FormatTSSP,
				SizeBytes:          100,
				KeyCount:           2,
				BlockCount:         3,
				QueryOverlapBlocks: 1,
			},
			{
				Path:               "00000002-0001-00000000.tssp",
				Format:             FormatTSSP,
				SizeBytes:          200,
				KeyCount:           4,
				BlockCount:         5,
				QueryOverlapBlocks: 2,
			},
		},
		Summary: Summary{
			TotalSizeBytes:     300,
			KeyCount:           6,
			BlockCount:         8,
			BlocksByType:       map[string]int{"chunk-meta": 2, "data": 6},
			QueryOverlapBlocks: 3,
			TombstoneFiles:     1,
		},
		DecodePath: &DecodePathSummary{
			Mode:                  "tssp-file-set-location-cursor-ascending",
			BaselineDecodeBlocks:  8,
			OptimizedDecodeBlocks: 3,
			SavedDecodeBlocks:     5,
			LocationBlocksByType:  map[string]int{"chunk-meta": 6, "meta-index": 2},
			DecodeBlocksByType:    map[string]int{"chunk-meta": 3},
			Recommendations: []string{
				"final TSSP file-set output samples include locally deduplicated rows",
			},
		},
	}

	result := report.Result()
	if got, want := len(result.Table.Rows), 3; got != want {
		t.Fatalf("table rows = %d, want %d", got, want)
	}
	if got, want := result.Metadata.RowCount, 3; got != want {
		t.Fatalf("metadata row count = %d, want %d", got, want)
	}
	row := result.Table.Rows[2]
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "file")], "<file-set>"; got != want {
		t.Fatalf("aggregate file = %v, want %v", got, want)
	}
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "format")], "file-set"; got != want {
		t.Fatalf("aggregate format = %v, want %v", got, want)
	}
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "size")], int64(300); got != want {
		t.Fatalf("aggregate size = %v, want %v", got, want)
	}
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "keys/series")], 6; got != want {
		t.Fatalf("aggregate keys/series = %v, want %v", got, want)
	}
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "blocks")], 8; got != want {
		t.Fatalf("aggregate blocks = %v, want %v", got, want)
	}
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "query_blocks")], 3; got != want {
		t.Fatalf("aggregate query blocks = %v, want %v", got, want)
	}
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "tombstone")], "1 files"; got != want {
		t.Fatalf("aggregate tombstone = %v, want %v", got, want)
	}
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "details")], "files=2; block_types chunk-meta:2 data:6"; got != want {
		t.Fatalf("aggregate details = %v, want %v", got, want)
	}
	decodeText := row[tableColumnIndex(t, result.Table.Columns, "decode_path")].(string)
	for _, want := range []string{
		"tssp-file-set-location-cursor-ascending",
		"blocks 8->3",
		"location_block_types chunk-meta:6 meta-index:2",
		"decode_block_types chunk-meta:3",
	} {
		if !strings.Contains(decodeText, want) {
			t.Fatalf("aggregate decode path = %q, want %q", decodeText, want)
		}
	}
	advice := row[tableColumnIndex(t, result.Table.Columns, "advice")].(string)
	if !strings.Contains(advice, "final TSSP file-set output samples") {
		t.Fatalf("aggregate advice = %q, want file-set recommendation", advice)
	}
}

func TestAccumulateSummaryIncludesBlockTypes(t *testing.T) {
	var summary Summary
	accumulateSummary(&summary, FileReport{
		SizeBytes:  100,
		KeyCount:   2,
		BlockCount: 4,
		BlocksByType: map[string]int{
			"float":   2,
			"integer": 1,
			"ignored": 0,
		},
	}, TimeRange{})
	accumulateSummary(&summary, FileReport{
		SizeBytes:  50,
		KeyCount:   1,
		BlockCount: 2,
		BlocksByType: map[string]int{
			"float":   3,
			"ignored": -1,
		},
	}, TimeRange{})

	if got, want := summary.TotalSizeBytes, int64(150); got != want {
		t.Fatalf("total size = %d, want %d", got, want)
	}
	if got, want := summary.KeyCount, 3; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := summary.BlockCount, 6; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := summary.BlocksByType["float"], 5; got != want {
		t.Fatalf("float blocks = %d, want %d", got, want)
	}
	if got, want := summary.BlocksByType["integer"], 1; got != want {
		t.Fatalf("integer blocks = %d, want %d", got, want)
	}
	if got := summary.BlocksByType["ignored"]; got != 0 {
		t.Fatalf("ignored blocks = %d, want omitted", got)
	}
}

func TestReportResultOmitsEmptyReportLevelBlockTypes(t *testing.T) {
	report := Report{
		Files: []FileReport{{
			Path:       "00000001-0001-00000000.tssp",
			Format:     FormatTSSP,
			SizeBytes:  100,
			KeyCount:   2,
			BlockCount: 3,
		}},
		Summary: Summary{
			TotalSizeBytes: 100,
			KeyCount:       2,
			BlockCount:     3,
		},
		DecodePath: &DecodePathSummary{
			Mode:                  "tssp-file-set-location-cursor-ascending",
			BaselineDecodeBlocks:  3,
			OptimizedDecodeBlocks: 1,
		},
	}

	result := report.Result()
	if got, want := len(result.Table.Rows), 2; got != want {
		t.Fatalf("table rows = %d, want %d", got, want)
	}
	row := result.Table.Rows[1]
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "file")], "<file-set>"; got != want {
		t.Fatalf("aggregate file = %v, want %v", got, want)
	}
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "details")], "files=1"; got != want {
		t.Fatalf("aggregate details = %v, want %v", got, want)
	}
}

func TestReportResultOmitsReportLevelDecodePathRowWhenUnavailable(t *testing.T) {
	report := Report{
		Files: []FileReport{
			{Path: "00000001-0001-00000000.tssp", Format: FormatTSSP},
			{Path: "00000002-0001-00000000.tssp", Format: FormatTSSP},
		},
	}

	result := report.Result()
	if got, want := len(result.Table.Rows), len(report.Files); got != want {
		t.Fatalf("table rows = %d, want %d", got, want)
	}
	if got, want := result.Metadata.RowCount, len(report.Files); got != want {
		t.Fatalf("metadata row count = %d, want %d", got, want)
	}
	fileColumn := tableColumnIndex(t, result.Table.Columns, "file")
	for i, row := range result.Table.Rows {
		if got := row[fileColumn]; got == "<file-set>" {
			t.Fatalf("row %d file = %v, want no aggregate row", i, got)
		}
	}
}

func TestReportResultIncludesStructuredStorageDetails(t *testing.T) {
	report := Report{
		Files: []FileReport{
			{
				Path:   "L0-00000001.tsi",
				Format: FormatTSI,
				BlocksByType: map[string]int{
					"measurement": 2,
					"tag-key":     3,
					"ignored":     0,
				},
				Index: &IndexSummary{
					MeasurementCount:                2,
					DeletedMeasurementCount:         1,
					SeriesRefs:                      10,
					TagKeyCount:                     3,
					TagValueCount:                   4,
					SeriesIDSetCardinality:          7,
					TombstoneSeriesIDSetCardinality: 2,
				},
			},
			{
				Path:   "fields.idx",
				Format: FormatFieldsIndex,
				BlocksByType: map[string]int{
					"field":              4,
					"measurement-fields": 2,
					"ignored":            -1,
				},
				Fields: &FieldIndexSummary{
					MeasurementCount:   2,
					FieldCount:         4,
					FieldsByType:       map[string]int{"float": 1, "integer": 1, "string": 1, "unsigned": 1},
					ChangeCount:        3,
					AddFieldChanges:    2,
					DeleteMeasurements: 1,
				},
			},
			{
				Path:   "primary.meta",
				Format: FormatOpenGeminiPKMeta,
				BlocksByType: map[string]int{
					"primary-key-meta-block":    2,
					"primary-key-schema-column": 3,
				},
				PrimaryKey: &PrimaryKeySummary{
					Type:                    "opengemini-detached-primary-meta",
					ColumnCount:             3,
					RowCount:                4,
					DataSizeBytes:           120,
					ValidDataBytes:          112,
					CRCMismatches:           1,
					DataOutOfBoundsBlocks:   2,
					ColumnOutOfBoundsBlocks: 3,
					ColumnUnorderedBlocks:   1,
					BlockIDRangeSet:         true,
					MinBlockID:              10,
					MaxBlockID:              13,
				},
			},
			{
				Path:   "00000001-0001-00000001.content.bf",
				Format: FormatOpenGeminiBloom,
				BlocksByType: map[string]int{
					"bloom-filter-crc-mismatch": 1,
					"bloom-filter-line-block":   2,
				},
				SecondaryIndex: &SecondaryIndexSummary{
					Type:                  "opengemini-bloom-filter",
					Layout:                "attached-line-filter",
					Field:                 "content",
					BlockCount:            2,
					CRCMismatches:         1,
					TrailingBytes:         3,
					DataOutOfBoundsBlocks: 1,
				},
			},
			{
				Path:   "41_1_part",
				Format: FormatMergeset,
				BlocksByType: map[string]int{
					"mergeset-block":         2,
					"mergeset-metaindex-row": 1,
				},
				SecondaryIndex: &SecondaryIndexSummary{
					Type:            "opengemini-clv-text-mergeset",
					Layout:          "mergeset-namespace",
					ItemCount:       4,
					DocumentCount:   1,
					TermCount:       1,
					DictionaryCount: 1,
					PositionCount:   2,
				},
			},
		},
	}

	result := report.Result()
	detailsColumn := tableColumnIndex(t, result.Table.Columns, "details")
	wants := [][]string{
		{"block_types measurement:2 tag-key:3", "index measurements=2", "series_refs=10", "series_ids=7", "tags=3 values=4", "deleted measurements=1 tag_keys=0 tag_values=0 series_ids=2"},
		{"block_types field:4 measurement-fields:2", "fields measurements=2 fields=4", "types=float:1 integer:1 string:1 unsigned:1", "changes=3 adds=2 deletes=1"},
		{"block_types primary-key-meta-block:2 primary-key-schema-column:3", "primary_key type=opengemini-detached-primary-meta", "columns=3", "rows=4", "block_ids=10..13", "data=120 valid=112", "crc=1 data_oob=2 column_oob=3 column_unordered=1"},
		{"block_types bloom-filter-crc-mismatch:1 bloom-filter-line-block:2", "secondary_index type=opengemini-bloom-filter", "layout=attached-line-filter", "field=content", "blocks=2", "crc=1 trailing=3 data_oob=1"},
		{"block_types mergeset-block:2 mergeset-metaindex-row:1", "secondary_index type=opengemini-clv-text-mergeset", "layout=mergeset-namespace", "items=4", "documents=1", "terms=1", "dictionaries=1", "positions=2"},
	}
	for rowIndex, wantParts := range wants {
		details := result.Table.Rows[rowIndex][detailsColumn].(string)
		for _, want := range wantParts {
			if !strings.Contains(details, want) {
				t.Fatalf("row %d details = %q, want %q", rowIndex, details, want)
			}
		}
	}
}

func TestReportResultIncludesTSMCursorDecodePathSummary(t *testing.T) {
	report := Report{
		Files: []FileReport{{
			Path:   "000000001-000000001.tsm",
			Format: FormatTSM,
			DecodePath: &DecodePathSummary{
				Mode:                     "tsm-key-cursor-ascending",
				BaselineDecodeBlocks:     4,
				OptimizedDecodeBlocks:    2,
				SavedDecodeBytes:         128,
				BaselineCursorReadCalls:  2,
				OptimizedCursorReadCalls: 1,
				IteratorCostFiles:        2,
				IteratorCostBlocks:       3,
				IteratorCostBytes:        256,
				ValueOutputMismatches:    1,
			},
		}},
	}

	result := report.Result()
	row := result.Table.Rows[0]
	decodeText := row[tableColumnIndex(t, result.Table.Columns, "decode_path")].(string)
	for _, want := range []string{
		"tsm-key-cursor-ascending",
		"blocks 4->2",
		"saved_bytes 128",
		"cursor_reads 2->1",
		"iterator_cost files=2 blocks=3 bytes=256",
		"mismatches 1",
	} {
		if !strings.Contains(decodeText, want) {
			t.Fatalf("decode path summary %q does not contain %q", decodeText, want)
		}
	}
}

func TestReportResultIncludesMergesetTableSearchDecodePathSummary(t *testing.T) {
	report := Report{
		Files: []FileReport{{
			Path:   "3_1_part",
			Format: FormatMergeset,
			DecodePath: &DecodePathSummary{
				Mode:                         "mergeset-file-set-item-search-ascending",
				BaselineDecodeBlocks:         4,
				OptimizedDecodeBlocks:        2,
				SavedDecodeBlocks:            2,
				TableSearchSeekCalls:         4,
				TableSearchHeapCandidates:    3,
				TableSearchHeapInserts:       3,
				TableSearchHeapPops:          2,
				TableSearchCursorAdvances:    1,
				TableSearchCursorExhaustions: 2,
				TableSearchOutputValues:      2,
				TableSearchExactMisses:       1,
				DeduplicatedOutputValues:     1,
				DuplicateOutputValues:        1,
				OptimizedDecodeValues:        4,
				OptimizedOutputValues:        2,
				CursorWindowCount:            2,
				MergeWindowCount:             1,
				MergeWindowBlocks:            2,
				MergeWindowKeys:              1,
				Recommendations: []string{
					"deduplicated exact TableSearch results",
				},
			},
		}},
	}

	result := report.Result()
	row := result.Table.Rows[0]
	decodeText := row[tableColumnIndex(t, result.Table.Columns, "decode_path")].(string)
	for _, want := range []string{
		"mergeset-file-set-item-search-ascending",
		"blocks 4->2",
		"table_search_heap inserts=3 pops=2",
		"table_search_cursor advances=1 exhaustions=2",
		"table_search seeks=4 candidates=3 outputs=2 exact_misses=1",
		"dedup outputs=1 duplicates=1",
	} {
		if !strings.Contains(decodeText, want) {
			t.Fatalf("decode path summary %q does not contain %q", decodeText, want)
		}
	}
	advice := row[tableColumnIndex(t, result.Table.Columns, "advice")].(string)
	if !strings.Contains(advice, "deduplicated exact TableSearch results") {
		t.Fatalf("advice = %q, want mergeset TableSearch recommendation", advice)
	}
}

func TestDecodePathStringListJSONRoundTrip(t *testing.T) {
	type payload struct {
		MergeFiles DecodePathStringList `json:"merge_files,omitempty"`
	}
	original := payload{
		MergeFiles: newDecodePathStringList([]string{"part,one", "part-two"}),
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), `{"merge_files":["part,one","part-two"]}`; got != want {
		t.Fatalf("json = %s, want %s", got, want)
	}
	var decoded payload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.MergeFiles != original.MergeFiles {
		t.Fatalf("merge files = %q, want %q", decoded.MergeFiles, original.MergeFiles)
	}

	var fallback DecodePathStringList
	if err := json.Unmarshal([]byte(`"part\u0000one"`), &fallback); err != nil {
		t.Fatal(err)
	}
	fallbackData, err := json.Marshal(fallback)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(fallbackData), `["part\u0000one"]`; got != want {
		t.Fatalf("fallback json = %s, want %s", got, want)
	}
}

func tableColumnIndex(t *testing.T, columns []string, name string) int {
	t.Helper()
	for i, column := range columns {
		if column == name {
			return i
		}
	}
	t.Fatalf("missing table column %q in %v", name, columns)
	return -1
}
