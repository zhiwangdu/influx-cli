package storage

import (
	"strings"
	"testing"
)

func TestReportResultIncludesTSSPDecodePathSummary(t *testing.T) {
	report := Report{
		Files: []FileReport{{
			Path:   "00000001-0001-00000000.tssp",
			Format: FormatTSSP,
			DecodePath: &DecodePathSummary{
				Mode:                  "tssp-location-cursor-ascending",
				BaselineDecodeBlocks:  3,
				OptimizedDecodeBlocks: 1,
				SavedDecodeBlocks:     2,
				BaselineDecodeBytes:   288,
				OptimizedDecodeBytes:  96,
				SavedDecodeBytes:      192,
				BaselineReadSegments:  3,
				OptimizedReadSegments: 1,
				SavedReadSegments:     2,
				IteratorCostFiles:     1,
				IteratorCostBlocks:    3,
				IteratorCostBytes:     273,
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
		"saved_bytes 192",
		"segments 3->1",
		"iterator_cost files=1 blocks=3 bytes=273",
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
