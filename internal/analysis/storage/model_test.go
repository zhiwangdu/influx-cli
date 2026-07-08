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
				Mode:                         "tssp-location-cursor-ascending",
				QueryRange:                   TimeRange{Min: 100, Max: 200, Set: true},
				CursorSeekTime:               100,
				KeyFilterApplied:             true,
				BaselineDecodeBlocks:         3,
				OptimizedDecodeBlocks:        1,
				SavedDecodeBlocks:            2,
				QueryKeys:                    []string{"cpu,host=a value", "cpu,host=b value"},
				MatchedKeys:                  []string{"cpu,host=a value"},
				MissingKeys:                  []string{"cpu,host=b value"},
				QuerySeriesIDs:               []uint64{7, 9},
				MatchedSeriesIDs:             []uint64{7},
				MissingSeriesIDs:             []uint64{9},
				QueryMetaIndexIDs:            []uint64{11, 12},
				MatchedMetaIndexIDs:          []uint64{11},
				MissingMetaIndexIDs:          []uint64{12},
				QueryColumns:                 []string{"missing", "value"},
				MatchedColumns:               []string{"value"},
				MissingColumns:               []string{"missing"},
				LocationBlocksByType:         map[string]int{"chunk-meta": 2, "meta-index": 1},
				DecodeBlocksByType:           map[string]int{"chunk-meta": 1},
				BaselineDecodeBytes:          288,
				OptimizedDecodeBytes:         96,
				SavedDecodeBytes:             192,
				BaselineDecodeValues:         9,
				OptimizedDecodeValues:        3,
				SavedDecodeValues:            6,
				BaselineReadSegments:         3,
				OptimizedReadSegments:        1,
				SavedReadSegments:            2,
				BaselineOutputValues:         6,
				OptimizedOutputValues:        2,
				BaselineCursorReadCalls:      3,
				OptimizedCursorReadCalls:     1,
				BaselineReadAtCalls:          6,
				OptimizedReadAtCalls:         2,
				SavedReadAtCalls:             4,
				IteratorCostFiles:            1,
				IteratorCostBlocks:           3,
				IteratorCostBytes:            273,
				BaselineValueOutputPoints:    6,
				OptimizedValueOutputPoints:   2,
				ComparedValueOutputPoints:    2,
				ValueOutputUnavailableBlocks: 1,
				ValueOutputMismatches:        1,
				BaselineCursorOutputPoints:   6,
				OptimizedCursorOutputPoints:  2,
				CursorOutputSamples: []DecodePathCursorOutput{
					{Key: "sid:7/value", Time: 1, Type: "float", OptimizedValue: "1.25"},
					{Key: "sid:9/value", Time: 2, Type: "float", OptimizedValue: "2.5"},
				},
				CursorFinalOutputSamples: []DecodePathCursorOutput{
					{Key: "sid:7/value", Time: 1, Type: "float", OptimizedValue: "1.25"},
				},
				CursorWindowCount:         2,
				MergeWindowCount:          1,
				MergeWindowBlocks:         2,
				MergeWindowKeys:           1,
				Amplification:             2.5,
				LocationBlocks:            3,
				FilteredDecodeBlocks:      1,
				SkippedByKeyBlocks:        1,
				SkippedByProjectionBlocks: 1,
				SkippedBeforeSeekBlocks:   1,
				SkippedAfterRangeBlocks:   1,
				FullyTombstonedBlocks:     1,
				Samples: []DecodePathBlockDecision{
					{
						Key:    "secret-series-key",
						Type:   "float",
						Reason: "range-match",
						OptimizedReadAtRanges: []DecodePathReadAtRange{
							{Segment: 0, Column: "secret-column", MinTime: 100, MaxTime: 150, Offset: 1024, SizeBytes: 24},
							{Segment: 0, Column: "time", MinTime: 100, MaxTime: 150, Offset: 1048, SizeBytes: 16},
						},
					},
				},
				CursorWindows: []DecodePathCursorWindow{
					{Key: "secret-window-key", MinTime: 1, MaxTime: 2, LocationBlocks: 2, DecodedBlocks: 1},
				},
				CursorExecutionSamples: []DecodePathCursorStep{
					{Step: 1, Type: "cursor", Action: "push", Key: "secret-cursor-key", CandidateValue: "secret-candidate"},
				},
				FilterExecutionSamples: []DecodePathCursorStep{
					{Step: 2, Type: "filter", Action: "match", Key: "secret-filter-key"},
				},
				DataBlockProbeBlocks:        4,
				DataBlockProbeBytes:         256,
				DataBlockProbeValidBlocks:   3,
				DataBlockProbeFailures:      4,
				DataBlockProbeCRCMismatches: 1,
				DataBlockProbeShortBlocks:   1,
				DataBlockProbeUnknownTypes:  1,
				DataBlockProbeReadErrors:    1,
				DataBlockProbeFailureReasons: map[string]int{
					"segment_overlap_data_crc_unavailable":    1,
					"segment_overlap_data_header_unavailable": 1,
					"segment_overlap_data_read_unavailable":   1,
				},
				DataBlockProbeRowCountBlocks: 3,
				DataBlockProbeRowUnknowns:    1,
				DataBlockProbeRowMismatches:  1,
				DataBlockProbeOutputPoints:   2,
				DataBlockProbeTypes: map[string]int{
					"float-full":   2,
					"integer-full": 1,
				},
				DataBlockProbeValueBlocks:   2,
				DataBlockProbeValueUnknowns: 1,
				DataBlockProbeValueReasons: map[string]int{
					"float-full-codec-7": 1,
				},
				DataBlockProbeNullValues:          3,
				DataBlockProbeRecordRows:          5,
				DataBlockProbeRecordSamples:       1,
				DataBlockProbeRecordOutputs:       2,
				DataBlockProbeRecordRangeRejects:  1,
				DataBlockProbeRecordFilterRejects: 2,
				QueryFields:                       []FieldFilter{{Key: "missing", Value: "x"}, {Key: "value", Op: ">", Value: "1.0"}},
				MatchedFields:                     []FieldFilter{{Key: "value", Op: ">", Value: "1.0"}},
				MissingFields:                     []FieldFilter{{Key: "missing", Value: "x"}},
				QueryAnyFields:                    []FieldFilter{{Key: "status", Value: "false"}, {Key: "zone", Value: "west"}},
				MatchedAnyFields:                  []FieldFilter{{Key: "status", Value: "false"}},
				MissingAnyFields:                  []FieldFilter{{Key: "zone", Value: "west"}},
				QueryNoneFields:                   []FieldFilter{{Key: "deleted", Value: "true"}},
				MatchedNoneFields:                 []FieldFilter{{Key: "deleted", Value: "true"}},
				DataBlockProbeFilterRows:          5,
				DataBlockProbeFilterMatches:       3,
				DataBlockProbeFilterRejects:       2,
				DataBlockProbeRangeRows:           7,
				DataBlockProbeRangeMatches:        5,
				DataBlockProbeRangeRejects:        2,
				DataBlockProbeFilterSkips:         4,
				DataBlockProbeAnySkips:            4,
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
		"query range=1970-01-01T00:00:00.0000001Z..1970-01-01T00:00:00.0000002Z seek=1970-01-01T00:00:00.0000001Z target_filter=true",
		"blocks 3->1 saved=2",
		"block_filter locations=3 decoded=1 skipped_key=1 skipped_projection=1 skipped_before=1 skipped_after=1 tombstoned=1",
		"keys=2/1/1 series_ids=2/1/1 meta_index_ids=2/1/1 columns=2/1/1",
		"location_block_types chunk-meta:2 meta-index:1",
		"decode_block_types chunk-meta:1",
		"decode_bytes 288->96 saved=192",
		"values decode=9->3 saved=6 output=6->2",
		"segments 3->1 saved=2",
		"cursor_reads 3->1",
		"read_at calls 6->2 saved=4",
		"read_at_ranges ranges=2 sampled_blocks=1 bytes=40 columns=2",
		"iterator_cost files=1 blocks=3 bytes=273",
		"value_output points=6->2 compared=2 unavailable_blocks=1 mismatches=1",
		"cursor_output points=6->2 samples=2 final_samples=1",
		"execution windows cursor=2 sampled=1 merge=1 merge_blocks=2 merge_keys=1 samples decisions=1 cursor_steps=1 filter_steps=1 amplification=2.50x",
		"data_probe blocks=4 bytes=256 valid=3 failures=4 crc_mismatches=1 short=1 unknown_types=1 read_errors=1 row_blocks=3 row_unknowns=1 row_mismatches=1 output_points=2 value_blocks=2 value_unknowns=1 nulls=3 record_samples=1 record_outputs=2",
		"data_probe_failure_reasons segment_overlap_data_crc_unavailable:1 segment_overlap_data_header_unavailable:1 segment_overlap_data_read_unavailable:1",
		"data_probe_types float-full:2 integer-full:1",
		"data_probe_value_unknown_reasons float-full-codec-7:1",
		"field_filters required=2 matched=1 missing=1 any=2 any_matched=1 any_missing=1 none=1 none_matched=1 none_missing=0",
		"record_filter rows=5 outputs=2 range_rejects=1 filter_rejects=2 samples=1",
		"field_filter rows=5 matches=3 rejects=2",
		"field_filter_ops =:3 between:2 not-between:1",
		"row_range rows=7 matches=5 rejects=2",
		"field_filter_short_circuit skips=4 required=0 any=4 none=0",
	} {
		if !strings.Contains(decodeText, want) {
			t.Fatalf("decode path summary %q does not contain %q", decodeText, want)
		}
	}
	if strings.Contains(decodeText, "secret") {
		t.Fatalf("decode path summary %q leaks sampled execution details", decodeText)
	}
	advice := row[tableColumnIndex(t, result.Table.Columns, "advice")].(string)
	if !strings.Contains(advice, "read 1 overlapping TSSP segment") {
		t.Fatalf("advice = %q, want TSSP segment recommendation", advice)
	}
}

func TestDecodePathTextOmitsEmptyOutputSummaries(t *testing.T) {
	text := decodePathText(&DecodePathSummary{})
	for _, notWant := range []string{"value_output", "cursor_output", "block_filter", "decode_bytes", "values", "query "} {
		if strings.Contains(text, notWant) {
			t.Fatalf("decode path text = %q, want no %s segment", text, notWant)
		}
	}
}

func TestDecodePathTextIncludesQueryContext(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		QueryRange:       TimeRange{Min: 100, Max: 200, Set: true},
		CursorSeekTime:   100,
		KeyFilterApplied: true,
	})
	want := "query range=1970-01-01T00:00:00.0000001Z..1970-01-01T00:00:00.0000002Z seek=1970-01-01T00:00:00.0000001Z target_filter=true"
	if !strings.Contains(text, want) {
		t.Fatalf("decode path text = %q, want %q", text, want)
	}
}

func TestDecodePathTextIncludesSparseQueryContext(t *testing.T) {
	for _, tc := range []struct {
		name     string
		summary  DecodePathSummary
		want     string
		notWants []string
	}{
		{
			name: "range-only",
			summary: DecodePathSummary{
				QueryRange: TimeRange{Min: 100, Max: 200, Set: true},
			},
			want:     "query range=1970-01-01T00:00:00.0000001Z..1970-01-01T00:00:00.0000002Z",
			notWants: []string{"seek=", "target_filter"},
		},
		{
			name: "wal-range-has-no-cursor-seek",
			summary: DecodePathSummary{
				Mode:       "wal-replay-filter",
				QueryRange: TimeRange{Min: 100, Max: 200, Set: true},
			},
			want:     "query range=1970-01-01T00:00:00.0000001Z..1970-01-01T00:00:00.0000002Z",
			notWants: []string{"seek=", "target_filter"},
		},
		{
			name: "epoch-cursor-seek",
			summary: DecodePathSummary{
				Mode:       "tsm-key-cursor-ascending",
				QueryRange: TimeRange{Min: 0, Max: 200, Set: true},
			},
			want:     "query range=1970-01-01T00:00:00Z..1970-01-01T00:00:00.0000002Z seek=1970-01-01T00:00:00Z",
			notWants: []string{"target_filter"},
		},
		{
			name: "recorded-seek",
			summary: DecodePathSummary{
				QueryRange:     TimeRange{Min: 100, Max: 200, Set: true},
				CursorSeekTime: 150,
			},
			want:     "query range=1970-01-01T00:00:00.0000001Z..1970-01-01T00:00:00.0000002Z seek=1970-01-01T00:00:00.00000015Z",
			notWants: []string{"target_filter"},
		},
		{
			name: "descending-epoch-cursor-seek",
			summary: DecodePathSummary{
				Mode:       "tssp-location-cursor-descending",
				QueryRange: TimeRange{Min: -100, Max: 0, Set: true},
			},
			want:     "query range=1969-12-31T23:59:59.9999999Z..1970-01-01T00:00:00Z seek=1970-01-01T00:00:00Z",
			notWants: []string{"target_filter"},
		},
		{
			name: "seek-only",
			summary: DecodePathSummary{
				CursorSeekTime: 200,
			},
			want:     "query seek=1970-01-01T00:00:00.0000002Z",
			notWants: []string{"range=", "target_filter"},
		},
		{
			name: "target-filter-only",
			summary: DecodePathSummary{
				KeyFilterApplied: true,
			},
			want:     "query target_filter=true",
			notWants: []string{"range=", "seek="},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := decodePathText(&tc.summary)
			if !strings.Contains(text, tc.want) {
				t.Fatalf("decode path text = %q, want %q", text, tc.want)
			}
			for _, notWant := range tc.notWants {
				if strings.Contains(text, notWant) {
					t.Fatalf("decode path text = %q, want no %q", text, notWant)
				}
			}
		})
	}
}

func TestDecodePathTextIncludesBlockFilterCounts(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		LocationBlocks:            4,
		FilteredDecodeBlocks:      2,
		SkippedByKeyBlocks:        1,
		SkippedByProjectionBlocks: 1,
		SkippedBeforeSeekBlocks:   1,
		SkippedAfterRangeBlocks:   3,
		FullyTombstonedBlocks:     1,
		BaselineDecodeBlocks:      4,
		OptimizedDecodeBlocks:     2,
	})
	want := "block_filter locations=4 decoded=2 skipped_key=1 skipped_projection=1 skipped_before=1 skipped_after=3 tombstoned=1"
	if !strings.Contains(text, want) {
		t.Fatalf("decode path text = %q, want %q", text, want)
	}
}

func TestDecodePathTextIncludesSparseBlockFilterCounts(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		SkippedByProjectionBlocks: 2,
		FullyTombstonedBlocks:     1,
	})
	want := "block_filter skipped_projection=2 tombstoned=1"
	if !strings.Contains(text, want) {
		t.Fatalf("decode path text = %q, want %q", text, want)
	}
	for _, notWant := range []string{"locations=", "decoded=", "skipped_key=", "skipped_before=", "skipped_after="} {
		if strings.Contains(text, notWant) {
			t.Fatalf("decode path text = %q, want no %q", text, notWant)
		}
	}
}

func TestDecodePathTextIncludesSavedDeltaCounts(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		BaselineDecodeBlocks:  4,
		OptimizedDecodeBlocks: 2,
		SavedDecodeBlocks:     2,
		BaselineReadSegments:  6,
		OptimizedReadSegments: 3,
		SavedReadSegments:     3,
		BaselineReadAtCalls:   5,
		OptimizedReadAtCalls:  2,
		SavedReadAtCalls:      3,
	})
	for _, want := range []string{
		"blocks 4->2 saved=2",
		"segments 6->3 saved=3",
		"read_at calls 5->2 saved=3",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("decode path text = %q, want %q", text, want)
		}
	}
}

func TestDecodePathTextIncludesSparseSavedDeltaCounts(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		SavedDecodeBlocks: 1,
		SavedReadSegments: 2,
		SavedReadAtCalls:  3,
	})
	for _, want := range []string{
		"blocks 0->0 saved=1",
		"segments 0->0 saved=2",
		"read_at calls 0->0 saved=3",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("decode path text = %q, want %q", text, want)
		}
	}
}

func TestDecodePathTextIncludesDecodeByteCounts(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		BaselineDecodeBytes:  288,
		OptimizedDecodeBytes: 96,
		SavedDecodeBytes:     192,
	})
	want := "decode_bytes 288->96 saved=192"
	if !strings.Contains(text, want) {
		t.Fatalf("decode path text = %q, want %q", text, want)
	}
}

func TestDecodePathTextIncludesSparseDecodeByteCounts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		summary  DecodePathSummary
		want     string
		notWants []string
	}{
		{
			name: "bytes-only",
			summary: DecodePathSummary{
				BaselineDecodeBytes:  288,
				OptimizedDecodeBytes: 96,
			},
			want:     "decode_bytes 288->96",
			notWants: []string{"saved="},
		},
		{
			name: "saved-with-baseline",
			summary: DecodePathSummary{
				BaselineDecodeBytes: 192,
				SavedDecodeBytes:    192,
			},
			want: "decode_bytes 192->0 saved=192",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := decodePathText(&tc.summary)
			if !strings.Contains(text, tc.want) {
				t.Fatalf("decode path text = %q, want %q", text, tc.want)
			}
			for _, notWant := range tc.notWants {
				if strings.Contains(text, notWant) {
					t.Fatalf("decode path text = %q, want no %q", text, notWant)
				}
			}
		})
	}
}

func TestDecodePathTextIncludesDecodeValueCounts(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		BaselineDecodeValues:  9,
		OptimizedDecodeValues: 4,
		SavedDecodeValues:     5,
		BaselineOutputValues:  7,
		OptimizedOutputValues: 3,
	})
	want := "values decode=9->4 saved=5 output=7->3"
	if !strings.Contains(text, want) {
		t.Fatalf("decode path text = %q, want %q", text, want)
	}
}

func TestDecodePathTextIncludesSparseDecodeValueCounts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		summary  DecodePathSummary
		want     string
		notWants []string
	}{
		{
			name: "saved-only",
			summary: DecodePathSummary{
				SavedDecodeValues: 3,
			},
			want:     "values decode=0->0 saved=3",
			notWants: []string{"output="},
		},
		{
			name: "output-only",
			summary: DecodePathSummary{
				OptimizedOutputValues: 2,
			},
			want:     "values output=0->2",
			notWants: []string{"decode="},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := decodePathText(&tc.summary)
			if !strings.Contains(text, tc.want) {
				t.Fatalf("decode path text = %q, want %q", text, tc.want)
			}
			for _, notWant := range tc.notWants {
				if strings.Contains(text, notWant) {
					t.Fatalf("decode path text = %q, want no %q", text, notWant)
				}
			}
		})
	}
}

func TestDecodePathTextIncludesValueOutputSparseCounters(t *testing.T) {
	for _, tc := range []struct {
		name     string
		summary  DecodePathSummary
		want     string
		notWants []string
	}{
		{
			name: "points-only",
			summary: DecodePathSummary{
				BaselineValueOutputPoints:  6,
				OptimizedValueOutputPoints: 4,
			},
			want:     "value_output points=6->4",
			notWants: []string{"compared=", "unavailable_blocks"},
		},
		{
			name: "compared-only",
			summary: DecodePathSummary{
				ComparedValueOutputPoints: 2,
			},
			want:     "value_output points=0->0 compared=2",
			notWants: []string{"unavailable_blocks"},
		},
		{
			name: "unavailable-only",
			summary: DecodePathSummary{
				ValueOutputUnavailableBlocks: 1,
			},
			want:     "value_output points=0->0 unavailable_blocks=1",
			notWants: []string{"compared="},
		},
		{
			name: "mismatch-only",
			summary: DecodePathSummary{
				ValueOutputMismatches: 1,
			},
			want:     "value_output points=0->0 mismatches=1",
			notWants: []string{"compared=", "unavailable_blocks"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := decodePathText(&tc.summary)
			if !strings.Contains(text, tc.want) {
				t.Fatalf("decode path text = %q, want %q", text, tc.want)
			}
			for _, notWant := range tc.notWants {
				if strings.Contains(text, notWant) {
					t.Fatalf("decode path text = %q, want no %q", text, notWant)
				}
			}
		})
	}
}

func TestDecodePathTextIncludesCursorOutputPointsWithoutSamples(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		BaselineCursorOutputPoints:  6,
		OptimizedCursorOutputPoints: 4,
	})
	if !strings.Contains(text, "cursor_output points=6->4") {
		t.Fatalf("decode path text = %q, want points-only cursor output summary", text)
	}
	if strings.Contains(text, "samples=") {
		t.Fatalf("decode path text = %q, want no cursor output samples segment", text)
	}
}

func TestDecodePathTextIncludesCursorOutputSamplesWithoutPointCounts(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		CursorOutputSamples: []DecodePathCursorOutput{
			{Key: "sid:7/value", Time: 1, Type: "float", OptimizedValue: "1.25"},
		},
		CursorFinalOutputSamples: []DecodePathCursorOutput{
			{Key: "sid:7/value", Time: 1, Type: "float", OptimizedValue: "1.25"},
		},
	})
	if !strings.Contains(text, "cursor_output samples=1 final_samples=1") {
		t.Fatalf("decode path text = %q, want samples-only cursor output summary", text)
	}
	if strings.Contains(text, "points=") {
		t.Fatalf("decode path text = %q, want no cursor output points segment", text)
	}
}

func TestDecodePathTextIncludesExecutionDiagnosticsCountsOnly(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		CursorWindowCount: 3,
		MergeWindowCount:  2,
		MergeWindowBlocks: 5,
		MergeWindowKeys:   4,
		Amplification:     1.75,
		Samples: []DecodePathBlockDecision{
			{Key: "secret-decision-key", Type: "float", Reason: "range-match"},
		},
		CursorWindows: []DecodePathCursorWindow{
			{Key: "secret-window-key", MinTime: 1, MaxTime: 2, LocationBlocks: 3, DecodedBlocks: 1},
			{Key: "another-secret-window-key", MinTime: 3, MaxTime: 4, LocationBlocks: 2, DecodedBlocks: 1},
		},
		CursorExecutionSamples: []DecodePathCursorStep{
			{Step: 1, Type: "cursor", Action: "push", Key: "secret-cursor-key", CandidateValue: "secret-candidate"},
			{Step: 2, Type: "cursor", Action: "advance", Key: "another-secret-cursor-key"},
		},
		RecordExecutionSamples: []DecodePathCursorStep{
			{Step: 3, Type: "record", Action: "record_row_output", Key: "secret-record-key", CandidateValue: "secret-record"},
			{Step: 4, Type: "record", Action: "record_row_filter_reject", Key: "another-secret-record-key", CandidateValue: "another-secret-record"},
		},
		FilterExecutionSamples: []DecodePathCursorStep{
			{Step: 5, Type: "filter", Action: "match", Key: "secret-filter-key"},
		},
	})
	want := "execution windows cursor=3 sampled=2 merge=2 merge_blocks=5 merge_keys=4 samples decisions=1 cursor_steps=2 record_steps=2 record_actions record_row_filter_reject:1 record_row_output:1 filter_steps=1 amplification=1.75x"
	if !strings.Contains(text, want) {
		t.Fatalf("decode path text = %q, want %q", text, want)
	}
	for _, notWant := range []string{"secret", "another-secret"} {
		if strings.Contains(text, notWant) {
			t.Fatalf("decode path text = %q, want no sampled detail %q", text, notWant)
		}
	}
}

func TestDecodePathTextUsesRecordExecutionActionCounts(t *testing.T) {
	summary := DecodePathSummary{
		RecordExecutionActions: map[string]int{
			"record_row_output":        2,
			"record_row_filter_reject": 1,
		},
	}
	text := decodePathText(&summary)
	want := "execution samples record_actions record_row_filter_reject:1 record_row_output:2"
	if !strings.Contains(text, want) {
		t.Fatalf("decode path text = %q, want %q", text, want)
	}

	data, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		RecordExecutionActions map[string]int `json:"record_execution_action_counts"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if got, want := len(decoded.RecordExecutionActions), 2; got != want {
		t.Fatalf("record execution action count entries = %d, want %d: %s", got, want, data)
	}
	if got, want := decoded.RecordExecutionActions["record_row_output"], 2; got != want {
		t.Fatalf("record_row_output count = %d, want %d", got, want)
	}
	if got, want := decoded.RecordExecutionActions["record_row_filter_reject"], 1; got != want {
		t.Fatalf("record_row_filter_reject count = %d, want %d", got, want)
	}
}

func TestDecodePathTextIncludesSparseExecutionDiagnostics(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		CursorWindows: []DecodePathCursorWindow{
			{Key: "secret-window-key", MinTime: 1, MaxTime: 2, LocationBlocks: 3, DecodedBlocks: 1},
		},
		MergeWindowBlocks: 5,
		Amplification:     0.004,
	})
	want := "execution windows sampled=1 merge_blocks=5"
	if !strings.Contains(text, want) {
		t.Fatalf("decode path text = %q, want %q", text, want)
	}
	for _, notWant := range []string{"cursor=", "merge=", "merge_keys=", "amplification=0.00x", "secret-window-key"} {
		if strings.Contains(text, notWant) {
			t.Fatalf("decode path text = %q, want no %q", text, notWant)
		}
	}
}

func TestDecodePathTextOmitsEmptyExecutionDiagnostics(t *testing.T) {
	text := decodePathText(&DecodePathSummary{})
	if strings.Contains(text, "execution") {
		t.Fatalf("decode path text = %q, want no execution diagnostics segment", text)
	}
}

func TestDecodePathTextOmitsEmptyQueryTargetSummary(t *testing.T) {
	text := decodePathText(&DecodePathSummary{})
	for _, notWant := range []string{"keys=", "series_ids=", "meta_index_ids=", "columns="} {
		if strings.Contains(text, notWant) {
			t.Fatalf("decode path text = %q, want no %s segment", text, notWant)
		}
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

func TestDecodePathTextOmitsEmptyFieldFilterSummary(t *testing.T) {
	text := decodePathText(&DecodePathSummary{})
	if strings.Contains(text, "field_filters") {
		t.Fatalf("decode path text = %q, want no field_filters segment", text)
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

func TestDecodePathTextOmitsEmptyDataProbeCountMaps(t *testing.T) {
	text := decodePathText(&DecodePathSummary{
		DataBlockProbeFailureReasons: map[string]int{"segment_overlap_data_header_unavailable": 0},
		DataBlockProbeTypes:          map[string]int{"integer-full": 0},
		DataBlockProbeValueReasons:   map[string]int{"float-full-codec-7": -1},
	})
	for _, notWant := range []string{"data_probe_failure_reasons", "data_probe_types", "data_probe_value_unknown_reasons"} {
		if strings.Contains(text, notWant) {
			t.Fatalf("decode path text = %q, want no %s segment", text, notWant)
		}
	}
}

func TestTombstoneTextIncludesQueryOverlapRanges(t *testing.T) {
	text := tombstoneText(TombstoneSummary{
		Exists:             true,
		SizeBytes:          128,
		RangeCount:         3,
		QueryOverlapRanges: 1,
		AffectedBlocks:     2,
	})
	want := "yes (128 bytes, 3 ranges, query_ranges=1, 2 blocks)"
	if text != want {
		t.Fatalf("tombstone text = %q, want %q", text, want)
	}
}

func TestTombstoneTextOmitsZeroQueryOverlapRanges(t *testing.T) {
	text := tombstoneText(TombstoneSummary{
		Exists:     true,
		SizeBytes:  64,
		RangeCount: 2,
	})
	want := "yes (64 bytes, 2 ranges)"
	if text != want {
		t.Fatalf("tombstone text = %q, want %q", text, want)
	}
	if strings.Contains(text, "query_ranges=0") {
		t.Fatalf("tombstone text = %q, want no zero query range count", text)
	}
}

func TestTombstoneTextEmptyAndBytesOnly(t *testing.T) {
	if got := tombstoneText(TombstoneSummary{}); got != "" {
		t.Fatalf("absent tombstone text = %q, want empty", got)
	}
	got := tombstoneText(TombstoneSummary{
		Exists:    true,
		SizeBytes: 32,
	})
	want := "yes (32 bytes)"
	if got != want {
		t.Fatalf("bytes-only tombstone text = %q, want %q", got, want)
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
				Notices:            []string{"secret file-level diagnostic"},
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
			TotalSizeBytes:              300,
			KeyCount:                    6,
			BlockCount:                  8,
			BlocksByType:                map[string]int{"chunk-meta": 2, "data": 6},
			QueryOverlapFiles:           2,
			QueryOverlapBlocks:          3,
			TombstoneFiles:              1,
			TombstoneSizeBytes:          64,
			TombstoneRanges:             3,
			TombstoneQueryOverlapRanges: 1,
			TombstoneAffectedBlocks:     2,
		},
		DecodePath: &DecodePathSummary{
			Mode:                       "tssp-file-set-location-cursor-ascending",
			BaselineDecodeBlocks:       8,
			OptimizedDecodeBlocks:      3,
			SavedDecodeBlocks:          5,
			LocationBlocksByType:       map[string]int{"chunk-meta": 6, "meta-index": 2},
			DecodeBlocksByType:         map[string]int{"chunk-meta": 3},
			DataBlockProbeBlocks:       6,
			DataBlockProbeValidBlocks:  5,
			DataBlockProbeFailures:     3,
			DataBlockProbeShortBlocks:  1,
			DataBlockProbeUnknownTypes: 1,
			DataBlockProbeReadErrors:   1,
			DataBlockProbeFailureReasons: map[string]int{
				"segment_overlap_data_header_unavailable": 2,
				"segment_overlap_data_read_unavailable":   1,
			},
			DataBlockProbeRowCountBlocks: 4,
			DataBlockProbeRowUnknowns:    1,
			DataBlockProbeRowMismatches:  1,
			DataBlockProbeOutputPoints:   5,
			DataBlockProbeTypes:          map[string]int{"float-full": 2, "integer-full": 4},
			DataBlockProbeValueReasons: map[string]int{
				"float-full-codec-7": 2,
			},
			Recommendations: []string{
				"final TSSP file-set output samples include locally deduplicated rows",
			},
		},
		Notices: []string{
			"00000001-0001-00000000.tssp: secret file-level diagnostic",
			"00000002-0001-00000000.tssp: secret aggregate diagnostic",
		},
	}

	result := report.Result()
	if got, want := len(result.Table.Rows), 3; got != want {
		t.Fatalf("table rows = %d, want %d", got, want)
	}
	if got, want := result.Metadata.RowCount, 3; got != want {
		t.Fatalf("metadata row count = %d, want %d", got, want)
	}
	fileDetails := result.Table.Rows[0][tableColumnIndex(t, result.Table.Columns, "details")].(string)
	if !strings.Contains(fileDetails, "notices=1") {
		t.Fatalf("file details = %q, want notice count", fileDetails)
	}
	if strings.Contains(fileDetails, "secret") {
		t.Fatalf("file details = %q, want count-only notice summary", fileDetails)
	}
	secondFileDetails := result.Table.Rows[1][tableColumnIndex(t, result.Table.Columns, "details")].(string)
	if strings.Contains(secondFileDetails, "notices=") {
		t.Fatalf("second file details = %q, want no zero notice summary", secondFileDetails)
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
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "tombstone")], "1 files, 64 bytes, 3 ranges, query_ranges=1, 2 blocks"; got != want {
		t.Fatalf("aggregate tombstone = %v, want %v", got, want)
	}
	if got, want := row[tableColumnIndex(t, result.Table.Columns, "details")], "files=2; query_files=2; tombstone_files=1; tombstone_bytes=64; tombstone_ranges=3; tombstone_query_ranges=1; tombstone_blocks=2; notices=2; block_types chunk-meta:2 data:6"; got != want {
		t.Fatalf("aggregate details = %v, want %v", got, want)
	}
	decodeText := row[tableColumnIndex(t, result.Table.Columns, "decode_path")].(string)
	for _, want := range []string{
		"tssp-file-set-location-cursor-ascending",
		"blocks 8->3 saved=5",
		"location_block_types chunk-meta:6 meta-index:2",
		"decode_block_types chunk-meta:3",
		"data_probe blocks=6 bytes=0 valid=5 failures=3 crc_mismatches=0 short=1 unknown_types=1 read_errors=1 row_blocks=4 row_unknowns=1 row_mismatches=1 output_points=5 value_blocks=0 value_unknowns=0 nulls=0 record_samples=0 record_outputs=0",
		"data_probe_failure_reasons segment_overlap_data_header_unavailable:2 segment_overlap_data_read_unavailable:1",
		"data_probe_types float-full:2 integer-full:4",
		"data_probe_value_unknown_reasons float-full-codec-7:2",
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

func TestReportMarkdownIncludesCountOrientedDiagnostics(t *testing.T) {
	report := Report{
		Files: []FileReport{
			{
				Path:               "/private/shard/00000001-0001-00000000.tssp",
				Format:             FormatTSSP,
				SizeBytes:          100,
				MinTime:            10,
				MaxTime:            20,
				KeyCount:           2,
				BlockCount:         3,
				QueryOverlapBlocks: 1,
				Tombstones: TombstoneSummary{
					Exists:             true,
					SizeBytes:          32,
					RangeCount:         2,
					QueryOverlapRanges: 1,
					AffectedBlocks:     1,
				},
				BlocksByType: map[string]int{"data": 2, "chunk-meta": 1},
				Notices:      []string{"private offset diagnostic"},
			},
		},
		Summary: Summary{
			FileCount:                   1,
			TotalSizeBytes:              100,
			KeyCount:                    2,
			BlockCount:                  3,
			BlocksByType:                map[string]int{"data": 2, "chunk-meta": 1},
			QueryOverlapFiles:           1,
			QueryOverlapBlocks:          1,
			TombstoneFiles:              1,
			TombstoneSizeBytes:          32,
			TombstoneRanges:             2,
			TombstoneQueryOverlapRanges: 1,
			TombstoneAffectedBlocks:     1,
		},
		DecodePath: &DecodePathSummary{
			Mode:                  "tssp-file-set-location-cursor-ascending",
			BaselineDecodeBlocks:  3,
			OptimizedDecodeBlocks: 1,
			SavedDecodeBlocks:     2,
			Recommendations:       []string{"copyable issue diagnostic"},
		},
		Notices: []string{"/private/shard/00000001-0001-00000000.tssp: private offset diagnostic"},
	}

	out := report.Markdown()
	for _, want := range []string{
		"# Storage Analyzer Report",
		"| files | 1 |",
		"| query_overlap_blocks | 1 |",
		"| tombstone_query_overlap_ranges | 1 |",
		"| notices | 1 |",
		"## Block Types",
		"| chunk-meta | 1 |",
		"## File-Set Decode Path",
		"- mode: `tssp-file-set-location-cursor-ascending`",
		"blocks 3->1 saved=2",
		"copyable issue diagnostic",
		"## Files",
		"| file-1 | tssp | 100 |",
		"yes (32 bytes, 2 ranges, query_ranges=1, 1 blocks)",
		"notices=1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("markdown report = %q, want %q", out, want)
		}
	}
	for _, notWant := range []string{"/private/shard", "private offset diagnostic"} {
		if strings.Contains(out, notWant) {
			t.Fatalf("markdown report = %q, want no raw diagnostic %q", out, notWant)
		}
	}
}

func TestReportMarkdownHandlesEmptyAndStableFileLabels(t *testing.T) {
	empty := Report{}.Markdown()
	if !strings.Contains(empty, "| files | 0 |") {
		t.Fatalf("empty markdown report = %q, want zero file summary", empty)
	}
	if strings.Contains(empty, "## Files") {
		t.Fatalf("empty markdown report = %q, want no files section", empty)
	}

	report := Report{
		Files: []FileReport{
			{
				Path:       "/private/a.tsm",
				Format:     FormatTSM,
				SizeBytes:  10,
				KeyCount:   1,
				BlockCount: 2,
			},
			{
				Path:       "/private/b.tssp",
				Format:     FormatTSSP,
				SizeBytes:  20,
				KeyCount:   3,
				BlockCount: 4,
			},
		},
		Summary: Summary{
			FileCount:      2,
			TotalSizeBytes: 30,
			KeyCount:       4,
			BlockCount:     6,
		},
	}

	out := report.Markdown()
	first := strings.Index(out, "| file-1 | tsm | 10 |")
	second := strings.Index(out, "| file-2 | tssp | 20 |")
	if first < 0 || second < 0 {
		t.Fatalf("markdown report = %q, want file-1 and file-2 rows", out)
	}
	if first > second {
		t.Fatalf("markdown report = %q, want stable input file order", out)
	}
	for _, notWant := range []string{"/private/a.tsm", "/private/b.tssp"} {
		if strings.Contains(out, notWant) {
			t.Fatalf("markdown report = %q, want stable file labels instead of %q", out, notWant)
		}
	}
}

func TestAccumulateSummaryIncludesBlockTypes(t *testing.T) {
	var summary Summary
	accumulateSummary(&summary, FileReport{
		SizeBytes:          100,
		KeyCount:           2,
		BlockCount:         4,
		QueryOverlapsFile:  true,
		QueryOverlapBlocks: 2,
		BlocksByType: map[string]int{
			"float":   2,
			"integer": 1,
			"ignored": 0,
		},
		Tombstones: TombstoneSummary{
			Exists:             true,
			SizeBytes:          64,
			RangeCount:         3,
			QueryOverlapRanges: 1,
			AffectedBlocks:     2,
		},
	}, Options{})
	accumulateSummary(&summary, FileReport{
		SizeBytes:  50,
		KeyCount:   1,
		BlockCount: 2,
		BlocksByType: map[string]int{
			"float":   3,
			"ignored": -1,
		},
		Tombstones: TombstoneSummary{
			Exists:             true,
			SizeBytes:          16,
			RangeCount:         1,
			QueryOverlapRanges: 2,
			AffectedBlocks:     3,
		},
	}, Options{})

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
	if got := summary.QueryOverlapFiles; got != 0 {
		t.Fatalf("query overlap files = %d, want omitted without query target", got)
	}
	if got := summary.QueryOverlapBlocks; got != 0 {
		t.Fatalf("query overlap blocks = %d, want omitted without query target", got)
	}
	if got, want := summary.TombstoneFiles, 2; got != want {
		t.Fatalf("tombstone files = %d, want %d", got, want)
	}
	if got, want := summary.TombstoneSizeBytes, int64(80); got != want {
		t.Fatalf("tombstone size bytes = %d, want %d", got, want)
	}
	if got, want := summary.TombstoneRanges, 4; got != want {
		t.Fatalf("tombstone ranges = %d, want %d", got, want)
	}
	if got, want := summary.TombstoneQueryOverlapRanges, 3; got != want {
		t.Fatalf("tombstone query overlap ranges = %d, want %d", got, want)
	}
	if got, want := summary.TombstoneAffectedBlocks, 5; got != want {
		t.Fatalf("tombstone affected blocks = %d, want %d", got, want)
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
				SeriesID: SeriesIDSummary{
					Min:   1,
					Max:   9,
					Count: 7,
				},
				Index: &IndexSummary{
					MeasurementCount:                2,
					DeletedMeasurementCount:         1,
					SeriesRefs:                      10,
					TagKeyCount:                     3,
					TagValueCount:                   4,
					SeriesIDSetCardinality:          7,
					TombstoneSeriesIDSetCardinality: 2,
					Query: &IndexQuerySummary{
						MeasurementFilterApplied: true,
						TagFilterApplied:         true,
						QueryMeasurements:        []string{"cpu", "mem"},
						MatchedMeasurements:      []string{"cpu"},
						MissingMeasurements:      []string{"mem"},
						QueryTags:                []TagFilter{{Key: "host", Value: "a"}, {Key: "region", Value: "missing"}},
						MatchedTags:              []TagFilter{{Key: "host", Value: "a"}},
						MissingTags:              []TagFilter{{Key: "region", Value: "missing"}},
						CandidateMeasurements:    1,
						SeriesRefs:               2,
					},
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
				Path:   "segment.idx",
				Format: FormatTSSPDetachedIndex,
				BlocksByType: map[string]int{
					"detached-meta-index": 3,
				},
				MetaIndexID: SeriesIDSummary{
					Min:   10,
					Max:   12,
					Count: 3,
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
					GroupCount:            3,
					PieceCount:            6,
					PayloadSizeBytes:      64,
					BlockSizeBytes:        96,
					PieceSizeBytes:        16,
					GroupSizeBytes:        32,
					ValidBytes:            128,
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
					Type:                   "opengemini-clv-text-mergeset",
					Layout:                 "mergeset-namespace",
					ItemCount:              4,
					DocumentCount:          1,
					TermCount:              1,
					DictionaryCount:        1,
					DictionaryVersionCount: 1,
					PositionCount:          2,
					SIDGroupCount:          3,
					DocumentIDCount:        4,
				},
			},
		},
	}

	result := report.Result()
	detailsColumn := tableColumnIndex(t, result.Table.Columns, "details")
	wants := [][]string{
		{"block_types measurement:2 tag-key:3", "series_id count=7 range=1..9", "index measurements=2", "series_refs=10", "series_ids=7", "tags=3 values=4", "deleted measurements=1 tag_keys=0 tag_values=0 series_ids=2", "query measurement_filter=true tag_filter=true measurements=2/1/1 tags=2/1/1 candidates=1 query_series_refs=2"},
		{"block_types field:4 measurement-fields:2", "fields measurements=2 fields=4", "types=float:1 integer:1 string:1 unsigned:1", "changes=3 adds=2 deletes=1"},
		{"block_types detached-meta-index:3", "meta_index_id count=3 range=10..12"},
		{"block_types primary-key-meta-block:2 primary-key-schema-column:3", "primary_key type=opengemini-detached-primary-meta", "columns=3", "rows=4", "block_ids=10..13", "data=120 valid=112", "crc=1 data_oob=2 column_oob=3 column_unordered=1"},
		{"block_types bloom-filter-crc-mismatch:1 bloom-filter-line-block:2", "secondary_index type=opengemini-bloom-filter", "layout=attached-line-filter", "field=content", "blocks=2", "groups=3", "pieces=6", "payload_bytes=64", "block_bytes=96", "piece_bytes=16", "group_bytes=32", "valid_bytes=128", "crc=1 trailing=3 data_oob=1"},
		{"block_types mergeset-block:2 mergeset-metaindex-row:1", "secondary_index type=opengemini-clv-text-mergeset", "layout=mergeset-namespace", "items=4", "documents=1", "terms=1", "dictionaries=1", "dictionary_versions=1", "positions=2", "sid_groups=3", "document_ids=4"},
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

func TestIndexQueryDetailsTextIncludesAppliedFiltersWithoutTerms(t *testing.T) {
	details := indexQueryDetailsText(&IndexQuerySummary{
		MeasurementFilterApplied: true,
		TagFilterApplied:         true,
	})
	want := "query measurement_filter=true tag_filter=true"
	if details != want {
		t.Fatalf("details = %q, want %q", details, want)
	}
}

func TestSeriesIDDetailsTextOmitsUnknownRange(t *testing.T) {
	if got, want := seriesIDDetailsText("series_id", SeriesIDSummary{Count: 7}), "series_id count=7"; got != want {
		t.Fatalf("details = %q, want %q", got, want)
	}
	if got := seriesIDDetailsText("series_id", SeriesIDSummary{}); got != "" {
		t.Fatalf("details = %q, want empty", got)
	}
}

func TestSecondaryIndexDetailsTextIncludesZeroAnomalyCounters(t *testing.T) {
	details := secondaryIndexDetailsText(&SecondaryIndexSummary{
		Type:          "opengemini-bloom-filter",
		CRCMismatches: 1,
	})
	want := "secondary_index type=opengemini-bloom-filter crc=1 trailing=0 data_oob=0"
	if details != want {
		t.Fatalf("details = %q, want %q", details, want)
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
				BaselineDecodeBytes:      256,
				OptimizedDecodeBytes:     128,
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
		"decode_bytes 256->128 saved=128",
		"cursor_reads 2->1",
		"iterator_cost files=2 blocks=3 bytes=256",
		"value_output points=0->0 mismatches=1",
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
