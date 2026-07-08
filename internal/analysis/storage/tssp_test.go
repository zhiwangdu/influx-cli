package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/snappy"
	ksnappy "github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

func TestAnalyzeTSSPMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSP(path); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatTSSP; got != want {
		t.Fatalf("format = %s, want %s", got, want)
	}
	if got, want := file.SeriesID.Count, int64(2); got != want {
		t.Fatalf("series count = %d, want %d", got, want)
	}
	if got, want := file.BlockCount, 5; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if got, want := file.Extra["measurement"], "cpu"; got != want {
		t.Fatalf("measurement = %q, want %q", got, want)
	}
	if got, want := file.Extra["query_overlap_precision"], "chunk-meta"; got != want {
		t.Fatalf("query overlap precision = %q, want %q", got, want)
	}
	if got, want := file.KeySamples[0], "measurement:cpu"; got != want {
		t.Fatalf("first key sample = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].Type, "chunk-meta"; got != want {
		t.Fatalf("first block type = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].ColumnCount, 2; got != want {
		t.Fatalf("first block column count = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].SegmentCount, 1; got != want {
		t.Fatalf("first block segment count = %d, want %d", got, want)
	}
	if got, want := file.Blocks[1].QueryOverlaps, true; got != want {
		t.Fatalf("second block query overlap = %t, want %t", got, want)
	}
	if file.DecodePath == nil {
		t.Fatal("expected TSSP decode path summary")
	}
	decode := file.DecodePath
	if got, want := decode.Mode, "tssp-location-cursor-ascending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.LocationBlocks, 3; got != want {
		t.Fatalf("decode location blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 3; got != want {
		t.Fatalf("baseline decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBlocks, 2; got != want {
		t.Fatalf("saved decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineReadSegments, 3; got != want {
		t.Fatalf("baseline read segments = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadSegments, 1; got != want {
		t.Fatalf("optimized read segments = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadSegments, 2; got != want {
		t.Fatalf("saved read segments = %d, want %d", got, want)
	}
	if got, want := decode.BaselineCursorReadCalls, 3; got != want {
		t.Fatalf("baseline cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedCursorReadCalls, 1; got != want {
		t.Fatalf("optimized cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.BaselineReadAtCalls, 6; got != want {
		t.Fatalf("baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 2; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadAtCalls, 4; got != want {
		t.Fatalf("saved ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeBytes, int64(288); got != want {
		t.Fatalf("baseline decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBytes, int64(96); got != want {
		t.Fatalf("optimized decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBytes, int64(192); got != want {
		t.Fatalf("saved decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.IteratorCostFiles, 1; got != want {
		t.Fatalf("iterator cost files = %d, want %d", got, want)
	}
	if got, want := decode.IteratorCostBlocks, 3; got != want {
		t.Fatalf("iterator cost blocks = %d, want %d", got, want)
	}
	if got, want := decode.IteratorCostBytes, int64(273); got != want {
		t.Fatalf("iterator cost bytes = %d, want %d", got, want)
	}
	if got, want := decode.SkippedBeforeSeekBlocks, 1; got != want {
		t.Fatalf("skipped before seek blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedAfterRangeBlocks, 3; got != want {
		t.Fatalf("skipped after range blocks = %d, want %d", got, want)
	}
	if got, want := len(decode.Samples), 3; got != want {
		t.Fatalf("decode samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 3; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 3; got != want {
		t.Fatalf("cursor window samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[1].Reason, "segment_overlap"; got != want {
		t.Fatalf("second cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[1].DecodedBlocks, 1; got != want {
		t.Fatalf("second cursor window decoded blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[1].SavedBlocks, 0; got != want {
		t.Fatalf("second cursor window saved blocks = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorExecutionSamples), 3; got != want {
		t.Fatalf("cursor execution samples = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorExecutionActions), 3; got != want {
		t.Fatalf("cursor execution action count entries = %d, want %d: %+v", got, want, decode.CursorExecutionActions)
	}
	for action, want := range map[string]int{
		"skip_before_seek": 1,
		"read_segments":    1,
		"skip_after_range": 1,
	} {
		if got := decode.CursorExecutionActions[action]; got != want {
			t.Fatalf("%s action count = %d, want %d", action, got, want)
		}
	}
	wantFirstStep := DecodePathCursorStep{
		Step:              1,
		Type:              "tssp-location-cursor-step",
		Action:            "skip_before_seek",
		Key:               "sid:7",
		CandidateValue:    "time_range=100:120 segments=0/1",
		CursorIndexBefore: 0,
		CursorIndexAfter:  1,
		CursorAdvanced:    true,
	}
	if got := decode.CursorExecutionSamples[0]; got != wantFirstStep {
		t.Fatalf("cursor execution sample[0] = %+v, want %+v", got, wantFirstStep)
	}
	if got := decode.CursorExecutionSamples[1]; got.Action != "read_segments" || got.Key != "sid:7" || got.CandidateValue != "time_range=150:180 segments=1/1" || got.CursorIndexBefore != 1 || got.CursorIndexAfter != 2 {
		t.Fatalf("cursor execution sample[1] = %+v, want overlapping segment read", got)
	}
	if got := decode.CursorExecutionSamples[2]; got.Action != "skip_after_range" || got.CursorIndexBefore != 2 || got.CursorIndexAfter != 3 || !got.CursorExhausted {
		t.Fatalf("cursor execution sample[2] = %+v, want after-range skip", got)
	}
	if got, want := decode.Samples[1].OutputSegments, 1; got != want {
		t.Fatalf("second decode sample output segments = %d, want %d", got, want)
	}
	if got, want := decode.Samples[1].Reason, "segment_overlap"; got != want {
		t.Fatalf("second decode sample reason = %q, want %q", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "issue 2 TSSP ReadAt call(s)") {
		t.Fatalf("recommendations = %v, want ReadAt call recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "TSSP location cursor execution samples") {
		t.Fatalf("recommendations = %v, want cursor execution recommendation", decode.Recommendations)
	}
	overlapSample := decode.Samples[1]
	if got, want := overlapSample.BaselineReadAtCalls, 2; got != want {
		t.Fatalf("overlap sample baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := overlapSample.OptimizedReadAtCalls, 2; got != want {
		t.Fatalf("overlap sample optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := len(overlapSample.OptimizedReadAtRanges), 2; got != want {
		t.Fatalf("overlap sample optimized ReadAt ranges = %d, want %d", got, want)
	}
	if got, want := overlapSample.OptimizedReadAtRanges[0].Column, "value"; got != want {
		t.Fatalf("first ReadAt range column = %q, want %q", got, want)
	}
	if got, want := overlapSample.OptimizedReadAtRanges[0].Offset, int64(1104); got != want {
		t.Fatalf("first ReadAt range offset = %d, want %d", got, want)
	}
	if got, want := overlapSample.OptimizedReadAtRanges[1].Column, "time"; got != want {
		t.Fatalf("second ReadAt range column = %q, want %q", got, want)
	}
	if got, want := overlapSample.OptimizedReadAtRanges[1].SizeBytes, uint32(16); got != want {
		t.Fatalf("second ReadAt range size = %d, want %d", got, want)
	}
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
	}
}

func TestBuildTSSPDecodePathSummarySamplesUnexpandedMetaIndexCursor(t *testing.T) {
	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}

	summary := buildTSSPDecodePathSummary([]tsspMetaIndex{{
		ID:      7,
		MinTime: 100,
		MaxTime: 200,
		Count:   3,
		Size:    64,
	}}, nil, Options{
		QueryRange:       queryRange,
		BlockSampleLimit: 3,
	}, nil)
	if summary == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := summary.LocationBlocks, 3; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := len(summary.CursorExecutionSamples), 1; got != want {
		t.Fatalf("cursor execution samples = %d, want %d", got, want)
	}
	wantStep := DecodePathCursorStep{
		Step:              1,
		Type:              "tssp-location-cursor-step",
		Action:            "read_unexpanded_chunk_metadata",
		Key:               "sid:7",
		CandidateValue:    "time_range=100:200 chunks=3",
		CursorIndexBefore: 0,
		CursorIndexAfter:  3,
		CursorAdvanced:    true,
		CursorExhausted:   true,
	}
	if got := summary.CursorExecutionSamples[0]; got != wantStep {
		t.Fatalf("cursor execution sample = %+v, want %+v", got, wantStep)
	}
}

func TestAnalyzeTSSPCursorExecutionSamplesFollowDescendingOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSP(path); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		BlockSampleLimit: 3,
		CursorDescending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.Mode, "tssp-location-cursor-descending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := len(decode.CursorExecutionSamples), 3; got != want {
		t.Fatalf("cursor execution samples = %d, want %d", got, want)
	}
	want := []struct {
		action string
		value  string
	}{
		{"skip_after_range", "time_range=190:200 segments=0/1"},
		{"read_segments", "time_range=150:180 segments=1/1"},
		{"skip_before_seek", "time_range=100:120 segments=0/1"},
	}
	for i, want := range want {
		got := decode.CursorExecutionSamples[i]
		if got.Action != want.action || got.CandidateValue != want.value || got.CursorIndexBefore != i || got.CursorIndexAfter != i+1 {
			t.Fatalf("cursor execution sample[%d] = %+v, want action=%s value=%s", i, got, want.action, want.value)
		}
	}
	if !decode.CursorExecutionSamples[2].CursorExhausted {
		t.Fatalf("last cursor execution sample = %+v, want exhausted", decode.CursorExecutionSamples[2])
	}
}

func TestAnalyzeTSSPSamplesAttachedOneRowValueBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithOneRowData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 333)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_checked"], "true"; got != want {
		t.Fatalf("data block probe checked = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_valid_blocks"], "2"; got != want {
		t.Fatalf("data block probe valid blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_short_blocks"], "0"; got != want {
		t.Fatalf("data block probe short blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_unknown_block_types"], "0"; got != want {
		t.Fatalf("data block probe unknown block types = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_read_errors"], "0"; got != want {
		t.Fatalf("data block probe read errors = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "integer-one:2"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeBlocks, 2; got != want {
		t.Fatalf("data block probe blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeValidBlocks, 2; got != want {
		t.Fatalf("data block probe valid blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeShortBlocks, 0; got != want {
		t.Fatalf("data block probe short blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeUnknownTypes, 0; got != want {
		t.Fatalf("data block probe unknown block types = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeReadErrors, 0; got != want {
		t.Fatalf("data block probe read errors = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRowCountBlocks, 2; got != want {
		t.Fatalf("data block probe row count blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeOutputPoints, 1; got != want {
		t.Fatalf("data block probe output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeValueBlocks, 2; got != want {
		t.Fatalf("data block probe value blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeTypes["integer-one"], 2; got != want {
		t.Fatalf("decode data block probe integer-one blocks = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	sample := decode.CursorOutputSamples[0]
	if got, want := sample.Key, "sid:7/value"; got != want {
		t.Fatalf("sample key = %q, want %q", got, want)
	}
	if got, want := sample.Time, int64(333); got != want {
		t.Fatalf("sample time = %d, want %d", got, want)
	}
	if got, want := sample.Type, "integer-one"; got != want {
		t.Fatalf("sample type = %q, want %q", got, want)
	}
	if got, want := sample.OptimizedValue, "99"; got != want {
		t.Fatalf("sample optimized value = %q, want %q", got, want)
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 1; got != want {
		t.Fatalf("decode sample value output points = %d, want %d", got, want)
	}
	if !decode.Samples[0].ValueOutputAvailable {
		t.Fatal("expected decode sample value output to be available")
	}
	if !containsStringWithPrefix(decode.Recommendations, "sampled 1 TSSP value output") {
		t.Fatalf("recommendations = %v, want value output recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPSamplesAttachedFloatFullBlocks(t *testing.T) {
	for _, tc := range []struct {
		name   string
		codec  byte
		values []float64
		want   []string
	}{
		{name: "raw", codec: 0, values: []float64{1.25, 2.5}, want: []string{"1.25", "2.5"}},
		{name: "old-gorilla", codec: 1, values: []float64{1.25, 2.5, 3.75}, want: []string{"1.25", "2.5", "3.75"}},
		{name: "snappy", codec: 2, values: []float64{1.25, 2.5, 3.75}, want: []string{"1.25", "2.5", "3.75"}},
		{name: "gorilla", codec: 3, values: []float64{1.25, 2.5, 3.75}, want: []string{"1.25", "2.5", "3.75"}},
		{name: "same", codec: 4, values: []float64{7.5, 7.5, 7.5}, want: []string{"7.5", "7.5", "7.5"}},
		{name: "rle", codec: 5, values: []float64{1.5, 1.5, 0, 0, 2.5}, want: []string{"1.5", "1.5", "0", "0", "2.5"}},
		{name: "mlf", codec: 6, values: []float64{1.11, 0, -2.22, 3.33, 4.44, 5.55, 6.66, 7.77, 8.88, 9.99, 10.01}, want: []string{"1.11", "0", "-2.22", "3.33", "4.44", "5.55", "6.66", "7.77", "8.88", "9.99", "10.01"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
			times, err := writeTestTSSPWithFloatFullValues(path, tc.values, tc.codec)
			if err != nil {
				t.Fatal(err)
			}
			queryRange, err := NewTimeRange(times[0], times[len(times)-1])
			if err != nil {
				t.Fatal(err)
			}

			report, err := Analyze(context.Background(), []string{path}, Options{
				Format:           FormatTSSP,
				QueryRange:       queryRange,
				KeySampleLimit:   3,
				BlockSampleLimit: len(tc.values) + 2,
			})
			if err != nil {
				t.Fatal(err)
			}
			file := report.Files[0]
			if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
				t.Fatalf("data block probe blocks = %q, want %q", got, want)
			}
			if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
				t.Fatalf("data block probe value blocks = %q, want %q", got, want)
			}
			if got, want := file.Extra["data_block_probe_types"], "float-full:1,integer-full:1"; got != want {
				t.Fatalf("data block probe types = %q, want %q", got, want)
			}
			decode := file.DecodePath
			if decode == nil {
				t.Fatal("decode path is nil")
			}
			if got, want := decode.OptimizedValueOutputPoints, len(tc.values); got != want {
				t.Fatalf("optimized value output points = %d, want %d", got, want)
			}
			if got, want := len(decode.CursorOutputSamples), len(tc.values); got != want {
				t.Fatalf("cursor output samples = %d, want %d", got, want)
			}
			for i, value := range tc.want {
				want := DecodePathCursorOutput{
					Key:            "sid:7/value",
					Time:           times[i],
					Type:           "float-full",
					OptimizedValue: value,
					Matches:        true,
				}
				got := decode.CursorOutputSamples[i]
				if got != want {
					t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
				}
			}
			if got, want := decode.Samples[0].ValueOutputPoints, len(tc.values); got != want {
				t.Fatalf("decode sample value output points = %d, want %d", got, want)
			}
		})
	}
}

func TestAnalyzeTSSPSamplesAttachedRegularFloatBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	values := []float64{1.25, 2.5, 3.75}
	times, err := writeTestTSSPWithRegularFloatValues(path, values)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: len(values) + 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "float:1,integer:1"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, len(values); got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), len(values); got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, value := range []string{"1.25", "2.5", "3.75"} {
		want := DecodePathCursorOutput{
			Key:            "sid:7/value",
			Time:           times[i],
			Type:           "float",
			OptimizedValue: value,
			Matches:        true,
		}
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestAnalyzeTSSPDataProbeFiltersDecodedRowsByQueryRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	values := []float64{1.25, 2.5, 3.75}
	times, err := writeTestTSSPWithRegularFloatValues(path, values)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[1], times[1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: len(values) + 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_range_rows"], "3"; got != want {
		t.Fatalf("data block probe range rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_range_matches"], "1"; got != want {
		t.Fatalf("data block probe range matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_range_rejects"], "2"; got != want {
		t.Fatalf("data block probe range rejects = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rows"], "0"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q without field predicates", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 1; got != want {
		t.Fatalf("decode sample value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRangeRows, 3; got != want {
		t.Fatalf("data block probe range rows = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRangeMatches, 1; got != want {
		t.Fatalf("data block probe range matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRangeRejects, 2; got != want {
		t.Fatalf("data block probe range rejects = %d, want %d", got, want)
	}
	if got, want := len(decode.RangeExecutionSamples), 3; got != want {
		t.Fatalf("range execution samples = %d, want %d", got, want)
	}
	if got, want := len(decode.RangeExecutionActions), 2; got != want {
		t.Fatalf("range execution action count entries = %d, want %d: %+v", got, want, decode.RangeExecutionActions)
	}
	if got, want := decode.RangeExecutionActions["range_row_reject"], 2; got != want {
		t.Fatalf("range_row_reject action count = %d, want %d", got, want)
	}
	if got, want := decode.RangeExecutionActions["range_row_match"], 1; got != want {
		t.Fatalf("range_row_match action count = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorStep{
		{
			Step:              1,
			Type:              "tssp-range-row-step",
			Action:            "range_row_reject",
			Key:               "sid:7/row:0",
			CandidateValue:    fmt.Sprintf("row=0 time=%d range=%d:%d result=reject_range", times[0], times[1], times[1]),
			CursorIndexBefore: 0,
			CursorIndexAfter:  1,
			CursorAdvanced:    true,
		},
		{
			Step:              2,
			Type:              "tssp-range-row-step",
			Action:            "range_row_match",
			Key:               "sid:7/row:1",
			CandidateValue:    fmt.Sprintf("row=1 time=%d range=%d:%d result=match", times[1], times[1], times[1]),
			CursorIndexBefore: 1,
			CursorIndexAfter:  2,
			CursorAdvanced:    true,
		},
		{
			Step:              3,
			Type:              "tssp-range-row-step",
			Action:            "range_row_reject",
			Key:               "sid:7/row:2",
			CandidateValue:    fmt.Sprintf("row=2 time=%d range=%d:%d result=reject_range", times[2], times[1], times[1]),
			CursorIndexBefore: 2,
			CursorIndexAfter:  3,
			CursorAdvanced:    true,
		},
	} {
		if got := decode.RangeExecutionSamples[i]; got != want {
			t.Fatalf("range execution sample[%d] = %+v, want %+v", i, got, want)
		}
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           times[1],
		Type:           "float",
		OptimizedValue: "2.5",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("cursor output sample = %+v, want %+v", got, want)
	}
	if !containsString(decode.Recommendations, "TSSP query range matched 1 of 3 decoded row timestamp") {
		t.Fatalf("recommendations = %v, want query range row-count recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPSamplesAttachedNullableRegularFloatBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	values := []float64{1.25, 0, 3.75}
	present := []bool{true, false, true}
	times, err := writeTestTSSPWithNullableRegularFloatValues(path, values, present)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: len(values) + 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_null_values"], "1"; got != want {
		t.Fatalf("data block probe null values = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "float:1,integer:1"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, len(values); got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeNullValues, 1; got != want {
		t.Fatalf("data block probe null values = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 2; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: times[0], Type: "float", OptimizedValue: "1.25", Matches: true},
		{Key: "sid:7/value", Time: times[2], Type: "float", OptimizedValue: "3.75", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestAnalyzeTSSPMaterializesAttachedRecordSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "3"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "3"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_record_samples"], "2"; got != want {
		t.Fatalf("data block probe record samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_record_rows"], "2"; got != want {
		t.Fatalf("data block probe record rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_record_outputs"], "2"; got != want {
		t.Fatalf("data block probe record outputs = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "boolean-full:1,float-full:1,integer-full:1"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, len(times); got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordSamples, 2; got != want {
		t.Fatalf("data block probe record samples = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordRows, 2; got != want {
		t.Fatalf("data block probe record rows = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordOutputs, 2; got != want {
		t.Fatalf("data block probe record outputs = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 6; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/record", Time: times[0], Type: "record", OptimizedValue: "status=true,value=1.25", Matches: true},
		{Key: "sid:7/record", Time: times[1], Type: "record", OptimizedValue: "status=false,value=2.5", OutputOrdinal: 1, Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if !containsStringWithPrefix(decode.Recommendations, "materialized 2 TSSP record output row(s) from decoded column blocks with 2 sampled") {
		t.Fatalf("recommendations = %v, want record materialization recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPRecordOutputOrdinalsContinueAcrossChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithTwoChunkRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	recordSamples := make([]DecodePathCursorOutput, 0, 4)
	for _, sample := range decode.CursorOutputSamples {
		if sample.Type == "record" {
			recordSamples = append(recordSamples, sample)
		}
	}
	if got, want := len(recordSamples), 4; got != want {
		t.Fatalf("record output samples = %d, want %d: %+v", got, want, recordSamples)
	}
	for i, sample := range recordSamples {
		if got, want := sample.OutputOrdinal, i; got != want {
			t.Fatalf("record output sample %d ordinal = %d, want %d: %+v", i, got, want, sample)
		}
	}
	if got, want := len(decode.RecordExecutionSamples), 4; got != want {
		t.Fatalf("record execution samples = %d, want %d", got, want)
	}
	if got, want := len(decode.RecordExecutionActions), 1; got != want {
		t.Fatalf("record execution action count entries = %d, want %d: %+v", got, want, decode.RecordExecutionActions)
	}
	if got, want := decode.RecordExecutionActions["record_row_output"], 4; got != want {
		t.Fatalf("record_row_output action count = %d, want %d", got, want)
	}
	for i, want := range []struct {
		key   string
		value string
	}{
		{"sid:7/record/row:0", fmt.Sprintf("row=0 local_input=0 local_output=0 time=%d range=%d:%d columns=2 values=status=true,value=1.25 result=output", times[0], times[0], times[len(times)-1])},
		{"sid:7/record/row:1", fmt.Sprintf("row=1 local_input=1 local_output=1 time=%d range=%d:%d columns=2 values=status=false,value=2.5 result=output", times[1], times[0], times[len(times)-1])},
		{"sid:7/record/row:0", fmt.Sprintf("row=0 local_input=2 local_output=2 time=%d range=%d:%d columns=2 values=status=false,value=3.25 result=output", times[2], times[0], times[len(times)-1])},
		{"sid:7/record/row:1", fmt.Sprintf("row=1 local_input=3 local_output=3 time=%d range=%d:%d columns=2 values=status=true,value=4.5 result=output", times[3], times[0], times[len(times)-1])},
	} {
		got := decode.RecordExecutionSamples[i]
		if got.Step != i+1 || got.Type != "tssp-record-row-step" || got.Action != "record_row_output" || got.Key != want.key || got.CandidateValue != want.value || got.CursorIndexBefore != i || got.CursorIndexAfter != i+1 || !got.CursorAdvanced {
			t.Fatalf("record execution sample[%d] = %+v, want key=%q value=%q indexes=%d->%d advanced", i, got, want.key, want.value, i, i+1)
		}
	}
}

func TestAnalyzeTSSPFieldFilterMatchesNullPredicates(t *testing.T) {
	for _, tc := range []struct {
		name              string
		filter            FieldFilter
		wantOutputPoints  string
		wantSamples       []DecodePathCursorOutput
		wantValueOutCount int
	}{
		{
			name:              "equals-null",
			filter:            FieldFilter{Key: "value", Value: "null"},
			wantOutputPoints:  "1",
			wantValueOutCount: 1,
		},
		{
			name:              "in-null",
			filter:            FieldFilter{Key: "value", Op: "in", Value: "(null)"},
			wantOutputPoints:  "1",
			wantValueOutCount: 1,
		},
		{
			name:              "is-null",
			filter:            FieldFilter{Key: "value", Op: "is", Value: "null"},
			wantOutputPoints:  "1",
			wantValueOutCount: 1,
		},
		{
			name:             "not-null",
			filter:           FieldFilter{Key: "value", Op: "!=", Value: "null"},
			wantOutputPoints: "2",
			wantSamples: []DecodePathCursorOutput{
				{Key: "sid:7/value", Time: 333, Type: "float", OptimizedValue: "1.25", Matches: true},
				{Key: "sid:7/value", Time: 555, Type: "float", OptimizedValue: "3.75", Matches: true},
			},
			wantValueOutCount: 2,
		},
		{
			name:             "angle-not-null",
			filter:           FieldFilter{Key: "value", Op: "<>", Value: "null"},
			wantOutputPoints: "2",
			wantSamples: []DecodePathCursorOutput{
				{Key: "sid:7/value", Time: 333, Type: "float", OptimizedValue: "1.25", Matches: true},
				{Key: "sid:7/value", Time: 555, Type: "float", OptimizedValue: "3.75", Matches: true},
			},
			wantValueOutCount: 2,
		},
		{
			name:             "is-not-null",
			filter:           FieldFilter{Key: "value", Op: "is-not", Value: "null"},
			wantOutputPoints: "2",
			wantSamples: []DecodePathCursorOutput{
				{Key: "sid:7/value", Time: 333, Type: "float", OptimizedValue: "1.25", Matches: true},
				{Key: "sid:7/value", Time: 555, Type: "float", OptimizedValue: "3.75", Matches: true},
			},
			wantValueOutCount: 2,
		},
		{
			name:             "not-in-null",
			filter:           FieldFilter{Key: "value", Op: "not-in", Value: "(null)"},
			wantOutputPoints: "2",
			wantSamples: []DecodePathCursorOutput{
				{Key: "sid:7/value", Time: 333, Type: "float", OptimizedValue: "1.25", Matches: true},
				{Key: "sid:7/value", Time: 555, Type: "float", OptimizedValue: "3.75", Matches: true},
			},
			wantValueOutCount: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
			times, err := writeTestTSSPWithNullableRegularFloatValues(path, []float64{1.25, 0, 3.75}, []bool{true, false, true})
			if err != nil {
				t.Fatal(err)
			}
			queryRange, err := NewTimeRange(times[0], times[len(times)-1])
			if err != nil {
				t.Fatal(err)
			}

			report, err := Analyze(context.Background(), []string{path}, Options{
				Format:           FormatTSSP,
				QueryRange:       queryRange,
				QueryFields:      []FieldFilter{tc.filter},
				KeySampleLimit:   3,
				BlockSampleLimit: 8,
			})
			if err != nil {
				t.Fatal(err)
			}
			file := report.Files[0]
			if got := file.Extra["data_block_probe_output_points"]; got != tc.wantOutputPoints {
				t.Fatalf("data block probe output points = %q, want %q", got, tc.wantOutputPoints)
			}
			decode := file.DecodePath
			if decode == nil {
				t.Fatal("decode path is nil")
			}
			if got := decode.OptimizedValueOutputPoints; got != tc.wantValueOutCount {
				t.Fatalf("optimized value output points = %d, want %d", got, tc.wantValueOutCount)
			}
			if got, want := len(decode.CursorOutputSamples), len(tc.wantSamples); got != want {
				t.Fatalf("cursor output samples = %d, want %d", got, want)
			}
			for i, want := range tc.wantSamples {
				got := decode.CursorOutputSamples[i]
				if got != want {
					t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
				}
			}
			if !decode.Samples[0].ValueOutputAvailable {
				t.Fatal("expected null predicate result to remain available")
			}
		})
	}
}

func TestAnalyzeTSSPFieldFilterMaterializesMatchingAttachedRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "status", Value: "true"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_record_rows"], "2"; got != want {
		t.Fatalf("data block probe record rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_record_range_rejects"], "0"; got != want {
		t.Fatalf("data block probe record range rejects = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_record_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe record filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "status", Value: "true"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.MatchedFields, []FieldFilter{{Key: "status", Value: "true"}}; !equalFieldFilters(got, want) {
		t.Fatalf("matched fields = %v, want %v", got, want)
	}
	if len(decode.MissingFields) != 0 {
		t.Fatalf("missing fields = %v, want none", decode.MissingFields)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordSamples, 1; got != want {
		t.Fatalf("data block probe record samples = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordOutputs, 1; got != want {
		t.Fatalf("data block probe record outputs = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordRows, 2; got != want {
		t.Fatalf("data block probe record rows = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordRangeRejects, 0; got != want {
		t.Fatalf("data block probe record range rejects = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordFilterRejects, 1; got != want {
		t.Fatalf("data block probe record filter rejects = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRows, 2; got != want {
		t.Fatalf("data block probe filter rows = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterMatches, 1; got != want {
		t.Fatalf("data block probe filter matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRejects, 1; got != want {
		t.Fatalf("data block probe filter rejects = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 3; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "rejected 1 TSSP record row(s) during local materialization: range=0 filters=1") {
		t.Fatalf("recommendations = %v, want record reject recommendation", decode.Recommendations)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/record", Time: times[0], Type: "record", OptimizedValue: "status=true,value=1.25", Matches: true},
		{Key: "sid:7/status", Time: times[0], Type: "boolean-full", OptimizedValue: "true", Matches: true},
		{Key: "sid:7/value", Time: times[0], Type: "float-full", OptimizedValue: "1.25", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if !containsStringWithPrefix(decode.Recommendations, "applied 1 TSSP field filter") {
		t.Fatalf("recommendations = %v, want field filter recommendation", decode.Recommendations)
	}
	if !containsStringWithPrefix(decode.Recommendations, "TSSP field filters matched 1 of 2 decoded record row") {
		t.Fatalf("recommendations = %v, want field filter row-count recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesDecodedTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryColumns:     []string{"value"},
		QueryFields:      []FieldFilter{{Key: "time", Op: ">=", Value: fmt.Sprint(times[1])}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryColumns, []string{"value"}; !equalStrings(got, want) {
		t.Fatalf("query columns = %v, want %v", got, want)
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "time", Op: ">=", Value: fmt.Sprint(times[1])}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.MatchedFields, []FieldFilter{{Key: "time", Op: ">=", Value: fmt.Sprint(times[1])}}; !equalFieldFilters(got, want) {
		t.Fatalf("matched fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.Samples[0].OptimizedReadAtRanges), 2; got != want {
		t.Fatalf("optimized ReadAt ranges = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].OptimizedReadAtRanges[0].Column, "value"; got != want {
		t.Fatalf("first ReadAt range column = %q, want %q", got, want)
	}
	if got, want := decode.Samples[0].OptimizedReadAtRanges[1].Column, "time"; got != want {
		t.Fatalf("second ReadAt range column = %q, want %q", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           times[1],
		Type:           "float-full",
		OptimizedValue: "2.5",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPAnyFieldFilterMatchesEitherPredicate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryAnyFields:   []FieldFilter{{Key: "status", Value: "true"}, {Key: "value", Value: "2.5"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "2"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "0"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	wantAny := []FieldFilter{{Key: "status", Value: "true"}, {Key: "value", Value: "2.5"}}
	if got := decode.QueryAnyFields; !equalFieldFilters(got, wantAny) {
		t.Fatalf("query any fields = %v, want %v", got, wantAny)
	}
	if got := decode.MatchedAnyFields; !equalFieldFilters(got, wantAny) {
		t.Fatalf("matched any fields = %v, want %v", got, wantAny)
	}
	if len(decode.MissingAnyFields) != 0 {
		t.Fatalf("missing any fields = %v, want none", decode.MissingAnyFields)
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "applied 2 TSSP OR field filter") {
		t.Fatalf("recommendations = %v, want OR field filter recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPAnyAndNoneFieldFiltersMatchDecodedTime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryColumns:     []string{"value"},
		QueryAnyFields:   []FieldFilter{{Key: "time", Value: fmt.Sprint(times[0])}, {Key: "value", Value: "2.5"}},
		QueryNoneFields:  []FieldFilter{{Key: "time", Value: fmt.Sprint(times[1])}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	wantAny := []FieldFilter{{Key: "time", Value: fmt.Sprint(times[0])}, {Key: "value", Value: "2.5"}}
	if got := decode.QueryAnyFields; !equalFieldFilters(got, wantAny) {
		t.Fatalf("query any fields = %v, want %v", got, wantAny)
	}
	if got := decode.MatchedAnyFields; !equalFieldFilters(got, wantAny) {
		t.Fatalf("matched any fields = %v, want %v", got, wantAny)
	}
	wantNone := []FieldFilter{{Key: "time", Value: fmt.Sprint(times[1])}}
	if got := decode.QueryNoneFields; !equalFieldFilters(got, wantNone) {
		t.Fatalf("query none fields = %v, want %v", got, wantNone)
	}
	if got := decode.MatchedNoneFields; !equalFieldFilters(got, wantNone) {
		t.Fatalf("matched none fields = %v, want %v", got, wantNone)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           times[0],
		Type:           "float-full",
		OptimizedValue: "1.25",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPAnyFieldFilterCombinesWithRequiredFilters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: ">", Value: "2.0"}},
		QueryAnyFields:   []FieldFilter{{Key: "status", Value: "true"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "0"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "2"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_evaluations"], "3"; got != want {
		t.Fatalf("data block probe filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_evaluations"], "2"; got != want {
		t.Fatalf("data block probe required filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_any_filter_evaluations"], "1"; got != want {
		t.Fatalf("data block probe any filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_none_filter_evaluations"], "0"; got != want {
		t.Fatalf("data block probe none filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_evaluation_matches"], "1"; got != want {
		t.Fatalf("data block probe filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_evaluation_misses"], "2"; got != want {
		t.Fatalf("data block probe filter evaluation misses = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_short_circuit_skips"], "1"; got != want {
		t.Fatalf("data block probe filter short-circuit skips = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_evaluation_matches"], "1"; got != want {
		t.Fatalf("data block probe required filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_evaluation_misses"], "1"; got != want {
		t.Fatalf("data block probe required filter evaluation misses = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_short_circuit_skips"], "0"; got != want {
		t.Fatalf("data block probe required filter short-circuit skips = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_any_filter_evaluation_matches"], "0"; got != want {
		t.Fatalf("data block probe any filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_any_filter_evaluation_misses"], "1"; got != want {
		t.Fatalf("data block probe any filter evaluation misses = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_any_filter_short_circuit_skips"], "1"; got != want {
		t.Fatalf("data block probe any filter short-circuit skips = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_none_filter_evaluation_matches"], "0"; got != want {
		t.Fatalf("data block probe none filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_none_filter_evaluation_misses"], "0"; got != want {
		t.Fatalf("data block probe none filter evaluation misses = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_none_filter_short_circuit_skips"], "0"; got != want {
		t.Fatalf("data block probe none filter short-circuit skips = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_operator_evaluations"], "=:1,>:2"; got != want {
		t.Fatalf("data block probe filter operator evaluations = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.DataBlockProbeFilterEvals, 3; got != want {
		t.Fatalf("decode filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRequiredEvals, 2; got != want {
		t.Fatalf("decode required filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeAnyEvals, 1; got != want {
		t.Fatalf("decode any filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeNoneEvals, 0; got != want {
		t.Fatalf("decode none filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterEvalHits, 1; got != want {
		t.Fatalf("decode filter evaluation matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterEvalMiss, 2; got != want {
		t.Fatalf("decode filter evaluation misses = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterSkips, 1; got != want {
		t.Fatalf("decode filter short-circuit skips = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRequiredHits, 1; got != want {
		t.Fatalf("decode required filter evaluation matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRequiredMiss, 1; got != want {
		t.Fatalf("decode required filter evaluation misses = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRequiredSkips, 0; got != want {
		t.Fatalf("decode required filter short-circuit skips = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeAnyHits, 0; got != want {
		t.Fatalf("decode any filter evaluation matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeAnyMiss, 1; got != want {
		t.Fatalf("decode any filter evaluation misses = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeAnySkips, 1; got != want {
		t.Fatalf("decode any filter short-circuit skips = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeNoneHits, 0; got != want {
		t.Fatalf("decode none filter evaluation matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeNoneMiss, 0; got != want {
		t.Fatalf("decode none filter evaluation misses = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeNoneSkips, 0; got != want {
		t.Fatalf("decode none filter short-circuit skips = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterOps["="], 1; got != want {
		t.Fatalf("decode equality filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterOps[">"], 2; got != want {
		t.Fatalf("decode greater-than filter evaluations = %d, want %d", got, want)
	}
	if got, want := len(decode.FilterExecutionSamples), 2; got != want {
		t.Fatalf("filter execution samples = %d, want %d", got, want)
	}
	if got, want := len(decode.FilterExecutionActions), 2; got != want {
		t.Fatalf("filter execution action count entries = %d, want %d: %+v", got, want, decode.FilterExecutionActions)
	}
	if got, want := decode.FilterExecutionActions["filter_row_reject_required"], 1; got != want {
		t.Fatalf("filter_row_reject_required action count = %d, want %d", got, want)
	}
	if got, want := decode.FilterExecutionActions["filter_row_reject_any"], 1; got != want {
		t.Fatalf("filter_row_reject_any action count = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorStep{
		{
			Step:              1,
			Type:              "tssp-filter-row-step",
			Action:            "filter_row_reject_required",
			Key:               "sid:7/row:0",
			CandidateValue:    fmt.Sprintf("row=0 time=%d required=0/1 any=0/0 none=0/0 skips=0/1/0 values=status=true,value=1.25 decision=required:value:>:2.0:miss result=reject_required", times[0]),
			CursorIndexBefore: 0,
			CursorIndexAfter:  1,
			CursorAdvanced:    true,
		},
		{
			Step:              2,
			Type:              "tssp-filter-row-step",
			Action:            "filter_row_reject_any",
			Key:               "sid:7/row:1",
			CandidateValue:    fmt.Sprintf("row=1 time=%d required=1/1 any=0/1 none=0/0 skips=0/0/0 values=status=false,value=2.5 decision=any:status:=:true:miss result=reject_any", times[1]),
			CursorIndexBefore: 1,
			CursorIndexAfter:  2,
			CursorAdvanced:    true,
		},
	} {
		if got := decode.FilterExecutionSamples[i]; got != want {
			t.Fatalf("filter execution sample[%d] = %+v, want %+v", i, got, want)
		}
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "applied 1 TSSP field filter") {
		t.Fatalf("recommendations = %v, want required field filter recommendation", decode.Recommendations)
	}
	if !containsStringWithPrefix(decode.Recommendations, "applied 1 TSSP OR field filter") {
		t.Fatalf("recommendations = %v, want OR field filter recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "executed 3 TSSP decoded-row field predicate evaluation") {
		t.Fatalf("recommendations = %v, want predicate evaluation recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "TSSP decoded-row field predicate operators: =:1 >:2") {
		t.Fatalf("recommendations = %v, want predicate operator recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "required=2 required_matches=1 required_misses=1 any=1 any_matches=0 any_misses=1 none=0 none_matches=0 none_misses=0") {
		t.Fatalf("recommendations = %v, want predicate clause/result breakdown", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "matches=1 misses=2") {
		t.Fatalf("recommendations = %v, want predicate match/miss breakdown", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "short-circuited 1 TSSP decoded-row field predicate evaluation") {
		t.Fatalf("recommendations = %v, want predicate short-circuit recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "required_skips=0 any_skips=1 none_skips=0") {
		t.Fatalf("recommendations = %v, want predicate short-circuit breakdown", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "TSSP filter execution samples show local decoded-row predicate decisions") {
		t.Fatalf("recommendations = %v, want predicate execution sample recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPFileSetSummarizesPredicateOperators(t *testing.T) {
	dir := t.TempDir()
	var times []int64
	for _, name := range []string{"00000001-0001-00000000.tssp", "00000002-0001-00000000.tssp"} {
		path := filepath.Join(dir, name)
		fileTimes, err := writeTestTSSPWithMultiColumnRecordData(path)
		if err != nil {
			t.Fatal(err)
		}
		if times == nil {
			times = fileTimes
		}
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: ">", Value: "1.0"}},
		QueryAnyFields:   []FieldFilter{{Key: "status", Value: "false"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level TSSP decode path summary")
	}
	if got, want := decode.DataBlockProbeFilterOps["="], 4; got != want {
		t.Fatalf("file-set equality filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterOps[">"], 4; got != want {
		t.Fatalf("file-set greater-than filter evaluations = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "TSSP decoded-row field predicate operators: =:4 >:4") {
		t.Fatalf("recommendations = %v, want file-set predicate operator recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPNoneFieldFilterRejectsMatchingRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryNoneFields:  []FieldFilter{{Key: "status", Value: "false"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_evaluations"], "2"; got != want {
		t.Fatalf("data block probe filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_evaluations"], "0"; got != want {
		t.Fatalf("data block probe required filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_any_filter_evaluations"], "0"; got != want {
		t.Fatalf("data block probe any filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_none_filter_evaluations"], "2"; got != want {
		t.Fatalf("data block probe none filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_evaluation_matches"], "1"; got != want {
		t.Fatalf("data block probe filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_evaluation_misses"], "1"; got != want {
		t.Fatalf("data block probe filter evaluation misses = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_evaluation_matches"], "0"; got != want {
		t.Fatalf("data block probe required filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_evaluation_misses"], "0"; got != want {
		t.Fatalf("data block probe required filter evaluation misses = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_any_filter_evaluation_matches"], "0"; got != want {
		t.Fatalf("data block probe any filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_any_filter_evaluation_misses"], "0"; got != want {
		t.Fatalf("data block probe any filter evaluation misses = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_none_filter_evaluation_matches"], "1"; got != want {
		t.Fatalf("data block probe none filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_none_filter_evaluation_misses"], "1"; got != want {
		t.Fatalf("data block probe none filter evaluation misses = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.DataBlockProbeNoneEvals, 2; got != want {
		t.Fatalf("decode none filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterEvalHits, 1; got != want {
		t.Fatalf("decode filter evaluation matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterEvalMiss, 1; got != want {
		t.Fatalf("decode filter evaluation misses = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeNoneHits, 1; got != want {
		t.Fatalf("decode none filter evaluation matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeNoneMiss, 1; got != want {
		t.Fatalf("decode none filter evaluation misses = %d, want %d", got, want)
	}
	if got, want := len(decode.FilterExecutionSamples), 2; got != want {
		t.Fatalf("filter execution samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorStep{
		{
			Step:              1,
			Type:              "tssp-filter-row-step",
			Action:            "filter_row_match",
			Key:               "sid:7/row:0",
			CandidateValue:    fmt.Sprintf("row=0 time=%d required=0/0 any=0/0 none=0/1 skips=0/0/0 values=status=true decision=none:status:=:false:miss result=match", times[0]),
			CursorIndexBefore: 0,
			CursorIndexAfter:  1,
			CursorAdvanced:    true,
		},
		{
			Step:              2,
			Type:              "tssp-filter-row-step",
			Action:            "filter_row_reject_none",
			Key:               "sid:7/row:1",
			CandidateValue:    fmt.Sprintf("row=1 time=%d required=0/0 any=0/0 none=1/1 skips=0/0/0 values=status=false decision=none:status:=:false:match result=reject_none", times[1]),
			CursorIndexBefore: 1,
			CursorIndexAfter:  2,
			CursorAdvanced:    true,
		},
	} {
		if got := decode.FilterExecutionSamples[i]; got != want {
			t.Fatalf("filter execution sample[%d] = %+v, want %+v", i, got, want)
		}
	}
	wantNone := []FieldFilter{{Key: "status", Value: "false"}}
	if got := decode.QueryNoneFields; !equalFieldFilters(got, wantNone) {
		t.Fatalf("query none fields = %v, want %v", got, wantNone)
	}
	if got := decode.MatchedNoneFields; !equalFieldFilters(got, wantNone) {
		t.Fatalf("matched none fields = %v, want %v", got, wantNone)
	}
	if len(decode.MissingNoneFields) != 0 {
		t.Fatalf("missing none fields = %v, want none", decode.MissingNoneFields)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 3; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/record", Time: times[0], Type: "record", OptimizedValue: "status=true,value=1.25", Matches: true},
		{Key: "sid:7/status", Time: times[0], Type: "boolean-full", OptimizedValue: "true", Matches: true},
		{Key: "sid:7/value", Time: times[0], Type: "float-full", OptimizedValue: "1.25", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if !containsStringWithPrefix(decode.Recommendations, "applied 1 TSSP NOT field filter") {
		t.Fatalf("recommendations = %v, want NOT field filter recommendation", decode.Recommendations)
	}
}

func TestTSSPFilterExecutionSamplesLimitAndRebase(t *testing.T) {
	blocks := map[string]tsspDetachedDataBlockInfo{
		"time": {
			Type:       "integer",
			Rows:       3,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"100", "200", "300"},
		},
		"value": {
			Type:       "integer",
			Rows:       3,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"1", "2", "3"},
		},
	}
	queryRange, err := NewTimeRange(100, 300)
	if err != nil {
		t.Fatal(err)
	}

	_, matchedRows, filterRows, stats, ok := tsspDataBlockFilterRows(blocks, []FieldFilter{{Key: "value", Op: ">", Value: "0"}}, nil, nil, 3, tsspTimeRange{Min: 100, Max: 300}, queryRange, "sid:7", 0, 2)
	if !ok {
		t.Fatal("filter rows should be available")
	}
	if got, want := matchedRows, 3; got != want {
		t.Fatalf("matched rows = %d, want %d", got, want)
	}
	if got, want := filterRows, 3; got != want {
		t.Fatalf("filter rows = %d, want %d", got, want)
	}
	if got, want := len(stats.FilterExecutionSamples), 2; got != want {
		t.Fatalf("filter execution samples = %d, want per-call cap %d", got, want)
	}

	var merged []DecodePathCursorStep
	appendTSSPFilterExecutionSamples(&merged, stats.FilterExecutionSamples, 4)
	if got, want := remainingTSSPExecutionSampleLimit(merged, 4), 2; got != want {
		t.Fatalf("remaining sample limit = %d, want %d", got, want)
	}
	_, _, _, secondStats, ok := tsspDataBlockFilterRows(blocks, []FieldFilter{{Key: "value", Op: ">", Value: "0"}}, nil, nil, 3, tsspTimeRange{Min: 100, Max: 300}, queryRange, "sid:8", 0, remainingTSSPExecutionSampleLimit(merged, 4))
	if !ok {
		t.Fatal("second filter rows should be available")
	}
	appendTSSPFilterExecutionSamples(&merged, secondStats.FilterExecutionSamples, 4)

	if got, want := len(merged), 4; got != want {
		t.Fatalf("merged filter execution samples = %d, want %d", got, want)
	}
	for i, want := range []struct {
		key         string
		value       string
		indexBefore int
		indexAfter  int
	}{
		{"sid:7/row:0", "row=0 time=100 required=1/1 any=0/0 none=0/0 skips=0/0/0 values=value=1 decision=required:value:>:0:match result=match", 0, 1},
		{"sid:7/row:1", "row=1 time=200 required=1/1 any=0/0 none=0/0 skips=0/0/0 values=value=2 decision=required:value:>:0:match result=match", 1, 2},
		{"sid:8/row:0", "row=0 time=100 required=1/1 any=0/0 none=0/0 skips=0/0/0 values=value=1 decision=required:value:>:0:match result=match", 2, 3},
		{"sid:8/row:1", "row=1 time=200 required=1/1 any=0/0 none=0/0 skips=0/0/0 values=value=2 decision=required:value:>:0:match result=match", 3, 4},
	} {
		got := merged[i]
		if got.Step != i+1 || got.Type != "tssp-filter-row-step" || got.Action != "filter_row_match" || got.Key != want.key || got.CandidateValue != want.value || got.CursorIndexBefore != want.indexBefore || got.CursorIndexAfter != want.indexAfter || !got.CursorAdvanced {
			t.Fatalf("merged filter execution sample[%d] = %+v, want key=%q value=%q indexes=%d->%d advanced", i, got, want.key, want.value, want.indexBefore, want.indexAfter)
		}
	}
}

func TestTSSPFilterExecutionSamplesIncludeSetPredicateDecision(t *testing.T) {
	blocks := map[string]tsspDetachedDataBlockInfo{
		"time": {
			Type:       "integer",
			Rows:       2,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"100", "200"},
		},
		"value": {
			Type:       "integer",
			Rows:       2,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"1", "2"},
		},
	}
	queryRange, err := NewTimeRange(100, 200)
	if err != nil {
		t.Fatal(err)
	}

	_, matchedRows, filterRows, stats, ok := tsspDataBlockFilterRows(blocks, []FieldFilter{{Key: "value", Op: "in", Value: "(1,3)"}}, nil, nil, 2, tsspTimeRange{Min: 100, Max: 200}, queryRange, "sid:7", 0, 2)
	if !ok {
		t.Fatal("filter rows should be available")
	}
	if got, want := matchedRows, 1; got != want {
		t.Fatalf("matched rows = %d, want %d", got, want)
	}
	if got, want := filterRows, 2; got != want {
		t.Fatalf("filter rows = %d, want %d", got, want)
	}
	for i, want := range []struct {
		action string
		value  string
	}{
		{"filter_row_match", "row=0 time=100 required=1/1 any=0/0 none=0/0 skips=0/0/0 values=value=1 decision=required:value:in:(1,3):match result=match"},
		{"filter_row_reject_required", "row=1 time=200 required=0/1 any=0/0 none=0/0 skips=0/0/0 values=value=2 decision=required:value:in:(1,3):miss result=reject_required"},
	} {
		got := stats.FilterExecutionSamples[i]
		if got.Step != i+1 || got.Type != "tssp-filter-row-step" || got.Action != want.action || got.CandidateValue != want.value || got.CursorIndexBefore != i || got.CursorIndexAfter != i+1 || !got.CursorAdvanced {
			t.Fatalf("filter execution sample[%d] = %+v, want action=%q value=%q indexes=%d->%d advanced", i, got, want.action, want.value, i, i+1)
		}
	}
}

func TestTSSPRangeExecutionSamplesLimitAndRebase(t *testing.T) {
	blocks := map[string]tsspDetachedDataBlockInfo{
		"time": {
			Type:       "integer",
			Rows:       3,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"100", "200", "300"},
		},
		"value": {
			Type:       "integer",
			Rows:       3,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"1", "2", "3"},
		},
	}
	queryRange, err := NewTimeRange(200, 200)
	if err != nil {
		t.Fatal(err)
	}

	_, matchedRows, filterRows, stats, ok := tsspDataBlockFilterRows(blocks, nil, nil, nil, 3, tsspTimeRange{Min: 100, Max: 300}, queryRange, "sid:7", 2, 0)
	if !ok {
		t.Fatal("range rows should be available")
	}
	if got, want := matchedRows, 1; got != want {
		t.Fatalf("matched rows = %d, want %d", got, want)
	}
	if got, want := filterRows, 1; got != want {
		t.Fatalf("filter rows = %d, want %d", got, want)
	}
	if got, want := len(stats.RangeExecutionSamples), 2; got != want {
		t.Fatalf("range execution samples = %d, want per-call cap %d", got, want)
	}

	var merged []DecodePathCursorStep
	appendTSSPRangeExecutionSamples(&merged, stats.RangeExecutionSamples, 4)
	if got, want := remainingTSSPExecutionSampleLimit(merged, 4), 2; got != want {
		t.Fatalf("remaining sample limit = %d, want %d", got, want)
	}
	_, _, _, secondStats, ok := tsspDataBlockFilterRows(blocks, nil, nil, nil, 3, tsspTimeRange{Min: 100, Max: 300}, queryRange, "sid:8", remainingTSSPExecutionSampleLimit(merged, 4), 0)
	if !ok {
		t.Fatal("second range rows should be available")
	}
	appendTSSPRangeExecutionSamples(&merged, secondStats.RangeExecutionSamples, 4)

	if got, want := len(merged), 4; got != want {
		t.Fatalf("merged range execution samples = %d, want %d", got, want)
	}
	for i, want := range []struct {
		key         string
		action      string
		value       string
		indexBefore int
		indexAfter  int
	}{
		{"sid:7/row:0", "range_row_reject", "row=0 time=100 range=200:200 result=reject_range", 0, 1},
		{"sid:7/row:1", "range_row_match", "row=1 time=200 range=200:200 result=match", 1, 2},
		{"sid:8/row:0", "range_row_reject", "row=0 time=100 range=200:200 result=reject_range", 2, 3},
		{"sid:8/row:1", "range_row_match", "row=1 time=200 range=200:200 result=match", 3, 4},
	} {
		got := merged[i]
		if got.Step != i+1 || got.Type != "tssp-range-row-step" || got.Action != want.action || got.Key != want.key || got.CandidateValue != want.value || got.CursorIndexBefore != want.indexBefore || got.CursorIndexAfter != want.indexAfter || !got.CursorAdvanced {
			t.Fatalf("merged range execution sample[%d] = %+v, want key=%q action=%q value=%q indexes=%d->%d advanced", i, got, want.key, want.action, want.value, want.indexBefore, want.indexAfter)
		}
	}
}

func TestTSSPRecordExecutionSamplesLimitAndRebase(t *testing.T) {
	blocks := map[string]tsspDetachedDataBlockInfo{
		"time": {
			Type:       "integer",
			Rows:       3,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"100", "200", "300"},
		},
		"status": {
			Type:       "boolean-full",
			Rows:       3,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"true", "false", "true"},
		},
		"value": {
			Type:       "integer",
			Rows:       3,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"1", "2", "3"},
		},
	}
	queryRange, err := NewTimeRange(100, 300)
	if err != nil {
		t.Fatal(err)
	}

	var outputs []DecodePathCursorOutput
	var merged []DecodePathCursorStep
	outputs, firstSteps, firstStats := appendTSSPDataProbeRecordSamples(outputs, "sid", 7, tsspTimeRange{Min: 100, Max: 300}, blocks, nil, queryRange, 5, remainingTSSPExecutionSampleLimit(merged, 5), 0, 0)
	appendTSSPRecordExecutionSamples(&merged, firstSteps, 5)
	if got, want := firstStats.Rows, 3; got != want {
		t.Fatalf("first record rows = %d, want %d", got, want)
	}
	if got, want := firstStats.Samples, 3; got != want {
		t.Fatalf("first record samples = %d, want %d", got, want)
	}
	if got, want := firstStats.Outputs, 3; got != want {
		t.Fatalf("first record outputs = %d, want %d", got, want)
	}
	if got, want := len(merged), 3; got != want {
		t.Fatalf("first record execution samples = %d, want %d", got, want)
	}
	if got, want := remainingTSSPExecutionSampleLimit(merged, 5), 2; got != want {
		t.Fatalf("remaining record execution sample limit = %d, want %d", got, want)
	}

	outputs, secondSteps, secondStats := appendTSSPDataProbeRecordSamples(outputs, "sid", 8, tsspTimeRange{Min: 100, Max: 300}, blocks, nil, queryRange, 5, remainingTSSPExecutionSampleLimit(merged, 5), 0, 0)
	appendTSSPRecordExecutionSamples(&merged, secondSteps, 5)
	if got, want := secondStats.Samples, 2; got != want {
		t.Fatalf("second record samples = %d, want remaining output cap %d", got, want)
	}
	if got, want := secondStats.Outputs, 3; got != want {
		t.Fatalf("second record outputs = %d, want full local record output count %d", got, want)
	}
	if got, want := len(outputs), 5; got != want {
		t.Fatalf("record output samples = %d, want %d", got, want)
	}
	if got, want := len(merged), 5; got != want {
		t.Fatalf("merged record execution samples = %d, want %d", got, want)
	}
	for i, want := range []struct {
		key         string
		value       string
		indexBefore int
		indexAfter  int
	}{
		{"sid:7/record/row:0", "row=0 local_input=0 local_output=0 time=100 range=100:300 columns=2 values=status=true,value=1 result=output", 0, 1},
		{"sid:7/record/row:1", "row=1 local_input=1 local_output=1 time=200 range=100:300 columns=2 values=status=false,value=2 result=output", 1, 2},
		{"sid:7/record/row:2", "row=2 local_input=2 local_output=2 time=300 range=100:300 columns=2 values=status=true,value=3 result=output", 2, 3},
		{"sid:8/record/row:0", "row=0 local_input=0 local_output=0 time=100 range=100:300 columns=2 values=status=true,value=1 result=output", 3, 4},
		{"sid:8/record/row:1", "row=1 local_input=1 local_output=1 time=200 range=100:300 columns=2 values=status=false,value=2 result=output", 4, 5},
	} {
		got := merged[i]
		if got.Step != i+1 || got.Type != "tssp-record-row-step" || got.Action != "record_row_output" || got.Key != want.key || got.CandidateValue != want.value || got.CursorIndexBefore != want.indexBefore || got.CursorIndexAfter != want.indexAfter || !got.CursorAdvanced {
			t.Fatalf("merged record execution sample[%d] = %+v, want key=%q value=%q indexes=%d->%d advanced", i, got, want.key, want.value, want.indexBefore, want.indexAfter)
		}
	}
}

func TestTSSPRecordExecutionSamplesDoNotRequireOutputSampleBudget(t *testing.T) {
	blocks := map[string]tsspDetachedDataBlockInfo{
		"time": {
			Type:       "integer",
			Rows:       2,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"100", "200"},
		},
		"status": {
			Type:       "boolean-full",
			Rows:       2,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"true", "false"},
		},
		"value": {
			Type:       "integer",
			Rows:       2,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"1", "2"},
		},
	}
	queryRange, err := NewTimeRange(100, 200)
	if err != nil {
		t.Fatal(err)
	}

	outputs := []DecodePathCursorOutput{{Key: "existing"}}
	outputs, steps, stats := appendTSSPDataProbeRecordSamples(outputs, "sid", 7, tsspTimeRange{Min: 100, Max: 200}, blocks, nil, queryRange, 1, 2, 9, 11)
	if got, want := len(outputs), 1; got != want {
		t.Fatalf("record output samples = %d, want unchanged cap %d", got, want)
	}
	if got, want := stats.Samples, 0; got != want {
		t.Fatalf("record output sample count = %d, want %d", got, want)
	}
	if got, want := stats.Outputs, 2; got != want {
		t.Fatalf("record outputs = %d, want %d", got, want)
	}
	if got, want := len(steps), 2; got != want {
		t.Fatalf("record execution samples = %d, want %d", got, want)
	}
	for i, want := range []struct {
		key   string
		value string
	}{
		{"sid:7/record/row:0", "row=0 local_input=11 local_output=9 time=100 range=100:200 columns=2 values=status=true,value=1 result=output"},
		{"sid:7/record/row:1", "row=1 local_input=12 local_output=10 time=200 range=100:200 columns=2 values=status=false,value=2 result=output"},
	} {
		got := steps[i]
		if got.Step != i+1 || got.Type != "tssp-record-row-step" || got.Action != "record_row_output" || got.Key != want.key || got.CandidateValue != want.value || got.CursorIndexBefore != i || got.CursorIndexAfter != i+1 || !got.CursorAdvanced {
			t.Fatalf("record execution sample[%d] = %+v, want key=%q value=%q indexes=%d->%d advanced", i, got, want.key, want.value, i, i+1)
		}
	}
}

func TestTSSPRecordMaterializationStatsCountRangeAndFilterRejects(t *testing.T) {
	blocks := map[string]tsspDetachedDataBlockInfo{
		"time": {
			Type:       "integer",
			Rows:       4,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"100", "200", "300", "400"},
		},
		"status": {
			Type:       "boolean-full",
			Rows:       4,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"true", "true", "false", "true"},
		},
		"value": {
			Type:       "integer",
			Rows:       4,
			RowsKnown:  true,
			ValueKnown: true,
			Values:     []string{"1", "2", "3", "4"},
		},
	}
	queryRange, err := NewTimeRange(150, 350)
	if err != nil {
		t.Fatal(err)
	}
	outputs, steps, stats := appendTSSPDataProbeRecordSamples(nil, "sid", 7, tsspTimeRange{Min: 100, Max: 400}, blocks, []bool{false, true, false, true}, queryRange, 4, 4, 0, 0)
	if got, want := stats.Rows, 4; got != want {
		t.Fatalf("record rows = %d, want %d", got, want)
	}
	if got, want := stats.Outputs, 1; got != want {
		t.Fatalf("record outputs = %d, want %d", got, want)
	}
	if got, want := stats.Samples, 1; got != want {
		t.Fatalf("record samples = %d, want %d", got, want)
	}
	if got, want := stats.RangeRejects, 2; got != want {
		t.Fatalf("record range rejects = %d, want %d", got, want)
	}
	if got, want := stats.FilterRejects, 1; got != want {
		t.Fatalf("record filter rejects = %d, want %d", got, want)
	}
	if got, want := len(outputs), 1; got != want {
		t.Fatalf("record output samples = %d, want %d", got, want)
	}
	if got, want := outputs[0].OutputOrdinal, 0; got != want {
		t.Fatalf("record output ordinal = %d, want %d", got, want)
	}
	if got, want := len(steps), 4; got != want {
		t.Fatalf("record execution samples = %d, want %d", got, want)
	}
	for i, want := range []struct {
		action string
		value  string
	}{
		{"record_row_range_reject", "row=0 local_input=0 local_output=none time=100 range=150:350 columns=2 values=status=true,value=1 result=range_reject"},
		{"record_row_output", "row=1 local_input=1 local_output=0 time=200 range=150:350 columns=2 values=status=true,value=2 result=output"},
		{"record_row_filter_reject", "row=2 local_input=2 local_output=none time=300 range=150:350 columns=2 values=status=false,value=3 result=filter_reject"},
		{"record_row_range_reject", "row=3 local_input=3 local_output=none time=400 range=150:350 columns=2 values=status=true,value=4 result=range_reject"},
	} {
		if got := steps[i]; got.Action != want.action || got.CandidateValue != want.value {
			t.Fatalf("record execution sample[%d] = %+v, want action=%q value=%q", i, got, want.action, want.value)
		}
	}
}

func TestTSSPRecordExecutionCandidateValueIncludesOptionalQueryRange(t *testing.T) {
	queryRange, err := NewTimeRange(100, 200)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tsspRecordExecutionCandidateValue(1, 12, 4, 150, 2, "status=true,value=1", queryRange, "output"), "row=1 local_input=12 local_output=4 time=150 range=100:200 columns=2 values=status=true,value=1 result=output"; got != want {
		t.Fatalf("record execution candidate = %q, want %q", got, want)
	}
	if got, want := tsspRecordExecutionCandidateValue(1, 12, 4, 150, 2, "status=true,value=1", TimeRange{}, "output"), "row=1 local_input=12 local_output=4 time=150 columns=2 values=status=true,value=1 result=output"; got != want {
		t.Fatalf("record execution candidate without range = %q, want %q", got, want)
	}
	if got, want := tsspRecordExecutionCandidateValue(2, 13, -1, 175, 2, "status=false,value=2", queryRange, "filter_reject"), "row=2 local_input=13 local_output=none time=175 range=100:200 columns=2 values=status=false,value=2 result=filter_reject"; got != want {
		t.Fatalf("record execution reject candidate = %q, want %q", got, want)
	}
}

func TestAppendTSSPFileDecodePathSamplesRebasesRangeExecutionSamples(t *testing.T) {
	dst := &DecodePathSummary{}
	src1 := &DecodePathSummary{
		RangeExecutionSamples: []DecodePathCursorStep{
			{
				Step:              1,
				Type:              "tssp-range-row-step",
				Action:            "range_row_reject",
				Key:               "sid:7/row:0",
				CandidateValue:    "row=0 time=100 range=200:200 result=reject_range",
				CursorIndexBefore: 0,
				CursorIndexAfter:  1,
				CursorAdvanced:    true,
			},
			{
				Step:              2,
				Type:              "tssp-range-row-step",
				Action:            "range_row_match",
				Key:               "sid:7/row:1",
				CandidateValue:    "row=1 time=200 range=200:200 result=match",
				CursorIndexBefore: 1,
				CursorIndexAfter:  2,
				CursorAdvanced:    true,
			},
		},
	}
	src2 := &DecodePathSummary{
		RangeExecutionSamples: []DecodePathCursorStep{
			{
				Step:              1,
				Type:              "tssp-range-row-step",
				Action:            "range_row_reject",
				Key:               "sid:8/row:0",
				CandidateValue:    "row=0 time=100 range=200:200 result=reject_range",
				CursorIndexBefore: 0,
				CursorIndexAfter:  1,
				CursorAdvanced:    true,
			},
			{
				Step:              2,
				Type:              "tssp-range-row-step",
				Action:            "range_row_match",
				Key:               "sid:8/row:1",
				CandidateValue:    "row=1 time=200 range=200:200 result=match",
				CursorIndexBefore: 1,
				CursorIndexAfter:  2,
				CursorAdvanced:    true,
			},
		},
	}

	appendTSSPFileDecodePathSamples(dst, src1, "a.tssp", 3, nil)
	appendTSSPFileDecodePathSamples(dst, src2, "b.tssp", 3, nil)

	if got, want := len(dst.RangeExecutionSamples), 3; got != want {
		t.Fatalf("range execution samples = %d, want sample limit %d", got, want)
	}
	if got, want := len(dst.RangeExecutionActions), 2; got != want {
		t.Fatalf("range execution action count entries = %d, want %d: %+v", got, want, dst.RangeExecutionActions)
	}
	if got, want := dst.RangeExecutionActions["range_row_reject"], 2; got != want {
		t.Fatalf("range_row_reject action count = %d, want %d", got, want)
	}
	if got, want := dst.RangeExecutionActions["range_row_match"], 1; got != want {
		t.Fatalf("range_row_match action count = %d, want %d", got, want)
	}
	for i, want := range []struct {
		file        string
		key         string
		indexBefore int
		indexAfter  int
	}{
		{"a.tssp", "sid:7/row:0", 0, 1},
		{"a.tssp", "sid:7/row:1", 1, 2},
		{"b.tssp", "sid:8/row:0", 2, 3},
	} {
		got := dst.RangeExecutionSamples[i]
		if got.Step != i+1 || got.File != want.file || got.Key != want.key || got.CursorIndexBefore != want.indexBefore || got.CursorIndexAfter != want.indexAfter || !got.CursorAdvanced {
			t.Fatalf("range execution sample[%d] = %+v, want file=%q key=%q indexes=%d->%d advanced", i, got, want.file, want.key, want.indexBefore, want.indexAfter)
		}
	}
}

func TestAppendTSSPFileDecodePathSamplesRebasesRecordExecutionSamples(t *testing.T) {
	dst := &DecodePathSummary{}
	src1 := &DecodePathSummary{
		RecordExecutionSamples: []DecodePathCursorStep{
			{
				Step:              1,
				Type:              "tssp-record-row-step",
				Action:            "record_row_output",
				Key:               "sid:7/record/row:0",
				CandidateValue:    "row=0 local_input=0 local_output=0 time=100 values=status=true,value=1 result=output",
				CursorIndexBefore: 0,
				CursorIndexAfter:  1,
				CursorAdvanced:    true,
			},
			{
				Step:              2,
				Type:              "tssp-record-row-step",
				Action:            "record_row_output",
				Key:               "sid:7/record/row:1",
				CandidateValue:    "row=1 local_input=1 local_output=1 time=200 values=status=false,value=2 result=output",
				CursorIndexBefore: 1,
				CursorIndexAfter:  2,
				CursorAdvanced:    true,
			},
		},
	}
	src2 := &DecodePathSummary{
		RecordExecutionSamples: []DecodePathCursorStep{
			{
				Step:              1,
				Type:              "tssp-record-row-step",
				Action:            "record_row_output",
				Key:               "sid:8/record/row:0",
				CandidateValue:    "row=0 local_input=0 local_output=0 time=100 values=status=true,value=1 result=output",
				CursorIndexBefore: 0,
				CursorIndexAfter:  1,
				CursorAdvanced:    true,
			},
			{
				Step:              2,
				Type:              "tssp-record-row-step",
				Action:            "record_row_output",
				Key:               "sid:8/record/row:1",
				CandidateValue:    "row=1 local_input=1 local_output=1 time=200 values=status=false,value=2 result=output",
				CursorIndexBefore: 1,
				CursorIndexAfter:  2,
				CursorAdvanced:    true,
			},
		},
	}

	appendTSSPFileDecodePathSamples(dst, src1, "a.tssp", 3, nil)
	appendTSSPFileDecodePathSamples(dst, src2, "b.tssp", 3, nil)

	if got, want := len(dst.RecordExecutionSamples), 3; got != want {
		t.Fatalf("record execution samples = %d, want sample limit %d", got, want)
	}
	if got, want := len(dst.RecordExecutionActions), 1; got != want {
		t.Fatalf("record execution action count entries = %d, want %d: %+v", got, want, dst.RecordExecutionActions)
	}
	if got, want := dst.RecordExecutionActions["record_row_output"], 3; got != want {
		t.Fatalf("record_row_output action count = %d, want %d", got, want)
	}
	for i, want := range []struct {
		file        string
		key         string
		value       string
		indexBefore int
		indexAfter  int
	}{
		{"a.tssp", "sid:7/record/row:0", "row=0 local_input=0 local_output=0 time=100 values=status=true,value=1 result=output", 0, 1},
		{"a.tssp", "sid:7/record/row:1", "row=1 local_input=1 local_output=1 time=200 values=status=false,value=2 result=output", 1, 2},
		{"b.tssp", "sid:8/record/row:0", "row=0 local_input=0 local_output=0 time=100 values=status=true,value=1 result=output", 2, 3},
	} {
		got := dst.RecordExecutionSamples[i]
		if got.Step != i+1 || got.File != want.file || got.Type != "tssp-record-row-step" || got.Action != "record_row_output" || got.Key != want.key || got.CandidateValue != want.value || got.CursorIndexBefore != want.indexBefore || got.CursorIndexAfter != want.indexAfter || !got.CursorAdvanced {
			t.Fatalf("record execution sample[%d] = %+v, want file=%q key=%q value=%q indexes=%d->%d advanced", i, got, want.file, want.key, want.value, want.indexBefore, want.indexAfter)
		}
	}
}

func TestAnalyzeTSSPNoneFieldFilterCombinesWithRequiredAndAnyFilters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: ">", Value: "1.0"}},
		QueryAnyFields:   []FieldFilter{{Key: "status", Value: "false"}},
		QueryNoneFields:  []FieldFilter{{Key: "value", Value: "2.5"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "0"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "2"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: ">", Value: "1.0"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.QueryAnyFields, []FieldFilter{{Key: "status", Value: "false"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query any fields = %v, want %v", got, want)
	}
	if got, want := decode.QueryNoneFields, []FieldFilter{{Key: "value", Value: "2.5"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query none fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for _, prefix := range []string{
		"applied 1 TSSP field filter",
		"applied 1 TSSP OR field filter",
		"applied 1 TSSP NOT field filter",
		"TSSP field filters matched 0 of 2 decoded record row",
	} {
		if !containsStringWithPrefix(decode.Recommendations, prefix) {
			t.Fatalf("recommendations = %v, want prefix %q", decode.Recommendations, prefix)
		}
	}
}

func TestAnalyzeTSSPFieldFilterWithColumnProjectionReadsPredicateColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryColumns:     []string{"value"},
		QueryFields:      []FieldFilter{{Key: "status", Value: "true"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryColumns, []string{"value"}; !equalStrings(got, want) {
		t.Fatalf("query columns = %v, want %v", got, want)
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "status", Value: "true"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if !readAtRangesContainColumn(decode.Samples[0].OptimizedReadAtRanges, "status") {
		t.Fatalf("optimized ReadAt ranges = %v, want predicate column status", decode.Samples[0].OptimizedReadAtRanges)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordSamples, 1; got != want {
		t.Fatalf("data block probe record samples = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordOutputs, 1; got != want {
		t.Fatalf("data block probe record outputs = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPNoneFieldFilterWithColumnProjectionReadsPredicateColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryColumns:     []string{"value"},
		QueryNoneFields:  []FieldFilter{{Key: "status", Value: "false"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryNoneFields, []FieldFilter{{Key: "status", Value: "false"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query none fields = %v, want %v", got, want)
	}
	if !readAtRangesContainColumn(decode.Samples[0].OptimizedReadAtRanges, "status") {
		t.Fatalf("optimized ReadAt ranges = %v, want NOT predicate column status", decode.Samples[0].OptimizedReadAtRanges)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPAnyFieldFilterWithColumnProjectionReadsPredicateColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryColumns:     []string{"value"},
		QueryAnyFields:   []FieldFilter{{Key: "status", Value: "true"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryAnyFields, []FieldFilter{{Key: "status", Value: "true"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query any fields = %v, want %v", got, want)
	}
	if !readAtRangesContainColumn(decode.Samples[0].OptimizedReadAtRanges, "status") {
		t.Fatalf("optimized ReadAt ranges = %v, want OR predicate column status", decode.Samples[0].OptimizedReadAtRanges)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMissingColumnReturnsZeroRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "missing", Value: "true"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "0"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if len(decode.MatchedFields) != 0 {
		t.Fatalf("matched fields = %v, want none", decode.MatchedFields)
	}
	if got, want := decode.MissingFields, []FieldFilter{{Key: "missing", Value: "true"}}; !equalFieldFilters(got, want) {
		t.Fatalf("missing fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if !decode.Samples[0].ValueOutputAvailable {
		t.Fatalf("value output available = false, want true for a decoded zero-row filter result")
	}
	if got, want := decode.Samples[0].Reason, "segment_overlap"; got != want {
		t.Fatalf("sample reason = %q, want %q", got, want)
	}
}

func TestAnalyzeTSSPNoneFieldFilterMissingColumnKeepsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryNoneFields:  []FieldFilter{{Key: "missing", Value: "true"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "2"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "0"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if len(decode.MatchedNoneFields) != 0 {
		t.Fatalf("matched none fields = %v, want none", decode.MatchedNoneFields)
	}
	if got, want := decode.MissingNoneFields, []FieldFilter{{Key: "missing", Value: "true"}}; !equalFieldFilters(got, want) {
		t.Fatalf("missing none fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 4; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesFloatEquivalentText(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Value: "1.250"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].Time, times[0]; got != want {
		t.Fatalf("first cursor output sample time = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesFloatComparison(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: ">", Value: "2.0"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: ">", Value: "2.0"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].Time, times[1]; got != want {
		t.Fatalf("first cursor output sample time = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesFloatBetween(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "between", Value: "(1.0,2.0)"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: "between", Value: "(1.0,2.0)"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 3; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/record", Time: times[0], Type: "record", OptimizedValue: "status=true,value=1.25", Matches: true},
		{Key: "sid:7/status", Time: times[0], Type: "boolean-full", OptimizedValue: "true", Matches: true},
		{Key: "sid:7/value", Time: times[0], Type: "float-full", OptimizedValue: "1.25", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if got, want := len(decode.RecordExecutionSamples), 2; got != want {
		t.Fatalf("record execution samples = %d, want %d", got, want)
	}
	for i, wantStep := range []DecodePathCursorStep{
		{
			Step:              1,
			Type:              "tssp-record-row-step",
			Action:            "record_row_output",
			Key:               "sid:7/record/row:0",
			CandidateValue:    fmt.Sprintf("row=0 local_input=0 local_output=0 time=%d range=%d:%d columns=2 values=status=true,value=1.25 result=output", times[0], times[0], times[len(times)-1]),
			CursorIndexBefore: 0,
			CursorIndexAfter:  1,
			CursorAdvanced:    true,
		},
		{
			Step:              2,
			Type:              "tssp-record-row-step",
			Action:            "record_row_filter_reject",
			Key:               "sid:7/record/row:1",
			CandidateValue:    fmt.Sprintf("row=1 local_input=1 local_output=none time=%d range=%d:%d columns=2 values=status=false,value=2.5 result=filter_reject", times[1], times[0], times[len(times)-1]),
			CursorIndexBefore: 1,
			CursorIndexAfter:  2,
			CursorAdvanced:    true,
		},
	} {
		if got := decode.RecordExecutionSamples[i]; got != wantStep {
			t.Fatalf("record execution sample[%d] = %+v, want %+v", i, got, wantStep)
		}
	}
	if !containsString(decode.Recommendations, "TSSP record execution samples show local decoded-row materialization steps") {
		t.Fatalf("recommendations = %v, want record execution sample recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesFloatInSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "in", Value: "(1.250,2.5)"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "2"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: "in", Value: "(1.250,2.5)"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordSamples, 2; got != want {
		t.Fatalf("data block probe record samples = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordOutputs, 2; got != want {
		t.Fatalf("data block probe record outputs = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].Time, times[0]; got != want {
		t.Fatalf("first cursor output sample time = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[1].Time, times[1]; got != want {
		t.Fatalf("second cursor output sample time = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "applied 1 TSSP field filter") {
		t.Fatalf("recommendations = %v, want field filter recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesBooleanInequality(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "status", Op: "!=", Value: "true"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "status", Op: "!=", Value: "true"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].Time, times[1]; got != want {
		t.Fatalf("first cursor output sample time = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesBooleanNotInSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "status", Op: "not in", Value: "(true)"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "status", Op: "not-in", Value: "(true)"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].Time, times[1]; got != want {
		t.Fatalf("first cursor output sample time = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesStringInSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "in", Value: "(blue,green)"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: "in", Value: "(blue,green)"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           444,
		Type:           "string-full",
		OptimizedValue: "blue",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "applied 1 TSSP field filter") {
		t.Fatalf("recommendations = %v, want field filter recommendation", decode.Recommendations)
	}
}

func TestFieldFilterSetValuesParsesQuotedLiterals(t *testing.T) {
	got := fieldFilterSetValues(`("red,primary","blue)","","quote \"x\"",plain)`)
	want := []string{"red,primary", "blue)", "", `quote "x"`, "plain"}
	if len(got) != len(want) {
		t.Fatalf("set values = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("set value %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestAnalyzeTSSPFieldFilterMatchesQuotedStringInSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullValues(path, []string{"red,primary", "blue)"}, 0); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "in", Value: `("red,primary","blue)")`}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "2"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_output_points"], "2"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "string-full", OptimizedValue: "red,primary", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "string-full", OptimizedValue: "blue)", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestAnalyzeTSSPFieldFilterMatchesQuoteWrappedStringInSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullValues(path, []string{`"red"`, "plain"}, 0); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "in", Value: `("\"red\"")`}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           333,
		Type:           "string-full",
		OptimizedValue: `"red"`,
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesQuotedStringEquality(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullValues(path, []string{"red,primary", "blue)"}, 0); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Value: `"red,primary"`}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           333,
		Type:           "string-full",
		OptimizedValue: "red,primary",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesStringComparison(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "<", Value: "red"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: "<", Value: "red"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRows, 2; got != want {
		t.Fatalf("data block probe filter rows = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterMatches, 1; got != want {
		t.Fatalf("data block probe filter matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRejects, 1; got != want {
		t.Fatalf("data block probe filter rejects = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           444,
		Type:           "string-full",
		OptimizedValue: "blue",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesStringBetween(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "between", Value: "(blue,red)"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "2"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "string-full", OptimizedValue: "red", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "string-full", OptimizedValue: "blue", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestAnalyzeTSSPFieldFilterMatchesStringRegex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "=~", Value: "^(red|blue)$"}},
		QueryNoneFields:  []FieldFilter{{Key: "value", Op: "!~", Value: "e$"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: "=~", Value: "^(red|blue)$"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.QueryNoneFields, []FieldFilter{{Key: "value", Op: "!~", Value: "e$"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query none fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           444,
		Type:           "string-full",
		OptimizedValue: "blue",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesStringContains(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "contains", Value: "e"}, {Key: "value", Op: "not contains", Value: "r"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	wantFields := []FieldFilter{{Key: "value", Op: "contains", Value: "e"}, {Key: "value", Op: "not-contains", Value: "r"}}
	if got := decode.QueryFields; !equalFieldFilters(got, wantFields) {
		t.Fatalf("query fields = %v, want %v", got, wantFields)
	}
	if got, want := decode.DataBlockProbeFilterOps["contains"], 2; got != want {
		t.Fatalf("contains operator evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterOps["not-contains"], 2; got != want {
		t.Fatalf("not-contains operator evaluations = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           444,
		Type:           "string-full",
		OptimizedValue: "blue",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
	if !containsString(decode.Recommendations, "TSSP decoded-row field predicate operators: contains:2 not-contains:2") {
		t.Fatalf("recommendations = %v, want contains operator recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesStringLike(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "like", Value: "b%"}, {Key: "value", Op: "not like", Value: "r_d"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	wantFields := []FieldFilter{{Key: "value", Op: "like", Value: "b%"}, {Key: "value", Op: "not-like", Value: "r_d"}}
	if got := decode.QueryFields; !equalFieldFilters(got, wantFields) {
		t.Fatalf("query fields = %v, want %v", got, wantFields)
	}
	if got, want := decode.DataBlockProbeFilterOps["like"], 2; got != want {
		t.Fatalf("like operator evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterOps["not-like"], 1; got != want {
		t.Fatalf("not-like operator evaluations = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           444,
		Type:           "string-full",
		OptimizedValue: "blue",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesStringPrefixSuffix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:     FormatTSSP,
		QueryRange: queryRange,
		QueryFields: []FieldFilter{
			{Key: "value", Op: "starts_with", Value: "b"},
			{Key: "value", Op: "ends-with", Value: "e"},
			{Key: "value", Op: "not_starts_with", Value: "r"},
			{Key: "value", Op: "not_ends_with", Value: "d"},
		},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	wantFields := []FieldFilter{
		{Key: "value", Op: "ends-with", Value: "e"},
		{Key: "value", Op: "not-ends-with", Value: "d"},
		{Key: "value", Op: "not-starts-with", Value: "r"},
		{Key: "value", Op: "starts-with", Value: "b"},
	}
	if got := decode.QueryFields; !equalFieldFilters(got, wantFields) {
		t.Fatalf("query fields = %v, want %v", got, wantFields)
	}
	for op, want := range map[string]int{
		"starts-with":     1,
		"ends-with":       2,
		"not-starts-with": 1,
		"not-ends-with":   1,
	} {
		if got := decode.DataBlockProbeFilterOps[op]; got != want {
			t.Fatalf("%s operator evaluations = %d, want %d", op, got, want)
		}
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           444,
		Type:           "string-full",
		OptimizedValue: "blue",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPFieldNoneFilterMatchesStringNotContains(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "contains", Value: "e"}},
		QueryNoneFields:  []FieldFilter{{Key: "value", Op: "not-contains", Value: "r"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "2"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: "contains", Value: "e"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.QueryNoneFields, []FieldFilter{{Key: "value", Op: "not-contains", Value: "r"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query none fields = %v, want %v", got, want)
	}
	if got, want := decode.DataBlockProbeFilterOps["contains"], 2; got != want {
		t.Fatalf("contains operator evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterOps["not-contains"], 2; got != want {
		t.Fatalf("not-contains operator evaluations = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "sid:7/value",
		Time:           333,
		Type:           "string-full",
		OptimizedValue: "red",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestTSSPDataBlockLiteralStringOnlyOperatorsMatchNonNullStringBlocks(t *testing.T) {
	stringBlock := tsspDetachedDataBlockInfo{
		Type:         "string-full",
		Rows:         2,
		RowsKnown:    true,
		Values:       []string{"blue", ""},
		ValuePresent: []bool{true, false},
		ValueKnown:   true,
	}
	if !tsspDataBlockLiteralMatches(stringBlock, 0, "contains", "lu") {
		t.Fatal("string contains did not match decoded string value")
	}
	if !tsspDataBlockLiteralMatches(stringBlock, 0, "not-contains", "zz") {
		t.Fatal("string not-contains did not match decoded string value")
	}
	if !tsspDataBlockLiteralMatches(stringBlock, 0, "like", "b%e") {
		t.Fatal("string like did not match decoded string value")
	}
	if !tsspDataBlockLiteralMatches(stringBlock, 0, "not-like", "r_d") {
		t.Fatal("string not-like did not match decoded string value")
	}
	if !tsspDataBlockLiteralMatches(stringBlock, 0, "starts-with", "bl") {
		t.Fatal("string starts-with did not match decoded string value")
	}
	if !tsspDataBlockLiteralMatches(stringBlock, 0, "not-starts-with", "zz") {
		t.Fatal("string not-starts-with did not match decoded string value")
	}
	if !tsspDataBlockLiteralMatches(stringBlock, 0, "ends-with", "ue") {
		t.Fatal("string ends-with did not match decoded string value")
	}
	if !tsspDataBlockLiteralMatches(stringBlock, 0, "not-ends-with", "zz") {
		t.Fatal("string not-ends-with did not match decoded string value")
	}
	if tsspDataBlockLiteralMatches(stringBlock, 1, "contains", "u") {
		t.Fatal("contains matched decoded null sentinel")
	}
	if tsspDataBlockLiteralMatches(stringBlock, 1, "not-contains", "zz") {
		t.Fatal("not-contains matched decoded null sentinel")
	}
	if tsspDataBlockLiteralMatches(stringBlock, 1, "like", "%") {
		t.Fatal("like matched decoded null sentinel")
	}
	if tsspDataBlockLiteralMatches(stringBlock, 1, "not-like", "zz") {
		t.Fatal("not-like matched decoded null sentinel")
	}
	if tsspDataBlockLiteralMatches(stringBlock, 1, "starts-with", "b") {
		t.Fatal("starts-with matched decoded null sentinel")
	}
	if tsspDataBlockLiteralMatches(stringBlock, 1, "not-starts-with", "zz") {
		t.Fatal("not-starts-with matched decoded null sentinel")
	}
	if tsspDataBlockLiteralMatches(stringBlock, 1, "ends-with", "e") {
		t.Fatal("ends-with matched decoded null sentinel")
	}
	if tsspDataBlockLiteralMatches(stringBlock, 1, "not-ends-with", "zz") {
		t.Fatal("not-ends-with matched decoded null sentinel")
	}

	integerBlock := tsspDetachedDataBlockInfo{
		Type:       "integer-full",
		Rows:       1,
		RowsKnown:  true,
		Values:     []string{"123"},
		ValueKnown: true,
	}
	if tsspDataBlockLiteralMatches(integerBlock, 0, "contains", "2") {
		t.Fatal("contains matched non-string block")
	}
	if tsspDataBlockLiteralMatches(integerBlock, 0, "not-contains", "9") {
		t.Fatal("not-contains matched non-string block")
	}
	if tsspDataBlockLiteralMatches(integerBlock, 0, "like", "1%") {
		t.Fatal("like matched non-string block")
	}
	if tsspDataBlockLiteralMatches(integerBlock, 0, "not-like", "9%") {
		t.Fatal("not-like matched non-string block")
	}
	if tsspDataBlockLiteralMatches(integerBlock, 0, "starts-with", "1") {
		t.Fatal("starts-with matched non-string block")
	}
	if tsspDataBlockLiteralMatches(integerBlock, 0, "not-starts-with", "9") {
		t.Fatal("not-starts-with matched non-string block")
	}
	if tsspDataBlockLiteralMatches(integerBlock, 0, "ends-with", "3") {
		t.Fatal("ends-with matched non-string block")
	}
	if tsspDataBlockLiteralMatches(integerBlock, 0, "not-ends-with", "9") {
		t.Fatal("not-ends-with matched non-string block")
	}
}

func TestTSSPStringLikeMatchesSQLWildcards(t *testing.T) {
	for _, tc := range []struct {
		value   string
		pattern string
		want    bool
	}{
		{value: "blue", pattern: "b%", want: true},
		{value: "blue", pattern: "b__e", want: true},
		{value: "blue", pattern: "%u%", want: true},
		{value: "blue", pattern: "b_e", want: false},
		{value: "blue", pattern: "blue_", want: false},
		{value: "cafe", pattern: "ca_e", want: true},
		{value: "", pattern: "%", want: true},
		{value: "", pattern: "_", want: false},
	} {
		if got := tsspStringLike(tc.value, tc.pattern); got != tc.want {
			t.Fatalf("like(%q, %q) = %t, want %t", tc.value, tc.pattern, got, tc.want)
		}
	}
}

func TestAnalyzeTSSPFieldFilterOrderedBooleanDoesNotMatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "status", Op: ">", Value: "false"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "0"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if !decode.Samples[0].ValueOutputAvailable {
		t.Fatal("expected ordered boolean predicate to produce an available zero-row result")
	}
}

func TestAnalyzeTSSPFieldFilterBooleanBetweenDoesNotMatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "status", Op: "not-between", Value: "(false,true)"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "0"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPFieldFilterMatchesIntegerComparison(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithIntegerConstDeltaData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 555)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "<=", Value: "100"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_output_points"], "2"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "integer-full", OptimizedValue: "99", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "integer-full", OptimizedValue: "100", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestAnalyzeTSSPFieldFilterMatchesIntegerBetween(t *testing.T) {
	for _, tc := range []struct {
		name              string
		filter            FieldFilter
		wantOutputPoints  string
		wantSamples       []DecodePathCursorOutput
		wantValueOutCount int
	}{
		{
			name:             "between",
			filter:           FieldFilter{Key: "value", Op: "between", Value: "(99,100)"},
			wantOutputPoints: "2",
			wantSamples: []DecodePathCursorOutput{
				{Key: "sid:7/value", Time: 333, Type: "integer-full", OptimizedValue: "99", Matches: true},
				{Key: "sid:7/value", Time: 444, Type: "integer-full", OptimizedValue: "100", Matches: true},
			},
			wantValueOutCount: 2,
		},
		{
			name:             "between-no-parens",
			filter:           FieldFilter{Key: "value", Op: "between", Value: "99,100"},
			wantOutputPoints: "2",
			wantSamples: []DecodePathCursorOutput{
				{Key: "sid:7/value", Time: 333, Type: "integer-full", OptimizedValue: "99", Matches: true},
				{Key: "sid:7/value", Time: 444, Type: "integer-full", OptimizedValue: "100", Matches: true},
			},
			wantValueOutCount: 2,
		},
		{
			name:             "not-between",
			filter:           FieldFilter{Key: "value", Op: "not-between", Value: "(99,100)"},
			wantOutputPoints: "1",
			wantSamples: []DecodePathCursorOutput{
				{Key: "sid:7/value", Time: 555, Type: "integer-full", OptimizedValue: "101", Matches: true},
			},
			wantValueOutCount: 1,
		},
		{
			name:              "inverted",
			filter:            FieldFilter{Key: "value", Op: "between", Value: "(100,99)"},
			wantOutputPoints:  "0",
			wantValueOutCount: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
			if err := writeTestTSSPWithIntegerConstDeltaData(path); err != nil {
				t.Fatal(err)
			}
			queryRange, err := NewTimeRange(333, 555)
			if err != nil {
				t.Fatal(err)
			}

			report, err := Analyze(context.Background(), []string{path}, Options{
				Format:           FormatTSSP,
				QueryRange:       queryRange,
				QueryFields:      []FieldFilter{tc.filter},
				KeySampleLimit:   3,
				BlockSampleLimit: 8,
			})
			if err != nil {
				t.Fatal(err)
			}
			file := report.Files[0]
			if got := file.Extra["data_block_probe_output_points"]; got != tc.wantOutputPoints {
				t.Fatalf("data block probe output points = %q, want %q", got, tc.wantOutputPoints)
			}
			decode := file.DecodePath
			if decode == nil {
				t.Fatal("decode path is nil")
			}
			if got := decode.OptimizedValueOutputPoints; got != tc.wantValueOutCount {
				t.Fatalf("optimized value output points = %d, want %d", got, tc.wantValueOutCount)
			}
			if got, want := len(decode.CursorOutputSamples), len(tc.wantSamples); got != want {
				t.Fatalf("cursor output samples = %d, want %d", got, want)
			}
			for i, want := range tc.wantSamples {
				got := decode.CursorOutputSamples[i]
				if got != want {
					t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
				}
			}
		})
	}
}

func TestAnalyzeTSSPColumnProjectionLimitsAttachedReadAtAndSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryColumns:     []string{" value ", "value", ""},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_record_samples"], "0"; got != want {
		t.Fatalf("data block probe record samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_record_outputs"], "0"; got != want {
		t.Fatalf("data block probe record outputs = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "float-full:1,integer-full:1"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryColumns, []string{"value"}; !equalStrings(got, want) {
		t.Fatalf("query columns = %v, want %v", got, want)
	}
	if got, want := decode.MatchedColumns, []string{"value"}; !equalStrings(got, want) {
		t.Fatalf("matched columns = %v, want %v", got, want)
	}
	if len(decode.MissingColumns) != 0 {
		t.Fatalf("missing columns = %v, want none", decode.MissingColumns)
	}
	if got, want := decode.BaselineReadAtCalls, 3; got != want {
		t.Fatalf("baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 2; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadAtCalls, 1; got != want {
		t.Fatalf("saved ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordSamples, 0; got != want {
		t.Fatalf("data block probe record samples = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordOutputs, 0; got != want {
		t.Fatalf("data block probe record outputs = %d, want %d", got, want)
	}
	if got, want := len(decode.Samples[0].OptimizedReadAtRanges), 2; got != want {
		t.Fatalf("optimized ReadAt ranges = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].OptimizedReadAtRanges[0].Column, "value"; got != want {
		t.Fatalf("first ReadAt range column = %q, want %q", got, want)
	}
	if got, want := decode.Samples[0].OptimizedReadAtRanges[1].Column, "time"; got != want {
		t.Fatalf("second ReadAt range column = %q, want %q", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 2; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: times[0], Type: "float-full", OptimizedValue: "1.25", Matches: true},
		{Key: "sid:7/value", Time: times[1], Type: "float-full", OptimizedValue: "2.5", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if !containsStringWithPrefix(decode.Recommendations, "column projection requested for 1 TSSP column") {
		t.Fatalf("recommendations = %v, want column projection recommendation", decode.Recommendations)
	}
	if !containsStringWithPrefix(decode.Recommendations, "issue 2 TSSP ReadAt call(s) instead of 3") {
		t.Fatalf("recommendations = %v, want projected ReadAt recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPColumnProjectionReportsMissingColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QueryColumns:     []string{"missing"},
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "0"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if _, ok := file.Extra["data_block_probe_types"]; ok {
		t.Fatalf("data block probe types = %q, want absent", file.Extra["data_block_probe_types"])
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if len(decode.MatchedColumns) != 0 {
		t.Fatalf("matched columns = %v, want none", decode.MatchedColumns)
	}
	if got, want := decode.MissingColumns, []string{"missing"}; !equalStrings(got, want) {
		t.Fatalf("missing columns = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 0; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByProjectionBlocks, 1; got != want {
		t.Fatalf("skipped by projection blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedAfterRangeBlocks, 0; got != want {
		t.Fatalf("skipped after range blocks = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].Reason, "projected_columns_unavailable"; got != want {
		t.Fatalf("sample reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "projected_columns_unavailable"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "1 query column(s) were not found") {
		t.Fatalf("recommendations = %v, want missing column recommendation", decode.Recommendations)
	}
	if !containsStringWithPrefix(decode.Recommendations, "column projection excludes 1 in-range TSSP chunk") {
		t.Fatalf("recommendations = %v, want projection skip recommendation", decode.Recommendations)
	}
}

func TestInspectTSSPDataBlockPayloadNullableRegularFloat(t *testing.T) {
	encoded, err := testTSSPFloatFullEncodedPayload([]float64{1.25, 3.75}, 0)
	if err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	writeTestTSSPAttachedRegularBlockWithPresent(&payload, 3, encoded, []bool{true, false, true})

	info, ok, reason := inspectTSSPDataBlockPayload(payload.Bytes())
	if !ok {
		t.Fatalf("inspect TSSP data block payload failed: %s", reason)
	}
	if got, want := info.Type, "float"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 3; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
		t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
	}
	if got, want := info.Nulls, 1; got != want {
		t.Fatalf("nulls = %d, want %d", got, want)
	}
	if got, want := info.Value, "1.25"; got != want {
		t.Fatalf("first value = %q, want %q", got, want)
	}
	if got, want := info.Values, []string{"1.25", "", "3.75"}; !equalStrings(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
	wantPresent := []bool{true, false, true}
	if len(info.ValuePresent) != len(wantPresent) {
		t.Fatalf("value present length = %d, want %d", len(info.ValuePresent), len(wantPresent))
	}
	for i, want := range wantPresent {
		if got := info.ValuePresent[i]; got != want {
			t.Fatalf("value present %d = %t, want %t", i, got, want)
		}
	}
}

func TestInspectTSSPDataBlockPayloadNullableRegularString(t *testing.T) {
	encoded, err := testTSSPStringFullEncodedPayload([]string{"red", "", "blue"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	writeTestTSSPAttachedRegularBlockWithPresent(&payload, 4, encoded, []bool{true, false, true})

	info, ok, reason := inspectTSSPDataBlockPayload(payload.Bytes())
	if !ok {
		t.Fatalf("inspect TSSP data block payload failed: %s", reason)
	}
	if got, want := info.Type, "string"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 3; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
		t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
	}
	if got, want := info.Nulls, 1; got != want {
		t.Fatalf("nulls = %d, want %d", got, want)
	}
	if got, want := info.Value, "red"; got != want {
		t.Fatalf("first value = %q, want %q", got, want)
	}
	if got, want := info.Values, []string{"red", "", "blue"}; !equalStrings(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
	wantPresent := []bool{true, false, true}
	if len(info.ValuePresent) != len(wantPresent) {
		t.Fatalf("value present length = %d, want %d", len(info.ValuePresent), len(wantPresent))
	}
	for i, want := range wantPresent {
		if got := info.ValuePresent[i]; got != want {
			t.Fatalf("value present %d = %t, want %t", i, got, want)
		}
	}
}

func TestInspectTSSPDataBlockPayloadFloatFullUnsupportedCodecReason(t *testing.T) {
	var payload bytes.Buffer
	payload.WriteByte(31) // openGemini encoding.BlockFloatFull.
	writeUint32(&payload, 2)
	payload.WriteByte(7 << 4)

	info, ok, reason := inspectTSSPDataBlockPayload(payload.Bytes())
	if !ok {
		t.Fatalf("inspect ok = false, reason = %q", reason)
	}
	if got, want := info.Type, "float-full"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 2; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown {
		t.Fatal("expected rows to be known")
	}
	if info.ValueKnown {
		t.Fatal("expected unsupported codec value to be unknown")
	}
	if got, want := info.ValueReason, "float-full-codec-7"; got != want {
		t.Fatalf("value reason = %q, want %q", got, want)
	}
}

func TestDecodeTSSPFloatFullMLFSpecialModes(t *testing.T) {
	rawValues := []float64{1.25, -2.5}
	var raw bytes.Buffer
	raw.WriteByte(6 << 4)
	writeUint16(&raw, uint16(len(rawValues)))
	raw.WriteByte(tsspMLFCompressModeNone)
	raw.Write(testTSSPFloatRawBytes(rawValues))

	var zeros bytes.Buffer
	zeros.WriteByte(6 << 4)
	writeUint16(&zeros, 3)
	zeros.WriteByte(tsspMLFCompressModeAllZero)

	var same bytes.Buffer
	same.WriteByte(6 << 4)
	writeUint16(&same, 4)
	same.WriteByte(tsspMLFCompressModeSame)
	writeUint64(&same, math.Float64bits(7.5))

	for _, tc := range []struct {
		name    string
		encoded []byte
		rows    int
		want    []string
	}{
		{name: "none", encoded: raw.Bytes(), rows: len(rawValues), want: []string{"1.25", "-2.5"}},
		{name: "all-zero", encoded: zeros.Bytes(), rows: 3, want: []string{"0", "0", "0"}},
		{name: "same", encoded: same.Bytes(), rows: 4, want: []string{"7.5", "7.5", "7.5", "7.5"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := decodeTSSPFloatFullValues(tc.encoded, tc.rows)
			if !ok {
				t.Fatal("decode returned false")
			}
			if !equalStrings(got, tc.want) {
				t.Fatalf("values = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAnalyzeTSSPSamplesAttachedFloatFullUnsupportedCodecReason(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	times, err := writeTestTSSPWithUnsupportedFloatFullCodec(path, 7)
	if err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: len(times) + 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "1"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_unknowns"], "1"; got != want {
		t.Fatalf("data block probe value unknowns = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_unknown_reasons"], "float-full-codec-7:1"; got != want {
		t.Fatalf("data block probe value unknown reasons = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "float-full:1,integer-full:1"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	if notices := strings.Join(file.Notices, "\n"); !strings.Contains(notices, "unavailable value samples: float-full-codec-7:1") {
		t.Fatalf("notices = %v, want unavailable value sample reason", file.Notices)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.DataBlockProbeValueBlocks, 1; got != want {
		t.Fatalf("decode data block probe value blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeValueUnknowns, 1; got != want {
		t.Fatalf("decode data block probe value unknowns = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeValidBlocks, 2; got != want {
		t.Fatalf("decode data block probe valid blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRowCountBlocks, 2; got != want {
		t.Fatalf("decode data block probe row count blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeOutputPoints, 0; got != want {
		t.Fatalf("decode data block probe output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeTypes["float-full"], 1; got != want {
		t.Fatalf("decode data block probe float-full blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeTypes["integer-full"], 1; got != want {
		t.Fatalf("decode data block probe integer-full blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeValueReasons["float-full-codec-7"], 1; got != want {
		t.Fatalf("decode data block probe value unknown reason = %d, want %d", got, want)
	}
	if got, want := decode.ValueOutputUnavailableBlocks, 1; got != want {
		t.Fatalf("value output unavailable blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "TSSP data block probe found 1 block(s) with unavailable value samples: float-full-codec-7:1") {
		t.Fatalf("recommendations = %v, want unavailable value sample recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPAttachedShortDataBlockBreakdown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithShortDataBlock(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_valid_blocks"], "1"; got != want {
		t.Fatalf("data block probe valid blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_failures"], "1"; got != want {
		t.Fatalf("data block probe failures = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_short_blocks"], "1"; got != want {
		t.Fatalf("data block probe short blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_unknown_block_types"], "0"; got != want {
		t.Fatalf("data block probe unknown block types = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_read_errors"], "0"; got != want {
		t.Fatalf("data block probe read errors = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_failure_reasons"], "segment_overlap_data_header_unavailable:1"; got != want {
		t.Fatalf("data block probe failure reasons = %q, want %q", got, want)
	}
	if !containsString(report.Notices, "TSSP data block probe found 1 invalid block") {
		t.Fatalf("notices = %v, want data block probe notice", report.Notices)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.DataBlockProbeBlocks, 2; got != want {
		t.Fatalf("data block probe blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeValidBlocks, 1; got != want {
		t.Fatalf("data block probe valid blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFailures, 1; got != want {
		t.Fatalf("data block probe failures = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeShortBlocks, 1; got != want {
		t.Fatalf("data block probe short blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeUnknownTypes, 0; got != want {
		t.Fatalf("data block probe unknown block types = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeReadErrors, 0; got != want {
		t.Fatalf("data block probe read errors = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFailureReasons["segment_overlap_data_header_unavailable"], 1; got != want {
		t.Fatalf("data block probe header failure reason = %d, want %d", got, want)
	}
	if got, want := decode.ValueOutputUnavailableBlocks, 1; got != want {
		t.Fatalf("value output unavailable blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].Reason, "segment_overlap_data_header_unavailable"; got != want {
		t.Fatalf("sample reason = %q, want %q", got, want)
	}
	if !containsString(decode.Recommendations, "TSSP data block probe found 1 invalid block(s): segment_overlap_data_header_unavailable:1") {
		t.Fatalf("recommendations = %v, want data block probe failure recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPSamplesAttachedIntegerFullUncompressedBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithIntegerFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "integer-full:2"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 2; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "integer-full", OptimizedValue: "99", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "integer-full", OptimizedValue: "100", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 2; got != want {
		t.Fatalf("decode sample value output points = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "sampled 2 TSSP value output") {
		t.Fatalf("recommendations = %v, want value output recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPSamplesAttachedIntegerFullConstDeltaBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithIntegerConstDeltaData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 555)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "integer-full:2"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 3; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 3; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "integer-full", OptimizedValue: "99", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "integer-full", OptimizedValue: "100", Matches: true},
		{Key: "sid:7/value", Time: 555, Type: "integer-full", OptimizedValue: "101", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 3; got != want {
		t.Fatalf("decode sample value output points = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "sampled 3 TSSP value output") {
		t.Fatalf("recommendations = %v, want value output recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPSamplesAttachedIntegerFullSimple8bBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithIntegerSimple8bData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 666)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "integer-full:2"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 3; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 3; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "integer-full", OptimizedValue: "99", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "integer-full", OptimizedValue: "100", Matches: true},
		{Key: "sid:7/value", Time: 666, Type: "integer-full", OptimizedValue: "102", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 3; got != want {
		t.Fatalf("decode sample value output points = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "sampled 3 TSSP value output") {
		t.Fatalf("recommendations = %v, want value output recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPSamplesAttachedIntegerFullZSTDBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithIntegerZSTDData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 666)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "integer-full:2"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 3; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 3; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "integer-full", OptimizedValue: "99", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "integer-full", OptimizedValue: "100", Matches: true},
		{Key: "sid:7/value", Time: 666, Type: "integer-full", OptimizedValue: "102", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 3; got != want {
		t.Fatalf("decode sample value output points = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "sampled 3 TSSP value output") {
		t.Fatalf("recommendations = %v, want value output recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPSamplesAttachedBooleanFullBitpackBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithBooleanFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "boolean-full:1,integer-full:1"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 2; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "boolean-full", OptimizedValue: "true", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "boolean-full", OptimizedValue: "false", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 2; got != want {
		t.Fatalf("decode sample value output points = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "sampled 2 TSSP value output") {
		t.Fatalf("recommendations = %v, want value output recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPSamplesAttachedStringFullUncompressedBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithStringFullData(path); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "integer-full:1,string-full:1"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 2; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "string-full", OptimizedValue: "red", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "string-full", OptimizedValue: "blue", Matches: true},
	} {
		got := decode.CursorOutputSamples[i]
		if got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 2; got != want {
		t.Fatalf("decode sample value output points = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "sampled 2 TSSP value output") {
		t.Fatalf("recommendations = %v, want value output recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPSamplesAttachedStringFullCompressedBlocks(t *testing.T) {
	values := []string{
		strings.Repeat("red-", 32),
		strings.Repeat("blue-", 24),
	}
	for _, tc := range []struct {
		name  string
		codec byte
	}{
		{name: "snappy", codec: 1},
		{name: "zstd", codec: 2},
		{name: "lz4", codec: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
			if err := writeTestTSSPWithStringFullValues(path, values, tc.codec); err != nil {
				t.Fatal(err)
			}
			queryRange, err := NewTimeRange(333, 444)
			if err != nil {
				t.Fatal(err)
			}

			report, err := Analyze(context.Background(), []string{path}, Options{
				Format:           FormatTSSP,
				QueryRange:       queryRange,
				KeySampleLimit:   3,
				BlockSampleLimit: 4,
			})
			if err != nil {
				t.Fatal(err)
			}
			file := report.Files[0]
			if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
				t.Fatalf("data block probe blocks = %q, want %q", got, want)
			}
			if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
				t.Fatalf("data block probe value blocks = %q, want %q", got, want)
			}
			if got, want := file.Extra["data_block_probe_types"], "integer-full:1,string-full:1"; got != want {
				t.Fatalf("data block probe types = %q, want %q", got, want)
			}
			decode := file.DecodePath
			if decode == nil {
				t.Fatal("decode path is nil")
			}
			if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
				t.Fatalf("optimized value output points = %d, want %d", got, want)
			}
			if got, want := len(decode.CursorOutputSamples), 2; got != want {
				t.Fatalf("cursor output samples = %d, want %d", got, want)
			}
			for i, want := range []DecodePathCursorOutput{
				{Key: "sid:7/value", Time: 333, Type: "string-full", OptimizedValue: values[0], Matches: true},
				{Key: "sid:7/value", Time: 444, Type: "string-full", OptimizedValue: values[1], Matches: true},
			} {
				got := decode.CursorOutputSamples[i]
				if got != want {
					t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
				}
			}
			if got, want := decode.Samples[0].ValueOutputPoints, 2; got != want {
				t.Fatalf("decode sample value output points = %d, want %d", got, want)
			}
		})
	}
}

func TestAnalyzeTSSPDecodePathDescendingCursor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSP(path); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		CursorDescending: true,
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("expected TSSP decode path summary")
	}
	if got, want := decode.Mode, "tssp-location-cursor-descending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.CursorSeekTime, int64(175); got != want {
		t.Fatalf("cursor seek time = %d, want %d", got, want)
	}
	if got, want := decode.LocationBlocks, 3; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadSegments, 1; got != want {
		t.Fatalf("optimized read segments = %d, want %d", got, want)
	}
	if got, want := decode.BaselineReadAtCalls, 6; got != want {
		t.Fatalf("baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 2; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadAtCalls, 4; got != want {
		t.Fatalf("saved ReadAt calls = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 3; got != want {
		t.Fatalf("cursor window samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].MinTime, int64(190); got != want {
		t.Fatalf("first cursor window min time = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "outside_query_range"; got != want {
		t.Fatalf("first cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[1].Reason, "segment_overlap"; got != want {
		t.Fatalf("second cursor window reason = %q, want %q", got, want)
	}
	if got, want := len(decode.Samples), 3; got != want {
		t.Fatalf("decode samples = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].MinTime, int64(190); got != want {
		t.Fatalf("first decode sample min time = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].BaselineReadAtCalls, 2; got != want {
		t.Fatalf("outside sample baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].OptimizedReadAtCalls, 0; got != want {
		t.Fatalf("outside sample optimized ReadAt calls = %d, want %d", got, want)
	}
	if got := len(decode.Samples[0].OptimizedReadAtRanges); got != 0 {
		t.Fatalf("outside sample optimized ReadAt ranges = %d, want none", got)
	}
}

func TestAnalyzeTSSPSnappyChunkMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithCompression(path, tsspChunkMetaCompressSnappy); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].Type, "chunk-meta"; got != want {
		t.Fatalf("first block type = %q, want %q", got, want)
	}
	if got, want := file.Extra["query_overlap_precision"], "chunk-meta"; got != want {
		t.Fatalf("query overlap precision = %q, want %q", got, want)
	}
	if got, want := file.Extra["chunk_meta_compress_supported"], "true"; got != want {
		t.Fatalf("chunk meta compression support = %q, want %q", got, want)
	}
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
	}
}

func TestAnalyzeTSSPLZ4ChunkMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithCompression(path, tsspChunkMetaCompressLZ4); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].Type, "chunk-meta"; got != want {
		t.Fatalf("first block type = %q, want %q", got, want)
	}
	if got, want := file.Extra["query_overlap_precision"], "chunk-meta"; got != want {
		t.Fatalf("query overlap precision = %q, want %q", got, want)
	}
}

func TestAnalyzeTSSPSelfCompressedChunkMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithCompression(path, tsspChunkMetaCompressSelf); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].Type, "chunk-meta"; got != want {
		t.Fatalf("first block type = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].ColumnCount, 2; got != want {
		t.Fatalf("first block column count = %d, want %d", got, want)
	}
	if got, want := file.Extra["query_overlap_precision"], "chunk-meta"; got != want {
		t.Fatalf("query overlap precision = %q, want %q", got, want)
	}
	if got, want := file.Extra["chunk_meta_header"], "2"; got != want {
		t.Fatalf("chunk meta header count = %q, want %q", got, want)
	}
	if got, want := file.Extra["chunk_meta_compress_supported"], "true"; got != want {
		t.Fatalf("chunk meta compression support = %q, want %q", got, want)
	}
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
	}
}

func TestAnalyzeTSSPDecodePathSeriesIDFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSP(path); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(300, 350)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QuerySeriesIDs:   []uint64{42, 9, 9},
		KeySampleLimit:   3,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.QueryOverlapBlocks, 2; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if !file.QueryOverlapsFile {
		t.Fatal("query overlaps file = false, want true for series-id hit")
	}
	if got, want := report.Summary.QueryOverlapFiles, 1; got != want {
		t.Fatalf("summary query overlap files = %d, want %d", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("expected TSSP decode path summary")
	}
	if got, want := decode.QuerySeriesIDs, []uint64{9, 42}; !equalUint64s(got, want) {
		t.Fatalf("query series ids = %v, want %v", got, want)
	}
	if got, want := decode.MatchedSeriesIDs, []uint64{9}; !equalUint64s(got, want) {
		t.Fatalf("matched series ids = %v, want %v", got, want)
	}
	if got, want := decode.MissingSeriesIDs, []uint64{42}; !equalUint64s(got, want) {
		t.Fatalf("missing series ids = %v, want %v", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 3; got != want {
		t.Fatalf("skipped by key blocks = %d, want %d", got, want)
	}
	if got, want := decode.LocationBlocks, 2; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadSegments, 2; got != want {
		t.Fatalf("optimized read segments = %d, want %d", got, want)
	}
	if got, want := decode.SkippedAfterRangeBlocks, 0; got != want {
		t.Fatalf("skipped after range blocks = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPSeriesIDFilterMissDoesNotOverlapFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSP(path); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(300, 350)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QuerySeriesIDs:   []uint64{42},
		KeySampleLimit:   3,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got := file.QueryOverlapBlocks; got != 0 {
		t.Fatalf("query overlap blocks = %d, want none", got)
	}
	if file.QueryOverlapsFile {
		t.Fatal("query overlaps file = true, want false for series-id miss")
	}
	if got := report.Summary.QueryOverlapFiles; got != 0 {
		t.Fatalf("summary query overlap files = %d, want none", got)
	}
	if got := report.Summary.QueryOverlapBlocks; got != 0 {
		t.Fatalf("summary query overlap blocks = %d, want none", got)
	}
}

func TestAnalyzeTSSPFileSetDecodePathAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "00000001-0001-00000000.tssp")
	path2 := filepath.Join(dir, "00000002-0001-00000000.tssp")
	if err := writeTestTSSP(path1); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPWithCompression(path2, tsspChunkMetaCompressSnappy); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QuerySeriesIDs:   []uint64{7},
		QueryColumns:     []string{"missing", "value"},
		QueryFields:      []FieldFilter{{Key: "value", Value: "99"}, {Key: "missing_field", Value: "x"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level TSSP decode path summary")
	}
	if got, want := decode.Mode, "tssp-file-set-location-cursor-ascending"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
	}
	if got, want := decode.QuerySeriesIDs, []uint64{7}; !equalUint64s(got, want) {
		t.Fatalf("query series ids = %v, want %v", got, want)
	}
	if got, want := decode.MatchedSeriesIDs, []uint64{7}; !equalUint64s(got, want) {
		t.Fatalf("matched series ids = %v, want %v", got, want)
	}
	if got, want := decode.QueryColumns, []string{"missing", "value"}; !equalStrings(got, want) {
		t.Fatalf("query columns = %v, want %v", got, want)
	}
	if got, want := decode.MatchedColumns, []string{"value"}; !equalStrings(got, want) {
		t.Fatalf("matched columns = %v, want %v", got, want)
	}
	if got, want := decode.MissingColumns, []string{"missing"}; !equalStrings(got, want) {
		t.Fatalf("missing columns = %v, want %v", got, want)
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "missing_field", Value: "x"}, {Key: "value", Value: "99"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.MatchedFields, []FieldFilter{{Key: "value", Value: "99"}}; !equalFieldFilters(got, want) {
		t.Fatalf("matched fields = %v, want %v", got, want)
	}
	if got, want := decode.MissingFields, []FieldFilter{{Key: "missing_field", Value: "x"}}; !equalFieldFilters(got, want) {
		t.Fatalf("missing fields = %v, want %v", got, want)
	}
	if len(decode.MissingSeriesIDs) != 0 {
		t.Fatalf("missing series ids = %v, want none", decode.MissingSeriesIDs)
	}
	if got, want := decode.LocationBlocks, 6; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 6; got != want {
		t.Fatalf("baseline decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("optimized decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.FilteredDecodeBlocks, 2; got != want {
		t.Fatalf("filtered decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBlocks, 4; got != want {
		t.Fatalf("saved decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeBytes, int64(576); got != want {
		t.Fatalf("baseline decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBytes, int64(192); got != want {
		t.Fatalf("optimized decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBytes, int64(384); got != want {
		t.Fatalf("saved decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeValues, 6; got != want {
		t.Fatalf("baseline decode values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeValues, 2; got != want {
		t.Fatalf("optimized decode values = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeValues, 4; got != want {
		t.Fatalf("saved decode values = %d, want %d", got, want)
	}
	if got, want := decode.IteratorCostFiles, 2; got != want {
		t.Fatalf("iterator cost files = %d, want %d", got, want)
	}
	if got, want := decode.IteratorCostBlocks, 6; got != want {
		t.Fatalf("iterator cost blocks = %d, want %d", got, want)
	}
	wantCostBytes := report.Files[0].DecodePath.IteratorCostBytes + report.Files[1].DecodePath.IteratorCostBytes
	if got := decode.IteratorCostBytes; got != wantCostBytes {
		t.Fatalf("iterator cost bytes = %d, want child sum %d", got, wantCostBytes)
	}
	if got, want := decode.BaselineReadSegments, 6; got != want {
		t.Fatalf("baseline read segments = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadSegments, 2; got != want {
		t.Fatalf("optimized read segments = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadSegments, 4; got != want {
		t.Fatalf("saved read segments = %d, want %d", got, want)
	}
	if got, want := decode.BaselineCursorReadCalls, 6; got != want {
		t.Fatalf("baseline cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedCursorReadCalls, 2; got != want {
		t.Fatalf("optimized cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.BaselineReadAtCalls, 12; got != want {
		t.Fatalf("baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 4; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadAtCalls, 8; got != want {
		t.Fatalf("saved ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 4; got != want {
		t.Fatalf("skipped by key blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedBeforeSeekBlocks, 2; got != want {
		t.Fatalf("skipped before seek blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedAfterRangeBlocks, 2; got != want {
		t.Fatalf("skipped after range blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 6; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := decode.LocationBlocksByType["chunk-meta"], 6; got != want {
		t.Fatalf("chunk-meta location count = %d, want %d", got, want)
	}
	if got, want := decode.DecodeBlocksByType["chunk-meta"], 2; got != want {
		t.Fatalf("chunk-meta decode count = %d, want %d", got, want)
	}
	if got, want := len(decode.Samples), 5; got != want {
		t.Fatalf("decode samples = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 5; got != want {
		t.Fatalf("cursor window samples = %d, want %d", got, want)
	}
	for _, sample := range decode.Samples {
		if sample.Path == "" {
			t.Fatalf("decode sample missing path: %+v", sample)
		}
	}
	for _, window := range decode.CursorWindows {
		if len(window.Files) == 0 {
			t.Fatalf("cursor window missing file: %+v", window)
		}
	}
	if got, want := len(decode.CursorExecutionSamples), 5; got != want {
		t.Fatalf("cursor execution samples = %d, want %d", got, want)
	}
	for i, sample := range decode.CursorExecutionSamples {
		if got, want := sample.Step, i+1; got != want {
			t.Fatalf("cursor execution sample[%d] step = %d, want %d", i, got, want)
		}
		if got, want := sample.CursorIndexBefore, i; got != want {
			t.Fatalf("cursor execution sample[%d] index before = %d, want %d", i, got, want)
		}
		if got, want := sample.CursorIndexAfter, i+1; got != want {
			t.Fatalf("cursor execution sample[%d] index after = %d, want %d", i, got, want)
		}
		if sample.File == "" {
			t.Fatalf("cursor execution sample[%d] missing file: %+v", i, sample)
		}
	}
	if decode.CursorExecutionSamples[4].CursorExhausted {
		t.Fatalf("last sampled file-set cursor execution sample = %+v, want not exhausted because sample limit clipped final location", decode.CursorExecutionSamples[4])
	}
}

func TestAnalyzeTSSPFileSetOutputSamplesIncludeFilesAndFinalDedup(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "00000001-0001-00000000.tssp")
	path2 := filepath.Join(dir, "00000002-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeTestTSSPWithMultiColumnRecordData(path2); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QuerySeriesIDs:   []uint64{7},
		QueryFields:      []FieldFilter{{Key: "status", Value: "true"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level TSSP decode path summary")
	}
	if got, want := decode.OptimizedValueOutputPoints, 2; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordSamples, 2; got != want {
		t.Fatalf("data block probe record samples = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRecordOutputs, 2; got != want {
		t.Fatalf("data block probe record outputs = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRows, 4; got != want {
		t.Fatalf("data block probe filter rows = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterMatches, 2; got != want {
		t.Fatalf("data block probe filter matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRejects, 2; got != want {
		t.Fatalf("data block probe filter rejects = %d, want %d", got, want)
	}
	if got, want := len(decode.FilterExecutionSamples), 4; got != want {
		t.Fatalf("filter execution samples = %d, want %d", got, want)
	}
	if got, want := len(decode.FilterExecutionActions), 2; got != want {
		t.Fatalf("filter execution action count entries = %d, want %d: %+v", got, want, decode.FilterExecutionActions)
	}
	if got, want := decode.FilterExecutionActions["filter_row_match"], 2; got != want {
		t.Fatalf("filter_row_match action count = %d, want %d", got, want)
	}
	if got, want := decode.FilterExecutionActions["filter_row_reject_required"], 2; got != want {
		t.Fatalf("filter_row_reject_required action count = %d, want %d", got, want)
	}
	for i, want := range []struct {
		file        string
		action      string
		indexBefore int
		indexAfter  int
	}{
		{path1, "filter_row_match", 0, 1},
		{path1, "filter_row_reject_required", 1, 2},
		{path2, "filter_row_match", 2, 3},
		{path2, "filter_row_reject_required", 3, 4},
	} {
		got := decode.FilterExecutionSamples[i]
		if got.Step != i+1 || got.File != want.file || got.Action != want.action || got.CursorIndexBefore != want.indexBefore || got.CursorIndexAfter != want.indexAfter || !got.CursorAdvanced {
			t.Fatalf("filter execution sample[%d] = %+v, want file=%q action=%q indexes=%d->%d advanced", i, got, want.file, want.action, want.indexBefore, want.indexAfter)
		}
	}
	if got, want := len(decode.RecordExecutionSamples), 4; got != want {
		t.Fatalf("record execution samples = %d, want %d", got, want)
	}
	if got, want := len(decode.RecordExecutionActions), 2; got != want {
		t.Fatalf("record execution action count entries = %d, want %d: %+v", got, want, decode.RecordExecutionActions)
	}
	if got, want := decode.RecordExecutionActions["record_row_output"], 2; got != want {
		t.Fatalf("record_row_output action count = %d, want %d", got, want)
	}
	if got, want := decode.RecordExecutionActions["record_row_filter_reject"], 2; got != want {
		t.Fatalf("record_row_filter_reject action count = %d, want %d", got, want)
	}
	for i, want := range []struct {
		file        string
		action      string
		key         string
		value       string
		indexBefore int
		indexAfter  int
	}{
		{path1, "record_row_output", "sid:7/record/row:0", fmt.Sprintf("row=0 local_input=0 local_output=0 time=%d range=%d:%d columns=2 values=status=true,value=1.25 result=output", times[0], times[0], times[len(times)-1]), 0, 1},
		{path1, "record_row_filter_reject", "sid:7/record/row:1", fmt.Sprintf("row=1 local_input=1 local_output=none time=%d range=%d:%d columns=2 values=status=false,value=2.5 result=filter_reject", times[1], times[0], times[len(times)-1]), 1, 2},
		{path2, "record_row_output", "sid:7/record/row:0", fmt.Sprintf("row=0 local_input=0 local_output=0 time=%d range=%d:%d columns=2 values=status=true,value=1.25 result=output", times[0], times[0], times[len(times)-1]), 2, 3},
		{path2, "record_row_filter_reject", "sid:7/record/row:1", fmt.Sprintf("row=1 local_input=1 local_output=none time=%d range=%d:%d columns=2 values=status=false,value=2.5 result=filter_reject", times[1], times[0], times[len(times)-1]), 3, 4},
	} {
		got := decode.RecordExecutionSamples[i]
		if got.Step != i+1 || got.File != want.file || got.Type != "tssp-record-row-step" || got.Action != want.action || got.Key != want.key || got.CandidateValue != want.value || got.CursorIndexBefore != want.indexBefore || got.CursorIndexAfter != want.indexAfter || !got.CursorAdvanced {
			t.Fatalf("record execution sample[%d] = %+v, want file=%q action=%q key=%q value=%q indexes=%d->%d advanced", i, got, want.file, want.action, want.key, want.value, want.indexBefore, want.indexAfter)
		}
	}
	if got, want := len(decode.CursorOutputSamples), 6; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, wantFile := range []string{path1, path1, path1, path2, path2, path2} {
		if got := decode.CursorOutputSamples[i].File; got != wantFile {
			t.Fatalf("cursor output sample[%d] file = %q, want %q", i, got, wantFile)
		}
		if !decode.CursorOutputSamples[i].Matches {
			t.Fatalf("cursor output sample[%d] should match", i)
		}
	}
	if got, want := len(decode.CursorFinalOutputSamples), 3; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	for i, want := range []DecodePathCursorOutput{
		{Key: "sid:7/record", Time: times[0], Type: "record", File: path1, OptimizedValue: "status=true,value=1.25", Matches: true, RequiresDedup: true, RequiresMerge: true, MergeFiles: newDecodePathStringList([]string{path1, path2})},
		{Key: "sid:7/status", Time: times[0], Type: "boolean-full", File: path1, OptimizedValue: "true", Matches: true, RequiresDedup: true, RequiresMerge: true, MergeFiles: newDecodePathStringList([]string{path1, path2})},
		{Key: "sid:7/value", Time: times[0], Type: "float-full", File: path1, OptimizedValue: "1.25", Matches: true, RequiresDedup: true, RequiresMerge: true, MergeFiles: newDecodePathStringList([]string{path1, path2})},
	} {
		got := decode.CursorFinalOutputSamples[i]
		if got != want {
			t.Fatalf("cursor final output sample[%d] = %+v, want %+v", i, got, want)
		}
	}
	if !containsStringWithPrefix(decode.Recommendations, "final TSSP file-set output samples") {
		t.Fatalf("recommendations = %v, want final file-set output recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPFileSetFinalRecordSamplesOmitLocalOutputOrdinal(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "00000001-0001-00000000.tssp")
	path2 := filepath.Join(dir, "00000002-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeTestTSSPWithMultiColumnRecordData(path2); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QuerySeriesIDs:   []uint64{7},
		KeySampleLimit:   3,
		BlockSampleLimit: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level TSSP decode path summary")
	}
	var localSecondRecord *DecodePathCursorOutput
	for i := range decode.CursorOutputSamples {
		sample := &decode.CursorOutputSamples[i]
		if sample.Type == "record" && sample.Time == times[1] {
			localSecondRecord = sample
			break
		}
	}
	if localSecondRecord == nil {
		t.Fatalf("cursor output samples = %+v, want local second record sample", decode.CursorOutputSamples)
	}
	if got, want := localSecondRecord.OutputOrdinal, 1; got != want {
		t.Fatalf("local record output ordinal = %d, want %d", got, want)
	}

	var finalSecondRecord *DecodePathCursorOutput
	for i := range decode.CursorFinalOutputSamples {
		sample := &decode.CursorFinalOutputSamples[i]
		if sample.Type == "record" && sample.Time == times[1] {
			finalSecondRecord = sample
			break
		}
	}
	if finalSecondRecord == nil {
		t.Fatalf("cursor final output samples = %+v, want final second record sample", decode.CursorFinalOutputSamples)
	}
	if got, want := finalSecondRecord.OutputOrdinal, 0; got != want {
		t.Fatalf("final record output ordinal = %d, want cleared local ordinal %d", got, want)
	}
	if !finalSecondRecord.RequiresDedup || !finalSecondRecord.RequiresMerge {
		t.Fatalf("final second record sample = %+v, want dedup and merge", *finalSecondRecord)
	}
}

func TestAnalyzeTSSPFileSetFinalOutputSamplesUseUntruncatedFileSamples(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "00000001-0001-00000000.tssp")
	path2 := filepath.Join(dir, "00000002-0001-00000000.tssp")
	times, err := writeTestTSSPWithMultiColumnRecordData(path1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeTestTSSPWithMultiColumnRecordData(path2); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(times[0], times[len(times)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QuerySeriesIDs:   []uint64{7},
		QueryFields:      []FieldFilter{{Key: "status", Value: "true"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level TSSP decode path summary")
	}
	if got, want := len(decode.CursorOutputSamples), 2; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i := range decode.CursorOutputSamples {
		if got := decode.CursorOutputSamples[i].File; got != path1 {
			t.Fatalf("cursor output sample[%d] file = %q, want %q from display cap", i, got, path1)
		}
	}
	if got, want := len(decode.CursorFinalOutputSamples), 2; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	wantMergeFiles := newDecodePathStringList([]string{path1, path2})
	for i, got := range decode.CursorFinalOutputSamples {
		if got.MergeFiles != wantMergeFiles {
			t.Fatalf("cursor final output sample[%d] merge files = %q, want %q", i, got.MergeFiles, wantMergeFiles)
		}
		if !got.RequiresDedup || !got.RequiresMerge {
			t.Fatalf("cursor final output sample[%d] = %+v, want dedup and merge despite display cap", i, got)
		}
	}
}

func TestTSSPFileSetFinalOutputSamplesSkipMissesAndMarkSingleFileDedup(t *testing.T) {
	outputGroups := newTSSPFileSetOutputSampleGroups()
	for _, output := range []DecodePathCursorOutput{
		{Key: "sid:7/value", Time: 333, Type: "float-full", File: "a.tssp", OptimizedValue: "1.25", Matches: true},
		{Key: "sid:7/value", Time: 333, Type: "float-full", File: "a.tssp", OptimizedValue: "1.25", Matches: true},
		{Key: "sid:7/value", Time: 444, Type: "float-full", File: "a.tssp", OptimizedValue: "2.5", Matches: false},
	} {
		outputGroups.add(output)
	}
	summary := &DecodePathSummary{}
	populateTSSPFileSetFinalOutputSamples(summary, outputGroups, 4)

	if got, want := len(summary.CursorFinalOutputSamples), 1; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	sample := summary.CursorFinalOutputSamples[0]
	if got, want := sample.OptimizedValue, "1.25"; got != want {
		t.Fatalf("cursor final output value = %q, want %q", got, want)
	}
	if !sample.RequiresDedup {
		t.Fatal("expected repeated same-file output to require dedup")
	}
	if sample.RequiresMerge || sample.MergeFiles != "" {
		t.Fatalf("cursor final output sample = %+v, want same-file dedup without merge", sample)
	}
}

func TestAnalyzeTSSPFileSetColumnProjectionReportsMissingColumns(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "00000001-0001-00000000.tssp")
	path2 := filepath.Join(dir, "00000002-0001-00000000.tssp")
	if err := writeTestTSSP(path1); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPWithCompression(path2, tsspChunkMetaCompressSnappy); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QuerySeriesIDs:   []uint64{7},
		QueryColumns:     []string{"missing"},
		KeySampleLimit:   3,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level TSSP decode path summary")
	}
	if len(decode.MatchedColumns) != 0 {
		t.Fatalf("matched columns = %v, want none", decode.MatchedColumns)
	}
	if got, want := decode.MissingColumns, []string{"missing"}; !equalStrings(got, want) {
		t.Fatalf("missing columns = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 0; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByProjectionBlocks, 2; got != want {
		t.Fatalf("skipped by projection blocks = %d, want %d", got, want)
	}
	if !containsStringWithPrefix(decode.Recommendations, "1 query column(s) were not found") {
		t.Fatalf("recommendations = %v, want missing column recommendation", decode.Recommendations)
	}
	if !containsStringWithPrefix(decode.Recommendations, "column projection excludes 2 in-range TSSP chunk") {
		t.Fatalf("recommendations = %v, want projection skip recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPFileSetDecodePathDescendingCursor(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "00000001-0001-00000000.tssp")
	path2 := filepath.Join(dir, "00000002-0001-00000000.tssp")
	if err := writeTestTSSP(path1); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPWithCompression(path2, tsspChunkMetaCompressSnappy); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(150, 175)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSSP,
		QueryRange:       queryRange,
		QuerySeriesIDs:   []uint64{7},
		CursorDescending: true,
		KeySampleLimit:   3,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level TSSP decode path summary")
	}
	if got, want := decode.Mode, "tssp-file-set-location-cursor-descending"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
	}
	if got, want := decode.CursorSeekTime, int64(175); got != want {
		t.Fatalf("cursor seek time = %d, want %d", got, want)
	}
	if got, want := decode.LocationBlocks, 6; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadSegments, 2; got != want {
		t.Fatalf("optimized read segments = %d, want %d", got, want)
	}
	if got, want := decode.BaselineReadAtCalls, 12; got != want {
		t.Fatalf("baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 4; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadAtCalls, 8; got != want {
		t.Fatalf("saved ReadAt calls = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 5; got != want {
		t.Fatalf("cursor window samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Files, []string{path2}; !equalStrings(got, want) {
		t.Fatalf("first cursor window files = %v, want %v", got, want)
	}
	if got, want := decode.CursorWindows[0].MinTime, int64(190); got != want {
		t.Fatalf("first cursor window min time = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[1].Reason, "segment_overlap"; got != want {
		t.Fatalf("second cursor window reason = %q, want %q", got, want)
	}
}

func TestParseTSSPChunkMetaBlockAllowsTrailingBytes(t *testing.T) {
	var buf bytes.Buffer
	writeTestTSSPChunkMeta(&buf, testTSSPChunkSpec{
		sid:     11,
		minTime: 10,
		maxTime: 20,
		offset:  1024,
		size:    64,
	})
	buf.Write([]byte{0xde, 0xad})

	chunk, err := parseTSSPChunkMetaBlock(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := chunk.SID, uint64(11); got != want {
		t.Fatalf("sid = %d, want %d", got, want)
	}
	if got, want := len(chunk.Columns), 2; got != want {
		t.Fatalf("column count = %d, want %d", got, want)
	}
}

func TestParseTSSPSelfCompressedChunkMetaBlockMultiSegment(t *testing.T) {
	header := []string{"value", "time"}
	var buf bytes.Buffer
	writeUint64(&buf, 11)
	buf.Write(binary.AppendUvarint(nil, 1024))
	buf.Write(binary.AppendUvarint(nil, 96))
	buf.Write(binary.AppendUvarint(nil, 2))
	buf.Write(binary.AppendUvarint(nil, 2))
	buf.Write(encodeTestTSSPInt64sWithScale(100, 120, 150, 180))
	writeTestTSSPSelfColumnMetaSegments(&buf, header, "value", 1, 1024, 40, 56)
	writeTestTSSPSelfColumnMetaSegments(&buf, header, "time", 0, 1120, 16, 16)

	chunk, err := parseTSSPSelfCompressedChunkMetaBlock(buf.Bytes(), header)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := chunk.SID, uint64(11); got != want {
		t.Fatalf("sid = %d, want %d", got, want)
	}
	if got, want := len(chunk.TimeRanges), 2; got != want {
		t.Fatalf("time range count = %d, want %d", got, want)
	}
	if got, want := chunk.TimeRanges[1].Min, int64(150); got != want {
		t.Fatalf("second time range min = %d, want %d", got, want)
	}
	valueColumn := chunk.Columns[0]
	if got, want := len(valueColumn.Segments), 2; got != want {
		t.Fatalf("value segment count = %d, want %d", got, want)
	}
	if got, want := valueColumn.Segments[1].Offset, int64(1064); got != want {
		t.Fatalf("second segment offset = %d, want %d", got, want)
	}
	if got, want := valueColumn.Segments[1].Size, uint32(56); got != want {
		t.Fatalf("second segment size = %d, want %d", got, want)
	}
}

func TestSplitTSSPChunkMetaDataRejectsNonIncreasingOffsets(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	var offsets bytes.Buffer
	writeUint32(&offsets, 0)
	writeUint32(&offsets, 0)
	data = append(data, offsets.Bytes()...)

	if _, _, err := splitTSSPChunkMetaData(data, 2); err == nil {
		t.Fatal("expected non-increasing offsets error")
	}
}

func TestDecompressTSSPChunkMetaBlockRoundTrip(t *testing.T) {
	payload := testTSSPChunkMetaPayload(
		testTSSPChunkSpec{sid: 7, minTime: 100, maxTime: 120, offset: 1024, size: 80},
		testTSSPChunkSpec{sid: 7, minTime: 150, maxTime: 180, offset: 1104, size: 80},
	)

	for _, mode := range []uint8{tsspChunkMetaCompressNone, tsspChunkMetaCompressSnappy, tsspChunkMetaCompressLZ4, tsspChunkMetaCompressSelf} {
		encoded, err := compressTestTSSPChunkMetaPayload(payload, mode)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decompressTSSPChunkMetaBlock(encoded, mode)
		if err != nil {
			t.Fatalf("mode %d decompress: %v", mode, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("mode %d decompressed payload mismatch", mode)
		}
	}
}

func TestDecompressTSSPChunkMetaBlockRejectsMalformedInputs(t *testing.T) {
	if _, err := decompressTSSPChunkMetaBlock([]byte{0x01, 0x02, 0x03}, tsspChunkMetaCompressLZ4); err == nil {
		t.Fatal("expected short LZ4 block error")
	}
	if _, err := decompressTSSPChunkMetaBlock([]byte{0x00, 0x00, 0x00, 0x00}, tsspChunkMetaCompressLZ4); err == nil {
		t.Fatal("expected zero-length LZ4 block error")
	}

	payload := []byte("chunk metadata payload")
	encoded, err := compressTestTSSPChunkMetaPayload(payload, tsspChunkMetaCompressLZ4)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(encoded[:4], uint32(len(payload)+1))
	if _, err := decompressTSSPChunkMetaBlock(encoded, tsspChunkMetaCompressLZ4); err == nil {
		t.Fatal("expected LZ4 length mismatch error")
	}
	if _, err := decompressTSSPChunkMetaBlock(payload, 99); err == nil {
		t.Fatal("expected unsupported mode error")
	}
}

func TestAnalyzeQuerySeriesIDsRequireRange(t *testing.T) {
	_, err := Analyze(context.Background(), []string{"missing.tssp"}, Options{
		Format:         FormatTSSP,
		QuerySeriesIDs: []uint64{9},
	})
	if err == nil || !strings.Contains(err.Error(), "series id filter requires query range") {
		t.Fatalf("error = %v, want series id range requirement", err)
	}
}

func TestAnalyzeQueryColumnsRequireRange(t *testing.T) {
	_, err := Analyze(context.Background(), []string{"missing.tssp"}, Options{
		Format:       FormatTSSP,
		QueryColumns: []string{"value"},
	})
	if err == nil || !strings.Contains(err.Error(), "column filter requires query range") {
		t.Fatalf("error = %v, want column range requirement", err)
	}
}

func TestAnalyzeQueryFieldsRequireRange(t *testing.T) {
	_, err := Analyze(context.Background(), []string{"missing.tssp"}, Options{
		Format:      FormatTSSP,
		QueryFields: []FieldFilter{{Key: "value", Value: "99"}},
	})
	if err == nil || !strings.Contains(err.Error(), "field filter requires query range") {
		t.Fatalf("error = %v, want field range requirement", err)
	}
}

func TestAnalyzeQueryAnyFieldsRequireRange(t *testing.T) {
	_, err := Analyze(context.Background(), []string{"missing.tssp"}, Options{
		Format:         FormatTSSP,
		QueryAnyFields: []FieldFilter{{Key: "value", Value: "99"}},
	})
	if err == nil || !strings.Contains(err.Error(), "field filter requires query range") {
		t.Fatalf("error = %v, want OR field range requirement", err)
	}
}

func TestAnalyzeQueryNoneFieldsRequireRange(t *testing.T) {
	_, err := Analyze(context.Background(), []string{"missing.tssp"}, Options{
		Format:          FormatTSSP,
		QueryNoneFields: []FieldFilter{{Key: "value", Value: "99"}},
	})
	if err == nil || !strings.Contains(err.Error(), "field filter requires query range") {
		t.Fatalf("error = %v, want NOT field range requirement", err)
	}
}

func TestAnalyzeQueryFieldsRejectInvalidOperator(t *testing.T) {
	queryRange, err := NewTimeRange(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Analyze(context.Background(), []string{"missing.tssp"}, Options{
		Format:      FormatTSSP,
		QueryRange:  queryRange,
		QueryFields: []FieldFilter{{Key: "value", Op: "~", Value: "99"}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported operator") {
		t.Fatalf("error = %v, want unsupported operator guidance", err)
	}
}

func TestAnalyzeQueryFieldsNormalizesSymbolOperatorAliases(t *testing.T) {
	filters := normalizeFieldFilters([]FieldFilter{
		{Key: " value ", Op: "==", Value: " 99 "},
		{Key: "value", Op: "=", Value: "99"},
		{Key: "status", Op: "<>", Value: "true"},
		{Key: "status", Op: "!=", Value: "true"},
		{Key: "device", Op: "!in", Value: "(cpu,mem)"},
		{Key: "rack", Op: "not in", Value: "(r1,r2)"},
		{Key: "rack_under", Op: "not_in", Value: "(r3,r4)"},
		{Key: "range", Op: "!between", Value: "(1,3)"},
		{Key: "range", Op: "not between", Value: "(1,3)"},
		{Key: "range_under", Op: "not_between", Value: "(4,5)"},
		{Key: "message", Op: "!contains", Value: "debug"},
		{Key: "message", Op: "not contains", Value: "debug"},
		{Key: "message", Op: "not-contains", Value: "debug"},
		{Key: "message_under", Op: "not_contains", Value: "trace"},
		{Key: "pattern", Op: "!like", Value: "tmp%"},
		{Key: "pattern", Op: "not like", Value: "tmp%"},
		{Key: "pattern", Op: "not-like", Value: "tmp%"},
		{Key: "pattern_under", Op: "not_like", Value: "trace%"},
		{Key: "prefix", Op: "!starts-with", Value: "edge"},
		{Key: "prefix", Op: "starts with", Value: "edge"},
		{Key: "prefix", Op: "starts-with", Value: "edge"},
		{Key: "prefix_under", Op: "starts_with", Value: "edge"},
		{Key: "prefix_under", Op: "not_starts_with", Value: "core"},
		{Key: "suffix", Op: "!ends-with", Value: "tmp"},
		{Key: "suffix", Op: "not ends with", Value: "tmp"},
		{Key: "suffix", Op: "not-ends-with", Value: "tmp"},
		{Key: "suffix_under", Op: "ends_with", Value: "tmp"},
		{Key: "suffix_under", Op: "not_ends_with", Value: "bak"},
		{Key: "missing", Op: "==", Value: "null"},
		{Key: "state", Op: "is_not", Value: "null"},
	})
	want := []FieldFilter{
		{Key: "device", Op: "not-in", Value: "(cpu,mem)"},
		{Key: "message", Op: "not-contains", Value: "debug"},
		{Key: "message_under", Op: "not-contains", Value: "trace"},
		{Key: "missing", Value: "null"},
		{Key: "pattern", Op: "not-like", Value: "tmp%"},
		{Key: "pattern_under", Op: "not-like", Value: "trace%"},
		{Key: "prefix", Op: "not-starts-with", Value: "edge"},
		{Key: "prefix", Op: "starts-with", Value: "edge"},
		{Key: "prefix_under", Op: "not-starts-with", Value: "core"},
		{Key: "prefix_under", Op: "starts-with", Value: "edge"},
		{Key: "rack", Op: "not-in", Value: "(r1,r2)"},
		{Key: "rack_under", Op: "not-in", Value: "(r3,r4)"},
		{Key: "range", Op: "not-between", Value: "(1,3)"},
		{Key: "range_under", Op: "not-between", Value: "(4,5)"},
		{Key: "state", Op: "!=", Value: "null"},
		{Key: "status", Op: "!=", Value: "true"},
		{Key: "suffix", Op: "not-ends-with", Value: "tmp"},
		{Key: "suffix_under", Op: "ends-with", Value: "tmp"},
		{Key: "suffix_under", Op: "not-ends-with", Value: "bak"},
		{Key: "value", Value: "99"},
	}
	if !equalFieldFilters(filters, want) {
		t.Fatalf("filters = %v, want %v", filters, want)
	}
	if err := validateFieldFilters(filters); err != nil {
		t.Fatalf("validate field filters: %v", err)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "value", Op: "==", Value: "99"}), "="; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "status", Op: "<>", Value: "true"}), "!="; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "message", Op: "not contains", Value: "debug"}), "not-contains"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "message", Op: "!contains", Value: "debug"}), "not-contains"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "device", Op: "!in", Value: "(cpu,mem)"}), "not-in"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "range", Op: "!between", Value: "(1,3)"}), "not-between"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "pattern", Op: "not like", Value: "tmp%"}), "not-like"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "pattern", Op: "!like", Value: "tmp%"}), "not-like"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "prefix", Op: "starts with", Value: "edge"}), "starts-with"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "prefix", Op: "not starts with", Value: "edge"}), "not-starts-with"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "prefix", Op: "!starts-with", Value: "edge"}), "not-starts-with"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "suffix", Op: "ends with", Value: "tmp"}), "ends-with"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "suffix", Op: "not ends with", Value: "tmp"}), "not-ends-with"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "suffix", Op: "!ends-with", Value: "tmp"}), "not-ends-with"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "suffix", Op: "not_ends_with", Value: "tmp"}), "not-ends-with"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "prefix", Op: "!starts_with", Value: "tmp"}), "not-starts-with"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "suffix", Op: "!ends_with", Value: "tmp"}), "not-ends-with"; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
	if got, want := fieldFilterOperator(FieldFilter{Key: "state", Op: "is_not", Value: "null"}), "!="; got != want {
		t.Fatalf("field filter operator = %q, want %q", got, want)
	}
}

func TestAnalyzeQueryFieldsRejectInvalidRegex(t *testing.T) {
	queryRange, err := NewTimeRange(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Analyze(context.Background(), []string{"missing.tssp"}, Options{
		Format:      FormatTSSP,
		QueryRange:  queryRange,
		QueryFields: []FieldFilter{{Key: "value", Op: "=~", Value: "["}},
	})
	if err == nil || !strings.Contains(err.Error(), "invalid regex") {
		t.Fatalf("error = %v, want invalid regex guidance", err)
	}
}

func TestAnalyzeQueryFieldsRejectEmptySetOperator(t *testing.T) {
	for _, op := range []string{"in", "not-in"} {
		t.Run(op, func(t *testing.T) {
			queryRange, err := NewTimeRange(1, 2)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Analyze(context.Background(), []string{"missing.tssp"}, Options{
				Format:      FormatTSSP,
				QueryRange:  queryRange,
				QueryFields: []FieldFilter{{Key: "value", Op: op, Value: "()"}},
			})
			if err == nil || !strings.Contains(err.Error(), "requires at least one value") {
				t.Fatalf("error = %v, want empty set guidance", err)
			}
		})
	}
}

func TestAnalyzeQueryFieldsRejectInvalidBetweenValueCount(t *testing.T) {
	for _, tc := range []struct {
		op    string
		value string
	}{
		{op: "between", value: "()"},
		{op: "between", value: "(1)"},
		{op: "between", value: "(1,2,3)"},
		{op: "not-between", value: "(1)"},
	} {
		t.Run(tc.op+"/"+tc.value, func(t *testing.T) {
			queryRange, err := NewTimeRange(1, 2)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Analyze(context.Background(), []string{"missing.tssp"}, Options{
				Format:      FormatTSSP,
				QueryRange:  queryRange,
				QueryFields: []FieldFilter{{Key: "value", Op: tc.op, Value: tc.value}},
			})
			if err == nil || !strings.Contains(err.Error(), "requires exactly two values") {
				t.Fatalf("error = %v, want invalid between value count guidance", err)
			}
		})
	}
}

func TestAnalyzeQueryFieldsRejectNullBetweenBounds(t *testing.T) {
	for _, tc := range []struct {
		op    string
		value string
	}{
		{op: "between", value: "(null,5)"},
		{op: "between", value: `("null",5)`},
		{op: "not-between", value: "(1,null)"},
	} {
		t.Run(tc.op+"/"+tc.value, func(t *testing.T) {
			queryRange, err := NewTimeRange(1, 2)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Analyze(context.Background(), []string{"missing.tssp"}, Options{
				Format:      FormatTSSP,
				QueryRange:  queryRange,
				QueryFields: []FieldFilter{{Key: "value", Op: tc.op, Value: tc.value}},
			})
			if err == nil || !strings.Contains(err.Error(), "does not support null bounds") {
				t.Fatalf("error = %v, want null between bound guidance", err)
			}
		})
	}
}

func writeTestTSSP(path string) error {
	return writeTestTSSPWithCompression(path, tsspChunkMetaCompressNone)
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalFieldFilters(a, b []FieldFilter) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func readAtRangesContainColumn(ranges []DecodePathReadAtRange, column string) bool {
	for _, readAtRange := range ranges {
		if readAtRange.Column == column {
			return true
		}
	}
	return false
}

func containsStringWithPrefix(values []string, prefix string) bool {
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func writeTestTSSPWithCompression(path string, chunkMetaCompress uint8) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	chunks7 := []testTSSPChunkSpec{
		{sid: 7, minTime: 100, maxTime: 120, offset: 1024, size: 80},
		{sid: 7, minTime: 150, maxTime: 180, offset: 1104, size: 80},
		{sid: 7, minTime: 190, maxTime: 200, offset: 1184, size: 80},
	}
	chunks9 := []testTSSPChunkSpec{
		{sid: 9, minTime: 300, maxTime: 330, offset: 1264, size: 96},
		{sid: 9, minTime: 340, maxTime: 400, offset: 1360, size: 96},
	}

	chunkMetaHeader := []string(nil)
	var payload7, payload9 []byte
	if chunkMetaCompress == tsspChunkMetaCompressSelf {
		chunkMetaHeader = []string{"value", "time"}
		payload7 = testTSSPSelfChunkMetaPayload(chunkMetaHeader, chunks7...)
		payload9 = testTSSPSelfChunkMetaPayload(chunkMetaHeader, chunks9...)
	} else {
		payload7 = testTSSPChunkMetaPayload(chunks7...)
		payload9 = testTSSPChunkMetaPayload(chunks9...)
	}

	var err error
	payload7, err = compressTestTSSPChunkMetaPayload(payload7, chunkMetaCompress)
	if err != nil {
		return err
	}
	payload9, err = compressTestTSSPChunkMetaPayload(payload9, chunkMetaCompress)
	if err != nil {
		return err
	}
	payload7Offset := int64(buf.Len())
	buf.Write(payload7)
	payload9Offset := int64(buf.Len())
	buf.Write(payload9)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: 100,
		MaxTime: 200,
		Offset:  payload7Offset,
		Count:   3,
		Size:    uint32(len(payload7)),
	})
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      9,
		MinTime: 300,
		MaxTime: 400,
		Offset:  payload9Offset,
		Count:   2,
		Size:    uint32(len(payload9)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         tsspHeaderSize,
		DataSize:           0,
		IndexSize:          metaOffset - tsspHeaderSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            2,
		MinID:              7,
		MaxID:              9,
		MinTime:            100,
		MaxTime:            400,
		MetaIndexItemCount: 2,
		ChunkMetaCompress:  chunkMetaCompress,
		ChunkMetaHeader:    chunkMetaHeader,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithOneRowData(path string) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize := writeTestTSSPAttachedIntegerOneBlock(&buf, 99)
	timeSize := writeTestTSSPAttachedIntegerOneBlock(&buf, 333)
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  333,
		maxTime:  333,
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: 333,
		MaxTime: 333,
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            333,
		MaxTime:            333,
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithFloatFullValues(path string, values []float64, codec byte) ([]int64, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("test TSSP float values must not be empty")
	}
	times := make([]int64, len(values))
	for i := range times {
		times[i] = 333 + int64(i)*111
	}

	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize, err := writeTestTSSPAttachedFloatFullBlock(&buf, values, codec)
	if err != nil {
		return nil, err
	}
	timeSize := writeTestTSSPAttachedIntegerFullBlock(&buf, times)
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  times[0],
		maxTime:  times[len(times)-1],
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: times[0],
		MaxTime: times[len(times)-1],
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            times[0],
		MaxTime:            times[len(times)-1],
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return times, os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithRegularFloatValues(path string, values []float64) ([]int64, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("test TSSP float values must not be empty")
	}
	times := make([]int64, len(values))
	for i := range times {
		times[i] = 333 + int64(i)*111
	}

	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize, err := writeTestTSSPAttachedRegularFloatBlock(&buf, values, 0)
	if err != nil {
		return nil, err
	}
	timeSize := writeTestTSSPAttachedRegularTimestampBlock(&buf, times)
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  times[0],
		maxTime:  times[len(times)-1],
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: times[0],
		MaxTime: times[len(times)-1],
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            times[0],
		MaxTime:            times[len(times)-1],
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return times, os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithNullableRegularFloatValues(path string, values []float64, present []bool) ([]int64, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("test TSSP float values must not be empty")
	}
	if len(values) != len(present) {
		return nil, fmt.Errorf("test TSSP float values and present bitmap length mismatch")
	}
	times := make([]int64, len(values))
	for i := range times {
		times[i] = 333 + int64(i)*111
	}

	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize, err := writeTestTSSPAttachedNullableRegularFloatBlock(&buf, values, present, 0)
	if err != nil {
		return nil, err
	}
	timeSize := writeTestTSSPAttachedRegularTimestampBlock(&buf, times)
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  times[0],
		maxTime:  times[len(times)-1],
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: times[0],
		MaxTime: times[len(times)-1],
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            times[0],
		MaxTime:            times[len(times)-1],
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return times, os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithUnsupportedFloatFullCodec(path string, codec byte) ([]int64, error) {
	times := []int64{333, 444}

	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize := writeTestTSSPAttachedUnsupportedFloatFullBlock(&buf, len(times), codec)
	timeSize := writeTestTSSPAttachedIntegerFullBlock(&buf, times)
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  times[0],
		maxTime:  times[len(times)-1],
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: times[0],
		MaxTime: times[len(times)-1],
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            times[0],
		MaxTime:            times[len(times)-1],
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return times, os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithShortDataBlock(path string) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	buf.WriteByte(31)
	valueSize := uint32(1)
	timeSize := writeTestTSSPAttachedIntegerFullBlock(&buf, []int64{333, 444})
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  333,
		maxTime:  444,
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: 333,
		MaxTime: 444,
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            333,
		MaxTime:            444,
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithIntegerFullData(path string) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize := writeTestTSSPAttachedIntegerFullBlock(&buf, []int64{99, 100})
	timeSize := writeTestTSSPAttachedIntegerFullBlock(&buf, []int64{333, 444})
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  333,
		maxTime:  444,
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: 333,
		MaxTime: 444,
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            333,
		MaxTime:            444,
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithMultiColumnRecordData(path string) ([]int64, error) {
	times := []int64{333, 444}

	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize, err := writeTestTSSPAttachedFloatFullBlock(&buf, []float64{1.25, 2.5}, 0)
	if err != nil {
		return nil, err
	}
	statusOffset := int64(buf.Len())
	statusSize := writeTestTSSPAttachedBooleanFullBlock(&buf, []bool{true, false})
	timeOffset := int64(buf.Len())
	timeSize := writeTestTSSPAttachedIntegerFullBlock(&buf, times)
	dataSize := int64(valueSize + statusSize + timeSize)

	payload := testTSSPMultiColumnChunkMetaPayload(7, times[0], times[1], []testTSSPColumnSpec{
		{name: "value", typ: 1, offset: valueOffset, size: valueSize},
		{name: "status", typ: 5, offset: statusOffset, size: statusSize},
		{name: "time", typ: 0, offset: timeOffset, size: timeSize},
	})
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: times[0],
		MaxTime: times[1],
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            times[0],
		MaxTime:            times[1],
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return times, os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithTwoChunkRecordData(path string) ([]int64, error) {
	times := []int64{333, 444, 555, 666}

	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	chunks := make([]testTSSPMultiColumnChunkSpec, 0, 2)
	for i := 0; i < 2; i++ {
		chunkTimes := times[i*2 : i*2+2]
		valueOffset := int64(buf.Len())
		valueSize, err := writeTestTSSPAttachedFloatFullBlock(&buf, []float64{float64(i*2) + 1.25, float64(i*2) + 2.5}, 0)
		if err != nil {
			return nil, err
		}
		statusOffset := int64(buf.Len())
		statusSize := writeTestTSSPAttachedBooleanFullBlock(&buf, []bool{i == 0, i != 0})
		timeOffset := int64(buf.Len())
		timeSize := writeTestTSSPAttachedIntegerFullBlock(&buf, chunkTimes)
		chunks = append(chunks, testTSSPMultiColumnChunkSpec{
			sid:     7,
			minTime: chunkTimes[0],
			maxTime: chunkTimes[len(chunkTimes)-1],
			columns: []testTSSPColumnSpec{
				{name: "value", typ: 1, offset: valueOffset, size: valueSize},
				{name: "status", typ: 5, offset: statusOffset, size: statusSize},
				{name: "time", typ: 0, offset: timeOffset, size: timeSize},
			},
		})
	}
	dataSize := int64(buf.Len()) - dataOffset

	payload := testTSSPMultiColumnChunkMetaPayloads(chunks...)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: times[0],
		MaxTime: times[len(times)-1],
		Offset:  payloadOffset,
		Count:   uint32(len(chunks)),
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            times[0],
		MaxTime:            times[len(times)-1],
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return times, os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithIntegerConstDeltaData(path string) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize := writeTestTSSPAttachedIntegerConstDeltaBlock(&buf, []int64{99, 100, 101})
	timeSize := writeTestTSSPAttachedIntegerConstDeltaBlock(&buf, []int64{333, 444, 555})
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  333,
		maxTime:  555,
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: 333,
		MaxTime: 555,
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            333,
		MaxTime:            555,
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithIntegerSimple8bData(path string) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize := writeTestTSSPAttachedIntegerSimple8bBlock(&buf, []int64{99, 100, 102})
	timeSize := writeTestTSSPAttachedIntegerSimple8bBlock(&buf, []int64{333, 444, 666})
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  333,
		maxTime:  666,
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: 333,
		MaxTime: 666,
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            333,
		MaxTime:            666,
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithIntegerZSTDData(path string) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize, err := writeTestTSSPAttachedIntegerZSTDBlock(&buf, []int64{99, 100, 102})
	if err != nil {
		return err
	}
	timeSize, err := writeTestTSSPAttachedIntegerZSTDBlock(&buf, []int64{333, 444, 666})
	if err != nil {
		return err
	}
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  333,
		maxTime:  666,
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: 333,
		MaxTime: 666,
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            333,
		MaxTime:            666,
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithBooleanFullData(path string) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize := writeTestTSSPAttachedBooleanFullBlock(&buf, []bool{true, false})
	timeSize := writeTestTSSPAttachedIntegerFullBlock(&buf, []int64{333, 444})
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  333,
		maxTime:  444,
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: 333,
		MaxTime: 444,
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            333,
		MaxTime:            444,
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPWithStringFullData(path string) error {
	return writeTestTSSPWithStringFullValues(path, []string{"red", "blue"}, 0)
}

func writeTestTSSPWithStringFullValues(path string, values []string, codec byte) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	dataOffset := int64(buf.Len())
	valueOffset := int64(buf.Len())
	valueSize, err := writeTestTSSPAttachedStringFullBlock(&buf, values, codec)
	if err != nil {
		return err
	}
	timeSize := writeTestTSSPAttachedIntegerFullBlock(&buf, []int64{333, 444})
	dataSize := int64(valueSize + timeSize)
	chunk := testTSSPChunkSpec{
		sid:      7,
		minTime:  333,
		maxTime:  444,
		offset:   valueOffset,
		size:     valueSize,
		timeSize: timeSize,
	}

	payload := testTSSPChunkMetaPayload(chunk)
	payloadOffset := int64(buf.Len())
	buf.Write(payload)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{
		ID:      7,
		MinTime: 333,
		MaxTime: 444,
		Offset:  payloadOffset,
		Count:   1,
		Size:    uint32(len(payload)),
	})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         dataOffset,
		DataSize:           dataSize,
		IndexSize:          metaOffset - dataOffset - dataSize,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            1,
		MinID:              7,
		MaxID:              7,
		MinTime:            333,
		MaxTime:            444,
		MetaIndexItemCount: 1,
		ChunkMetaCompress:  tsspChunkMetaCompressNone,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPAttachedIntegerOneBlock(buf *bytes.Buffer, value int64) uint32 {
	var payload [9]byte
	payload[0] = 18 // openGemini encoding.BlockIntegerOne.
	binary.LittleEndian.PutUint64(payload[1:], uint64(value))
	buf.Write(payload[:])
	return uint32(len(payload))
}

func writeTestTSSPAttachedFloatFullBlock(buf *bytes.Buffer, values []float64, codec byte) (uint32, error) {
	encoded, err := testTSSPFloatFullEncodedPayload(values, codec)
	if err != nil {
		return 0, err
	}
	start := buf.Len()
	buf.WriteByte(31) // openGemini encoding.BlockFloatFull.
	writeUint32(buf, uint32(len(values)))
	buf.Write(encoded)
	return uint32(buf.Len() - start), nil
}

func writeTestTSSPAttachedUnsupportedFloatFullBlock(buf *bytes.Buffer, rows int, codec byte) uint32 {
	start := buf.Len()
	buf.WriteByte(31) // openGemini encoding.BlockFloatFull.
	writeUint32(buf, uint32(rows))
	buf.WriteByte(codec << 4)
	return uint32(buf.Len() - start)
}

func writeTestTSSPAttachedRegularFloatBlock(buf *bytes.Buffer, values []float64, codec byte) (uint32, error) {
	encoded, err := testTSSPFloatFullEncodedPayload(values, codec)
	if err != nil {
		return 0, err
	}
	return writeTestTSSPAttachedRegularBlock(buf, 3, len(values), encoded), nil
}

func writeTestTSSPAttachedNullableRegularFloatBlock(buf *bytes.Buffer, values []float64, present []bool, codec byte) (uint32, error) {
	if len(values) != len(present) {
		return 0, fmt.Errorf("test TSSP float values and present bitmap length mismatch")
	}
	nonNullValues := make([]float64, 0, len(values))
	for i, ok := range present {
		if ok {
			nonNullValues = append(nonNullValues, values[i])
		}
	}
	encoded, err := testTSSPFloatFullEncodedPayload(nonNullValues, codec)
	if err != nil {
		return 0, err
	}
	return writeTestTSSPAttachedRegularBlockWithPresent(buf, 3, encoded, present), nil
}

func writeTestTSSPAttachedRegularTimestampBlock(buf *bytes.Buffer, values []int64) uint32 {
	return writeTestTSSPAttachedRegularBlock(buf, 1, len(values), testTSSPTimestampUncompressedPayload(values))
}

func writeTestTSSPAttachedRegularBlock(buf *bytes.Buffer, blockType byte, rows int, encoded []byte) uint32 {
	present := make([]bool, rows)
	for i := range present {
		present[i] = true
	}
	return writeTestTSSPAttachedRegularBlockWithPresent(buf, blockType, encoded, present)
}

func writeTestTSSPAttachedRegularBlockWithPresent(buf *bytes.Buffer, blockType byte, encoded []byte, present []bool) uint32 {
	start := buf.Len()
	buf.WriteByte(blockType)
	bitmap, nilCount := testTSSPBitmapFromPresent(present)
	writeUint32(buf, uint32(len(bitmap)))
	buf.Write(bitmap)
	writeUint32(buf, 0)
	writeUint32(buf, uint32(nilCount))
	buf.Write(encoded)
	return uint32(buf.Len() - start)
}

func testTSSPFullBitmap(rows int) []byte {
	present := make([]bool, rows)
	for i := range present {
		present[i] = true
	}
	bitmap, _ := testTSSPBitmapFromPresent(present)
	return bitmap
}

func testTSSPBitmapFromPresent(present []bool) ([]byte, int) {
	if len(present) == 0 {
		return nil, 0
	}
	bitmap := make([]byte, (len(present)+7)/8)
	nilCount := 0
	for i, ok := range present {
		if ok {
			bitmap[i/8] |= 1 << uint(i%8)
			continue
		}
		nilCount++
	}
	return bitmap, nilCount
}

func testTSSPTimestampUncompressedPayload(values []int64) []byte {
	var payload bytes.Buffer
	payload.WriteByte(4 << 4) // openGemini encoding timeUncompressed.
	writeUint32(&payload, uint32(len(values)*8))
	for _, value := range values {
		writeUint64(&payload, uint64(value))
	}
	return payload.Bytes()
}

func testTSSPFloatFullPayload(values []float64, codec byte) ([]byte, error) {
	encoded, err := testTSSPFloatFullEncodedPayload(values, codec)
	if err != nil {
		return nil, err
	}
	var payload bytes.Buffer
	payload.WriteByte(31) // openGemini encoding.BlockFloatFull.
	writeUint32(&payload, uint32(len(values)))
	payload.Write(encoded)
	return payload.Bytes(), nil
}

func testTSSPFloatFullEncodedPayload(values []float64, codec byte) ([]byte, error) {
	var payload bytes.Buffer
	payload.WriteByte(codec << 4)
	raw := testTSSPFloatRawBytes(values)
	switch codec {
	case 0:
		payload.Write(raw)
	case 1:
		writeUint32(&payload, uint32(len(values)))
		payload.Write(testTSMFloatValueBlock(values)[1:])
	case 2:
		payload.Write(snappy.Encode(nil, raw))
	case 3:
		payload.Write(testTSMFloatValueBlock(values))
	case 4:
		encoded, err := testTSSPFloatSamePayload(values)
		if err != nil {
			return nil, err
		}
		payload.Write(encoded)
	case 5:
		payload.Write(testTSSPFloatRLEPayload(values))
	case 6:
		encoded, err := testTSSPFloatMLFPayload(values)
		if err != nil {
			return nil, err
		}
		payload.Write(encoded)
	default:
		return nil, fmt.Errorf("unsupported test TSSP float codec %d", codec)
	}
	return payload.Bytes(), nil
}

func testTSSPFloatRawBytes(values []float64) []byte {
	raw := make([]byte, len(values)*8)
	for i, value := range values {
		binary.LittleEndian.PutUint64(raw[i*8:], math.Float64bits(value))
	}
	return raw
}

func testTSSPFloatSamePayload(values []float64) ([]byte, error) {
	if len(values) > 1 {
		for _, value := range values[1:] {
			if value != values[0] {
				return nil, fmt.Errorf("same-value TSSP float payload received mixed values")
			}
		}
	}
	var payload bytes.Buffer
	writeUint16(&payload, uint16(len(values)))
	if len(values) > 0 && values[0] != 0 {
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], math.Float64bits(values[0]))
		payload.Write(raw[:])
	}
	return payload.Bytes(), nil
}

func testTSSPFloatRLEPayload(values []float64) []byte {
	var payload bytes.Buffer
	for i := 0; i < len(values); {
		value := values[i]
		count := 1
		for i+count < len(values) && values[i+count] == value && count < 1<<14 {
			count++
		}
		if value == 0 {
			writeUint16(&payload, uint16(count)|(uint16(1)<<15))
		} else {
			writeUint16(&payload, uint16(count))
			var raw [8]byte
			binary.LittleEndian.PutUint64(raw[:], math.Float64bits(value))
			payload.Write(raw[:])
		}
		i += count
	}
	return payload.Bytes()
}

type testTSSPMLFEncodeContext struct {
	flags            []uint8
	factors          []uint64
	min              float64
	max              float64
	precision        float64
	maxPrecisionSize int
	allSkip          bool
	allZero          bool
}

func testTSSPFloatMLFPayload(values []float64) ([]byte, error) {
	ctx := testTSSPPrepareMLF(values)
	var payload bytes.Buffer
	writeUint16(&payload, uint16(len(values)))
	if ctx.allZero {
		payload.WriteByte(tsspMLFCompressModeAllZero)
		return payload.Bytes(), nil
	}

	precisionPow10 := tsspMLFPow10[ctx.maxPrecisionSize]
	multiplicand := ctx.max + ctx.precision*1.1
	payload.WriteByte(byte(ctx.maxPrecisionSize))
	uncompressedCountOffset := payload.Len()
	writeUint16(&payload, 0)
	uncompressedCount := 0
	factors := make([]uint64, 0, len(values))
	maxFactorBitSize := 0

	for i, value := range values {
		if value == 0 {
			ctx.flags[i] = tsspMLFFlagZero
			continue
		}
		absValue := value
		if value < 0 {
			ctx.flags[i] = tsspMLFFlagNegative
			absValue = -absValue
		}
		factor, size := testTSSPEncodeMLFFactor(absValue, multiplicand, ctx.precision)
		if size >= 0 && testTSSPInvalidMLFFactor(factor, multiplicand, precisionPow10, absValue) {
			size = -1
		}
		if size == -1 {
			uncompressedCount++
			ctx.flags[i] = tsspMLFFlagSkip
			writeUint64(&payload, math.Float64bits(value))
			continue
		}
		factors = append(factors, factor)
		if size > maxFactorBitSize {
			maxFactorBitSize = size
		}
	}
	if uncompressedCount == len(values) {
		payload.Truncate(2)
		payload.WriteByte(tsspMLFCompressModeNone)
		payload.Write(testTSSPFloatRawBytes(values))
		return payload.Bytes(), nil
	}
	raw := payload.Bytes()
	binary.BigEndian.PutUint16(raw[uncompressedCountOffset:uncompressedCountOffset+2], uint16(uncompressedCount))
	testTSSPWriteMLFBitmap(&payload, ctx.flags)
	if len(factors) > 0 {
		writeUint64(&payload, math.Float64bits(multiplicand))
		publicPrefixSize := testTSSPMLFPublicPrefixSize(ctx.min / multiplicand)
		testTSSPWriteMLFFactors(&payload, factors, maxFactorBitSize, publicPrefixSize)
	}
	return payload.Bytes(), nil
}

func testTSSPPrepareMLF(values []float64) testTSSPMLFEncodeContext {
	ctx := testTSSPMLFEncodeContext{
		flags:   make([]uint8, len(values)),
		min:     math.MaxFloat64,
		max:     0,
		allSkip: true,
		allZero: true,
	}
	limit := len(values) / 10
	if limit < 16 {
		limit = 16
	}
	for _, value := range values {
		absValue := value
		if value == 0 {
			ctx.allSkip = false
			continue
		}
		ctx.allZero = false
		if absValue < 0 {
			absValue = -absValue
		}
		if limit > 0 {
			precisionSize := testTSSPMLFPrecision(absValue, ctx.maxPrecisionSize)
			if precisionSize != -1 {
				if precisionSize > ctx.maxPrecisionSize {
					ctx.maxPrecisionSize = precisionSize
				}
				limit--
				ctx.allSkip = false
			}
		}
		if ctx.max < absValue {
			ctx.max = absValue
		}
		if ctx.min > absValue {
			ctx.min = absValue
		}
	}
	ctx.precision = 1 / tsspMLFPow10[ctx.maxPrecisionSize]
	ctx.precision -= ctx.precision / 10
	return ctx
}

func testTSSPMLFPrecision(value float64, begin int) int {
	for i := begin; i < len(tsspMLFPow10); i++ {
		scaled := value * tsspMLFPow10[i]
		if scaled >= 1<<52 {
			break
		}
		if scaled >= 1 && testTSSPMLFIsInt(scaled) {
			return i
		}
	}
	return -1
}

func testTSSPMLFIsInt(value float64) bool {
	bits := math.Float64bits(value)
	shift := bits>>tsspMLFMantissaBits - tsspMLFMiddleNumber
	mask := uint64(1)<<(tsspMLFMantissaBits-shift) - 1
	bits &= mask
	return bits < 8 || bits == mask
}

func testTSSPEncodeMLFFactor(value, multiplicand, precision float64) (uint64, int) {
	if value >= multiplicand {
		return 0, -1
	}
	u1 := math.Float64bits((value + multiplicand) / multiplicand)
	u2 := math.Float64bits((value + precision + multiplicand) / multiplicand)
	size := bits.LeadingZeros64(u1^u2) - 11
	if size > tsspMLFMaxFactorBits || size <= 0 {
		return 0, -1
	}
	factor := (u1>>uint(tsspMLFMantissaBits-size) | 1) << uint(tsspMLFMantissaBits-size)
	return factor, size
}

func testTSSPInvalidMLFFactor(factor uint64, multiplicand, precision, expected float64) bool {
	coefficient := math.Float64frombits(factor) - 1
	return math.Floor(multiplicand*coefficient*precision)/precision != expected
}

func testTSSPWriteMLFBitmap(payload *bytes.Buffer, flags []uint8) {
	hasFlags := false
	for _, flag := range flags {
		if flag != 0 {
			hasFlags = true
			break
		}
	}
	if !hasFlags {
		payload.WriteByte(tsspMLFBitmapEmpty)
		return
	}
	payload.WriteByte(tsspMLFBitmapNormal)
	bitmap := make([]byte, tsspMLFBitmapSize(len(flags)))
	for i, flag := range flags {
		if flag == 0 {
			continue
		}
		index := i / 4
		shift := uint(6 - 2*(i%4))
		bitmap[index] |= flag << shift
	}
	payload.Write(bitmap)
}

func testTSSPMLFPublicPrefixSize(min float64) int {
	value := math.Float64bits(1+min) ^ (uint64(1)<<62 - 1)
	return bits.LeadingZeros64(value) - 12
}

func testTSSPWriteMLFFactors(payload *bytes.Buffer, factors []uint64, bitSize int, publicPrefixSize int) {
	itemSize := bitSize - publicPrefixSize
	payload.WriteByte(byte(itemSize))
	payload.WriteByte(byte(publicPrefixSize))
	var swap uint64
	swapSize := 0
	shift := publicPrefixSize + 12
	for _, factor := range factors {
		factor <<= uint(shift)
		if swapSize+itemSize < 64 {
			swap |= factor >> uint(swapSize)
			swapSize += itemSize
			continue
		}
		capacity := 64 - swapSize
		writeUint64(payload, swap|(factor>>uint(swapSize)))
		swap = factor << uint(capacity)
		swapSize = itemSize - capacity
	}
	if swapSize > 0 {
		writeUint64(payload, swap)
	}
}

func writeTestTSSPAttachedIntegerFullBlock(buf *bytes.Buffer, values []int64) uint32 {
	start := buf.Len()
	buf.WriteByte(32) // openGemini encoding.BlockIntegerFull.
	writeUint32(buf, uint32(len(values)))
	buf.WriteByte(64) // openGemini encoding intUncompressed << 4.
	writeUint32(buf, uint32(len(values)*8))
	for _, value := range values {
		writeGeminiInt64(buf, value)
	}
	return uint32(buf.Len() - start)
}

func writeTestTSSPAttachedIntegerConstDeltaBlock(buf *bytes.Buffer, values []int64) uint32 {
	start := buf.Len()
	buf.WriteByte(32) // openGemini encoding.BlockIntegerFull.
	writeUint32(buf, uint32(len(values)))
	buf.WriteByte(16) // openGemini encoding intCompressedConstDelta << 4.
	writeGeminiInt64(buf, values[0])
	writeUvarint(buf, encodeGeminiZigZagInt64(values[1]-values[0]))
	writeUvarint(buf, uint64(len(values)-1))
	return uint32(buf.Len() - start)
}

func writeTestTSSPAttachedIntegerSimple8bBlock(buf *bytes.Buffer, values []int64) uint32 {
	start := buf.Len()
	buf.WriteByte(32) // openGemini encoding.BlockIntegerFull.
	writeUint32(buf, uint32(len(values)))
	buf.WriteByte(32) // openGemini encoding intCompressedSimple8b << 4.
	writeUint32(buf, 2)
	writeUint32(buf, uint32(len(values)))
	writeUint64(buf, encodeGeminiZigZagInt64(values[0]))
	writeUint64(buf, testTSSPSimple8bPack2(
		encodeGeminiZigZagInt64(values[1]-values[0]),
		encodeGeminiZigZagInt64(values[2]-values[1]),
	))
	return uint32(buf.Len() - start)
}

func testTSSPSimple8bPack2(first, second uint64) uint64 {
	return 14<<60 | first | second<<30
}

func writeTestTSSPAttachedIntegerZSTDBlock(buf *bytes.Buffer, values []int64) (uint32, error) {
	raw := testTSSPIntegerRawBytes(values)
	compressed, err := testTSSPZSTDCompress(raw)
	if err != nil {
		return 0, err
	}
	start := buf.Len()
	buf.WriteByte(32) // openGemini encoding.BlockIntegerFull.
	writeUint32(buf, uint32(len(values)))
	buf.WriteByte(48) // openGemini encoding intCompressZSTD << 4.
	writeUint32(buf, uint32(len(raw)))
	writeUint32(buf, uint32(len(compressed)))
	buf.Write(compressed)
	return uint32(buf.Len() - start), nil
}

func testTSSPIntegerRawBytes(values []int64) []byte {
	raw := make([]byte, len(values)*8)
	for i, value := range values {
		binary.LittleEndian.PutUint64(raw[i*8:], uint64(value))
	}
	return raw
}

func testTSSPZSTDCompress(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderLevel(zstd.SpeedFastest),
	)
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil), nil
}

func writeTestTSSPAttachedStringFullBlock(buf *bytes.Buffer, values []string, codec byte) (uint32, error) {
	encoded, err := testTSSPStringFullEncodedPayload(values, codec)
	if err != nil {
		return 0, err
	}
	start := buf.Len()
	buf.WriteByte(34) // openGemini encoding.BlockStringFull.
	writeUint32(buf, uint32(len(values)))
	buf.Write(encoded)
	return uint32(buf.Len() - start), nil
}

func testTSSPStringFullPayload(values []string, codec byte) ([]byte, error) {
	encoded, err := testTSSPStringFullEncodedPayload(values, codec)
	if err != nil {
		return nil, err
	}
	var payload bytes.Buffer
	payload.WriteByte(34) // openGemini encoding.BlockStringFull.
	writeUint32(&payload, uint32(len(values)))
	payload.Write(encoded)
	return payload.Bytes(), nil
}

func testTSSPStringFullEncodedPayload(values []string, codec byte) ([]byte, error) {
	packed := tsspPackedStringV2Payload(values)
	compressed, err := testTSSPStringCompressedPayload(packed, codec)
	if err != nil {
		return nil, err
	}
	var payload bytes.Buffer
	payload.WriteByte(codec << 4)
	writeUint32(&payload, uint32(len(packed)))
	writeUint32(&payload, uint32(len(compressed)))
	payload.Write(compressed)
	return payload.Bytes(), nil
}

func testTSSPStringCompressedPayload(packed []byte, codec byte) ([]byte, error) {
	switch codec {
	case 0:
		return packed, nil
	case 1:
		return ksnappy.Encode(nil, packed), nil
	case 2:
		return testTSSPZSTDCompress(packed)
	case 3:
		dst := make([]byte, lz4.CompressBlockBound(len(packed)))
		n, err := lz4.CompressBlock(packed, dst, nil)
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return nil, fmt.Errorf("test LZ4 string compression produced empty output")
		}
		return dst[:n], nil
	default:
		return nil, fmt.Errorf("unsupported test TSSP string codec %d", codec)
	}
}

func tsspPackedStringV2Payload(values []string) []byte {
	var data bytes.Buffer
	for _, value := range values {
		data.WriteString(value)
	}
	var payload bytes.Buffer
	writeUint32(&payload, tsspStringEncodingV2)
	writeUint32(&payload, uint32(data.Len()))
	payload.Write(data.Bytes())
	writeUint32(&payload, uint32(len(values)))
	for _, value := range values {
		writeUint32(&payload, uint32(len(value)))
	}
	return payload.Bytes()
}

func writeTestTSSPAttachedBooleanFullBlock(buf *bytes.Buffer, values []bool) uint32 {
	start := buf.Len()
	buf.WriteByte(33) // openGemini encoding.BlockBooleanFull.
	writeUint32(buf, uint32(len(values)))
	buf.WriteByte(16) // openGemini encoding boolCompressedBitpack << 4.
	writeUint32(buf, uint32(len(values)))
	for i := 0; i < len(values); i += 8 {
		var b byte
		limit := i + 8
		if limit > len(values) {
			limit = len(values)
		}
		for j := i; j < limit; j++ {
			if values[j] {
				b |= 0x80 >> uint(j-i)
			}
		}
		buf.WriteByte(b)
	}
	return uint32(buf.Len() - start)
}

func compressTestTSSPChunkMetaPayload(payload []byte, mode uint8) ([]byte, error) {
	switch mode {
	case tsspChunkMetaCompressNone, tsspChunkMetaCompressSelf:
		return payload, nil
	case tsspChunkMetaCompressSnappy:
		return snappy.Encode(nil, payload), nil
	case tsspChunkMetaCompressLZ4:
		dst := make([]byte, lz4.CompressBlockBound(len(payload)))
		n, err := lz4.CompressBlock(payload, dst, nil)
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return nil, fmt.Errorf("test LZ4 compression produced empty output")
		}
		var out bytes.Buffer
		writeUint32(&out, uint32(len(payload)))
		out.Write(dst[:n])
		return out.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported test compression mode %d", mode)
	}
}

type testTSSPChunkSpec struct {
	sid      uint64
	minTime  int64
	maxTime  int64
	offset   int64
	size     uint32
	timeSize uint32
}

type testTSSPColumnSpec struct {
	name   string
	typ    byte
	offset int64
	size   uint32
}

type testTSSPMultiColumnChunkSpec struct {
	sid     uint64
	minTime int64
	maxTime int64
	columns []testTSSPColumnSpec
}

func testTSSPChunkMetaPayload(chunks ...testTSSPChunkSpec) []byte {
	var data bytes.Buffer
	var offsets bytes.Buffer
	for _, chunk := range chunks {
		writeUint32(&offsets, uint32(data.Len()))
		writeTestTSSPChunkMeta(&data, chunk)
	}
	data.Write(offsets.Bytes())
	return data.Bytes()
}

func testTSSPMultiColumnChunkMetaPayload(sid uint64, minTime, maxTime int64, columns []testTSSPColumnSpec) []byte {
	return testTSSPMultiColumnChunkMetaPayloads(testTSSPMultiColumnChunkSpec{
		sid:     sid,
		minTime: minTime,
		maxTime: maxTime,
		columns: columns,
	})
}

func testTSSPMultiColumnChunkMetaPayloads(chunks ...testTSSPMultiColumnChunkSpec) []byte {
	var data bytes.Buffer
	var offsets bytes.Buffer
	for _, chunk := range chunks {
		writeUint32(&offsets, uint32(data.Len()))
		writeUint64(&data, chunk.sid)
		chunkOffset := int64(0)
		chunkSize := uint32(0)
		if len(chunk.columns) > 0 {
			chunkOffset = chunk.columns[0].offset
		}
		for _, column := range chunk.columns {
			chunkSize += column.size
		}
		writeGeminiInt64(&data, chunkOffset)
		writeUint32(&data, chunkSize)
		writeUint32(&data, uint32(len(chunk.columns)))
		writeUint32(&data, 1)
		writeGeminiInt64(&data, chunk.minTime)
		writeGeminiInt64(&data, chunk.maxTime)
		for _, column := range chunk.columns {
			writeTestTSSPColumnMeta(&data, column.name, column.typ, column.offset, column.size)
		}
	}
	data.Write(offsets.Bytes())
	return data.Bytes()
}

func testTSSPSelfChunkMetaPayload(header []string, chunks ...testTSSPChunkSpec) []byte {
	var data bytes.Buffer
	var offsets bytes.Buffer
	for _, chunk := range chunks {
		writeUint32(&offsets, uint32(data.Len()))
		writeTestTSSPSelfChunkMeta(&data, header, chunk)
	}
	data.Write(offsets.Bytes())
	return data.Bytes()
}

func writeTestTSSPChunkMeta(buf *bytes.Buffer, chunk testTSSPChunkSpec) {
	writeUint64(buf, chunk.sid)
	writeGeminiInt64(buf, chunk.offset)
	writeUint32(buf, chunk.size)
	writeUint32(buf, 2)
	writeUint32(buf, 1)
	writeGeminiInt64(buf, chunk.minTime)
	writeGeminiInt64(buf, chunk.maxTime)
	writeTestTSSPColumnMeta(buf, "value", 1, chunk.offset, chunk.size)
	writeTestTSSPColumnMeta(buf, "time", 0, chunk.offset+int64(chunk.size), chunk.testTimeSize())
}

func writeTestTSSPSelfChunkMeta(buf *bytes.Buffer, header []string, chunk testTSSPChunkSpec) {
	writeUint64(buf, chunk.sid)
	buf.Write(binary.AppendUvarint(nil, uint64(chunk.offset)))
	buf.Write(binary.AppendUvarint(nil, uint64(chunk.size)))
	buf.Write(binary.AppendUvarint(nil, 2))
	buf.Write(binary.AppendUvarint(nil, 1))
	buf.Write(encodeTestTSSPInt64sWithScale(chunk.minTime, chunk.maxTime))
	writeTestTSSPSelfColumnMeta(buf, header, "value", 1, chunk.offset, chunk.size)
	writeTestTSSPSelfColumnMeta(buf, header, "time", 0, chunk.offset+int64(chunk.size), chunk.testTimeSize())
}

func (chunk testTSSPChunkSpec) testTimeSize() uint32 {
	if chunk.timeSize != 0 {
		return chunk.timeSize
	}
	return 16
}

func writeTestTSSPColumnMeta(buf *bytes.Buffer, name string, typ byte, offset int64, size uint32) {
	writeUint16(buf, uint16(len(name)))
	buf.WriteString(name)
	buf.WriteByte(typ)
	writeUint16(buf, 0)
	writeGeminiInt64(buf, offset)
	writeUint32(buf, size)
}

func writeTestTSSPSelfColumnMeta(buf *bytes.Buffer, header []string, name string, typ byte, offset int64, size uint32) {
	writeTestTSSPSelfColumnMetaSegments(buf, header, name, typ, offset, size)
}

func writeTestTSSPSelfColumnMetaSegments(buf *bytes.Buffer, header []string, name string, typ byte, offset int64, sizes ...uint32) {
	buf.Write(binary.AppendUvarint(nil, uint64(testTSSPHeaderIndex(header, name))))
	buf.WriteByte(typ)
	buf.WriteByte(0)
	writeUint64(buf, uint64(offset))
	for _, size := range sizes {
		writeUint32(buf, size)
	}
}

func testTSSPHeaderIndex(header []string, name string) int {
	for i, value := range header {
		if value == name {
			return i
		}
	}
	return len(header)
}

func encodeTestTSSPInt64sWithScale(values ...int64) []byte {
	scaleIndex := 3
	for _, value := range values {
		for i := len(tsspInt64Scales) - 1; i >= 0; i-- {
			if value%tsspInt64Scales[i] == 0 {
				if i < scaleIndex {
					scaleIndex = i
				}
				break
			}
		}
	}
	scale := tsspInt64Scales[scaleIndex]
	dst := []byte{byte(scaleIndex)}
	var previous int64
	for i, value := range values {
		delta := value
		if i > 0 {
			delta -= previous
		}
		dst = binary.AppendUvarint(dst, uint64(delta/scale))
		previous = value
	}
	return dst
}

func writeTestTSSPMetaIndex(buf *bytes.Buffer, item tsspMetaIndex) {
	writeUint64(buf, item.ID)
	writeGeminiInt64(buf, item.MinTime)
	writeGeminiInt64(buf, item.MaxTime)
	writeGeminiInt64(buf, item.Offset)
	writeUint32(buf, item.Count)
	writeUint32(buf, item.Size)
}

func writeTestTSSPTrailer(buf *bytes.Buffer, trailer tsspTrailer) {
	writeGeminiInt64(buf, trailer.DataOffset)
	writeGeminiInt64(buf, trailer.DataSize)
	writeGeminiInt64(buf, trailer.IndexSize)
	writeGeminiInt64(buf, trailer.MetaIndexSize)
	writeGeminiInt64(buf, trailer.BloomSize)
	writeGeminiInt64(buf, trailer.IDTimeSize)
	writeGeminiInt64(buf, trailer.IDCount)
	writeUint64(buf, trailer.MinID)
	writeUint64(buf, trailer.MaxID)
	writeGeminiInt64(buf, trailer.MinTime)
	writeGeminiInt64(buf, trailer.MaxTime)
	writeGeminiInt64(buf, trailer.MetaIndexItemCount)
	writeUint64(buf, trailer.BloomM)
	writeUint64(buf, trailer.BloomK)
	if len(trailer.ChunkMetaHeader) > 0 {
		writeUint16(buf, 8)
		var extra bytes.Buffer
		writeLittleUint64(&extra, 0)
		writeUint16(&extra, uint16(len(trailer.ChunkMetaHeader)))
		for _, value := range trailer.ChunkMetaHeader {
			writeUint16(&extra, uint16(len(value)))
			extra.WriteString(value)
		}
		extraBytes := extra.Bytes()
		flags := uint64(trailer.TimeStoreFlag) |
			uint64(trailer.ChunkMetaCompress)<<8 |
			uint64(uint32(len(extraBytes)))<<32
		binary.LittleEndian.PutUint64(extraBytes[:8], flags)
		buf.Write(extraBytes)
	} else if trailer.TimeStoreFlag != 0 || trailer.ChunkMetaCompress != 0 {
		writeUint16(buf, 2)
		buf.WriteByte(trailer.TimeStoreFlag)
		buf.WriteByte(trailer.ChunkMetaCompress)
	} else {
		writeUint16(buf, 0)
	}
	writeUint16(buf, uint16(len(trailer.MeasurementName)))
	buf.WriteString(trailer.MeasurementName)
}

func writeGeminiInt64(buf *bytes.Buffer, value int64) {
	writeUint64(buf, encodeGeminiZigZagInt64(value))
}

func encodeGeminiZigZagInt64(value int64) uint64 {
	return uint64((value << 1) ^ (value >> 63))
}

func writeUvarint(buf *bytes.Buffer, value uint64) {
	var tmp [10]byte
	n := binary.PutUvarint(tmp[:], value)
	buf.Write(tmp[:n])
}

func writeLittleUint64(buf *bytes.Buffer, value uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], value)
	buf.Write(b[:])
}

func writeUint64(buf *bytes.Buffer, value uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], value)
	buf.Write(b[:])
}

func writeUint32(buf *bytes.Buffer, value uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], value)
	buf.Write(b[:])
}

func writeUint16(buf *bytes.Buffer, value uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], value)
	buf.Write(b[:])
}
