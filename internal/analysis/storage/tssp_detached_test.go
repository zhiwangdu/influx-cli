package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeTSSPDetachedMetaIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, tsspDetachedMetaIndexFileName)
	if err := writeTestTSSPDetachedMetaIndex(path, []tsspMetaIndex{
		{ID: 10, MinTime: 100, MaxTime: 150, Offset: 64, Size: 40},
		{ID: 11, MinTime: 200, MaxTime: 260, Offset: 104, Size: 80},
		{ID: 12, MinTime: 400, MaxTime: 450, Offset: 184, Size: 60},
	}); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(180, 220)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   4,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatTSSPDetachedIndex; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.BlockCount, 3; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["detached-meta-index"], 3; got != want {
		t.Fatalf("detached meta-index blocks = %d, want %d", got, want)
	}
	if got, want := file.MinTime, int64(100); got != want {
		t.Fatalf("min time = %d, want %d", got, want)
	}
	if got, want := file.MaxTime, int64(450); got != want {
		t.Fatalf("max time = %d, want %d", got, want)
	}
	if got, want := file.MetaIndexID.Min, uint64(10); got != want {
		t.Fatalf("min id = %d, want %d", got, want)
	}
	if got, want := file.MetaIndexID.Max, uint64(12); got != want {
		t.Fatalf("max id = %d, want %d", got, want)
	}
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if got, want := file.Extra["layout"], "detached"; got != want {
		t.Fatalf("layout = %q, want %q", got, want)
	}
	if got, want := file.KeySamples[0], "meta-index-id:10"; got != want {
		t.Fatalf("first key sample = %q, want %q", got, want)
	}

	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.Mode, "tssp-detached-meta-index-ascending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 3; got != want {
		t.Fatalf("baseline blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBlocks, 2; got != want {
		t.Fatalf("saved blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineReadAtCalls, 3; got != want {
		t.Fatalf("baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 1; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadAtCalls, 2; got != want {
		t.Fatalf("saved ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.BaselineCursorReadCalls, 1; got != want {
		t.Fatalf("baseline cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedCursorReadCalls, 1; got != want {
		t.Fatalf("optimized cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 1; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].LocationBlocks, 3; got != want {
		t.Fatalf("cursor window location blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].DecodedBlocks, 1; got != want {
		t.Fatalf("cursor window decoded blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].SavedBlocks, 2; got != want {
		t.Fatalf("cursor window saved blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "detached_chunk_meta_batch_filtered"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if got, want := len(decode.Samples), 3; got != want {
		t.Fatalf("sample count = %d, want %d", got, want)
	}
	if got, want := decode.Samples[1].Reason, "overlaps_query_range"; got != want {
		t.Fatalf("overlap sample reason = %q, want %q", got, want)
	}
	if got, want := len(decode.Samples[1].OptimizedReadAtRanges), 1; got != want {
		t.Fatalf("overlap sample ReadAt ranges = %d, want %d", got, want)
	}
	if got, want := decode.Samples[1].OptimizedReadAtRanges[0].Offset, int64(104); got != want {
		t.Fatalf("overlap ReadAt offset = %d, want %d", got, want)
	}
	if got, want := report.Summary.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("summary overlap blocks = %d, want %d", got, want)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexDescendingAndIDFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), tsspDetachedMetaIndexFileName)
	if err := writeTestTSSPDetachedMetaIndex(path, []tsspMetaIndex{
		{ID: 10, MinTime: 100, MaxTime: 150, Offset: 64, Size: 40},
		{ID: 11, MinTime: 200, MaxTime: 260, Offset: 104, Size: 80},
		{ID: 12, MinTime: 400, MaxTime: 450, Offset: 184, Size: 60},
	}); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(1, 500)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatTSSPDetachedIndex,
		KeySampleLimit:    4,
		BlockSampleLimit:  4,
		QueryRange:        queryRange,
		QueryMetaIndexIDs: []uint64{11},
		CursorDescending:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.Mode, "tssp-detached-meta-index-descending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 1; got != want {
		t.Fatalf("baseline blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 2; got != want {
		t.Fatalf("skipped by id blocks = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].MetaIndexID, uint64(11); got != want {
		t.Fatalf("first sample id = %d, want %d", got, want)
	}
	if !equalUint64s(decode.MatchedMetaIndexIDs, []uint64{11}) {
		t.Fatalf("matched ids = %v, want [11]", decode.MatchedMetaIndexIDs)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexChunkMetaReadBatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), tsspDetachedMetaIndexFileName)
	items := make([]tsspMetaIndex, 17)
	for i := range items {
		id := uint64(i + 1)
		minTime := int64(i * 100)
		items[i] = tsspMetaIndex{
			ID:      id,
			MinTime: minTime,
			MaxTime: minTime + 10,
			Offset:  int64(64 + i*40),
			Size:    40,
		}
	}
	if err := writeTestTSSPDetachedMetaIndex(path, items); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(1600, 1610)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.BaselineReadAtCalls, 17; got != want {
		t.Fatalf("baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 1; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.BaselineCursorReadCalls, 2; got != want {
		t.Fatalf("baseline cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedCursorReadCalls, 1; got != want {
		t.Fatalf("optimized cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 2; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 2; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].LocationBlocks, 16; got != want {
		t.Fatalf("first cursor window location blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].DecodedBlocks, 0; got != want {
		t.Fatalf("first cursor window decoded blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "outside_query_range"; got != want {
		t.Fatalf("first cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[1].LocationBlocks, 1; got != want {
		t.Fatalf("second cursor window location blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[1].DecodedBlocks, 1; got != want {
		t.Fatalf("second cursor window decoded blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[1].Reason, "detached_chunk_meta_batch_overlap"; got != want {
		t.Fatalf("second cursor window reason = %q, want %q", got, want)
	}
	if !containsString(decode.Recommendations, "batch read") {
		t.Fatalf("recommendations = %v, want batch read recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexExpandsChunkMetaSidecar(t *testing.T) {
	dir := t.TempDir()
	chunks := []testTSSPChunkSpec{
		{sid: 10, minTime: 100, maxTime: 150, offset: 1000, size: 40},
		{sid: 11, minTime: 200, maxTime: 260, offset: 2000, size: 80},
	}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedData(filepath.Join(dir, tsspDetachedDataFileName), 2200, chunks...); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(210, 220)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["chunk_meta_expanded"], "true"; got != want {
		t.Fatalf("chunk metadata expanded = %q, want %q", got, want)
	}
	if got, want := file.Extra["query_overlap_precision"], "detached-chunk-meta"; got != want {
		t.Fatalf("query overlap precision = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_file_checked"], "true"; got != want {
		t.Fatalf("data file checked = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_range_count"], "4"; got != want {
		t.Fatalf("data range count = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_invalid_ranges"], "0"; got != want {
		t.Fatalf("data invalid ranges = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_checked"], "true"; got != want {
		t.Fatalf("data block probe checked = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_blocks"], "2"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_valid_blocks"], "2"; got != want {
		t.Fatalf("data block probe valid blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_crc_mismatches"], "0"; got != want {
		t.Fatalf("data block probe crc mismatches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_row_count_blocks"], "2"; got != want {
		t.Fatalf("data block probe row count blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_output_points"], "1"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "integer-full:2"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	if got, want := file.BlockCount, 2; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["detached-chunk-meta"], 2; got != want {
		t.Fatalf("detached chunk metadata blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["detached-meta-index"], 2; got != want {
		t.Fatalf("detached meta-index blocks = %d, want %d", got, want)
	}
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if got, want := len(file.Blocks), 2; got != want {
		t.Fatalf("block samples = %d, want %d", got, want)
	}
	if got, want := file.Blocks[1].Type, "detached-chunk-meta"; got != want {
		t.Fatalf("second block type = %q, want %q", got, want)
	}
	if got, want := file.Blocks[1].MetaIndexID, uint64(11); got != want {
		t.Fatalf("second block id = %d, want %d", got, want)
	}
	if got, want := file.Blocks[1].ColumnCount, 2; got != want {
		t.Fatalf("second block columns = %d, want %d", got, want)
	}
	if got, want := file.Blocks[1].SegmentCount, 1; got != want {
		t.Fatalf("second block segments = %d, want %d", got, want)
	}
	if !file.Blocks[1].QueryOverlaps {
		t.Fatal("expected second detached chunk metadata block to overlap query")
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.Mode, "tssp-detached-location-cursor-ascending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 2; got != want {
		t.Fatalf("baseline blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineReadSegments, 2; got != want {
		t.Fatalf("baseline read segments = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadSegments, 1; got != want {
		t.Fatalf("optimized read segments = %d, want %d", got, want)
	}
	if got, want := decode.BaselineReadAtCalls, 4; got != want {
		t.Fatalf("baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 2; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadAtCalls, 2; got != want {
		t.Fatalf("saved ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.ValueOutputUnavailableBlocks, 0; got != want {
		t.Fatalf("value output unavailable blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeBlocks, 2; got != want {
		t.Fatalf("data block probe blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFailures, 0; got != want {
		t.Fatalf("data block probe failures = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeCRCMismatches, 0; got != want {
		t.Fatalf("data block probe crc mismatches = %d, want %d", got, want)
	}
	if got, want := decode.LocationBlocksByType["detached-chunk-meta"], 2; got != want {
		t.Fatalf("location detached chunk metadata blocks = %d, want %d", got, want)
	}
	if got, want := decode.DecodeBlocksByType["detached-chunk-meta"], 1; got != want {
		t.Fatalf("decoded detached chunk metadata blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineCursorReadCalls, 1; got != want {
		t.Fatalf("baseline cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedCursorReadCalls, 1; got != want {
		t.Fatalf("optimized cursor read calls = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 1; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "detached_chunk_meta_batch_filtered"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if got, want := len(decode.Samples), 2; got != want {
		t.Fatalf("decode samples = %d, want %d", got, want)
	}
	if got, want := decode.Samples[1].Type, "detached-chunk-meta"; got != want {
		t.Fatalf("second sample type = %q, want %q", got, want)
	}
	if got, want := decode.Samples[1].Reason, "segment_overlap"; got != want {
		t.Fatalf("second sample reason = %q, want %q", got, want)
	}
	if !decode.Samples[1].ValueOutputAvailable {
		t.Fatal("expected second sample value output to be available")
	}
	if got, want := decode.Samples[1].ValueOutputPoints, 1; got != want {
		t.Fatalf("second sample value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.Samples[1].OptimizedReadAtRanges), 2; got != want {
		t.Fatalf("second sample ReadAt ranges = %d, want %d", got, want)
	}
	if got, want := decode.Samples[1].OptimizedReadAtRanges[0].Column, "value"; got != want {
		t.Fatalf("first ReadAt range column = %q, want %q", got, want)
	}
	if got, want := decode.Samples[1].OptimizedReadAtRanges[0].Offset, int64(2000); got != want {
		t.Fatalf("first ReadAt range offset = %d, want %d", got, want)
	}
	if got, want := decode.Samples[1].OptimizedReadAtRanges[1].Column, "time"; got != want {
		t.Fatalf("second ReadAt range column = %q, want %q", got, want)
	}
	if got, want := decode.Samples[1].OptimizedReadAtRanges[1].Offset, int64(2080); got != want {
		t.Fatalf("second ReadAt range offset = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "detached TSSP data ReadAt") {
		t.Fatalf("recommendations = %v, want detached data ReadAt recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "verified 2 detached TSSP data block") {
		t.Fatalf("recommendations = %v, want detached data block probe recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "materialized 1 detached TSSP output point") {
		t.Fatalf("recommendations = %v, want detached row-count materialization recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexSamplesOneRowValueBlocks(t *testing.T) {
	dir := t.TempDir()
	chunks := []testTSSPChunkSpec{
		{sid: 42, minTime: 333, maxTime: 333, offset: 1000, size: 13, timeSize: 13},
	}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedOneRowData(filepath.Join(dir, tsspDetachedDataFileName), 1200, chunks[0], 99, 333); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 333)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_value_blocks"], "2"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_unknowns"], "0"; got != want {
		t.Fatalf("data block probe value unknowns = %q, want %q", got, want)
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
	if got, want := decode.DataBlockProbeValueBlocks, 2; got != want {
		t.Fatalf("data block probe value blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeValueUnknowns, 0; got != want {
		t.Fatalf("data block probe value unknowns = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	sample := decode.CursorOutputSamples[0]
	if got, want := sample.Key, "meta-index-id:42/value"; got != want {
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
	if !sample.Matches {
		t.Fatal("expected sample to be marked as matched optimized output")
	}
	if !containsString(decode.Recommendations, "sampled 1 detached TSSP value output") {
		t.Fatalf("recommendations = %v, want value output recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedColumnProjectionLimitsDataReadAt(t *testing.T) {
	dir := t.TempDir()
	chunks := []testTSSPChunkSpec{
		{sid: 42, minTime: 333, maxTime: 333, offset: 1000, size: 13, timeSize: 13},
	}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedOneRowData(filepath.Join(dir, tsspDetachedDataFileName), 1200, chunks[0], 99, 333); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 333)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
		QueryColumns:     []string{"time"},
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "1"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_value_blocks"], "1"; got != want {
		t.Fatalf("data block probe value blocks = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "integer-one:1"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryColumns, []string{"time"}; !equalStrings(got, want) {
		t.Fatalf("query columns = %v, want %v", got, want)
	}
	if got, want := decode.MatchedColumns, []string{"time"}; !equalStrings(got, want) {
		t.Fatalf("matched columns = %v, want %v", got, want)
	}
	if len(decode.MissingColumns) != 0 {
		t.Fatalf("missing columns = %v, want none", decode.MissingColumns)
	}
	if got, want := decode.BaselineReadAtCalls, 2; got != want {
		t.Fatalf("baseline ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadAtCalls, 1; got != want {
		t.Fatalf("optimized ReadAt calls = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].OptimizedReadAtRanges[0].Column, "time"; got != want {
		t.Fatalf("projected ReadAt column = %q, want %q", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "column projection requested for 1 detached TSSP column") {
		t.Fatalf("recommendations = %v, want detached column projection recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedColumnProjectionReportsMissingColumn(t *testing.T) {
	dir := t.TempDir()
	chunks := []testTSSPChunkSpec{
		{sid: 42, minTime: 333, maxTime: 333, offset: 1000, size: 13, timeSize: 13},
	}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedOneRowData(filepath.Join(dir, tsspDetachedDataFileName), 1200, chunks[0], 99, 333); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 333)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
		QueryColumns:     []string{"missing"},
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_blocks"], "0"; got != want {
		t.Fatalf("data block probe blocks = %q, want %q", got, want)
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
	if got, want := decode.Samples[0].Reason, "projected_columns_unavailable"; got != want {
		t.Fatalf("sample reason = %q, want %q", got, want)
	}
	if !containsString(decode.Recommendations, "1 query column(s) were not found") {
		t.Fatalf("recommendations = %v, want missing column recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "column projection excludes 1 in-range detached TSSP chunk") {
		t.Fatalf("recommendations = %v, want projection skip recommendation", decode.Recommendations)
	}
	if containsString(decode.Recommendations, "outside the query range") {
		t.Fatalf("recommendations = %v, want no range skip recommendation for projection miss", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedFieldFilterSuppressesNonMatchingRows(t *testing.T) {
	dir := t.TempDir()
	chunks := []testTSSPChunkSpec{
		{sid: 42, minTime: 333, maxTime: 333, offset: 1000, size: 13, timeSize: 13},
	}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedOneRowData(filepath.Join(dir, tsspDetachedDataFileName), 1200, chunks[0], 99, 333); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 333)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Value: "100"}},
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
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Value: "100"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.MatchedFields, []FieldFilter{{Key: "value", Value: "100"}}; !equalFieldFilters(got, want) {
		t.Fatalf("matched fields = %v, want %v", got, want)
	}
	if len(decode.MissingFields) != 0 {
		t.Fatalf("missing fields = %v, want none", decode.MissingFields)
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if !decode.Samples[0].ValueOutputAvailable {
		t.Fatal("expected filtered zero-row output to remain available")
	}
	if !containsString(decode.Recommendations, "applied 1 detached TSSP field filter") {
		t.Fatalf("recommendations = %v, want detached field filter recommendation", decode.Recommendations)
	}
}

func TestInspectTSSPDetachedDataBlockEmptyRows(t *testing.T) {
	payload := make([]byte, 5)
	payload[0] = 42 // openGemini encoding.BlockIntegerEmpty.
	binary.BigEndian.PutUint32(payload[1:5], 3)
	info, ok, reason := inspectTSSPDetachedDataBlock(testTSSPDetachedDataPayload(payload))
	if !ok {
		t.Fatalf("inspect detached data block failed: %s", reason)
	}
	if got, want := info.Type, "integer-empty"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 3; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown {
		t.Fatal("expected rows to be known")
	}
	if !info.ValueKnown || !info.ValueNull {
		t.Fatalf("value known/null = %v/%v, want true/true", info.ValueKnown, info.ValueNull)
	}
}

func TestInspectTSSPDataBlockPayloadFloatFull(t *testing.T) {
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := testTSSPFloatFullPayload(tc.values, tc.codec)
			if err != nil {
				t.Fatal(err)
			}

			info, ok, reason := inspectTSSPDataBlockPayload(payload)
			if !ok {
				t.Fatalf("inspect TSSP data block payload failed: %s", reason)
			}
			if got, want := info.Type, "float-full"; got != want {
				t.Fatalf("type = %q, want %q", got, want)
			}
			if got, want := info.Rows, len(tc.values); got != want {
				t.Fatalf("rows = %d, want %d", got, want)
			}
			if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
				t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
			}
			if got, want := info.Value, tc.want[0]; got != want {
				t.Fatalf("first value = %q, want %q", got, want)
			}
			if got, want := info.Values, tc.want; !equalStrings(got, want) {
				t.Fatalf("values = %v, want %v", got, want)
			}
		})
	}
}

func TestInspectTSSPDataBlockPayloadIntegerFullUncompressed(t *testing.T) {
	var payload bytes.Buffer
	payload.WriteByte(32) // openGemini encoding.BlockIntegerFull.
	writeUint32(&payload, 2)
	payload.WriteByte(64) // openGemini encoding intUncompressed << 4.
	writeUint32(&payload, 16)
	writeGeminiInt64(&payload, 99)
	writeGeminiInt64(&payload, 100)

	info, ok, reason := inspectTSSPDataBlockPayload(payload.Bytes())
	if !ok {
		t.Fatalf("inspect TSSP data block payload failed: %s", reason)
	}
	if got, want := info.Type, "integer-full"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 2; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
		t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
	}
	if got, want := info.Value, "99"; got != want {
		t.Fatalf("first value = %q, want %q", got, want)
	}
	if got, want := info.Values, []string{"99", "100"}; !equalStrings(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

func TestInspectTSSPDataBlockPayloadIntegerFullConstDelta(t *testing.T) {
	var payload bytes.Buffer
	payload.WriteByte(32) // openGemini encoding.BlockIntegerFull.
	writeUint32(&payload, 3)
	payload.WriteByte(16) // openGemini encoding intCompressedConstDelta << 4.
	writeGeminiInt64(&payload, 10)
	writeUvarint(&payload, encodeGeminiZigZagInt64(5))
	writeUvarint(&payload, 2)

	info, ok, reason := inspectTSSPDataBlockPayload(payload.Bytes())
	if !ok {
		t.Fatalf("inspect TSSP data block payload failed: %s", reason)
	}
	if got, want := info.Type, "integer-full"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 3; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
		t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
	}
	if got, want := info.Values, []string{"10", "15", "20"}; !equalStrings(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

func TestInspectTSSPDataBlockPayloadIntegerFullSimple8b(t *testing.T) {
	var payload bytes.Buffer
	payload.WriteByte(32) // openGemini encoding.BlockIntegerFull.
	writeUint32(&payload, 3)
	payload.WriteByte(32) // openGemini encoding intCompressedSimple8b << 4.
	writeUint32(&payload, 2)
	writeUint32(&payload, 3)
	writeUint64(&payload, encodeGeminiZigZagInt64(10))
	writeUint64(&payload, testTSSPSimple8bPack2(encodeGeminiZigZagInt64(5), encodeGeminiZigZagInt64(7)))

	info, ok, reason := inspectTSSPDataBlockPayload(payload.Bytes())
	if !ok {
		t.Fatalf("inspect TSSP data block payload failed: %s", reason)
	}
	if got, want := info.Type, "integer-full"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 3; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
		t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
	}
	if got, want := info.Values, []string{"10", "15", "22"}; !equalStrings(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

func TestInspectTSSPDataBlockPayloadIntegerFullZSTD(t *testing.T) {
	raw := testTSSPIntegerRawBytes([]int64{10, 15, 22})
	compressed, err := testTSSPZSTDCompress(raw)
	if err != nil {
		t.Fatal(err)
	}

	var payload bytes.Buffer
	payload.WriteByte(32) // openGemini encoding.BlockIntegerFull.
	writeUint32(&payload, 3)
	payload.WriteByte(48) // openGemini encoding intCompressZSTD << 4.
	writeUint32(&payload, uint32(len(raw)))
	writeUint32(&payload, uint32(len(compressed)))
	payload.Write(compressed)

	info, ok, reason := inspectTSSPDataBlockPayload(payload.Bytes())
	if !ok {
		t.Fatalf("inspect TSSP data block payload failed: %s", reason)
	}
	if got, want := info.Type, "integer-full"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 3; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
		t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
	}
	if got, want := info.Values, []string{"10", "15", "22"}; !equalStrings(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

func TestInspectTSSPDataBlockPayloadBooleanFullBitpack(t *testing.T) {
	var payload bytes.Buffer
	payload.WriteByte(33) // openGemini encoding.BlockBooleanFull.
	writeUint32(&payload, 3)
	payload.WriteByte(16) // openGemini encoding boolCompressedBitpack << 4.
	writeUint32(&payload, 3)
	payload.WriteByte(0xa0) // true, false, true; bits are written MSB first.

	info, ok, reason := inspectTSSPDataBlockPayload(payload.Bytes())
	if !ok {
		t.Fatalf("inspect TSSP data block payload failed: %s", reason)
	}
	if got, want := info.Type, "boolean-full"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 3; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
		t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
	}
	if got, want := info.Value, "true"; got != want {
		t.Fatalf("first value = %q, want %q", got, want)
	}
	if got, want := info.Values, []string{"true", "false", "true"}; !equalStrings(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

func TestInspectTSSPDataBlockPayloadStringFullUncompressed(t *testing.T) {
	var packed bytes.Buffer
	packed.Write(tsspPackedStringV2Payload([]string{"red", "blue"}))

	var payload bytes.Buffer
	payload.WriteByte(34) // openGemini encoding.BlockStringFull.
	writeUint32(&payload, 2)
	payload.WriteByte(0) // openGemini encoding stringUncompressed << 4.
	writeUint32(&payload, uint32(packed.Len()))
	writeUint32(&payload, uint32(packed.Len()))
	payload.Write(packed.Bytes())

	info, ok, reason := inspectTSSPDataBlockPayload(payload.Bytes())
	if !ok {
		t.Fatalf("inspect TSSP data block payload failed: %s", reason)
	}
	if got, want := info.Type, "string-full"; got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}
	if got, want := info.Rows, 2; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
		t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
	}
	if got, want := info.Value, "red"; got != want {
		t.Fatalf("first value = %q, want %q", got, want)
	}
	if got, want := info.Values, []string{"red", "blue"}; !equalStrings(got, want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
}

func TestInspectTSSPDataBlockPayloadStringFullCompressed(t *testing.T) {
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
			payload, err := testTSSPStringFullPayload(values, tc.codec)
			if err != nil {
				t.Fatal(err)
			}

			info, ok, reason := inspectTSSPDataBlockPayload(payload)
			if !ok {
				t.Fatalf("inspect TSSP data block payload failed: %s", reason)
			}
			if got, want := info.Type, "string-full"; got != want {
				t.Fatalf("type = %q, want %q", got, want)
			}
			if got, want := info.Rows, 2; got != want {
				t.Fatalf("rows = %d, want %d", got, want)
			}
			if !info.RowsKnown || !info.ValueKnown || info.ValueNull {
				t.Fatalf("known/null flags rows=%v value=%v null=%v, want true/true/false", info.RowsKnown, info.ValueKnown, info.ValueNull)
			}
			if got, want := info.Value, values[0]; got != want {
				t.Fatalf("first value = %q, want %q", got, want)
			}
			if got, want := info.Values, values; !equalStrings(got, want) {
				t.Fatalf("values = %v, want %v", got, want)
			}
		})
	}
}

func TestAnalyzeTSSPDetachedMetaIndexDataCRCMismatch(t *testing.T) {
	dir := t.TempDir()
	chunks := []testTSSPChunkSpec{
		{sid: 10, minTime: 100, maxTime: 150, offset: 1000, size: 40},
	}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(dir, tsspDetachedDataFileName)
	if err := writeTestTSSPDetachedData(dataPath, 1200, chunks...); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	data[chunks[0].offset+int64(crc32.Size)+1] ^= 0xff
	if err := os.WriteFile(dataPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(100, 150)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
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
	if got, want := file.Extra["data_block_probe_crc_mismatches"], "1"; got != want {
		t.Fatalf("data block probe crc mismatches = %q, want %q", got, want)
	}
	if !containsString(report.Notices, "detached data block probe found 1 invalid block") {
		t.Fatalf("notices = %v, want data block probe notice", report.Notices)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.ValueOutputUnavailableBlocks, 1; got != want {
		t.Fatalf("value output unavailable blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeBlocks, 2; got != want {
		t.Fatalf("data block probe blocks = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFailures, 1; got != want {
		t.Fatalf("data block probe failures = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeCRCMismatches, 1; got != want {
		t.Fatalf("data block probe crc mismatches = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].Reason, "segment_overlap_data_crc_unavailable"; got != want {
		t.Fatalf("sample reason = %q, want %q", got, want)
	}
	if !containsString(decode.Recommendations, "detached TSSP data block probe found 1 invalid block") {
		t.Fatalf("recommendations = %v, want data block probe failure recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexBadDataRangeNotice(t *testing.T) {
	dir := t.TempDir()
	chunks := []testTSSPChunkSpec{
		{sid: 10, minTime: 100, maxTime: 150, offset: 1000, size: 40},
		{sid: 11, minTime: 200, maxTime: 260, offset: 2000, size: 80},
	}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedData(filepath.Join(dir, tsspDetachedDataFileName), 2080, chunks...); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(210, 220)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_file_checked"], "true"; got != want {
		t.Fatalf("data file checked = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_invalid_ranges"], "1"; got != want {
		t.Fatalf("data invalid ranges = %q, want %q", got, want)
	}
	if !containsString(report.Notices, "invalid column segment range") {
		t.Fatalf("notices = %v, want invalid data range notice", report.Notices)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.ValueOutputUnavailableBlocks, 1; got != want {
		t.Fatalf("value output unavailable blocks = %d, want %d", got, want)
	}
	if got, want := decode.Samples[1].Reason, "segment_overlap_data_range_unavailable"; got != want {
		t.Fatalf("second sample reason = %q, want %q", got, want)
	}
	if decode.Samples[1].ValueOutputAvailable {
		t.Fatal("expected second sample value output to be unavailable")
	}
}

func TestAnalyzeTSSPDetachedMetaIndexBadChunkMetaSidecarNotice(t *testing.T) {
	dir := t.TempDir()
	chunks := []testTSSPChunkSpec{
		{sid: 10, minTime: 100, maxTime: 150, offset: 1000, size: 40},
	}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, tsspDetachedChunkMetaFileName))
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(filepath.Join(dir, tsspDetachedChunkMetaFileName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(100, 150)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["chunk_meta_expanded"], "false"; got != want {
		t.Fatalf("chunk metadata expanded = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].Type, "detached-meta-index"; got != want {
		t.Fatalf("fallback block type = %q, want %q", got, want)
	}
	if !containsString(report.Notices, "detached chunk metadata expansion unavailable") {
		t.Fatalf("notices = %v, want detached chunk metadata expansion notice", report.Notices)
	}
}

func TestAnalyzeDiscoversTSSPDetachedMetaIndexInDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, tsspDetachedMetaIndexFileName)
	if err := writeTestTSSPDetachedMetaIndex(path, []tsspMetaIndex{
		{ID: 10, MinTime: 100, MaxTime: 150, Offset: 64, Size: 40},
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:         FormatAuto,
		KeySampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got, want := report.Files[0].Format, FormatTSSPDetachedIndex; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexRejectsCRCMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), tsspDetachedMetaIndexFileName)
	if err := writeTestTSSPDetachedMetaIndex(path, []tsspMetaIndex{
		{ID: 10, MinTime: 100, MaxTime: 150, Offset: 64, Size: 40},
	}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = analyzeFile(path, Options{Format: FormatTSSPDetachedIndex})
	if err == nil || !strings.Contains(err.Error(), "crc mismatch") {
		t.Fatalf("error = %v, want crc mismatch", err)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexEmptyPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), tsspDetachedMetaIndexFileName)
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	file, err := analyzeFile(path, Options{Format: FormatTSSPDetachedIndex})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := file.BlockCount, 0; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if file.QueryOverlapsFile {
		t.Fatal("query overlaps file = true, want false for empty sidecar")
	}
}

func TestAnalyzeTSSPDetachedMetaIndexRejectsShortHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), tsspDetachedMetaIndexFileName)
	if err := os.WriteFile(path, []byte(tsspMagic), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := analyzeFile(path, Options{Format: FormatTSSPDetachedIndex})
	if err == nil || !strings.Contains(err.Error(), "file too small") {
		t.Fatalf("error = %v, want file too small", err)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexRejectsNonMultiplePayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), tsspDetachedMetaIndexFileName)
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	buf.WriteByte(0xff)
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := analyzeFile(path, Options{Format: FormatTSSPDetachedIndex})
	if err == nil || !strings.Contains(err.Error(), "not a multiple") {
		t.Fatalf("error = %v, want non-multiple payload", err)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexIDRequiresRange(t *testing.T) {
	_, err := Analyze(context.Background(), []string{"missing"}, Options{
		QueryMetaIndexIDs: []uint64{10},
	})
	if err == nil || !strings.Contains(err.Error(), "query meta-index id filter requires query range") {
		t.Fatalf("error = %v, want meta-index id range requirement", err)
	}
}

func writeTestTSSPDetachedMetaIndex(path string, items []tsspMetaIndex) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	for _, item := range items {
		var payload bytes.Buffer
		writeUint64(&payload, item.ID)
		writeGeminiInt64(&payload, item.MinTime)
		writeGeminiInt64(&payload, item.MaxTime)
		writeGeminiInt64(&payload, item.Offset)
		writeUint32(&payload, item.Size)
		payloadBytes := payload.Bytes()

		var crc [4]byte
		binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(payloadBytes))
		buf.Write(crc[:])
		buf.Write(payloadBytes)
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPDetachedChunkMeta(path string, chunks []testTSSPChunkSpec) ([]tsspMetaIndex, error) {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	metaIndexes := make([]tsspMetaIndex, 0, len(chunks))
	for _, chunk := range chunks {
		offset := int64(buf.Len())
		var payload bytes.Buffer
		writeTestTSSPChunkMeta(&payload, chunk)
		// Detached readers validate CRC over the full payload, then let chunk
		// metadata unmarshal ignore any future extension bytes.
		payload.Write([]byte{0xa5, 0x5a})
		payloadBytes := payload.Bytes()
		var crc [4]byte
		binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(payloadBytes))
		buf.Write(crc[:])
		buf.Write(payloadBytes)
		metaIndexes = append(metaIndexes, tsspMetaIndex{
			ID:      chunk.sid,
			MinTime: chunk.minTime,
			MaxTime: chunk.maxTime,
			Offset:  offset,
			Size:    uint32(len(crc) + len(payloadBytes)),
		})
	}
	return metaIndexes, os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPDetachedData(path string, size int, chunks ...testTSSPChunkSpec) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	for buf.Len() < size {
		buf.WriteByte(0)
	}
	for _, chunk := range chunks {
		if err := writeTestTSSPDetachedDataBlock(buf.Bytes(), chunk.offset, chunk.size); err != nil {
			return err
		}
		if err := writeTestTSSPDetachedDataBlock(buf.Bytes(), chunk.offset+int64(chunk.size), 16); err != nil {
			return err
		}
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPDetachedOneRowData(path string, size int, chunk testTSSPChunkSpec, value, timestamp int64) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	for buf.Len() < size {
		buf.WriteByte(0)
	}
	if err := writeTestTSSPDetachedIntegerOneBlock(buf.Bytes(), chunk.offset, chunk.size, value); err != nil {
		return err
	}
	if err := writeTestTSSPDetachedIntegerOneBlock(buf.Bytes(), chunk.offset+int64(chunk.size), chunk.testTimeSize(), timestamp); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPDetachedDataBlock(data []byte, offset int64, size uint32) error {
	if offset < 0 || int64(len(data)) < offset+int64(size) {
		return nil
	}
	if size < crc32.Size+5 {
		return nil
	}
	payload := make([]byte, int(size)-crc32.Size)
	payload[0] = 32 // openGemini encoding.BlockIntegerFull.
	binary.BigEndian.PutUint32(payload[1:5], 1)
	binary.BigEndian.PutUint32(data[offset:], crc32.ChecksumIEEE(payload))
	copy(data[offset+crc32.Size:], payload)
	return nil
}

func writeTestTSSPDetachedIntegerOneBlock(data []byte, offset int64, size uint32, value int64) error {
	if offset < 0 || int64(len(data)) < offset+int64(size) {
		return nil
	}
	if size != crc32.Size+1+8 {
		return nil
	}
	payload := make([]byte, int(size)-crc32.Size)
	payload[0] = 18 // openGemini encoding.BlockIntegerOne.
	binary.LittleEndian.PutUint64(payload[1:9], uint64(value))
	binary.BigEndian.PutUint32(data[offset:], crc32.ChecksumIEEE(payload))
	copy(data[offset+crc32.Size:], payload)
	return nil
}

func testTSSPDetachedDataPayload(payload []byte) []byte {
	block := make([]byte, crc32.Size+len(payload))
	binary.BigEndian.PutUint32(block[:crc32.Size], crc32.ChecksumIEEE(payload))
	copy(block[crc32.Size:], payload)
	return block
}
