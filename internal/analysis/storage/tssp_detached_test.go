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
	if err := writeTestTSSPDetachedData(filepath.Join(dir, tsspDetachedDataFileName), 2200); err != nil {
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
	if err := writeTestTSSPDetachedData(filepath.Join(dir, tsspDetachedDataFileName), 2080); err != nil {
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

func writeTestTSSPDetachedData(path string, size int) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	for buf.Len() < size {
		buf.WriteByte(0)
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}
