package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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
	if got, want := len(decode.CursorExecutionSamples), 1; got != want {
		t.Fatalf("cursor execution samples = %d, want %d", got, want)
	}
	wantStep := DecodePathCursorStep{
		Step:              1,
		Type:              "tssp-detached-chunk-meta-batch-step",
		Action:            "read_batch_filtered",
		Key:               "meta-index-id:10-12",
		CandidateValue:    "time_range=100:450 decoded=1/3",
		CursorIndexBefore: 0,
		CursorIndexAfter:  3,
		CursorAdvanced:    true,
		CursorExhausted:   true,
	}
	if got := decode.CursorExecutionSamples[0]; got != wantStep {
		t.Fatalf("cursor execution sample = %+v, want %+v", got, wantStep)
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
	if !containsString(decode.Recommendations, "detached TSSP cursor execution samples") {
		t.Fatalf("recommendations = %v, want cursor execution recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedMetaIndexFileSetDecodePathAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "segment-a", tsspDetachedMetaIndexFileName)
	path2 := filepath.Join(dir, "segment-b", tsspDetachedMetaIndexFileName)
	if err := os.MkdirAll(filepath.Dir(path1), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path2), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(path1, []tsspMetaIndex{
		{ID: 10, MinTime: 100, MaxTime: 150, Offset: 64, Size: 40},
		{ID: 11, MinTime: 190, MaxTime: 210, Offset: 104, Size: 80},
		{ID: 12, MinTime: 300, MaxTime: 350, Offset: 184, Size: 60},
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(path2, []tsspMetaIndex{
		{ID: 20, MinTime: 200, MaxTime: 260, Offset: 64, Size: 40},
		{ID: 21, MinTime: 400, MaxTime: 450, Offset: 104, Size: 60},
	}); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(180, 220)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:            FormatTSSPDetachedIndex,
		Recursive:         true,
		KeySampleLimit:    4,
		BlockSampleLimit:  6,
		QueryRange:        queryRange,
		QueryMetaIndexIDs: []uint64{20, 99, 10, 11},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level detached TSSP decode path summary")
	}
	if got, want := decode.Mode, "tssp-detached-file-set-location-cursor-ascending"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
	}
	if got, want := decode.QueryMetaIndexIDs, []uint64{10, 11, 20, 99}; !equalUint64s(got, want) {
		t.Fatalf("query meta-index ids = %v, want %v", got, want)
	}
	if got, want := decode.MatchedMetaIndexIDs, []uint64{10, 11, 20}; !equalUint64s(got, want) {
		t.Fatalf("matched meta-index ids = %v, want %v", got, want)
	}
	if got, want := decode.MissingMetaIndexIDs, []uint64{99}; !equalUint64s(got, want) {
		t.Fatalf("missing meta-index ids = %v, want %v", got, want)
	}
	if got, want := decode.LocationBlocks, 3; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 3; got != want {
		t.Fatalf("baseline blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBlocks, 1; got != want {
		t.Fatalf("saved blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 2; got != want {
		t.Fatalf("skipped by id blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedBeforeSeekBlocks, 1; got != want {
		t.Fatalf("skipped before seek blocks = %d, want %d", got, want)
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
	if got, want := decode.BaselineCursorReadCalls, 2; got != want {
		t.Fatalf("baseline cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedCursorReadCalls, 2; got != want {
		t.Fatalf("optimized cursor read calls = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 2; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := decode.LocationBlocksByType["detached-meta-index"], 3; got != want {
		t.Fatalf("detached meta-index location count = %d, want %d", got, want)
	}
	if got, want := decode.DecodeBlocksByType["detached-meta-index"], 2; got != want {
		t.Fatalf("detached meta-index decode count = %d, want %d", got, want)
	}
	if got, want := len(decode.Samples), 3; got != want {
		t.Fatalf("sample count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 2; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
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
	if got, want := len(decode.CursorExecutionSamples), 2; got != want {
		t.Fatalf("cursor execution samples = %d, want %d", got, want)
	}
	if got := decode.CursorExecutionSamples[0]; got.Step != 1 || got.CursorIndexBefore != 0 || got.CursorIndexAfter != 2 || got.File == "" || got.CursorExhausted {
		t.Fatalf("cursor execution sample[0] = %+v, want first aggregate batch step", got)
	}
	if got := decode.CursorExecutionSamples[1]; got.Step != 2 || got.CursorIndexBefore != 2 || got.CursorIndexAfter != 3 || got.File == "" || !got.CursorExhausted {
		t.Fatalf("cursor execution sample[1] = %+v, want final aggregate batch step", got)
	}
	if got, want := report.Summary.QueryOverlapBlocks, 2; got != want {
		t.Fatalf("summary overlap blocks = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "detached TSSP meta-index record") {
		t.Fatalf("recommendations = %v, want meta-index wording", decode.Recommendations)
	}
	if containsString(decode.Recommendations, "detached TSSP data ReadAt") {
		t.Fatalf("recommendations = %v, want no data ReadAt wording for meta-index-only file set", decode.Recommendations)
	}

	descReport, err := Analyze(context.Background(), []string{dir}, Options{
		Format:            FormatTSSPDetachedIndex,
		Recursive:         true,
		KeySampleLimit:    4,
		BlockSampleLimit:  6,
		QueryRange:        queryRange,
		QueryMetaIndexIDs: []uint64{20, 99, 10, 11},
		CursorDescending:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	desc := descReport.DecodePath
	if desc == nil {
		t.Fatal("expected descending report-level detached TSSP decode path summary")
	}
	if got, want := desc.Mode, "tssp-detached-file-set-location-cursor-descending"; got != want {
		t.Fatalf("descending mode = %q, want %q", got, want)
	}
	if got, want := desc.Samples[0].MetaIndexID, uint64(20); got != want {
		t.Fatalf("descending first sample meta-index id = %d, want %d", got, want)
	}
	if got, want := desc.Samples[1].MetaIndexID, uint64(11); got != want {
		t.Fatalf("descending second sample meta-index id = %d, want %d", got, want)
	}
	if got, want := desc.Samples[2].MetaIndexID, uint64(10); got != want {
		t.Fatalf("descending third sample meta-index id = %d, want %d", got, want)
	}
	if got, want := desc.CursorWindows[0].Files[0], path2; got != want {
		t.Fatalf("descending first cursor window file = %q, want %q", got, want)
	}
	if got, want := desc.CursorWindows[1].Files[0], path1; got != want {
		t.Fatalf("descending second cursor window file = %q, want %q", got, want)
	}
}

func TestAnalyzeTSSPDetachedFileSetDecodePathWithChunkMetaExpansion(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "segment-a", tsspDetachedMetaIndexFileName)
	path2 := filepath.Join(dir, "segment-b", tsspDetachedMetaIndexFileName)
	if err := os.MkdirAll(filepath.Dir(path1), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path2), 0o700); err != nil {
		t.Fatal(err)
	}

	chunk1 := testTSSPChunkSpec{sid: 7, minTime: 100, maxTime: 200, offset: 64, size: 16}
	metaIndexes1, err := writeTestTSSPDetachedChunkMeta(filepath.Join(filepath.Dir(path1), tsspDetachedChunkMetaFileName), []testTSSPChunkSpec{chunk1})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(path1, metaIndexes1); err != nil {
		t.Fatal(err)
	}
	chunk2 := testTSSPChunkSpec{sid: 8, minTime: 300, maxTime: 400, offset: 64, size: 16}
	metaIndexes2, err := writeTestTSSPDetachedChunkMeta(filepath.Join(filepath.Dir(path2), tsspDetachedChunkMetaFileName), []testTSSPChunkSpec{chunk2})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(path2, metaIndexes2); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(150, 180)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSSPDetachedIndex,
		Recursive:        true,
		KeySampleLimit:   4,
		BlockSampleLimit: 6,
		QueryRange:       queryRange,
		QueryColumns:     []string{"missing", "value"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	for _, file := range report.Files {
		if got, want := file.Extra["chunk_meta_expanded"], "true"; got != want {
			t.Fatalf("file %s chunk_meta_expanded = %q, want %q", file.Path, got, want)
		}
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level detached TSSP decode path summary")
	}
	if got, want := decode.Mode, "tssp-detached-file-set-location-cursor-ascending"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
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
	if got, want := decode.LocationBlocks, 2; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 2; got != want {
		t.Fatalf("baseline blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBlocks, 1; got != want {
		t.Fatalf("saved blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineReadSegments, 2; got != want {
		t.Fatalf("baseline read segments = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadSegments, 1; got != want {
		t.Fatalf("optimized read segments = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadSegments, 1; got != want {
		t.Fatalf("saved read segments = %d, want %d", got, want)
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
	if got, want := decode.SkippedAfterRangeBlocks, 1; got != want {
		t.Fatalf("skipped after range blocks = %d, want %d", got, want)
	}
	if got, want := decode.LocationBlocksByType["detached-chunk-meta"], 2; got != want {
		t.Fatalf("detached chunk-meta location count = %d, want %d", got, want)
	}
	if got, want := decode.DecodeBlocksByType["detached-chunk-meta"], 1; got != want {
		t.Fatalf("detached chunk-meta decode count = %d, want %d", got, want)
	}
	if got, want := len(decode.Samples), 2; got != want {
		t.Fatalf("sample count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 2; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
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
	if got, want := report.Summary.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("summary overlap blocks = %d, want %d", got, want)
	}

	descReport, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSSPDetachedIndex,
		Recursive:        true,
		KeySampleLimit:   4,
		BlockSampleLimit: 6,
		QueryRange:       queryRange,
		QueryColumns:     []string{"missing", "value"},
		CursorDescending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	desc := descReport.DecodePath
	if desc == nil {
		t.Fatal("expected descending report-level detached TSSP decode path summary")
	}
	if got, want := desc.Mode, "tssp-detached-file-set-location-cursor-descending"; got != want {
		t.Fatalf("descending mode = %q, want %q", got, want)
	}
	if got, want := desc.Samples[0].MetaIndexID, uint64(8); got != want {
		t.Fatalf("descending first sample meta-index id = %d, want %d", got, want)
	}
	if got, want := desc.Samples[1].MetaIndexID, uint64(7); got != want {
		t.Fatalf("descending second sample meta-index id = %d, want %d", got, want)
	}
	if got, want := desc.CursorWindows[0].Files[0], path2; got != want {
		t.Fatalf("descending first cursor window file = %q, want %q", got, want)
	}
	if got, want := desc.CursorWindows[1].Files[0], path1; got != want {
		t.Fatalf("descending second cursor window file = %q, want %q", got, want)
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
	if got, want := len(decode.CursorExecutionSamples), 2; got != want {
		t.Fatalf("cursor execution samples = %d, want %d", got, want)
	}
	if got := decode.CursorExecutionSamples[0]; got.Type != "tssp-detached-location-cursor-step" || got.Action != "skip_before_seek" || got.Key != "meta-index-id:10" || got.CursorIndexBefore != 0 || got.CursorIndexAfter != 1 {
		t.Fatalf("cursor execution sample[0] = %+v, want detached location skip", got)
	}
	if got := decode.CursorExecutionSamples[1]; got.Type != "tssp-detached-location-cursor-step" || got.Action != "read_segments" || got.Key != "meta-index-id:11" || got.CandidateValue != "time_range=200:260 segments=1/1" || got.CursorIndexBefore != 1 || got.CursorIndexAfter != 2 || !got.CursorExhausted {
		t.Fatalf("cursor execution sample[1] = %+v, want detached segment read", got)
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
	if !containsString(decode.Recommendations, "detached TSSP cursor execution samples") {
		t.Fatalf("recommendations = %v, want detached cursor execution recommendation", decode.Recommendations)
	}
}

func TestTSSPDetachedCursorExecutionSamplesFollowDescendingOrder(t *testing.T) {
	queryRange, err := NewTimeRange(210, 220)
	if err != nil {
		t.Fatal(err)
	}
	metaIndexes := []tsspMetaIndex{
		{ID: 10, MinTime: 100, MaxTime: 150, Offset: 64, Size: 40},
		{ID: 11, MinTime: 200, MaxTime: 260, Offset: 104, Size: 80},
		{ID: 12, MinTime: 400, MaxTime: 450, Offset: 184, Size: 60},
	}

	metaSummary := buildTSSPDetachedMetaIndexDecodePathSummary(metaIndexes, Options{
		QueryRange:       queryRange,
		BlockSampleLimit: 4,
		CursorDescending: true,
	})
	if metaSummary == nil {
		t.Fatal("detached meta-index decode path is nil")
	}
	if got, want := len(metaSummary.CursorExecutionSamples), 1; got != want {
		t.Fatalf("meta-index cursor execution samples = %d, want %d", got, want)
	}
	if got := metaSummary.CursorExecutionSamples[0]; got.Type != "tssp-detached-chunk-meta-batch-step" || got.Action != "read_batch_filtered" || got.Key != "meta-index-id:12-10" || got.CursorIndexBefore != 0 || got.CursorIndexAfter != 3 || !got.CursorExhausted {
		t.Fatalf("meta-index cursor execution sample = %+v, want descending batch step", got)
	}

	chunkSummary := buildTSSPDetachedChunkDecodePathSummary(metaIndexes[:2], []tsspChunkMeta{
		{SID: 10, TimeRanges: []tsspTimeRange{{Min: 100, Max: 150}}},
		{SID: 11, TimeRanges: []tsspTimeRange{{Min: 200, Max: 260}}},
	}, Options{
		QueryRange:       queryRange,
		BlockSampleLimit: 4,
		CursorDescending: true,
	}, nil, nil)
	if chunkSummary == nil {
		t.Fatal("detached chunk decode path is nil")
	}
	if got, want := len(chunkSummary.CursorExecutionSamples), 2; got != want {
		t.Fatalf("chunk cursor execution samples = %d, want %d", got, want)
	}
	if got := chunkSummary.CursorExecutionSamples[0]; got.Type != "tssp-detached-location-cursor-step" || got.Action != "read_segments" || got.Key != "meta-index-id:11" || got.CursorIndexBefore != 0 || got.CursorIndexAfter != 1 {
		t.Fatalf("chunk cursor execution sample[0] = %+v, want descending overlap", got)
	}
	if got := chunkSummary.CursorExecutionSamples[1]; got.Type != "tssp-detached-location-cursor-step" || got.Action != "skip_before_seek" || got.Key != "meta-index-id:10" || got.CursorIndexBefore != 1 || got.CursorIndexAfter != 2 || !got.CursorExhausted {
		t.Fatalf("chunk cursor execution sample[1] = %+v, want descending before-seek skip", got)
	}
}

func TestTSSPDetachedChunkFileSetCursorExecutionSamplesUseLocationBlockIndex(t *testing.T) {
	queryRange, err := NewTimeRange(210, 220)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		QueryRange:       queryRange,
		BlockSampleLimit: 4,
	}
	file1Decode := buildTSSPDetachedChunkDecodePathSummary([]tsspMetaIndex{
		{ID: 10, MinTime: 100, MaxTime: 150, Offset: 64, Size: 0},
		{ID: 11, MinTime: 200, MaxTime: 260, Offset: 104, Size: 80},
	}, []tsspChunkMeta{
		{SID: 11, TimeRanges: []tsspTimeRange{{Min: 200, Max: 260}}},
	}, options, nil, nil)
	if file1Decode == nil {
		t.Fatal("file1 decode path is nil")
	}
	if got, want := file1Decode.LocationBlocks, 1; got != want {
		t.Fatalf("file1 location blocks = %d, want %d", got, want)
	}
	if got, want := len(file1Decode.CursorExecutionSamples), 1; got != want {
		t.Fatalf("file1 cursor execution samples = %d, want %d", got, want)
	}
	if got := file1Decode.CursorExecutionSamples[0]; got.CursorIndexBefore != 0 || got.CursorIndexAfter != 1 || !got.CursorExhausted {
		t.Fatalf("file1 cursor execution sample = %+v, want location-block aligned exhausted sample", got)
	}

	file2Decode := buildTSSPDetachedChunkDecodePathSummary([]tsspMetaIndex{
		{ID: 20, MinTime: 205, MaxTime: 230, Offset: 64, Size: 80},
	}, []tsspChunkMeta{
		{SID: 20, TimeRanges: []tsspTimeRange{{Min: 205, Max: 230}}},
	}, options, nil, nil)
	if file2Decode == nil {
		t.Fatal("file2 decode path is nil")
	}

	fileSet := buildTSSPDetachedFileSetDecodePathSummary([]FileReport{
		{Path: "segment-a/segment.idx", Format: FormatTSSPDetachedIndex, DecodePath: file1Decode},
		{Path: "segment-b/segment.idx", Format: FormatTSSPDetachedIndex, DecodePath: file2Decode},
	}, options)
	if fileSet == nil {
		t.Fatal("file-set decode path is nil")
	}
	if got, want := fileSet.LocationBlocks, 2; got != want {
		t.Fatalf("file-set location blocks = %d, want %d", got, want)
	}
	if got, want := len(fileSet.CursorExecutionSamples), 2; got != want {
		t.Fatalf("file-set cursor execution samples = %d, want %d", got, want)
	}
	if got := fileSet.CursorExecutionSamples[0]; got.Step != 1 || got.CursorIndexBefore != 0 || got.CursorIndexAfter != 1 || got.File != "segment-a/segment.idx" || got.CursorExhausted {
		t.Fatalf("file-set cursor execution sample[0] = %+v, want first aggregate location step", got)
	}
	if got := fileSet.CursorExecutionSamples[1]; got.Step != 2 || got.CursorIndexBefore != 1 || got.CursorIndexAfter != 2 || got.File != "segment-b/segment.idx" || !got.CursorExhausted {
		t.Fatalf("file-set cursor execution sample[1] = %+v, want final aggregate location step", got)
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

func TestAnalyzeTSSPDetachedSamplesMLFFloatFullBlocks(t *testing.T) {
	dir := t.TempDir()
	values := []float64{1.11, 0, -2.22, 3.33, 4.44, 5.55, 6.66, 7.77, 8.88, 9.99, 10.01}
	wantValues := []string{"1.11", "0", "-2.22", "3.33", "4.44", "5.55", "6.66", "7.77", "8.88", "9.99", "10.01"}
	timestamps := []int64{100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200}
	valueSize, err := testTSSPDetachedFloatFullBlockSize(values, 6)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []testTSSPChunkSpec{{
		sid:      43,
		minTime:  timestamps[0],
		maxTime:  timestamps[len(timestamps)-1],
		offset:   1200,
		size:     valueSize,
		timeSize: testTSSPDetachedIntegerFullBlockSize(timestamps),
	}}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedFloatFullData(filepath.Join(dir, tsspDetachedDataFileName), 1600, chunks[0], values, 6, timestamps); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(timestamps[0], timestamps[len(timestamps)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: len(values) + 2,
		QueryRange:       queryRange,
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
	if got, want := file.Extra["data_block_probe_value_unknowns"], "0"; got != want {
		t.Fatalf("data block probe value unknowns = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "float-full:1,integer-full:1"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_output_points"], "11"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.ValueOutputUnavailableBlocks, 0; got != want {
		t.Fatalf("value output unavailable blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, len(values); got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), len(values); got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, value := range wantValues {
		want := DecodePathCursorOutput{
			Key:            "meta-index-id:43/value",
			Time:           timestamps[i],
			Type:           "float-full",
			OptimizedValue: value,
			Matches:        true,
		}
		if got := decode.CursorOutputSamples[i]; got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
	}
}

func TestAnalyzeTSSPDetachedSamplesZSTDIntegerFullBlocks(t *testing.T) {
	dir := t.TempDir()
	values := []int64{10, 15, 22}
	timestamps := []int64{300, 330, 360}
	valueSize, err := testTSSPDetachedIntegerZSTDBlockSize(values)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []testTSSPChunkSpec{{
		sid:      44,
		minTime:  timestamps[0],
		maxTime:  timestamps[len(timestamps)-1],
		offset:   1200,
		size:     valueSize,
		timeSize: testTSSPDetachedIntegerFullBlockSize(timestamps),
	}}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedIntegerZSTDData(filepath.Join(dir, tsspDetachedDataFileName), 1600, chunks[0], values, timestamps); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(timestamps[0], timestamps[len(timestamps)-1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: len(values) + 2,
		QueryRange:       queryRange,
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
	if got, want := file.Extra["data_block_probe_value_unknowns"], "0"; got != want {
		t.Fatalf("data block probe value unknowns = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_types"], "integer-full:2"; got != want {
		t.Fatalf("data block probe types = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_output_points"], "3"; got != want {
		t.Fatalf("data block probe output points = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.ValueOutputUnavailableBlocks, 0; got != want {
		t.Fatalf("value output unavailable blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, len(values); got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), len(values); got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, value := range []string{"10", "15", "22"} {
		want := DecodePathCursorOutput{
			Key:            "meta-index-id:44/value",
			Time:           timestamps[i],
			Type:           "integer-full",
			OptimizedValue: value,
			Matches:        true,
		}
		if got := decode.CursorOutputSamples[i]; got != want {
			t.Fatalf("cursor output sample %d = %+v, want %+v", i, got, want)
		}
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
	if got, want := file.Extra["data_block_probe_filter_rows"], "1"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "0"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
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
	if got, want := decode.DataBlockProbeFilterRows, 1; got != want {
		t.Fatalf("data block probe filter rows = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterMatches, 0; got != want {
		t.Fatalf("data block probe filter matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRejects, 1; got != want {
		t.Fatalf("data block probe filter rejects = %d, want %d", got, want)
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
	if !containsString(decode.Recommendations, "detached TSSP field filters matched 0 of 1 decoded record row") {
		t.Fatalf("recommendations = %v, want detached field filter row-count recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedDataProbeFiltersDecodedRowsByQueryRange(t *testing.T) {
	dir := t.TempDir()
	values := []float64{1.25, 2.5, 3.75}
	present := []bool{true, true, true}
	timestamps := []int64{333, 444, 555}
	valueSize, err := testTSSPDetachedNullableRegularFloatBlockSize(values, present)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []testTSSPChunkSpec{{
		sid:      42,
		minTime:  333,
		maxTime:  555,
		offset:   1200,
		size:     valueSize,
		timeSize: testTSSPDetachedIntegerFullBlockSize(timestamps),
	}}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedNullableRegularFloatData(filepath.Join(dir, tsspDetachedDataFileName), 1600, chunks[0], values, present, timestamps); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(timestamps[1], timestamps[1])
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Value: "2.5"}},
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
	if got, want := file.Extra["data_block_probe_range_rows"], "3"; got != want {
		t.Fatalf("data block probe range rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_range_matches"], "1"; got != want {
		t.Fatalf("data block probe range matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_range_rejects"], "2"; got != want {
		t.Fatalf("data block probe range rejects = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rows"], "1"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "0"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Value: "2.5"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
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
	if got, want := decode.DataBlockProbeFilterRows, 1; got != want {
		t.Fatalf("data block probe filter rows = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterMatches, 1; got != want {
		t.Fatalf("data block probe filter matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRejects, 0; got != want {
		t.Fatalf("data block probe filter rejects = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 1; got != want {
		t.Fatalf("decode sample value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "meta-index-id:42/value",
		Time:           timestamps[1],
		Type:           "float",
		OptimizedValue: "2.5",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("cursor output sample = %+v, want %+v", got, want)
	}
	if !containsString(decode.Recommendations, "detached TSSP query range matched 1 of 3 decoded row timestamp") {
		t.Fatalf("recommendations = %v, want detached query range row-count recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedAnyFieldFilterMatchesEitherPredicate(t *testing.T) {
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
		QueryAnyFields:   []FieldFilter{{Key: "value", Value: "0"}, {Key: "value", Value: "99"}, {Key: "value", Value: "x"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "1"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_evaluations"], "2"; got != want {
		t.Fatalf("data block probe filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_evaluations"], "0"; got != want {
		t.Fatalf("data block probe required filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_any_filter_evaluations"], "2"; got != want {
		t.Fatalf("data block probe any filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_none_filter_evaluations"], "0"; got != want {
		t.Fatalf("data block probe none filter evaluations = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_evaluation_matches"], "1"; got != want {
		t.Fatalf("data block probe filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_evaluation_misses"], "1"; got != want {
		t.Fatalf("data block probe filter evaluation misses = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_short_circuit_skips"], "1"; got != want {
		t.Fatalf("data block probe filter short-circuit skips = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_evaluation_matches"], "0"; got != want {
		t.Fatalf("data block probe required filter evaluation matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_evaluation_misses"], "0"; got != want {
		t.Fatalf("data block probe required filter evaluation misses = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_required_filter_short_circuit_skips"], "0"; got != want {
		t.Fatalf("data block probe required filter short-circuit skips = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_any_filter_evaluation_matches"], "1"; got != want {
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
	if got, want := file.Extra["data_block_probe_filter_operator_evaluations"], "=:2"; got != want {
		t.Fatalf("data block probe filter operator evaluations = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.DataBlockProbeFilterEvals, 2; got != want {
		t.Fatalf("decode filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeRequiredEvals, 0; got != want {
		t.Fatalf("decode required filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeAnyEvals, 2; got != want {
		t.Fatalf("decode any filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeNoneEvals, 0; got != want {
		t.Fatalf("decode none filter evaluations = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterEvalHits, 1; got != want {
		t.Fatalf("decode filter evaluation matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterEvalMiss, 1; got != want {
		t.Fatalf("decode filter evaluation misses = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterSkips, 1; got != want {
		t.Fatalf("decode filter short-circuit skips = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeAnyHits, 1; got != want {
		t.Fatalf("decode any filter evaluation matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeAnyMiss, 1; got != want {
		t.Fatalf("decode any filter evaluation misses = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeAnySkips, 1; got != want {
		t.Fatalf("decode any filter short-circuit skips = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterOps["="], 2; got != want {
		t.Fatalf("decode equality filter evaluations = %d, want %d", got, want)
	}
	if got, want := len(decode.FilterExecutionSamples), 1; got != want {
		t.Fatalf("filter execution samples = %d, want %d", got, want)
	}
	wantFilterStep := DecodePathCursorStep{
		Step:              1,
		Type:              "tssp-filter-row-step",
		Action:            "filter_row_match",
		Key:               "meta-index-id:42/row:0",
		CandidateValue:    "row=0 time=333 required=0/0 any=1/2 none=0/0 skips=0/1/0 result=match",
		CursorIndexBefore: 0,
		CursorIndexAfter:  1,
		CursorAdvanced:    true,
	}
	if got := decode.FilterExecutionSamples[0]; got != wantFilterStep {
		t.Fatalf("filter execution sample = %+v, want %+v", got, wantFilterStep)
	}
	wantAny := []FieldFilter{{Key: "value", Value: "0"}, {Key: "value", Value: "99"}, {Key: "value", Value: "x"}}
	if got := decode.QueryAnyFields; !equalFieldFilters(got, wantAny) {
		t.Fatalf("query any fields = %v, want %v", got, wantAny)
	}
	if got := decode.MatchedAnyFields; !equalFieldFilters(got, wantAny) {
		t.Fatalf("matched any fields = %v, want %v", got, wantAny)
	}
	if len(decode.MissingAnyFields) != 0 {
		t.Fatalf("missing any fields = %v, want none", decode.MissingAnyFields)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "applied 3 detached TSSP OR field filter") {
		t.Fatalf("recommendations = %v, want detached OR field filter recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "executed 2 detached TSSP decoded-row field predicate evaluation") {
		t.Fatalf("recommendations = %v, want detached predicate evaluation recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "required=0 required_matches=0 required_misses=0 any=2 any_matches=1 any_misses=1 none=0 none_matches=0 none_misses=0") {
		t.Fatalf("recommendations = %v, want detached predicate clause/result breakdown", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "matches=1 misses=1") {
		t.Fatalf("recommendations = %v, want detached predicate match/miss breakdown", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "short-circuited 1 detached TSSP decoded-row field predicate evaluation") {
		t.Fatalf("recommendations = %v, want detached predicate short-circuit recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "required_skips=0 any_skips=1 none_skips=0") {
		t.Fatalf("recommendations = %v, want detached predicate short-circuit breakdown", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "detached TSSP filter execution samples show local decoded-row predicate decisions") {
		t.Fatalf("recommendations = %v, want detached predicate execution sample recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedNoneFieldFilterRejectsMatchingRows(t *testing.T) {
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
		QueryNoneFields:  []FieldFilter{{Key: "value", Value: "99"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Extra["data_block_probe_filter_rows"], "1"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "0"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "1"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	wantNone := []FieldFilter{{Key: "value", Value: "99"}}
	if got := decode.QueryNoneFields; !equalFieldFilters(got, wantNone) {
		t.Fatalf("query none fields = %v, want %v", got, wantNone)
	}
	if got := decode.MatchedNoneFields; !equalFieldFilters(got, wantNone) {
		t.Fatalf("matched none fields = %v, want %v", got, wantNone)
	}
	if len(decode.MissingNoneFields) != 0 {
		t.Fatalf("missing none fields = %v, want none", decode.MissingNoneFields)
	}
	if got, want := decode.OptimizedValueOutputPoints, 0; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	wantFilterStep := DecodePathCursorStep{
		Step:              1,
		Type:              "tssp-filter-row-step",
		Action:            "filter_row_reject_none",
		Key:               "meta-index-id:42/row:0",
		CandidateValue:    "row=0 time=333 required=0/0 any=0/0 none=1/1 skips=0/0/0 result=reject_none",
		CursorIndexBefore: 0,
		CursorIndexAfter:  1,
		CursorAdvanced:    true,
	}
	if got, want := len(decode.FilterExecutionSamples), 1; got != want {
		t.Fatalf("filter execution samples = %d, want %d", got, want)
	}
	if got := decode.FilterExecutionSamples[0]; got != wantFilterStep {
		t.Fatalf("filter execution sample = %+v, want %+v", got, wantFilterStep)
	}
	if !containsString(decode.Recommendations, "applied 1 detached TSSP NOT field filter") {
		t.Fatalf("recommendations = %v, want detached NOT field filter recommendation", decode.Recommendations)
	}
}

func TestAnalyzeTSSPDetachedFieldFilterMatchesIntegerComparison(t *testing.T) {
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
		QueryFields:      []FieldFilter{{Key: "value", Op: ">=", Value: "99"}},
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
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: ">=", Value: "99"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].OptimizedValue, "99"; got != want {
		t.Fatalf("cursor output value = %q, want %q", got, want)
	}
}

func TestAnalyzeTSSPDetachedFieldFilterMatchesIntegerBetween(t *testing.T) {
	for _, tc := range []struct {
		name             string
		filter           FieldFilter
		wantOutputPoints string
		wantSamples      []DecodePathCursorOutput
	}{
		{
			name:             "between",
			filter:           FieldFilter{Key: "value", Op: "between", Value: "(90,99)"},
			wantOutputPoints: "1",
			wantSamples: []DecodePathCursorOutput{
				{Key: "meta-index-id:42/value", Time: 333, Type: "integer-one", OptimizedValue: "99", Matches: true},
			},
		},
		{
			name:             "not-between",
			filter:           FieldFilter{Key: "value", Op: "not-between", Value: "(100,110)"},
			wantOutputPoints: "1",
			wantSamples: []DecodePathCursorOutput{
				{Key: "meta-index-id:42/value", Time: 333, Type: "integer-one", OptimizedValue: "99", Matches: true},
			},
		},
		{
			name:             "inverted",
			filter:           FieldFilter{Key: "value", Op: "between", Value: "(100,99)"},
			wantOutputPoints: "0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				QueryFields:      []FieldFilter{tc.filter},
			})
			if err != nil {
				t.Fatal(err)
			}
			file := report.Files[0]
			if got := file.Extra["data_block_probe_output_points"]; got != tc.wantOutputPoints {
				t.Fatalf("data block probe output points = %q, want %q", got, tc.wantOutputPoints)
			}
			if got, want := file.Extra["data_block_probe_filter_rows"], "1"; got != want {
				t.Fatalf("data block probe filter rows = %q, want %q", got, want)
			}
			decode := file.DecodePath
			if decode == nil {
				t.Fatal("decode path is nil")
			}
			if got, want := decode.QueryFields, []FieldFilter{tc.filter}; !equalFieldFilters(got, want) {
				t.Fatalf("query fields = %v, want %v", got, want)
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

func TestAnalyzeTSSPDetachedFieldFilterMatchesFloatBetween(t *testing.T) {
	dir := t.TempDir()
	values := []float64{1.25, 2.5, 3.75}
	present := []bool{true, true, true}
	timestamps := []int64{333, 444, 555}
	valueSize, err := testTSSPDetachedNullableRegularFloatBlockSize(values, present)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []testTSSPChunkSpec{{
		sid:      42,
		minTime:  333,
		maxTime:  555,
		offset:   1200,
		size:     valueSize,
		timeSize: testTSSPDetachedIntegerFullBlockSize(timestamps),
	}}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedNullableRegularFloatData(filepath.Join(dir, tsspDetachedDataFileName), 1600, chunks[0], values, present, timestamps); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 555)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "between", Value: "(2.0,3.0)"}},
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
	if got, want := file.Extra["data_block_probe_filter_rows"], "3"; got != want {
		t.Fatalf("data block probe filter rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_matches"], "1"; got != want {
		t.Fatalf("data block probe filter matches = %q, want %q", got, want)
	}
	if got, want := file.Extra["data_block_probe_filter_rejects"], "2"; got != want {
		t.Fatalf("data block probe filter rejects = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: "between", Value: "(2.0,3.0)"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRows, 3; got != want {
		t.Fatalf("data block probe filter rows = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterMatches, 1; got != want {
		t.Fatalf("data block probe filter matches = %d, want %d", got, want)
	}
	if got, want := decode.DataBlockProbeFilterRejects, 2; got != want {
		t.Fatalf("data block probe filter rejects = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "meta-index-id:42/value",
		Time:           444,
		Type:           "float",
		OptimizedValue: "2.5",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPDetachedFieldFilterMatchesStringComparison(t *testing.T) {
	dir := t.TempDir()
	values := []string{"red", "blue"}
	timestamps := []int64{333, 444}
	valueSize, err := testTSSPDetachedStringFullBlockSize(values)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []testTSSPChunkSpec{{
		sid:      42,
		minTime:  333,
		maxTime:  444,
		offset:   1200,
		size:     valueSize,
		timeSize: testTSSPDetachedIntegerFullBlockSize(timestamps),
	}}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedStringFullData(filepath.Join(dir, tsspDetachedDataFileName), 1400, chunks[0], values, timestamps); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "<", Value: "red"}},
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
		Key:            "meta-index-id:42/value",
		Time:           444,
		Type:           "string-full",
		OptimizedValue: "blue",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPDetachedFieldFilterMatchesDecodedTime(t *testing.T) {
	dir := t.TempDir()
	values := []string{"red", "blue"}
	timestamps := []int64{333, 444}
	valueSize, err := testTSSPDetachedStringFullBlockSize(values)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []testTSSPChunkSpec{{
		sid:      42,
		minTime:  333,
		maxTime:  444,
		offset:   1200,
		size:     valueSize,
		timeSize: testTSSPDetachedIntegerFullBlockSize(timestamps),
	}}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedStringFullData(filepath.Join(dir, tsspDetachedDataFileName), 1400, chunks[0], values, timestamps); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
		QueryColumns:     []string{"value"},
		QueryFields:      []FieldFilter{{Key: "time", Value: fmt.Sprint(timestamps[1])}},
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
	if got, want := decode.QueryFields, []FieldFilter{{Key: "time", Value: fmt.Sprint(timestamps[1])}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.MatchedFields, []FieldFilter{{Key: "time", Value: fmt.Sprint(timestamps[1])}}; !equalFieldFilters(got, want) {
		t.Fatalf("matched fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "meta-index-id:42/value",
		Time:           444,
		Type:           "string-full",
		OptimizedValue: "blue",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPDetachedFieldFilterMatchesStringRegex(t *testing.T) {
	dir := t.TempDir()
	values := []string{"red", "blue"}
	timestamps := []int64{333, 444}
	valueSize, err := testTSSPDetachedStringFullBlockSize(values)
	if err != nil {
		t.Fatal(err)
	}
	chunks := []testTSSPChunkSpec{{
		sid:      42,
		minTime:  333,
		maxTime:  444,
		offset:   1200,
		size:     valueSize,
		timeSize: testTSSPDetachedIntegerFullBlockSize(timestamps),
	}}
	metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSPDetachedStringFullData(filepath.Join(dir, tsspDetachedDataFileName), 1400, chunks[0], values, timestamps); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(333, 444)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
		Format:           FormatTSSPDetachedIndex,
		BlockSampleLimit: 4,
		QueryRange:       queryRange,
		QueryFields:      []FieldFilter{{Key: "value", Op: "=~", Value: "^b"}},
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
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.QueryFields, []FieldFilter{{Key: "value", Op: "=~", Value: "^b"}}; !equalFieldFilters(got, want) {
		t.Fatalf("query fields = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	want := DecodePathCursorOutput{
		Key:            "meta-index-id:42/value",
		Time:           444,
		Type:           "string-full",
		OptimizedValue: "blue",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != want {
		t.Fatalf("first cursor output sample = %+v, want %+v", got, want)
	}
}

func TestAnalyzeTSSPDetachedFieldFilterMatchesNullPredicates(t *testing.T) {
	for _, tc := range []struct {
		name              string
		filter            FieldFilter
		wantFilter        FieldFilter
		wantOutputPoints  string
		wantMatches       string
		wantRejects       string
		wantSamples       []DecodePathCursorOutput
		wantValueOutCount int
	}{
		{
			name:              "equals-null",
			filter:            FieldFilter{Key: "value", Value: "null"},
			wantFilter:        FieldFilter{Key: "value", Value: "null"},
			wantOutputPoints:  "1",
			wantMatches:       "1",
			wantRejects:       "2",
			wantValueOutCount: 1,
		},
		{
			name:              "is-null",
			filter:            FieldFilter{Key: "value", Op: "is", Value: "null"},
			wantFilter:        FieldFilter{Key: "value", Value: "null"},
			wantOutputPoints:  "1",
			wantMatches:       "1",
			wantRejects:       "2",
			wantValueOutCount: 1,
		},
		{
			name:              "in-null",
			filter:            FieldFilter{Key: "value", Op: "in", Value: "(null)"},
			wantFilter:        FieldFilter{Key: "value", Op: "in", Value: "(null)"},
			wantOutputPoints:  "1",
			wantMatches:       "1",
			wantRejects:       "2",
			wantValueOutCount: 1,
		},
		{
			name:             "not-null",
			filter:           FieldFilter{Key: "value", Op: "!=", Value: "null"},
			wantFilter:       FieldFilter{Key: "value", Op: "!=", Value: "null"},
			wantOutputPoints: "2",
			wantMatches:      "2",
			wantRejects:      "1",
			wantSamples: []DecodePathCursorOutput{
				{Key: "meta-index-id:42/value", Time: 333, Type: "float", OptimizedValue: "1.25", Matches: true},
				{Key: "meta-index-id:42/value", Time: 555, Type: "float", OptimizedValue: "3.75", Matches: true},
			},
			wantValueOutCount: 2,
		},
		{
			name:             "is-not-null",
			filter:           FieldFilter{Key: "value", Op: "is-not", Value: "null"},
			wantFilter:       FieldFilter{Key: "value", Op: "!=", Value: "null"},
			wantOutputPoints: "2",
			wantMatches:      "2",
			wantRejects:      "1",
			wantSamples: []DecodePathCursorOutput{
				{Key: "meta-index-id:42/value", Time: 333, Type: "float", OptimizedValue: "1.25", Matches: true},
				{Key: "meta-index-id:42/value", Time: 555, Type: "float", OptimizedValue: "3.75", Matches: true},
			},
			wantValueOutCount: 2,
		},
		{
			name:             "not-in-null",
			filter:           FieldFilter{Key: "value", Op: "not-in", Value: "(null)"},
			wantFilter:       FieldFilter{Key: "value", Op: "not-in", Value: "(null)"},
			wantOutputPoints: "2",
			wantMatches:      "2",
			wantRejects:      "1",
			wantSamples: []DecodePathCursorOutput{
				{Key: "meta-index-id:42/value", Time: 333, Type: "float", OptimizedValue: "1.25", Matches: true},
				{Key: "meta-index-id:42/value", Time: 555, Type: "float", OptimizedValue: "3.75", Matches: true},
			},
			wantValueOutCount: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			values := []float64{1.25, 0, 3.75}
			present := []bool{true, false, true}
			timestamps := []int64{333, 444, 555}
			valueSize, err := testTSSPDetachedNullableRegularFloatBlockSize(values, present)
			if err != nil {
				t.Fatal(err)
			}
			chunks := []testTSSPChunkSpec{{
				sid:      42,
				minTime:  333,
				maxTime:  555,
				offset:   1200,
				size:     valueSize,
				timeSize: testTSSPDetachedIntegerFullBlockSize(timestamps),
			}}
			metaIndexes, err := writeTestTSSPDetachedChunkMeta(filepath.Join(dir, tsspDetachedChunkMetaFileName), chunks)
			if err != nil {
				t.Fatal(err)
			}
			if err := writeTestTSSPDetachedMetaIndex(filepath.Join(dir, tsspDetachedMetaIndexFileName), metaIndexes); err != nil {
				t.Fatal(err)
			}
			if err := writeTestTSSPDetachedNullableRegularFloatData(filepath.Join(dir, tsspDetachedDataFileName), 1600, chunks[0], values, present, timestamps); err != nil {
				t.Fatal(err)
			}
			queryRange, err := NewTimeRange(333, 555)
			if err != nil {
				t.Fatal(err)
			}

			report, err := Analyze(context.Background(), []string{filepath.Join(dir, tsspDetachedMetaIndexFileName)}, Options{
				Format:           FormatTSSPDetachedIndex,
				QueryRange:       queryRange,
				QueryFields:      []FieldFilter{tc.filter},
				KeySampleLimit:   3,
				BlockSampleLimit: 8,
			})
			if err != nil {
				t.Fatal(err)
			}
			file := report.Files[0]
			if got, want := file.Extra["data_block_probe_null_values"], "1"; got != want {
				t.Fatalf("data block probe null values = %q, want %q", got, want)
			}
			if got := file.Extra["data_block_probe_output_points"]; got != tc.wantOutputPoints {
				t.Fatalf("data block probe output points = %q, want %q", got, tc.wantOutputPoints)
			}
			if got, want := file.Extra["data_block_probe_filter_rows"], "3"; got != want {
				t.Fatalf("data block probe filter rows = %q, want %q", got, want)
			}
			if got := file.Extra["data_block_probe_filter_matches"]; got != tc.wantMatches {
				t.Fatalf("data block probe filter matches = %q, want %q", got, tc.wantMatches)
			}
			if got := file.Extra["data_block_probe_filter_rejects"]; got != tc.wantRejects {
				t.Fatalf("data block probe filter rejects = %q, want %q", got, tc.wantRejects)
			}
			decode := file.DecodePath
			if decode == nil {
				t.Fatal("decode path is nil")
			}
			if got, want := decode.QueryFields, []FieldFilter{tc.wantFilter}; !equalFieldFilters(got, want) {
				t.Fatalf("query fields = %v, want %v", got, want)
			}
			if got, want := decode.MatchedFields, []FieldFilter{tc.wantFilter}; !equalFieldFilters(got, want) {
				t.Fatalf("matched fields = %v, want %v", got, want)
			}
			if got := decode.OptimizedValueOutputPoints; got != tc.wantValueOutCount {
				t.Fatalf("optimized value output points = %d, want %d", got, tc.wantValueOutCount)
			}
			if got, want := decode.DataBlockProbeFilterRows, 3; got != want {
				t.Fatalf("data block probe filter rows = %d, want %d", got, want)
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
		{name: "mlf", codec: 6, values: []float64{1.11, 0, -2.22, 3.33, 4.44, 5.55, 6.66, 7.77, 8.88, 9.99, 10.01}, want: []string{"1.11", "0", "-2.22", "3.33", "4.44", "5.55", "6.66", "7.77", "8.88", "9.99", "10.01"}},
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

func writeTestTSSPDetachedStringFullData(path string, size int, chunk testTSSPChunkSpec, values []string, timestamps []int64) error {
	valuePayload, err := testTSSPStringFullPayload(values, 0)
	if err != nil {
		return err
	}
	timePayload := testTSSPIntegerFullPayload(timestamps)
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	for buf.Len() < size {
		buf.WriteByte(0)
	}
	if err := writeTestTSSPDetachedPayloadBlock(buf.Bytes(), chunk.offset, chunk.size, valuePayload); err != nil {
		return err
	}
	if err := writeTestTSSPDetachedPayloadBlock(buf.Bytes(), chunk.offset+int64(chunk.size), chunk.testTimeSize(), timePayload); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPDetachedFloatFullData(path string, size int, chunk testTSSPChunkSpec, values []float64, codec byte, timestamps []int64) error {
	valuePayload, err := testTSSPFloatFullPayload(values, codec)
	if err != nil {
		return err
	}
	timePayload := testTSSPIntegerFullPayload(timestamps)
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	for buf.Len() < size {
		buf.WriteByte(0)
	}
	if err := writeTestTSSPDetachedPayloadBlock(buf.Bytes(), chunk.offset, chunk.size, valuePayload); err != nil {
		return err
	}
	if err := writeTestTSSPDetachedPayloadBlock(buf.Bytes(), chunk.offset+int64(chunk.size), chunk.testTimeSize(), timePayload); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPDetachedIntegerZSTDData(path string, size int, chunk testTSSPChunkSpec, values []int64, timestamps []int64) error {
	valuePayload, err := testTSSPIntegerZSTDFullPayload(values)
	if err != nil {
		return err
	}
	timePayload := testTSSPIntegerFullPayload(timestamps)
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	for buf.Len() < size {
		buf.WriteByte(0)
	}
	if err := writeTestTSSPDetachedPayloadBlock(buf.Bytes(), chunk.offset, chunk.size, valuePayload); err != nil {
		return err
	}
	if err := writeTestTSSPDetachedPayloadBlock(buf.Bytes(), chunk.offset+int64(chunk.size), chunk.testTimeSize(), timePayload); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSSPDetachedNullableRegularFloatData(path string, size int, chunk testTSSPChunkSpec, values []float64, present []bool, timestamps []int64) error {
	var valuePayload bytes.Buffer
	if _, err := writeTestTSSPAttachedNullableRegularFloatBlock(&valuePayload, values, present, 0); err != nil {
		return err
	}
	timePayload := testTSSPIntegerFullPayload(timestamps)
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)
	for buf.Len() < size {
		buf.WriteByte(0)
	}
	if err := writeTestTSSPDetachedPayloadBlock(buf.Bytes(), chunk.offset, chunk.size, valuePayload.Bytes()); err != nil {
		return err
	}
	if err := writeTestTSSPDetachedPayloadBlock(buf.Bytes(), chunk.offset+int64(chunk.size), chunk.testTimeSize(), timePayload); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func testTSSPDetachedStringFullBlockSize(values []string) (uint32, error) {
	payload, err := testTSSPStringFullPayload(values, 0)
	if err != nil {
		return 0, err
	}
	return uint32(crc32.Size + len(payload)), nil
}

func testTSSPDetachedFloatFullBlockSize(values []float64, codec byte) (uint32, error) {
	payload, err := testTSSPFloatFullPayload(values, codec)
	if err != nil {
		return 0, err
	}
	return uint32(crc32.Size + len(payload)), nil
}

func testTSSPDetachedIntegerZSTDBlockSize(values []int64) (uint32, error) {
	payload, err := testTSSPIntegerZSTDFullPayload(values)
	if err != nil {
		return 0, err
	}
	return uint32(crc32.Size + len(payload)), nil
}

func testTSSPDetachedNullableRegularFloatBlockSize(values []float64, present []bool) (uint32, error) {
	var payload bytes.Buffer
	if _, err := writeTestTSSPAttachedNullableRegularFloatBlock(&payload, values, present, 0); err != nil {
		return 0, err
	}
	return uint32(crc32.Size + payload.Len()), nil
}

func testTSSPDetachedIntegerFullBlockSize(values []int64) uint32 {
	return uint32(crc32.Size + len(testTSSPIntegerFullPayload(values)))
}

func testTSSPIntegerFullPayload(values []int64) []byte {
	var payload bytes.Buffer
	writeTestTSSPAttachedIntegerFullBlock(&payload, values)
	return payload.Bytes()
}

func testTSSPIntegerZSTDFullPayload(values []int64) ([]byte, error) {
	var payload bytes.Buffer
	if _, err := writeTestTSSPAttachedIntegerZSTDBlock(&payload, values); err != nil {
		return nil, err
	}
	return payload.Bytes(), nil
}

func writeTestTSSPDetachedPayloadBlock(data []byte, offset int64, size uint32, payload []byte) error {
	if offset < 0 || int64(len(data)) < offset+int64(size) {
		return fmt.Errorf("detached block range offset=%d size=%d exceeds %d bytes", offset, size, len(data))
	}
	if int(size) != crc32.Size+len(payload) {
		return fmt.Errorf("detached block size = %d, want %d", size, crc32.Size+len(payload))
	}
	binary.BigEndian.PutUint32(data[offset:], crc32.ChecksumIEEE(payload))
	copy(data[offset+crc32.Size:], payload)
	return nil
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
