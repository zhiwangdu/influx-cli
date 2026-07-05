package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestAnalyzeTSMMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000000001-000000001.tsm")
	if err := writeTestTSM(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000000001-000000001.tombstone"), []byte("delete"), 0o600); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(15, 15)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSM,
		QueryRange:       queryRange,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.KeyCount, 2; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.BlockCount, 2; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["float"], 1; got != want {
		t.Fatalf("float block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["integer"], 1; got != want {
		t.Fatalf("integer block count = %d, want %d", got, want)
	}
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if !file.Tombstones.Exists {
		t.Fatalf("expected tombstone summary")
	}
	if got, want := file.Tombstones.Version, "v1"; got != want {
		t.Fatalf("tombstone version = %q, want %q", got, want)
	}
	if got, want := file.Tombstones.RangeCount, 1; got != want {
		t.Fatalf("tombstone range count = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].ValueCount, 3; got != want {
		t.Fatalf("value count = %d, want %d", got, want)
	}
	if file.DecodePath == nil {
		t.Fatal("expected decode path summary")
	}
	if got, want := file.DecodePath.LocationBlocks, 2; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.BaselineDecodeBlocks, 2; got != want {
		t.Fatalf("baseline decode blocks = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized decode blocks = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.FilteredDecodeBlocks, 1; got != want {
		t.Fatalf("filtered decode blocks = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.SavedDecodeBlocks, 1; got != want {
		t.Fatalf("saved decode blocks = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.BaselineDecodeBytes, int64(56); got != want {
		t.Fatalf("baseline decode bytes = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.OptimizedDecodeBytes, int64(32); got != want {
		t.Fatalf("optimized decode bytes = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.SavedDecodeBytes, int64(24); got != want {
		t.Fatalf("saved decode bytes = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.BaselineDecodeValues, 5; got != want {
		t.Fatalf("baseline decode values = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.OptimizedDecodeValues, 3; got != want {
		t.Fatalf("optimized decode values = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.SavedDecodeValues, 2; got != want {
		t.Fatalf("saved decode values = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.CursorWindowCount, 2; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(file.DecodePath.CursorWindows), 2; got != want {
		t.Fatalf("cursor window samples = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.CursorWindows[0].DecodedBlocks, 1; got != want {
		t.Fatalf("first cursor window decoded blocks = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.CursorWindows[1].Reason, "outside_query_range"; got != want {
		t.Fatalf("second cursor window reason = %q, want %q", got, want)
	}
	if got, want := file.DecodePath.SkippedAfterRangeBlocks, 1; got != want {
		t.Fatalf("skipped after range blocks = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.Samples[1].Reason, "outside_query_range"; got != want {
		t.Fatalf("second decode sample reason = %q, want %q", got, want)
	}
	if got, want := file.DecodePath.Samples[0].SizeBytes, uint32(32); got != want {
		t.Fatalf("first decode sample size = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.Samples[0].ValueCount, 3; got != want {
		t.Fatalf("first decode sample values = %d, want %d", got, want)
	}
}

func TestAnalyzeTSMTombstoneRanges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000000001-000000001.tsm")
	if err := writeTestTSM(path); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSMTombstoneV3(tsmTombstonePath(path),
		tsmTombstoneEntry{Key: "cpu,host=a value", Min: 20, Max: 25},
		tsmTombstoneEntry{Key: "mem,host=a value", Min: 110, Max: 115},
		tsmTombstoneEntry{Key: "disk,host=a value", Min: 200, Max: 300},
	); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(22, 22)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSM,
		QueryRange:       queryRange,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.Tombstones.Version, "v3"; got != want {
		t.Fatalf("tombstone version = %q, want %q", got, want)
	}
	if got, want := file.Tombstones.RangeCount, 3; got != want {
		t.Fatalf("tombstone range count = %d, want %d", got, want)
	}
	if got, want := file.Tombstones.KeyCount, 3; got != want {
		t.Fatalf("tombstone key count = %d, want %d", got, want)
	}
	if got, want := file.Tombstones.QueryOverlapRanges, 1; got != want {
		t.Fatalf("tombstone query overlap ranges = %d, want %d", got, want)
	}
	if got, want := file.Tombstones.AffectedBlocks, 2; got != want {
		t.Fatalf("tombstone affected blocks = %d, want %d", got, want)
	}
	if got, want := len(file.Tombstones.RangeSamples), 2; got != want {
		t.Fatalf("tombstone range samples = %d, want %d", got, want)
	}
	if got, want := file.Tombstones.RangeSamples[0].AffectedBlocks, 1; got != want {
		t.Fatalf("first tombstone sample affected blocks = %d, want %d", got, want)
	}
	if got, want := file.Tombstones.RangeSamples[0].QueryOverlaps, true; got != want {
		t.Fatalf("first tombstone sample query overlaps = %t, want %t", got, want)
	}
	if got, want := file.Tombstones.KeySamples[0], "cpu,host=a value"; got != want {
		t.Fatalf("first tombstone key sample = %q, want %q", got, want)
	}
}

func TestAnalyzeTSMDecodePathMergeAndTombstone(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000000001-000000001.tsm")
	if err := writeTestTSMWithBlocks(path, []testTSMBlockSpec{
		{key: "cpu,host=a value", typ: tsmBlockFloat, minTime: 10, maxTime: 30, timestamps: []int64{10, 20, 30}},
		{key: "cpu,host=a value", typ: tsmBlockFloat, minTime: 20, maxTime: 40, timestamps: []int64{20, 30, 40}},
		{key: "mem,host=a value", typ: tsmBlockInteger, minTime: 50, maxTime: 60, timestamps: []int64{50, 60}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSMTombstoneV3(tsmTombstonePath(path),
		tsmTombstoneEntry{Key: "mem,host=a value", Min: 50, Max: 60},
	); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(25, 25)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSM,
		QueryRange:       queryRange,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("expected decode path summary")
	}
	if got, want := decode.LocationBlocks, 2; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.FilteredDecodeBlocks, 2; got != want {
		t.Fatalf("filtered decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.FullyTombstonedBlocks, 1; got != want {
		t.Fatalf("fully tombstoned blocks = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowKeys, 1; got != want {
		t.Fatalf("merge window keys = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowCount, 1; got != want {
		t.Fatalf("merge window count = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowBlocks, 2; got != want {
		t.Fatalf("merge window blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "merge_overlap"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[0].DecodedBlocks, 2; got != want {
		t.Fatalf("cursor window decoded blocks = %d, want %d", got, want)
	}
	if got, want := decode.DecodeBlocksByType["float"], 2; got != want {
		t.Fatalf("float decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.Samples[2].Reason, "fully_tombstoned"; got != want {
		t.Fatalf("third decode sample reason = %q, want %q", got, want)
	}
}

func TestAnalyzeTSMDecodePathQueryKeyFilter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000000001-000000001.tsm")
	if err := writeTestTSMWithBlocks(path, []testTSMBlockSpec{
		{key: "cpu,host=a value", typ: tsmBlockFloat, minTime: 10, maxTime: 30, timestamps: []int64{10, 20, 30}},
		{key: "cpu,host=a value", typ: tsmBlockFloat, minTime: 20, maxTime: 40, timestamps: []int64{20, 30, 40}},
		{key: "mem,host=a value", typ: tsmBlockInteger, minTime: 20, maxTime: 40, timestamps: []int64{20, 30, 40}},
	}); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(25, 25)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSM,
		QueryRange:       queryRange,
		QueryKeys:        []string{"missing value", "cpu,host=a value", "cpu,host=a value", " "},
		KeySampleLimit:   2,
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
		t.Fatal("expected file to overlap selected query key")
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("expected decode path summary")
	}
	if !decode.KeyFilterApplied {
		t.Fatal("expected key filter to be applied")
	}
	if got, want := decode.QueryKeys, []string{"cpu,host=a value", "missing value"}; !equalStrings(got, want) {
		t.Fatalf("query keys = %v, want %v", got, want)
	}
	if got, want := decode.MatchedKeys, []string{"cpu,host=a value"}; !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got, want := decode.MissingKeys, []string{"missing value"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
	}
	if got, want := decode.LocationBlocks, 2; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 2; got != want {
		t.Fatalf("baseline decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("optimized decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].RequiresMerge, true; got != want {
		t.Fatalf("cursor window requires merge = %t, want %t", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 1; got != want {
		t.Fatalf("skipped by key blocks = %d, want %d", got, want)
	}
	if got, want := decode.Samples[2].Reason, "key_not_selected"; got != want {
		t.Fatalf("third decode sample reason = %q, want %q", got, want)
	}
}

func TestAnalyzeTSMDecodePathMissingQueryKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000000001-000000001.tsm")
	if err := writeTestTSM(path); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(15, 15)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSM,
		QueryRange:       queryRange,
		QueryKeys:        []string{"disk value"},
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if file.QueryOverlapsFile {
		t.Fatal("expected file not to overlap missing query key")
	}
	if got, want := file.QueryOverlapBlocks, 0; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	decode := file.DecodePath
	if got, want := decode.LocationBlocks, 0; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 2; got != want {
		t.Fatalf("skipped by key blocks = %d, want %d", got, want)
	}
	if got, want := decode.MissingKeys, []string{"disk value"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
	}
}

func TestAnalyzeTSMDecodePathDoesNotMergeNonOverlappingBlocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000000001-000000001.tsm")
	if err := writeTestTSMWithBlocks(path, []testTSMBlockSpec{
		{key: "cpu,host=a value", typ: tsmBlockFloat, minTime: 10, maxTime: 20, timestamps: []int64{10, 20}},
		{key: "cpu,host=a value", typ: tsmBlockFloat, minTime: 30, maxTime: 40, timestamps: []int64{30, 40}},
	}); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(10, 40)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSM,
		QueryRange:       queryRange,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if got, want := decode.FilteredDecodeBlocks, 2; got != want {
		t.Fatalf("filtered decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 2; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowCount, 0; got != want {
		t.Fatalf("merge window count = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowKeys, 0; got != want {
		t.Fatalf("merge window keys = %d, want %d", got, want)
	}
	if decode.CursorWindows[0].RequiresMerge || decode.CursorWindows[1].RequiresMerge {
		t.Fatalf("cursor windows = %+v, want no merge", decode.CursorWindows)
	}
}

func TestAnalyzeTSMDecodePathDeduplicatesOutputTimestamps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000000001-000000001.tsm")
	if err := writeTestTSMWithBlocks(path, []testTSMBlockSpec{
		{key: "cpu,host=a value", typ: tsmBlockFloat, minTime: 10, maxTime: 30, timestamps: []int64{10, 10, 10}},
		{key: "cpu,host=a value", typ: tsmBlockFloat, minTime: 20, maxTime: 40, timestamps: []int64{20, 10, 10}},
	}); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(20, 30)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSM,
		QueryRange:       queryRange,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if got, want := decode.BaselineOutputValues, 4; got != want {
		t.Fatalf("baseline output values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedOutputValues, 4; got != want {
		t.Fatalf("optimized output values = %d, want %d", got, want)
	}
	if got, want := decode.DeduplicatedOutputValues, 2; got != want {
		t.Fatalf("deduplicated output values = %d, want %d", got, want)
	}
	if got, want := decode.DuplicateOutputValues, 2; got != want {
		t.Fatalf("duplicate output values = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].OutputValues, 2; got != want {
		t.Fatalf("first sample output values = %d, want %d", got, want)
	}
}

func TestAnalyzeTSMDecodePathComparesIntegerValueOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000000001-000000001.tsm")
	if err := writeTestTSMWithBlocks(path, []testTSMBlockSpec{
		{key: "cpu,host=a value", typ: tsmBlockInteger, minTime: 10, maxTime: 30, timestamps: []int64{10, 10, 10}, values: []int64{1, 2, 3}},
		{key: "cpu,host=a value", typ: tsmBlockInteger, minTime: 20, maxTime: 40, timestamps: []int64{20, 10, 10}, values: []int64{20, 30, 40}},
	}); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(20, 30)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSM,
		QueryRange:       queryRange,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if got, want := decode.BaselineValueOutputPoints, 4; got != want {
		t.Fatalf("baseline value output points = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 4; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.ComparedValueOutputPoints, 2; got != want {
		t.Fatalf("compared value output points = %d, want %d", got, want)
	}
	if got, want := decode.ValueOutputMismatches, 0; got != want {
		t.Fatalf("value output mismatches = %d, want %d", got, want)
	}
	if got, want := decode.ValueOutputUnavailableBlocks, 0; got != want {
		t.Fatalf("value output unavailable blocks = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 2; got != want {
		t.Fatalf("first sample value output points = %d, want %d", got, want)
	}
	if !decode.Samples[0].ValueOutputAvailable {
		t.Fatal("expected first sample value output to be available")
	}

	data, err := readTSMFileStoreData(path)
	if err != nil {
		t.Fatal(err)
	}
	baseline := map[tsmOutputPointKey]tsmPoint{}
	for _, entry := range data.entries {
		points, ok := tsmOutputPoints(entry, nil, queryRange)
		if !ok {
			t.Fatalf("points unavailable for entry %+v", entry)
		}
		addTSMOutputPoints(baseline, entry.Key, points)
	}
	for timestamp, want := range map[int64]string{20: "20", 30: "30"} {
		point, ok := baseline[tsmOutputPointKey{key: "cpu,host=a value", timestamp: timestamp, typ: tsmBlockInteger}]
		if !ok {
			t.Fatalf("missing merged point at %d", timestamp)
		}
		if point.Value != want {
			t.Fatalf("merged point at %d = %q, want %q", timestamp, point.Value, want)
		}
	}
}

func TestAnalyzeTSMFileStoreDecodePathAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "000000001-000000001.tsm")
	if err := writeTestTSMWithBlocks(path1, []testTSMBlockSpec{
		{key: "cpu,host=a value", typ: tsmBlockInteger, minTime: 10, maxTime: 30, timestamps: []int64{10, 10, 10}, values: []int64{1, 2, 3}},
		{key: "cpu,host=a value", typ: tsmBlockInteger, minTime: 100, maxTime: 120, timestamps: []int64{100, 10, 10}, values: []int64{100, 110, 120}},
		{key: "mem,host=a value", typ: tsmBlockInteger, minTime: 20, maxTime: 40, timestamps: []int64{20, 10, 10}},
	}); err != nil {
		t.Fatal(err)
	}
	path2 := filepath.Join(dir, "000000002-000000001.tsm")
	if err := writeTestTSMWithBlocks(path2, []testTSMBlockSpec{
		{key: "cpu,host=a value", typ: tsmBlockInteger, minTime: 20, maxTime: 40, timestamps: []int64{20, 10, 10}, values: []int64{20, 30, 40}},
	}); err != nil {
		t.Fatal(err)
	}

	queryRange, err := NewTimeRange(20, 30)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatTSM,
		QueryRange:       queryRange,
		QueryKeys:        []string{"cpu,host=a value"},
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected report-level decode path summary")
	}
	if got, want := decode.Mode, "tsm-filestore-key-cursor-ascending"; got != want {
		t.Fatalf("mode = %q, want %q", got, want)
	}
	if got, want := decode.LocationBlocks, 3; got != want {
		t.Fatalf("location blocks = %d, want %d", got, want)
	}
	if got, want := decode.FilteredDecodeBlocks, 2; got != want {
		t.Fatalf("filtered decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBlocks, 1; got != want {
		t.Fatalf("saved decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeBytes, int64(168); got != want {
		t.Fatalf("baseline decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBytes, int64(112); got != want {
		t.Fatalf("optimized decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBytes, int64(56); got != want {
		t.Fatalf("saved decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeValues, 9; got != want {
		t.Fatalf("baseline decode values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeValues, 6; got != want {
		t.Fatalf("optimized decode values = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeValues, 3; got != want {
		t.Fatalf("saved decode values = %d, want %d", got, want)
	}
	if got, want := decode.BaselineOutputValues, 4; got != want {
		t.Fatalf("baseline output values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedOutputValues, 4; got != want {
		t.Fatalf("optimized output values = %d, want %d", got, want)
	}
	if got, want := decode.DeduplicatedOutputValues, 2; got != want {
		t.Fatalf("deduplicated output values = %d, want %d", got, want)
	}
	if got, want := decode.DuplicateOutputValues, 2; got != want {
		t.Fatalf("duplicate output values = %d, want %d", got, want)
	}
	if got, want := decode.BaselineValueOutputPoints, 4; got != want {
		t.Fatalf("baseline value output points = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 4; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := decode.ComparedValueOutputPoints, 2; got != want {
		t.Fatalf("compared value output points = %d, want %d", got, want)
	}
	if got, want := decode.ValueOutputMismatches, 0; got != want {
		t.Fatalf("value output mismatches = %d, want %d", got, want)
	}
	if got, want := decode.ValueOutputUnavailableBlocks, 0; got != want {
		t.Fatalf("value output unavailable blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 1; got != want {
		t.Fatalf("skipped by key blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 2; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowCount, 1; got != want {
		t.Fatalf("merge window count = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowBlocks, 2; got != want {
		t.Fatalf("merge window blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "merge_overlap"; got != want {
		t.Fatalf("first cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[0].Files, []string{path1, path2}; !equalStrings(got, want) {
		t.Fatalf("first cursor window files = %v, want %v", got, want)
	}
	if got, want := decode.CursorWindows[1].Reason, "outside_query_range"; got != want {
		t.Fatalf("second cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.Samples[0].Path, path1; got != want {
		t.Fatalf("first sample path = %q, want %q", got, want)
	}
	if got, want := decode.Samples[0].SizeBytes, uint32(56); got != want {
		t.Fatalf("first sample size = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].ValueCount, 3; got != want {
		t.Fatalf("first sample values = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].OutputValues, 2; got != want {
		t.Fatalf("first sample output values = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 2; got != want {
		t.Fatalf("first sample value output points = %d, want %d", got, want)
	}
	if !decode.Samples[0].ValueOutputAvailable {
		t.Fatal("expected first sample value output to be available")
	}
}

func TestDecodeTSMSimple8bSelectorRunsAreOnes(t *testing.T) {
	values, err := decodeTSMSimple8bValues(make([]byte, 8))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(values), 240; got != want {
		t.Fatalf("values = %d, want %d", got, want)
	}
	for i, value := range values {
		if value != 1 {
			t.Fatalf("value %d = %d, want 1", i, value)
		}
	}
}

func TestDecodeTSMUnsignedValuesUsesIntegerEncoding(t *testing.T) {
	encoded := testTSMUnsignedValueBlock([]uint64{1, 3, 6})
	values, err := decodeTSMUnsignedValues(encoded)
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{1, 3, 6}
	if len(values) != len(want) {
		t.Fatalf("values = %v, want %v", values, want)
	}
	for i := range want {
		if values[i] != want[i] {
			t.Fatalf("values = %v, want %v", values, want)
		}
	}
}

func TestReadTSMTombstoneV4MultiStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000000001-000000001.tombstone")
	if err := writeTestTSMTombstoneV4(path,
		[]tsmTombstoneEntry{{Key: "cpu value", Min: 1, Max: 2}},
		[]tsmTombstoneEntry{{Key: "mem value", Min: 3, Max: 4}},
	); err != nil {
		t.Fatal(err)
	}

	tombstones, version, err := readTSMTombstones(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := version, "v4"; got != want {
		t.Fatalf("version = %q, want %q", got, want)
	}
	if got, want := len(tombstones), 2; got != want {
		t.Fatalf("tombstones = %d, want %d", got, want)
	}
	if got, want := tombstones[1].Key, "mem value"; got != want {
		t.Fatalf("second key = %q, want %q", got, want)
	}
}

func TestAnalyzeTSMWithZeroBlockSampleLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000000001-000000001.tsm")
	if err := writeTestTSM(path); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSM,
		KeySampleLimit:   1,
		BlockSampleLimit: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.BlockCount, 2; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got := len(file.Blocks); got != 0 {
		t.Fatalf("sampled blocks = %d, want 0", got)
	}
}

func TestAnalyzeQueryKeysRequireRange(t *testing.T) {
	_, err := Analyze(context.Background(), []string{"unused.tsm"}, Options{
		Format:    FormatTSM,
		QueryKeys: []string{"cpu value"},
	})
	if err == nil || !strings.Contains(err.Error(), "query key filter requires query range") {
		t.Fatalf("error = %v, want query-key range requirement", err)
	}
}

func TestAnalyzeAutoDetectsStorageFormats(t *testing.T) {
	dir := t.TempDir()
	tsmPath := filepath.Join(dir, "000000001-000000001.tsm")
	if err := writeTestTSM(tsmPath); err != nil {
		t.Fatal(err)
	}
	tsspPath := filepath.Join(dir, "00000001-0001-00000000.tssp")
	if err := writeTestTSSP(tsspPath); err != nil {
		t.Fatal(err)
	}
	tsiPath := filepath.Join(dir, "L0-00000001.tsi")
	if err := writeTestTSI(tsiPath); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{tsmPath, tsspPath, tsiPath}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 3; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	formats := map[Format]bool{}
	for _, file := range report.Files {
		formats[file.Format] = true
	}
	if !formats[FormatTSM] || !formats[FormatTSSP] || !formats[FormatTSI] {
		t.Fatalf("formats = %v, want %s, %s, and %s", formats, FormatTSM, FormatTSSP, FormatTSI)
	}
}

func TestAnalyzeDirectoryExpansion(t *testing.T) {
	dir := t.TempDir()
	tsmPath := filepath.Join(dir, "000000001-000000001.tsm")
	if err := writeTestTSM(tsmPath); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(dir, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeTestTSSP(filepath.Join(nested, "00000001-0001-00000000.tssp")); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("non-recursive file count = %d, want %d", got, want)
	}

	report, err = Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatAuto,
		Recursive:        true,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 2; got != want {
		t.Fatalf("recursive file count = %d, want %d", got, want)
	}
}

func writeTestTSM(path string) error {
	return writeTestTSMWithBlocks(path, []testTSMBlockSpec{
		{key: "cpu,host=a value", typ: tsmBlockFloat, minTime: 10, maxTime: 30, timestamps: []int64{10, 10, 10}},
		{key: "mem,host=a value", typ: tsmBlockInteger, minTime: 100, maxTime: 120, timestamps: []int64{100, 20}},
	})
}

func equalStrings(a, b []string) bool {
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

type testTSMBlockSpec struct {
	key        string
	typ        byte
	minTime    int64
	maxTime    int64
	timestamps []int64
	values     []int64
}

func writeTestTSMWithBlocks(path string, specs []testTSMBlockSpec) error {
	var buf bytes.Buffer
	var header [5]byte
	binary.BigEndian.PutUint32(header[:4], tsmMagicNumber)
	header[4] = tsmVersion
	buf.Write(header[:])

	type indexGroup struct {
		key     string
		typ     byte
		entries []tsmIndexEntry
	}
	groupsByKey := map[string]*indexGroup{}
	var groupKeys []string
	for _, spec := range specs {
		block := testTSMBlock(spec.typ, spec.timestamps, spec.values)
		offset := int64(buf.Len())
		buf.Write(testTSMBlockWithCRC(block))
		groupKey := string([]byte{spec.typ}) + spec.key
		group := groupsByKey[groupKey]
		if group == nil {
			group = &indexGroup{key: spec.key, typ: spec.typ}
			groupsByKey[groupKey] = group
			groupKeys = append(groupKeys, groupKey)
		}
		group.entries = append(group.entries, tsmIndexEntry{
			MinTime: spec.minTime,
			MaxTime: spec.maxTime,
			Offset:  offset,
			Size:    uint32(len(block) + tsmBlockCRCSize),
		})
	}
	sort.Strings(groupKeys)

	indexOffset := int64(buf.Len())
	for _, groupKey := range groupKeys {
		group := groupsByKey[groupKey]
		writeTestTSMIndexKey(&buf, group.key, group.typ, group.entries)
	}

	var footer [8]byte
	binary.BigEndian.PutUint64(footer[:], uint64(indexOffset))
	buf.Write(footer[:])
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSMTombstoneV3(path string, entries ...tsmTombstoneEntry) error {
	var buf bytes.Buffer
	writeUint32(&buf, tsmTombstoneV3Header)
	if err := writeTestTSMTombstoneGzipMember(&buf, entries); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSMTombstoneV4(path string, entryGroups ...[]tsmTombstoneEntry) error {
	var buf bytes.Buffer
	writeUint32(&buf, tsmTombstoneV4Header)
	for _, entries := range entryGroups {
		if err := writeTestTSMTombstoneGzipMember(&buf, entries); err != nil {
			return err
		}
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSMTombstoneGzipMember(buf *bytes.Buffer, entries []tsmTombstoneEntry) error {
	gz := gzip.NewWriter(buf)
	for _, entry := range entries {
		if err := writeTestTSMTombstoneEntry(gz, entry); err != nil {
			_ = gz.Close()
			return err
		}
	}
	return gz.Close()
}

func writeTestTSMTombstoneEntry(buf *gzip.Writer, entry tsmTombstoneEntry) error {
	var keyLen [4]byte
	binary.BigEndian.PutUint32(keyLen[:], uint32(len(entry.Key)))
	if _, err := buf.Write(keyLen[:]); err != nil {
		return err
	}
	if _, err := buf.Write([]byte(entry.Key)); err != nil {
		return err
	}
	var times [16]byte
	binary.BigEndian.PutUint64(times[:8], uint64(entry.Min))
	binary.BigEndian.PutUint64(times[8:], uint64(entry.Max))
	_, err := buf.Write(times[:])
	return err
}

func testTSMBlock(blockType byte, timestamps, values []int64) []byte {
	var ts bytes.Buffer
	ts.WriteByte(0)
	for _, timestamp := range timestamps {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(timestamp))
		ts.Write(b[:])
	}
	var block bytes.Buffer
	block.WriteByte(blockType)
	block.Write(binary.AppendUvarint(nil, uint64(ts.Len())))
	block.Write(ts.Bytes())
	if blockType == tsmBlockInteger && len(values) > 0 {
		block.Write(testTSMIntegerValueBlock(values))
	} else {
		block.WriteByte(0)
	}
	return block.Bytes()
}

func testTSMIntegerValueBlock(values []int64) []byte {
	var block bytes.Buffer
	block.WriteByte(0)
	var previous int64
	for _, value := range values {
		delta := value - previous
		previous = value
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], tsmZigZagEncode(delta))
		block.Write(b[:])
	}
	return block.Bytes()
}

func testTSMUnsignedValueBlock(values []uint64) []byte {
	var block bytes.Buffer
	block.WriteByte(0)
	var previous uint64
	for _, value := range values {
		delta := value - previous
		previous = value
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], tsmZigZagEncode(int64(delta)))
		block.Write(b[:])
	}
	return block.Bytes()
}

func testTSMBlockWithCRC(block []byte) []byte {
	var out bytes.Buffer
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(block))
	out.Write(crc[:])
	out.Write(block)
	return out.Bytes()
}

func writeTestTSMIndexKey(buf *bytes.Buffer, key string, blockType byte, entries []tsmIndexEntry) {
	var keyLen [2]byte
	binary.BigEndian.PutUint16(keyLen[:], uint16(len(key)))
	buf.Write(keyLen[:])
	buf.WriteString(key)
	buf.WriteByte(blockType)
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(entries)))
	buf.Write(count[:])
	for _, entry := range entries {
		var b [tsmIndexEntrySize]byte
		binary.BigEndian.PutUint64(b[:8], uint64(entry.MinTime))
		binary.BigEndian.PutUint64(b[8:16], uint64(entry.MaxTime))
		binary.BigEndian.PutUint64(b[16:24], uint64(entry.Offset))
		binary.BigEndian.PutUint32(b[24:28], entry.Size)
		buf.Write(b[:])
	}
}
