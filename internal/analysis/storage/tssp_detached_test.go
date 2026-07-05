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
