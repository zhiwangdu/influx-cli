package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/snappy"
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
	if got, want := decode.BaselineDecodeBytes, int64(288); got != want {
		t.Fatalf("baseline decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBytes, int64(96); got != want {
		t.Fatalf("optimized decode bytes = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeBytes, int64(192); got != want {
		t.Fatalf("saved decode bytes = %d, want %d", got, want)
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
	if got, want := decode.Samples[1].OutputSegments, 1; got != want {
		t.Fatalf("second decode sample output segments = %d, want %d", got, want)
	}
	if got, want := decode.Samples[1].Reason, "segment_overlap"; got != want {
		t.Fatalf("second decode sample reason = %q, want %q", got, want)
	}
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
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
	if got, want := decode.BaselineReadSegments, 6; got != want {
		t.Fatalf("baseline read segments = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedReadSegments, 2; got != want {
		t.Fatalf("optimized read segments = %d, want %d", got, want)
	}
	if got, want := decode.SavedReadSegments, 4; got != want {
		t.Fatalf("saved read segments = %d, want %d", got, want)
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
	for _, sample := range decode.Samples {
		if sample.Path == "" {
			t.Fatalf("decode sample missing path: %+v", sample)
		}
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
	sid     uint64
	minTime int64
	maxTime int64
	offset  int64
	size    uint32
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
	writeTestTSSPColumnMeta(buf, "time", 0, chunk.offset+int64(chunk.size), 16)
}

func writeTestTSSPSelfChunkMeta(buf *bytes.Buffer, header []string, chunk testTSSPChunkSpec) {
	writeUint64(buf, chunk.sid)
	buf.Write(binary.AppendUvarint(nil, uint64(chunk.offset)))
	buf.Write(binary.AppendUvarint(nil, uint64(chunk.size)))
	buf.Write(binary.AppendUvarint(nil, 2))
	buf.Write(binary.AppendUvarint(nil, 1))
	buf.Write(encodeTestTSSPInt64sWithScale(chunk.minTime, chunk.maxTime))
	writeTestTSSPSelfColumnMeta(buf, header, "value", 1, chunk.offset, chunk.size)
	writeTestTSSPSelfColumnMeta(buf, header, "time", 0, chunk.offset+int64(chunk.size), 16)
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
	encoded := uint64((value << 1) ^ (value >> 63))
	writeUint64(buf, encoded)
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
