package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
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
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
	}
}

func TestAnalyzeTSSPCompressedChunkMetadataFallsBackToMetaIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000001-0001-00000000.tssp")
	if err := writeTestTSSPWithCompression(path, 1); err != nil {
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
	if got, want := file.QueryOverlapBlocks, 3; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].Type, "meta-index"; got != want {
		t.Fatalf("first block type = %q, want %q", got, want)
	}
	if got, want := file.Extra["query_overlap_precision"], "meta-index"; got != want {
		t.Fatalf("query overlap precision = %q, want %q", got, want)
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

func writeTestTSSP(path string) error {
	return writeTestTSSPWithCompression(path, 0)
}

func writeTestTSSPWithCompression(path string, chunkMetaCompress uint8) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	payload7 := testTSSPChunkMetaPayload(
		testTSSPChunkSpec{sid: 7, minTime: 100, maxTime: 120, offset: 1024, size: 80},
		testTSSPChunkSpec{sid: 7, minTime: 150, maxTime: 180, offset: 1104, size: 80},
		testTSSPChunkSpec{sid: 7, minTime: 190, maxTime: 200, offset: 1184, size: 80},
	)
	payload9 := testTSSPChunkMetaPayload(
		testTSSPChunkSpec{sid: 9, minTime: 300, maxTime: 330, offset: 1264, size: 96},
		testTSSPChunkSpec{sid: 9, minTime: 340, maxTime: 400, offset: 1360, size: 96},
	)
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
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
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

func writeTestTSSPColumnMeta(buf *bytes.Buffer, name string, typ byte, offset int64, size uint32) {
	writeUint16(buf, uint16(len(name)))
	buf.WriteString(name)
	buf.WriteByte(typ)
	writeUint16(buf, 0)
	writeGeminiInt64(buf, offset)
	writeUint32(buf, size)
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
	if trailer.TimeStoreFlag != 0 || trailer.ChunkMetaCompress != 0 {
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
