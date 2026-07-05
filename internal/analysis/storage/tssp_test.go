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
	if got, want := file.QueryOverlapBlocks, 3; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if got, want := file.Extra["measurement"], "cpu"; got != want {
		t.Fatalf("measurement = %q, want %q", got, want)
	}
	if got, want := file.KeySamples[0], "measurement:cpu"; got != want {
		t.Fatalf("first key sample = %q, want %q", got, want)
	}
}

func writeTestTSSP(path string) error {
	var buf bytes.Buffer
	buf.WriteString(tsspMagic)
	writeUint64(&buf, 2)

	metaOffset := int64(buf.Len())
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{ID: 7, MinTime: 100, MaxTime: 200, Offset: 128, Count: 3, Size: 64})
	writeTestTSSPMetaIndex(&buf, tsspMetaIndex{ID: 9, MinTime: 300, MaxTime: 400, Offset: 192, Count: 2, Size: 64})

	trailerOffset := int64(buf.Len())
	writeTestTSSPTrailer(&buf, tsspTrailer{
		DataOffset:         tsspHeaderSize,
		DataSize:           0,
		IndexSize:          0,
		MetaIndexSize:      int64(buf.Len()) - metaOffset,
		BloomSize:          0,
		IDTimeSize:         0,
		IDCount:            2,
		MinID:              7,
		MaxID:              9,
		MinTime:            100,
		MaxTime:            400,
		MetaIndexItemCount: 2,
		MeasurementName:    "cpu",
	})
	writeGeminiInt64(&buf, trailerOffset)
	return os.WriteFile(path, buf.Bytes(), 0o600)
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
	writeUint16(buf, 0)
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
