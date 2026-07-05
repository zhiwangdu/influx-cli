package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
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
	if got, want := file.Blocks[0].ValueCount, 3; got != want {
		t.Fatalf("value count = %d, want %d", got, want)
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

	report, err := Analyze(context.Background(), []string{tsmPath, tsspPath}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	formats := map[Format]bool{}
	for _, file := range report.Files {
		formats[file.Format] = true
	}
	if !formats[FormatTSM] || !formats[FormatTSSP] {
		t.Fatalf("formats = %v, want %s and %s", formats, FormatTSM, FormatTSSP)
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
	var buf bytes.Buffer
	var header [5]byte
	binary.BigEndian.PutUint32(header[:4], tsmMagicNumber)
	header[4] = tsmVersion
	buf.Write(header[:])

	block1 := testTSMBlock(tsmBlockFloat, []int64{10, 10, 10})
	offset1 := int64(buf.Len())
	buf.Write(testTSMBlockWithCRC(block1))
	block2 := testTSMBlock(tsmBlockInteger, []int64{100, 20})
	offset2 := int64(buf.Len())
	buf.Write(testTSMBlockWithCRC(block2))

	indexOffset := int64(buf.Len())
	writeTestTSMIndexKey(&buf, "cpu,host=a value", tsmBlockFloat, []tsmIndexEntry{{
		MinTime: 10,
		MaxTime: 30,
		Offset:  offset1,
		Size:    uint32(len(block1) + tsmBlockCRCSize),
	}})
	writeTestTSMIndexKey(&buf, "mem,host=a value", tsmBlockInteger, []tsmIndexEntry{{
		MinTime: 100,
		MaxTime: 120,
		Offset:  offset2,
		Size:    uint32(len(block2) + tsmBlockCRCSize),
	}})

	var footer [8]byte
	binary.BigEndian.PutUint64(footer[:], uint64(indexOffset))
	buf.Write(footer[:])
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func testTSMBlock(blockType byte, timestamps []int64) []byte {
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
	block.WriteByte(0)
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
