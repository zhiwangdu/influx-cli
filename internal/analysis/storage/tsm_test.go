package storage

import (
	"bytes"
	"compress/gzip"
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
	if got, want := file.Tombstones.Version, "v1"; got != want {
		t.Fatalf("tombstone version = %q, want %q", got, want)
	}
	if got, want := file.Tombstones.RangeCount, 1; got != want {
		t.Fatalf("tombstone range count = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].ValueCount, 3; got != want {
		t.Fatalf("value count = %d, want %d", got, want)
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
