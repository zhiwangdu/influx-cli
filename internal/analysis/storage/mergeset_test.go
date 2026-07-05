package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestAnalyzeMergesetPartMetadata(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "41_2_1847A3A45055EEF0")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  41,
		BlocksCount: 2,
		FirstItem:   "6161",
		LastItem:    "7a7a",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatMergeset; got != want {
		t.Fatalf("format = %s, want %s", got, want)
	}
	if got, want := file.KeyCount, 41; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.BlockCount, 2; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["mergeset-block"], 2; got != want {
		t.Fatalf("mergeset block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["mergeset-metaindex-row"], 1; got != want {
		t.Fatalf("mergeset metaindex row count = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{"first:6161", "last:7a7a"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if got, want := file.Extra["part_suffix"], "1847A3A45055EEF0"; got != want {
		t.Fatalf("part suffix = %q, want %q", got, want)
	}
	if got, want := file.Extra["items_count"], "41"; got != want {
		t.Fatalf("items count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["first_item_bytes"], "2"; got != want {
		t.Fatalf("first item bytes = %q, want %q", got, want)
	}
	if got, want := file.Extra["metaindex_row_count"], "1"; got != want {
		t.Fatalf("metaindex row count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["metaindex_block_headers"], "2"; got != want {
		t.Fatalf("metaindex block headers extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["metaindex_index_bytes"], "9"; got != want {
		t.Fatalf("metaindex index bytes extra = %q, want %q", got, want)
	}
	if got, want := len(file.Blocks), 1; got != want {
		t.Fatalf("block samples = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].Type, "mergeset-metaindex-row"; got != want {
		t.Fatalf("block sample type = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].Key, "6161"; got != want {
		t.Fatalf("block sample key = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].ValueCount, 2; got != want {
		t.Fatalf("block sample value count = %d, want %d", got, want)
	}
	if file.SizeBytes == 0 {
		t.Fatal("expected non-zero component size")
	}
}

func TestAnalyzeMergesetDirectoryExpansion(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "3_1_0000000000000001")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  3,
		BlocksCount: 1,
		FirstItem:   "01",
		LastItem:    "03",
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file.unsupported"), []byte("ignored"), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:         FormatAuto,
		KeySampleLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got, want := report.Files[0].Path, partPath; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetBlocksMismatchNotice(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "10_2_0000000000000001")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  10,
		BlocksCount: 1,
		FirstItem:   "01",
		LastItem:    "02",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format: FormatMergeset,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.BlockCount, 1; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if len(file.Notices) != 1 || !strings.Contains(file.Notices[0], "differs from metadata") {
		t.Fatalf("notices = %v, want block-count mismatch notice", file.Notices)
	}
	if len(report.Notices) != 1 || !strings.Contains(report.Notices[0], "differs from metadata") {
		t.Fatalf("report notices = %v, want propagated block-count mismatch notice", report.Notices)
	}
}

func TestAnalyzeMergesetItemsMismatchErrors(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "10_1_0000000000000001")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  9,
		BlocksCount: 1,
		FirstItem:   "01",
		LastItem:    "02",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format: FormatMergeset,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 0; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if len(report.Notices) != 1 || !strings.Contains(report.Notices[0], "invalid mergeset ItemsCount") {
		t.Fatalf("report notices = %v, want items-count mismatch notice", report.Notices)
	}
}

func TestAnalyzeMergesetBadMetaindexNotice(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "5_1_0000000000000001")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  5,
		BlocksCount: 1,
		FirstItem:   "01",
		LastItem:    "05",
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partPath, mergesetMetaindexFile), []byte("not-zstd"), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format: FormatMergeset,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if len(file.Notices) != 1 || !strings.Contains(file.Notices[0], "mergeset metaindex decode unavailable") {
		t.Fatalf("notices = %v, want metaindex decode notice", file.Notices)
	}
	if _, ok := file.Extra["metaindex_row_count"]; ok {
		t.Fatalf("unexpected metaindex row count extra after decode failure: %q", file.Extra["metaindex_row_count"])
	}
	if got := file.BlocksByType["mergeset-metaindex-row"]; got != 0 {
		t.Fatalf("metaindex row block type count = %d, want 0", got)
	}
}

func writeTestMergesetPart(path string, metadata mergesetPartMetadata) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetMetadataFile), data, 0o600); err != nil {
		return err
	}
	indexData := []byte(mergesetIndexFile)
	if err := os.WriteFile(filepath.Join(path, mergesetIndexFile), indexData, 0o600); err != nil {
		return err
	}
	firstItem, err := decodeMergesetHexItem(metadata.FirstItem, "FirstItem")
	if err != nil {
		return err
	}
	if metadata.BlocksCount > uint64(^uint32(0)) {
		return fmt.Errorf("test mergeset metadata BlocksCount too large: %d", metadata.BlocksCount)
	}
	metaindex, err := encodeTestMergesetMetaindexRows([]mergesetMetaindexRow{
		{
			FirstItem:         firstItem,
			BlockHeadersCount: uint32(metadata.BlocksCount),
			IndexBlockOffset:  0,
			IndexBlockSize:    uint32(len(indexData)),
		},
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetMetaindexFile), metaindex, 0o600); err != nil {
		return err
	}
	for _, name := range []string{
		mergesetItemsFile,
		mergesetLensFile,
	} {
		if err := os.WriteFile(filepath.Join(path, name), []byte(name), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func encodeTestMergesetMetaindexRows(rows []mergesetMetaindexRow) ([]byte, error) {
	var data []byte
	for _, row := range rows {
		data = binary.AppendUvarint(data, uint64(len(row.FirstItem)))
		data = append(data, row.FirstItem...)
		data = appendTestBigEndianUint32(data, row.BlockHeadersCount)
		data = appendTestBigEndianUint64(data, row.IndexBlockOffset)
		data = appendTestBigEndianUint32(data, row.IndexBlockSize)
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil), nil
}

func appendTestBigEndianUint32(dst []byte, value uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	return append(dst, buf[:]...)
}

func appendTestBigEndianUint64(dst []byte, value uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return append(dst, buf[:]...)
}
