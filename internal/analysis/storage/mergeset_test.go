package storage

import (
	"bytes"
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
	if got, want := file.Extra["metaindex_index_bytes"], file.Extra["index_size"]; got == "" || got != want {
		t.Fatalf("metaindex index bytes extra = %q, want index size %q", got, want)
	}
	if got, want := file.Extra["index_block_count"], "1"; got != want {
		t.Fatalf("index block count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["index_blocks_decoded"], "1"; got != want {
		t.Fatalf("index blocks decoded extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["index_block_headers"], "2"; got != want {
		t.Fatalf("index block headers extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_count_from_blocks"], "41"; got != want {
		t.Fatalf("item count from blocks extra = %q, want %q", got, want)
	}
	if got := file.Extra["items_block_bytes"]; got == "" || got == "0" {
		t.Fatalf("items block bytes extra = %q, want non-zero", got)
	}
	if got := file.Extra["lens_block_bytes"]; got == "" || got == "0" {
		t.Fatalf("lens block bytes extra = %q, want non-zero", got)
	}
	if got, want := file.Extra["plain_block_headers"], "2"; got != want {
		t.Fatalf("plain block headers extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["zstd_block_headers"], "0"; got != want {
		t.Fatalf("zstd block headers extra = %q, want %q", got, want)
	}
	if got, want := len(file.Blocks), 2; got != want {
		t.Fatalf("block samples = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].Type, "mergeset-block"; got != want {
		t.Fatalf("block sample type = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].Key, "6161"; got != want {
		t.Fatalf("block sample key = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].ValueCount, 21; got != want {
		t.Fatalf("block sample value count = %d, want %d", got, want)
	}
	if got, want := file.Blocks[1].ValueCount, 20; got != want {
		t.Fatalf("second block sample value count = %d, want %d", got, want)
	}
	if got, want := file.Extra["item_payload_block_count"], "2"; got != want {
		t.Fatalf("payload block count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_blocks_decoded"], "2"; got != want {
		t.Fatalf("payload blocks decoded extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_items_decoded"], "41"; got != want {
		t.Fatalf("payload items decoded extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_first_item_hex"], "6161"; got != want {
		t.Fatalf("payload first item extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_last_item_hex"], "7a7a"; got != want {
		t.Fatalf("payload last item extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_samples_hex"], "6161,61610000000000000001"; got != want {
		t.Fatalf("payload samples extra = %q, want %q", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("expected mergeset table scan decode path")
	}
	if got, want := decode.Mode, "mergeset-table-scan-ascending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 2; got != want {
		t.Fatalf("baseline blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeValues, 41; got != want {
		t.Fatalf("baseline values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeValues, 41; got != want {
		t.Fatalf("optimized values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedOutputValues, 41; got != want {
		t.Fatalf("optimized output values = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 2; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 2; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Key, "6161"; got != want {
		t.Fatalf("first cursor window key = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[0].DecodedBlocks, 1; got != want {
		t.Fatalf("first cursor window decoded blocks = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 2; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].OptimizedValue, "aa"; got != want {
		t.Fatalf("first cursor output sample = %q, want %q", got, want)
	}
	wantSecondOutput := []byte{'a', 'a', 0, 0, 0, 0, 0, 0, 0, 1}
	if got := []byte(decode.CursorOutputSamples[1].OptimizedValue); !bytes.Equal(got, wantSecondOutput) {
		t.Fatalf("second cursor output sample = %x, want %x", got, wantSecondOutput)
	}
	if !containsString(decode.Recommendations, "scan 41 decoded mergeset item") {
		t.Fatalf("recommendations = %v, want scan recommendation", decode.Recommendations)
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

func TestAnalyzeMergesetFileSetTableScan(t *testing.T) {
	dir := t.TempDir()
	partPath1 := filepath.Join(dir, "3_1_1847A3A45055EEF0")
	if err := writeTestMergesetPart(partPath1, mergesetPartMetadata{
		ItemsCount:  3,
		BlocksCount: 1,
		FirstItem:   "6161",
		LastItem:    "6164",
	}); err != nil {
		t.Fatal(err)
	}
	partPath2 := filepath.Join(dir, "2_1_1847A3A45055EEF1")
	if err := writeTestMergesetPart(partPath2, mergesetPartMetadata{
		ItemsCount:  2,
		BlocksCount: 1,
		FirstItem:   "7a61",
		LastItem:    "7a62",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   2,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.Mode, "mergeset-file-set-table-scan-ascending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 2; got != want {
		t.Fatalf("baseline blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeValues, 5; got != want {
		t.Fatalf("baseline values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeValues, 5; got != want {
		t.Fatalf("optimized values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedOutputValues, 5; got != want {
		t.Fatalf("optimized output values = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchSeekCalls, 2; got != want {
		t.Fatalf("table search seek calls = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapCandidates, 2; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 5; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
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
	if got, want := len(decode.CursorWindows), 2; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Files, []string{partPath2}; !equalStrings(got, want) {
		t.Fatalf("first cursor window files = %v, want %v", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 4; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].OptimizedValue, "aa"; got != want {
		t.Fatalf("first cursor output sample = %q, want %q", got, want)
	}
	wantSecondOutput := []byte{'a', 'a', 0, 0, 0, 0, 0, 0, 0, 1}
	if got := []byte(decode.CursorOutputSamples[1].OptimizedValue); !bytes.Equal(got, wantSecondOutput) {
		t.Fatalf("second cursor output sample = %x, want %x", got, wantSecondOutput)
	}
	if got, want := decode.CursorOutputSamples[2].OptimizedValue, "ad"; got != want {
		t.Fatalf("third cursor output sample = %q, want %q", got, want)
	}
	if got, want := decode.CursorOutputSamples[3].OptimizedValue, "za"; got != want {
		t.Fatalf("fourth cursor output sample = %q, want %q", got, want)
	}
	if !containsString(decode.Recommendations, "TableSearch-style heap ordering") {
		t.Fatalf("recommendations = %v, want file-set scan recommendation", decode.Recommendations)
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

func TestAnalyzeMergesetBadIndexBlockNotice(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "5_1_0000000000000001")
	metadata := mergesetPartMetadata{
		ItemsCount:  5,
		BlocksCount: 1,
		FirstItem:   "01",
		LastItem:    "05",
	}
	if err := writeTestMergesetPart(partPath, metadata); err != nil {
		t.Fatal(err)
	}
	indexData := []byte("not-zstd")
	if err := os.WriteFile(filepath.Join(partPath, mergesetIndexFile), indexData, 0o600); err != nil {
		t.Fatal(err)
	}
	firstItem, err := decodeMergesetHexItem(metadata.FirstItem, "FirstItem")
	if err != nil {
		t.Fatal(err)
	}
	metaindex, err := encodeTestMergesetMetaindexRows([]mergesetMetaindexRow{
		{
			FirstItem:         firstItem,
			BlockHeadersCount: 1,
			IndexBlockOffset:  0,
			IndexBlockSize:    uint32(len(indexData)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partPath, mergesetMetaindexFile), metaindex, 0o600); err != nil {
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
	if !containsString(file.Notices, "mergeset index block decode unavailable") {
		t.Fatalf("notices = %v, want index block decode notice", file.Notices)
	}
	if got, want := file.Extra["index_block_count"], "1"; got != want {
		t.Fatalf("index block count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["index_blocks_decoded"], "0"; got != want {
		t.Fatalf("index blocks decoded extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["index_block_headers"], "0"; got != want {
		t.Fatalf("index block headers extra = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetZSTDItemPayload(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "4_1_0000000000000001")
	if err := writeTestMergesetPartWithMarshalTypes(partPath, mergesetPartMetadata{
		ItemsCount:  4,
		BlocksCount: 1,
		FirstItem:   "6161",
		LastItem:    "617a",
	}, []byte{mergesetMarshalTypeZSTD}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:         FormatMergeset,
		KeySampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
	}
	if got, want := file.Extra["plain_block_headers"], "0"; got != want {
		t.Fatalf("plain block headers extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["zstd_block_headers"], "1"; got != want {
		t.Fatalf("zstd block headers extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_items_decoded"], "4"; got != want {
		t.Fatalf("payload items decoded extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_first_item_hex"], "6161"; got != want {
		t.Fatalf("payload first item extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_last_item_hex"], "617a"; got != want {
		t.Fatalf("payload last item extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_samples_hex"], "6161,61610000000000000001,61610000000000000002"; got != want {
		t.Fatalf("payload samples extra = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetQueryKeySearch(t *testing.T) {
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
		Format:           FormatMergeset,
		QueryKeys:        []string{"aa", "0"},
		KeySampleLimit:   2,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if !file.QueryOverlapsFile {
		t.Fatal("expected query key search to match the mergeset part")
	}
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	decode := file.DecodePath
	if decode == nil {
		t.Fatal("expected decode path")
	}
	if got, want := decode.Mode, "mergeset-item-search-ascending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.QueryKeys, []string{"0", "aa"}; !equalStrings(got, want) {
		t.Fatalf("query keys = %v, want %v", got, want)
	}
	if got, want := decode.MatchedKeys, []string{"aa"}; !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got, want := decode.MissingKeys, []string{"0"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
	}
	if !decode.KeyFilterApplied {
		t.Fatal("expected key filter applied")
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
	if got, want := decode.BaselineDecodeValues, 41; got != want {
		t.Fatalf("baseline values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeValues, 21; got != want {
		t.Fatalf("optimized values = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeValues, 20; got != want {
		t.Fatalf("saved values = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 1; got != want {
		t.Fatalf("skipped by key blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 2; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].Key, "0"; got != want {
		t.Fatalf("first cursor output sample key = %q, want %q", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].OptimizedValue, "aa"; got != want {
		t.Fatalf("first cursor output sample value = %q, want %q", got, want)
	}
	if decode.CursorOutputSamples[0].Matches {
		t.Fatal("expected first cursor output sample to be a non-exact table seek result")
	}
	if got, want := decode.CursorOutputSamples[1].Key, "aa"; got != want {
		t.Fatalf("second cursor output sample key = %q, want %q", got, want)
	}
	if got, want := decode.CursorOutputSamples[1].OptimizedValue, "aa"; got != want {
		t.Fatalf("second cursor output sample value = %q, want %q", got, want)
	}
	if !decode.CursorOutputSamples[1].Matches {
		t.Fatal("expected second cursor output sample to match exactly")
	}
	if got, want := decode.TableSearchSeekCalls, 2; got != want {
		t.Fatalf("table search seek calls = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapCandidates, 2; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 2; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchExactMisses, 1; got != want {
		t.Fatalf("table search exact misses = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].Reason, "key_range_candidate"; got != want {
		t.Fatalf("first sample reason = %q, want %q", got, want)
	}
	if got, want := decode.Samples[1].Reason, "key_not_in_block_range"; got != want {
		t.Fatalf("second sample reason = %q, want %q", got, want)
	}
	if len(decode.Recommendations) == 0 {
		t.Fatal("expected recommendations")
	}
}

func TestAnalyzeMergesetQueryKeyBelowFirstItemSeeksFirstBlock(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "3_1_1847A3A45055EEF0")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  3,
		BlocksCount: 1,
		FirstItem:   "6161",
		LastItem:    "6164",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"0"},
		KeySampleLimit:   2,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("expected decode path")
	}
	if got, want := decode.MatchedKeys, []string(nil); !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got, want := decode.MissingKeys, []string{"0"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchSeekCalls, 1; got != want {
		t.Fatalf("table search seek calls = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapCandidates, 1; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 1; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchExactMisses, 1; got != want {
		t.Fatalf("table search exact misses = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	sample := decode.CursorOutputSamples[0]
	if got, want := sample.Key, "0"; got != want {
		t.Fatalf("cursor output sample key = %q, want %q", got, want)
	}
	if got, want := sample.OptimizedValue, "aa"; got != want {
		t.Fatalf("cursor output sample value = %q, want %q", got, want)
	}
	if sample.Matches {
		t.Fatal("expected cursor output sample to be a non-exact table seek result")
	}
}

func TestAnalyzeMergesetFileSetQueryKeySearch(t *testing.T) {
	dir := t.TempDir()
	partPath1 := filepath.Join(dir, "41_2_1847A3A45055EEF0")
	if err := writeTestMergesetPart(partPath1, mergesetPartMetadata{
		ItemsCount:  41,
		BlocksCount: 2,
		FirstItem:   "6161",
		LastItem:    "6164",
	}); err != nil {
		t.Fatal(err)
	}
	partPath2 := filepath.Join(dir, "2_1_1847A3A45055EEF1")
	if err := writeTestMergesetPart(partPath2, mergesetPartMetadata{
		ItemsCount:  2,
		BlocksCount: 1,
		FirstItem:   "7a61",
		LastItem:    "7a62",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"za", "aa", "0"},
		KeySampleLimit:   2,
		BlockSampleLimit: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 2; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.Mode, "mergeset-file-set-item-search-ascending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.QueryKeys, []string{"0", "aa", "za"}; !equalStrings(got, want) {
		t.Fatalf("query keys = %v, want %v", got, want)
	}
	if got, want := decode.MatchedKeys, []string{"aa", "za"}; !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got, want := decode.MissingKeys, []string{"0"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
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
	if got, want := decode.BaselineDecodeValues, 43; got != want {
		t.Fatalf("baseline values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeValues, 23; got != want {
		t.Fatalf("optimized values = %d, want %d", got, want)
	}
	if got, want := decode.SavedDecodeValues, 20; got != want {
		t.Fatalf("saved values = %d, want %d", got, want)
	}
	if got, want := decode.BaselineOutputValues, 3; got != want {
		t.Fatalf("baseline output values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedOutputValues, 2; got != want {
		t.Fatalf("optimized output values = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 1; got != want {
		t.Fatalf("skipped by key blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 2; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 3; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].Key, "0"; got != want {
		t.Fatalf("first cursor output sample key = %q, want %q", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].OptimizedValue, "aa"; got != want {
		t.Fatalf("first cursor output sample value = %q, want %q", got, want)
	}
	if decode.CursorOutputSamples[0].Matches {
		t.Fatal("expected first cursor output sample to be a non-exact table seek result")
	}
	if got, want := decode.CursorOutputSamples[1].Key, "aa"; got != want {
		t.Fatalf("second cursor output sample key = %q, want %q", got, want)
	}
	if !decode.CursorOutputSamples[1].Matches {
		t.Fatal("expected second cursor output sample to match exactly")
	}
	if got, want := decode.CursorOutputSamples[2].Key, "za"; got != want {
		t.Fatalf("third cursor output sample key = %q, want %q", got, want)
	}
	if !decode.CursorOutputSamples[2].Matches {
		t.Fatal("expected third cursor output sample to match exactly")
	}
	if got, want := decode.TableSearchSeekCalls, 6; got != want {
		t.Fatalf("table search seek calls = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapCandidates, 5; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 3; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchExactMisses, 1; got != want {
		t.Fatalf("table search exact misses = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].Path, partPath2; got != want {
		t.Fatalf("first sample path = %q, want %q", got, want)
	}
	if got, want := decode.Samples[1].Path, partPath1; got != want {
		t.Fatalf("second sample path = %q, want %q", got, want)
	}
	if len(decode.Recommendations) == 0 {
		t.Fatal("expected recommendations")
	}
}

func TestAnalyzeMergesetFileSetDuplicateKeyMergeWindow(t *testing.T) {
	dir := t.TempDir()
	partPath1 := filepath.Join(dir, "3_1_1847A3A45055EEF0")
	if err := writeTestMergesetPart(partPath1, mergesetPartMetadata{
		ItemsCount:  3,
		BlocksCount: 1,
		FirstItem:   "6161",
		LastItem:    "6164",
	}); err != nil {
		t.Fatal(err)
	}
	partPath2 := filepath.Join(dir, "3_1_1847A3A45055EEF1")
	if err := writeTestMergesetPart(partPath2, mergesetPartMetadata{
		ItemsCount:  3,
		BlocksCount: 1,
		FirstItem:   "6161",
		LastItem:    "6165",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"aa"},
		KeySampleLimit:   2,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.MatchedKeys, []string{"aa"}; !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got := len(decode.MissingKeys); got != 0 {
		t.Fatalf("missing keys = %v, want none", decode.MissingKeys)
	}
	if got, want := decode.BaselineDecodeBlocks, 2; got != want {
		t.Fatalf("baseline blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedOutputValues, 2; got != want {
		t.Fatalf("optimized output values = %d, want %d", got, want)
	}
	if got, want := decode.DeduplicatedOutputValues, 1; got != want {
		t.Fatalf("deduplicated output values = %d, want %d", got, want)
	}
	if got, want := decode.DuplicateOutputValues, 1; got != want {
		t.Fatalf("duplicate output values = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowCount, 1; got != want {
		t.Fatalf("merge window count = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowBlocks, 2; got != want {
		t.Fatalf("merge window blocks = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowKeys, 1; got != want {
		t.Fatalf("merge window keys = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchSeekCalls, 2; got != want {
		t.Fatalf("table search seek calls = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapCandidates, 2; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 1; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchExactMisses, 0; got != want {
		t.Fatalf("table search exact misses = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 1; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	window := decode.CursorWindows[0]
	if got, want := window.Key, "aa"; got != want {
		t.Fatalf("cursor window key = %q, want %q", got, want)
	}
	if !window.RequiresMerge {
		t.Fatal("expected cursor window to require merge")
	}
	if got, want := window.Files, []string{partPath1, partPath2}; !equalStrings(got, want) {
		t.Fatalf("cursor window files = %v, want %v", got, want)
	}
	if !containsString(decode.Recommendations, "merge/dedup") {
		t.Fatalf("recommendations = %v, want merge/dedup recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetBadItemPayloadNotice(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "5_1_0000000000000001")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  5,
		BlocksCount: 1,
		FirstItem:   "01",
		LastItem:    "05",
	}); err != nil {
		t.Fatal(err)
	}
	lensInfo, err := os.Stat(filepath.Join(partPath, mergesetLensFile))
	if err != nil {
		t.Fatal(err)
	}
	badLens := bytesOf(0xff, int(lensInfo.Size()))
	if err := os.WriteFile(filepath.Join(partPath, mergesetLensFile), badLens, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format: FormatMergeset,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if !containsString(file.Notices, "mergeset item payload decode unavailable") {
		t.Fatalf("notices = %v, want item payload decode notice", file.Notices)
	}
	if got, want := file.Extra["item_payload_blocks_decoded"], "0"; got != want {
		t.Fatalf("payload blocks decoded extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["item_payload_items_decoded"], "0"; got != want {
		t.Fatalf("payload items decoded extra = %q, want %q", got, want)
	}
}

func writeTestMergesetPart(path string, metadata mergesetPartMetadata) error {
	return writeTestMergesetPartWithMarshalTypes(path, metadata, nil)
}

func writeTestMergesetPartWithMarshalTypes(path string, metadata mergesetPartMetadata, marshalTypes []byte) error {
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
	if metadata.BlocksCount > uint64(^uint32(0)) {
		return fmt.Errorf("test mergeset metadata BlocksCount too large: %d", metadata.BlocksCount)
	}
	headers, itemsData, lensData, err := testMergesetBlockHeaders(metadata, marshalTypes)
	if err != nil {
		return err
	}
	indexData, err := encodeTestMergesetIndexBlock(headers)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetIndexFile), indexData, 0o600); err != nil {
		return err
	}
	metaindex, err := encodeTestMergesetMetaindexRows([]mergesetMetaindexRow{
		{
			FirstItem:         headers[0].FirstItem,
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
	if err := os.WriteFile(filepath.Join(path, mergesetItemsFile), itemsData, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetLensFile), lensData, 0o600); err != nil {
		return err
	}
	return nil
}

func testMergesetBlockHeaders(metadata mergesetPartMetadata, marshalTypes []byte) ([]mergesetBlockHeader, []byte, []byte, error) {
	items, err := testMergesetItems(metadata)
	if err != nil {
		return nil, nil, nil, err
	}
	blockItemCounts, err := testMergesetBlockItemCounts(metadata)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(marshalTypes) > 0 && len(marshalTypes) != len(blockItemCounts) {
		return nil, nil, nil, fmt.Errorf("test mergeset marshal type count=%d, want %d", len(marshalTypes), len(blockItemCounts))
	}
	headers := make([]mergesetBlockHeader, 0, metadata.BlocksCount)
	var itemsData []byte
	var lensData []byte
	itemOffset := 0
	for i, itemCount := range blockItemCounts {
		blockItems := items[itemOffset : itemOffset+itemCount]
		itemOffset += itemCount
		marshalType := mergesetMarshalTypePlain
		if len(marshalTypes) > 0 {
			marshalType = marshalTypes[i]
		}
		commonPrefix := []byte(nil)
		if marshalType == mergesetMarshalTypeZSTD {
			commonPrefix = testCommonPrefix(blockItems)
		}
		blockItemsData, blockLensData, err := encodeTestMergesetBlockPayload(blockItems, commonPrefix, marshalType)
		if err != nil {
			return nil, nil, nil, err
		}
		headers = append(headers, mergesetBlockHeader{
			CommonPrefix:     commonPrefix,
			FirstItem:        append([]byte(nil), blockItems[0]...),
			MarshalType:      marshalType,
			ItemsCount:       uint32(len(blockItems)),
			ItemsBlockOffset: uint64(len(itemsData)),
			LensBlockOffset:  uint64(len(lensData)),
			ItemsBlockSize:   uint32(len(blockItemsData)),
			LensBlockSize:    uint32(len(blockLensData)),
		})
		itemsData = append(itemsData, blockItemsData...)
		lensData = append(lensData, blockLensData...)
	}
	return headers, itemsData, lensData, nil
}

func testMergesetItems(metadata mergesetPartMetadata) ([][]byte, error) {
	firstItem, err := decodeMergesetHexItem(metadata.FirstItem, "FirstItem")
	if err != nil {
		return nil, err
	}
	lastItem, err := decodeMergesetHexItem(metadata.LastItem, "LastItem")
	if err != nil {
		return nil, err
	}
	if bytes.Compare(firstItem, lastItem) > 0 {
		return nil, fmt.Errorf("test mergeset first item must be <= last item")
	}
	if metadata.ItemsCount == 0 {
		return nil, fmt.Errorf("test mergeset metadata ItemsCount cannot be zero")
	}
	items := make([][]byte, 0, metadata.ItemsCount)
	items = append(items, firstItem)
	for i := uint64(1); i+1 < metadata.ItemsCount; i++ {
		item := append([]byte(nil), firstItem...)
		item = binary.BigEndian.AppendUint64(item, i)
		items = append(items, item)
	}
	if metadata.ItemsCount > 1 {
		items = append(items, lastItem)
	}
	for i := 1; i < len(items); i++ {
		if bytes.Compare(items[i-1], items[i]) >= 0 {
			return nil, fmt.Errorf("test mergeset generated unsorted item at %d: %x >= %x", i, items[i-1], items[i])
		}
	}
	return items, nil
}

func testMergesetBlockItemCounts(metadata mergesetPartMetadata) ([]int, error) {
	if metadata.BlocksCount == 0 {
		return nil, fmt.Errorf("test mergeset metadata BlocksCount cannot be zero")
	}
	baseItems := metadata.ItemsCount / metadata.BlocksCount
	remainder := metadata.ItemsCount % metadata.BlocksCount
	counts := make([]int, 0, metadata.BlocksCount)
	for i := uint64(0); i < metadata.BlocksCount; i++ {
		itemCount := baseItems
		if i < remainder {
			itemCount++
		}
		if itemCount == 0 || itemCount > uint64(^uint32(0)) {
			return nil, fmt.Errorf("test mergeset block item count out of range: %d", itemCount)
		}
		counts = append(counts, int(itemCount))
	}
	return counts, nil
}

func encodeTestMergesetBlockPayload(items [][]byte, commonPrefix []byte, marshalType byte) ([]byte, []byte, error) {
	for _, item := range items {
		if !bytes.HasPrefix(item, commonPrefix) {
			return nil, nil, fmt.Errorf("test item %x does not start with common prefix %x", item, commonPrefix)
		}
	}
	switch marshalType {
	case mergesetMarshalTypePlain:
		return encodeTestMergesetPlainBlockPayload(items, commonPrefix)
	case mergesetMarshalTypeZSTD:
		return encodeTestMergesetZSTDBlockPayload(items, commonPrefix)
	default:
		return nil, nil, fmt.Errorf("unsupported test marshal type %d", marshalType)
	}
}

func encodeTestMergesetPlainBlockPayload(items [][]byte, commonPrefix []byte) ([]byte, []byte, error) {
	cpLen := len(commonPrefix)
	var itemsData []byte
	var lensData []byte
	for _, item := range items[1:] {
		itemsData = append(itemsData, item[cpLen:]...)
		lensData = appendTestBigEndianUint64(lensData, uint64(len(item)-cpLen))
	}
	return itemsData, lensData, nil
}

func encodeTestMergesetZSTDBlockPayload(items [][]byte, commonPrefix []byte) ([]byte, []byte, error) {
	cpLen := len(commonPrefix)
	firstItem := items[0]
	var itemsPayload []byte
	var lensPayload []byte
	prefixXORs := make([]uint64, 0, len(items)-1)
	lengthXORs := make([]uint64, 0, len(items)-1)
	prevItem := firstItem[cpLen:]
	var prevPrefixLen uint64
	for _, item := range items[1:] {
		itemSuffix := item[cpLen:]
		prefixLen := uint64(commonPrefixLenBytes(prevItem, itemSuffix))
		itemsPayload = append(itemsPayload, itemSuffix[prefixLen:]...)
		prefixXORs = append(prefixXORs, prefixLen^prevPrefixLen)
		prevPrefixLen = prefixLen
		prevItem = itemSuffix
	}
	prevItemLen := uint64(len(firstItem) - cpLen)
	for _, item := range items[1:] {
		itemLen := uint64(len(item) - cpLen)
		lengthXORs = append(lengthXORs, itemLen^prevItemLen)
		prevItemLen = itemLen
	}
	for _, value := range prefixXORs {
		lensPayload = binary.AppendUvarint(lensPayload, value)
	}
	for _, value := range lengthXORs {
		lensPayload = binary.AppendUvarint(lensPayload, value)
	}
	itemsData, err := encodeTestZSTD(itemsPayload)
	if err != nil {
		return nil, nil, err
	}
	lensData, err := encodeTestZSTD(lensPayload)
	if err != nil {
		return nil, nil, err
	}
	return itemsData, lensData, nil
}

func testCommonPrefix(items [][]byte) []byte {
	if len(items) == 0 {
		return nil
	}
	prefix := append([]byte(nil), items[0]...)
	for _, item := range items[1:] {
		prefix = prefix[:commonPrefixLenBytes(prefix, item)]
	}
	return prefix
}

func commonPrefixLenBytes(a, b []byte) int {
	limit := len(a)
	if len(b) < limit {
		limit = len(b)
	}
	for i := 0; i < limit; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return limit
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

func encodeTestMergesetIndexBlock(headers []mergesetBlockHeader) ([]byte, error) {
	var data []byte
	for _, header := range headers {
		data = binary.AppendUvarint(data, uint64(len(header.CommonPrefix)))
		data = append(data, header.CommonPrefix...)
		data = binary.AppendUvarint(data, uint64(len(header.FirstItem)))
		data = append(data, header.FirstItem...)
		data = append(data, header.MarshalType)
		data = appendTestBigEndianUint32(data, header.ItemsCount)
		data = appendTestBigEndianUint64(data, header.ItemsBlockOffset)
		data = appendTestBigEndianUint64(data, header.LensBlockOffset)
		data = appendTestBigEndianUint32(data, header.ItemsBlockSize)
		data = appendTestBigEndianUint32(data, header.LensBlockSize)
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil), nil
}

func encodeTestZSTD(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(data, nil), nil
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

func bytesOf(value byte, count int) []byte {
	data := make([]byte, count)
	for i := range data {
		data[i] = value
	}
	return data
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
