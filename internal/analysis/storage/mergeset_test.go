package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
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
	if got, want := decode.TableSearchHeapInserts, 5; got != want {
		t.Fatalf("table search heap inserts = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapPops, 5; got != want {
		t.Fatalf("table search heap pops = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchCursorAdvances, 3; got != want {
		t.Fatalf("table search cursor advances = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchCursorExhaustions, 2; got != want {
		t.Fatalf("table search cursor exhaustions = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 5; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.DeduplicatedOutputValues, 5; got != want {
		t.Fatalf("deduplicated output values = %d, want %d", got, want)
	}
	if got, want := decode.DuplicateOutputValues, 0; got != want {
		t.Fatalf("duplicate output values = %d, want %d", got, want)
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
	if got, want := decode.CursorOutputSamples[0].File, partPath1; got != want {
		t.Fatalf("first cursor output files = %v, want %v", got, want)
	}
	wantSecondOutput := []byte{'a', 'a', 0, 0, 0, 0, 0, 0, 0, 1}
	if got := []byte(decode.CursorOutputSamples[1].OptimizedValue); !bytes.Equal(got, wantSecondOutput) {
		t.Fatalf("second cursor output sample = %x, want %x", got, wantSecondOutput)
	}
	if got, want := decode.CursorOutputSamples[1].File, partPath1; got != want {
		t.Fatalf("second cursor output files = %v, want %v", got, want)
	}
	if got, want := decode.CursorOutputSamples[2].OptimizedValue, "ad"; got != want {
		t.Fatalf("third cursor output sample = %q, want %q", got, want)
	}
	if got, want := decode.CursorOutputSamples[3].OptimizedValue, "za"; got != want {
		t.Fatalf("fourth cursor output sample = %q, want %q", got, want)
	}
	if got, want := decode.CursorOutputSamples[3].File, partPath2; got != want {
		t.Fatalf("fourth cursor output files = %v, want %v", got, want)
	}
	if got, want := len(decode.CursorFinalOutputSamples), 4; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	for i, want := range []struct {
		value string
		file  string
	}{
		{value: "aa", file: partPath1},
		{value: string(wantSecondOutput), file: partPath1},
		{value: "ad", file: partPath1},
		{value: "za", file: partPath2},
	} {
		got := decode.CursorFinalOutputSamples[i]
		if got.OptimizedValue != want.value {
			t.Fatalf("cursor final output sample[%d] value = %q, want %q", i, got.OptimizedValue, want.value)
		}
		if got.File != want.file {
			t.Fatalf("cursor final output sample[%d] file = %q, want %q", i, got.File, want.file)
		}
		if got.RequiresDedup || got.RequiresMerge {
			t.Fatalf("cursor final output sample[%d] = %+v, want no dedup/merge", i, got)
		}
	}
	if !containsString(decode.Recommendations, "TableSearch-style heap ordering") {
		t.Fatalf("recommendations = %v, want file-set scan recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "deduplicated TableSearch cursor output") {
		t.Fatalf("recommendations = %v, want final output recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "advanced 3 local mergeset part cursor step") {
		t.Fatalf("recommendations = %v, want cursor advance recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetFileSetTableScanSingleStreamHeapAccounting(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "3_1_1847A3A45055EEF0")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  3,
		BlocksCount: 1,
		FirstItem:   "6161",
		LastItem:    "6164",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.TableSearchHeapCandidates, 1; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapInserts, 3; got != want {
		t.Fatalf("table search heap inserts = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapPops, 3; got != want {
		t.Fatalf("table search heap pops = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchCursorAdvances, 2; got != want {
		t.Fatalf("table search cursor advances = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchCursorExhaustions, 1; got != want {
		t.Fatalf("table search cursor exhaustions = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 3; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorExecutionSamples), 2; got != want {
		t.Fatalf("cursor execution samples = %d, want %d", got, want)
	}
	wantFirstStep := DecodePathCursorStep{
		Step:                1,
		Type:                "mergeset-table-search-heap-step",
		Action:              "heap_pop_cursor_advance",
		Key:                 "aa",
		File:                partPath,
		HeapSizeBefore:      1,
		HeapSizeAfterPop:    0,
		HeapSizeAfterAction: 1,
		CursorIndexBefore:   0,
		CursorIndexAfter:    1,
		CursorAdvanced:      true,
	}
	if got := decode.CursorExecutionSamples[0]; got != wantFirstStep {
		t.Fatalf("cursor execution sample[0] = %+v, want %+v", got, wantFirstStep)
	}
	firstStepJSON, err := json.Marshal(decode.CursorExecutionSamples[0])
	if err != nil {
		t.Fatalf("marshal cursor execution sample: %v", err)
	}
	for _, want := range []string{`"key":"aa"`, `"heap_size_after_pop":0`, `"cursor_index_before":0`, `"cursor_exhausted":false`} {
		if !strings.Contains(string(firstStepJSON), want) {
			t.Fatalf("cursor execution sample json = %s, want %s", firstStepJSON, want)
		}
	}
	if got := decode.CursorExecutionSamples[1]; got.Step != 2 || got.Action != "heap_pop_cursor_advance" || got.CursorIndexBefore != 1 || got.CursorIndexAfter != 2 || !got.CursorAdvanced {
		t.Fatalf("cursor execution sample[1] = %+v, want second local advance", got)
	}
}

func TestAnalyzeMergesetFileSetTableScanDuplicateHeapOutput(t *testing.T) {
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
		BlockSampleLimit: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.TableSearchOutputValues, 6; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapInserts, 6; got != want {
		t.Fatalf("table search heap inserts = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapPops, 6; got != want {
		t.Fatalf("table search heap pops = %d, want %d", got, want)
	}
	if got, want := decode.DeduplicatedOutputValues, 4; got != want {
		t.Fatalf("deduplicated output values = %d, want %d", got, want)
	}
	if got, want := decode.DuplicateOutputValues, 2; got != want {
		t.Fatalf("duplicate output values = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowKeys, 2; got != want {
		t.Fatalf("merge window keys = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorExecutionSamples), 6; got != want {
		t.Fatalf("cursor execution samples = %d, want %d", got, want)
	}
	wantFirstStep := DecodePathCursorStep{
		Step:                1,
		Type:                "mergeset-table-search-heap-step",
		Action:              "heap_pop_cursor_advance",
		Key:                 "aa",
		File:                partPath1,
		HeapSizeBefore:      2,
		HeapSizeAfterPop:    1,
		HeapSizeAfterAction: 2,
		CursorIndexBefore:   0,
		CursorIndexAfter:    1,
		CursorAdvanced:      true,
	}
	if got := decode.CursorExecutionSamples[0]; got != wantFirstStep {
		t.Fatalf("cursor execution sample[0] = %+v, want %+v", got, wantFirstStep)
	}
	wantLastStep := DecodePathCursorStep{
		Step:                6,
		Type:                "mergeset-table-search-heap-step",
		Action:              "heap_pop_cursor_exhaust",
		Key:                 "ae",
		File:                partPath2,
		HeapSizeBefore:      1,
		HeapSizeAfterPop:    0,
		HeapSizeAfterAction: 0,
		CursorIndexBefore:   2,
		CursorIndexAfter:    3,
		CursorExhausted:     true,
	}
	if got := decode.CursorExecutionSamples[5]; got != wantLastStep {
		t.Fatalf("cursor execution sample[5] = %+v, want %+v", got, wantLastStep)
	}
	if got, want := len(decode.CursorOutputSamples), 6; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	wantValues := [][]byte{
		[]byte("aa"),
		[]byte("aa"),
		[]byte{'a', 'a', 0, 0, 0, 0, 0, 0, 0, 1},
		[]byte{'a', 'a', 0, 0, 0, 0, 0, 0, 0, 1},
		[]byte("ad"),
		[]byte("ae"),
	}
	for i, want := range wantValues {
		if got := []byte(decode.CursorOutputSamples[i].OptimizedValue); !bytes.Equal(got, want) {
			t.Fatalf("cursor output sample[%d] = %x, want %x", i, got, want)
		}
	}
	for i := 0; i < 4; i++ {
		wantFile := partPath1
		if i%2 == 1 {
			wantFile = partPath2
		}
		if got := decode.CursorOutputSamples[i].File; got != wantFile {
			t.Fatalf("cursor output sample[%d] file = %q, want %q", i, got, wantFile)
		}
		wantMergeFiles := newDecodePathStringList([]string{partPath1, partPath2})
		if got := decode.CursorOutputSamples[i].MergeFiles; got != wantMergeFiles {
			t.Fatalf("cursor output sample[%d] merge files = %q, want %q", i, got, wantMergeFiles)
		}
		if !decode.CursorOutputSamples[i].RequiresDedup {
			t.Fatalf("cursor output sample[%d] should require dedup", i)
		}
		if !decode.CursorOutputSamples[i].RequiresMerge {
			t.Fatalf("cursor output sample[%d] should require merge", i)
		}
	}
	if got, want := len(decode.CursorFinalOutputSamples), 4; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	wantFinalValues := [][]byte{
		[]byte("aa"),
		[]byte{'a', 'a', 0, 0, 0, 0, 0, 0, 0, 1},
		[]byte("ad"),
		[]byte("ae"),
	}
	for i, want := range wantFinalValues {
		got := decode.CursorFinalOutputSamples[i]
		if !bytes.Equal([]byte(got.OptimizedValue), want) {
			t.Fatalf("cursor final output sample[%d] = %x, want %x", i, []byte(got.OptimizedValue), want)
		}
		if i < 2 {
			if got.MergeFiles != newDecodePathStringList([]string{partPath1, partPath2}) {
				t.Fatalf("cursor final output sample[%d] merge files = %q, want both parts", i, got.MergeFiles)
			}
			if !got.RequiresDedup || !got.RequiresMerge {
				t.Fatalf("cursor final output sample[%d] = %+v, want dedup and merge", i, got)
			}
		} else if got.RequiresDedup || got.RequiresMerge || got.MergeFiles != "" {
			t.Fatalf("cursor final output sample[%d] = %+v, want no dedup/merge", i, got)
		}
	}
	duplicateWindowCount := 0
	for _, window := range decode.CursorWindows {
		if window.Reason == "duplicate_item_merge" {
			duplicateWindowCount++
			if !window.RequiresMerge {
				t.Fatalf("duplicate merge window %#v should require merge", window)
			}
			if got, want := window.Files, []string{partPath1, partPath2}; !equalStrings(got, want) {
				t.Fatalf("duplicate merge window files = %v, want %v", got, want)
			}
		}
	}
	if got, want := duplicateWindowCount, 2; got != want {
		t.Fatalf("duplicate merge windows = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "merge/dedup 2 duplicate table-scan item candidate") {
		t.Fatalf("recommendations = %v, want duplicate heap output recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "heap pop/advance/exhaust steps") {
		t.Fatalf("recommendations = %v, want heap execution sample recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetFileSetTableScanIntraPartDuplicateHeapOutput(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "3_1_dupinpart")
	items := [][][]byte{{
		[]byte("aa"),
		[]byte("aa"),
		[]byte("ab"),
	}}
	if err := writeTestMergesetPartWithItemBlocks(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItemBlocks() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.DuplicateOutputValues, 1; got != want {
		t.Fatalf("duplicate output values = %d, want %d", got, want)
	}
	if got, want := decode.MergeWindowKeys, 0; got != want {
		t.Fatalf("merge window keys = %d, want %d", got, want)
	}
	for i := 0; i < 2; i++ {
		if got, want := decode.CursorOutputSamples[i].File, partPath; got != want {
			t.Fatalf("cursor output sample[%d] file = %q, want %q", i, got, want)
		}
		if !decode.CursorOutputSamples[i].RequiresDedup {
			t.Fatalf("cursor output sample[%d] should require dedup", i)
		}
		if decode.CursorOutputSamples[i].RequiresMerge {
			t.Fatalf("cursor output sample[%d] should not require merge", i)
		}
		if decode.CursorOutputSamples[i].MergeFiles != "" {
			t.Fatalf("cursor output sample[%d] merge files = %q, want empty", i, decode.CursorOutputSamples[i].MergeFiles)
		}
	}
	if got, want := len(decode.CursorFinalOutputSamples), 2; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	if got := decode.CursorFinalOutputSamples[0]; got.OptimizedValue != "aa" || !got.RequiresDedup || got.RequiresMerge || got.MergeFiles != "" {
		t.Fatalf("first cursor final output sample = %+v, want deduped intra-part aa", got)
	}
	if got := decode.CursorFinalOutputSamples[1]; got.OptimizedValue != "ab" || got.RequiresDedup || got.RequiresMerge {
		t.Fatalf("second cursor final output sample = %+v, want plain ab", got)
	}
	if !containsString(decode.Recommendations, "dedup 1 duplicate table-scan item candidate") {
		t.Fatalf("recommendations = %v, want intra-part duplicate recommendation", decode.Recommendations)
	}
	if containsString(decode.Recommendations, "sampled 0 of") {
		t.Fatalf("recommendations = %v, want no false duplicate window cap recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetFileSetTableScanDuplicateWindowSampling(t *testing.T) {
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
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := len(decode.CursorWindows), 2; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	duplicateWindowCount := 0
	for _, window := range decode.CursorWindows {
		if window.Reason == "duplicate_item_merge" {
			duplicateWindowCount++
		}
	}
	if got, want := duplicateWindowCount, 2; got != want {
		t.Fatalf("duplicate merge windows = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "evicted 2 part-level cursor window sample") {
		t.Fatalf("recommendations = %v, want cursor window eviction recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetFileSetTableScanDuplicateWindowCapRecommendation(t *testing.T) {
	dir := t.TempDir()
	items := [][]byte{[]byte("aa"), []byte("ab"), []byte("ac")}
	partPath1 := filepath.Join(dir, "3_1_dupa")
	if err := writeTestMergesetPartWithItems(partPath1, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}
	partPath2 := filepath.Join(dir, "3_1_dupb")
	if err := writeTestMergesetPartWithItems(partPath2, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.MergeWindowKeys, 3; got != want {
		t.Fatalf("merge window keys = %d, want %d", got, want)
	}
	duplicateWindowCount := 0
	for _, window := range decode.CursorWindows {
		if window.Reason == "duplicate_item_merge" {
			duplicateWindowCount++
		}
	}
	if got, want := duplicateWindowCount, 2; got != want {
		t.Fatalf("duplicate merge windows = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "sampled 2 of 3 duplicate merge window") {
		t.Fatalf("recommendations = %v, want duplicate merge window cap recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "evicted 2 part-level cursor window sample") {
		t.Fatalf("recommendations = %v, want cursor window eviction recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetFileSetTableScanDuplicateWindowSamplingDisabled(t *testing.T) {
	dir := t.TempDir()
	items := [][]byte{[]byte("aa"), []byte("ab")}
	partPath1 := filepath.Join(dir, "2_1_dupa")
	if err := writeTestMergesetPartWithItems(partPath1, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}
	partPath2 := filepath.Join(dir, "2_1_dupb")
	if err := writeTestMergesetPartWithItems(partPath2, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		BlockSampleLimit: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.MergeWindowKeys, 2; got != want {
		t.Fatalf("merge window keys = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapInserts, 4; got != want {
		t.Fatalf("table search heap inserts = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapPops, 4; got != want {
		t.Fatalf("table search heap pops = %d, want %d", got, want)
	}
	if got := len(decode.CursorWindows); got != 0 {
		t.Fatalf("cursor windows = %d, want 0", got)
	}
	if got := len(decode.CursorOutputSamples); got != 0 {
		t.Fatalf("cursor output samples = %d, want 0", got)
	}
	if got := len(decode.CursorFinalOutputSamples); got != 0 {
		t.Fatalf("cursor final output samples = %d, want 0", got)
	}
	if got := len(decode.CursorExecutionSamples); got != 0 {
		t.Fatalf("cursor execution samples = %d, want 0", got)
	}
	if containsString(decode.Recommendations, "sampled 0 of") {
		t.Fatalf("recommendations = %v, want no sampled-0 recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetFileSetTableScanDescendingDuplicateHeapOutput(t *testing.T) {
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
		BlockSampleLimit: 6,
		CursorDescending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.Mode, "mergeset-file-set-table-scan-descending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.MergeWindowKeys, 2; got != want {
		t.Fatalf("merge window keys = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapInserts, 6; got != want {
		t.Fatalf("table search heap inserts = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapPops, 6; got != want {
		t.Fatalf("table search heap pops = %d, want %d", got, want)
	}
	wantValues := []string{"ae", "ad"}
	for i, want := range wantValues {
		if got := decode.CursorOutputSamples[i].OptimizedValue; got != want {
			t.Fatalf("cursor output sample[%d] = %q, want %q", i, got, want)
		}
		if decode.CursorOutputSamples[i].RequiresDedup {
			t.Fatalf("cursor output sample[%d] should not require dedup", i)
		}
		if decode.CursorOutputSamples[i].RequiresMerge {
			t.Fatalf("cursor output sample[%d] should not require merge", i)
		}
		if decode.CursorOutputSamples[i].MergeFiles != "" {
			t.Fatalf("cursor output sample[%d] merge files = %q, want empty", i, decode.CursorOutputSamples[i].MergeFiles)
		}
	}
	wantSecondOutput := []byte{'a', 'a', 0, 0, 0, 0, 0, 0, 0, 1}
	for i := 2; i < 4; i++ {
		if got := []byte(decode.CursorOutputSamples[i].OptimizedValue); !bytes.Equal(got, wantSecondOutput) {
			t.Fatalf("cursor output sample[%d] = %x, want %x", i, got, wantSecondOutput)
		}
		if got, want := decode.CursorOutputSamples[i].MergeFiles, newDecodePathStringList([]string{partPath1, partPath2}); got != want {
			t.Fatalf("cursor output sample[%d] merge files = %q, want %q", i, got, want)
		}
		if !decode.CursorOutputSamples[i].RequiresDedup {
			t.Fatalf("cursor output sample[%d] should require dedup", i)
		}
		if !decode.CursorOutputSamples[i].RequiresMerge {
			t.Fatalf("cursor output sample[%d] should require merge", i)
		}
	}
	for i := 4; i < 6; i++ {
		if got, want := decode.CursorOutputSamples[i].OptimizedValue, "aa"; got != want {
			t.Fatalf("cursor output sample[%d] = %q, want %q", i, got, want)
		}
		if got, want := decode.CursorOutputSamples[i].MergeFiles, newDecodePathStringList([]string{partPath1, partPath2}); got != want {
			t.Fatalf("cursor output sample[%d] merge files = %q, want %q", i, got, want)
		}
		if !decode.CursorOutputSamples[i].RequiresDedup {
			t.Fatalf("cursor output sample[%d] should require dedup", i)
		}
		if !decode.CursorOutputSamples[i].RequiresMerge {
			t.Fatalf("cursor output sample[%d] should require merge", i)
		}
	}
	if got, want := len(decode.CursorFinalOutputSamples), 4; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	wantFinalValues := []string{"ae", "ad", string(wantSecondOutput), "aa"}
	for i, want := range wantFinalValues {
		got := decode.CursorFinalOutputSamples[i]
		if got.OptimizedValue != want {
			t.Fatalf("cursor final output sample[%d] = %q, want %q", i, got.OptimizedValue, want)
		}
		if i < 2 {
			if got.RequiresDedup || got.RequiresMerge || got.MergeFiles != "" {
				t.Fatalf("cursor final output sample[%d] = %+v, want no dedup/merge", i, got)
			}
		} else {
			if got.MergeFiles != newDecodePathStringList([]string{partPath1, partPath2}) {
				t.Fatalf("cursor final output sample[%d] merge files = %q, want both parts", i, got.MergeFiles)
			}
			if !got.RequiresDedup || !got.RequiresMerge {
				t.Fatalf("cursor final output sample[%d] = %+v, want dedup and merge", i, got)
			}
		}
	}
	duplicateWindowCount := 0
	for _, window := range decode.CursorWindows {
		if window.Reason == "duplicate_item_merge" {
			duplicateWindowCount++
			if got, want := window.Files, []string{partPath1, partPath2}; !equalStrings(got, want) {
				t.Fatalf("duplicate merge window files = %v, want %v", got, want)
			}
		}
	}
	if got, want := duplicateWindowCount, 2; got != want {
		t.Fatalf("duplicate merge windows = %d, want %d", got, want)
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
	if got, want := len(decode.CursorFinalOutputSamples), 1; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	wantFinal := DecodePathCursorOutput{
		Key:            "aa",
		Type:           "mergeset-item-search-final-output-item",
		OptimizedValue: "aa",
		Matches:        true,
	}
	if got := decode.CursorFinalOutputSamples[0]; got != wantFinal {
		t.Fatalf("cursor final output sample = %+v, want %+v", got, wantFinal)
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
	if !containsString(decode.Recommendations, "final item-search output samples show exact local mergeset seek results") {
		t.Fatalf("recommendations = %v, want final output recommendation", decode.Recommendations)
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
	if got, want := len(decode.CursorFinalOutputSamples), 0; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
}

func TestAnalyzeMergesetQueryKeySearchSampleLimitZeroSuppressesSamples(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "2_1_searchlimit")
	if err := writeTestMergesetPartWithItems(partPath, [][]byte{
		[]byte("aa"),
		[]byte("ab"),
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"aa"},
		BlockSampleLimit: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("expected decode path")
	}
	if got, want := decode.MatchedKeys, []string{"aa"}; !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got := len(decode.CursorWindows); got != 0 {
		t.Fatalf("cursor windows = %d, want 0", got)
	}
	if got := len(decode.CursorOutputSamples); got != 0 {
		t.Fatalf("cursor output samples = %d, want 0", got)
	}
	if got := len(decode.CursorFinalOutputSamples); got != 0 {
		t.Fatalf("cursor final output samples = %d, want 0", got)
	}
}

func TestAnalyzeMergesetQueryKeySearchDescending(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "3_1_descsearch")
	if err := writeTestMergesetPartWithItems(partPath, [][]byte{
		[]byte("aa"),
		[]byte("ad"),
		[]byte("za"),
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"zz", "ab", "0", "ad"},
		BlockSampleLimit: 4,
		CursorDescending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("expected decode path")
	}
	if got, want := decode.Mode, "mergeset-item-search-descending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.QueryKeys, []string{"0", "ab", "ad", "zz"}; !equalStrings(got, want) {
		t.Fatalf("query keys = %v, want %v", got, want)
	}
	if got, want := decode.MatchedKeys, []string{"ad"}; !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got, want := decode.MissingKeys, []string{"0", "ab", "zz"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchSeekCalls, 4; got != want {
		t.Fatalf("table search seek calls = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapCandidates, 3; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 3; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchExactMisses, 3; got != want {
		t.Fatalf("table search exact misses = %d, want %d", got, want)
	}
	wantSamples := []DecodePathCursorOutput{
		{Key: "ab", Type: "mergeset-item", OptimizedValue: "aa", Matches: false},
		{Key: "ad", Type: "mergeset-item", OptimizedValue: "ad", Matches: true},
		{Key: "zz", Type: "mergeset-item", OptimizedValue: "za", Matches: false},
	}
	if got, want := len(decode.CursorOutputSamples), len(wantSamples); got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range wantSamples {
		if got := decode.CursorOutputSamples[i]; got != want {
			t.Fatalf("cursor output sample[%d] = %+v, want %+v", i, got, want)
		}
	}
	if got, want := len(decode.CursorFinalOutputSamples), 1; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	wantFinal := DecodePathCursorOutput{
		Key:            "ad",
		Type:           "mergeset-item-search-final-output-item",
		OptimizedValue: "ad",
		Matches:        true,
	}
	if got := decode.CursorFinalOutputSamples[0]; got != wantFinal {
		t.Fatalf("cursor final output sample = %+v, want %+v", got, wantFinal)
	}
}

func TestAnalyzeMergesetQueryKeySearchDescendingMultiBlockExactMatch(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "4_2_descmultiblock")
	if err := writeTestMergesetPartWithItemBlocks(partPath, [][][]byte{
		{
			[]byte("aa"),
			[]byte("ab"),
		},
		{
			[]byte("az"),
			[]byte("ba"),
		},
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItemBlocks() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"ab", "ba"},
		BlockSampleLimit: 6,
		CursorDescending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("expected file decode path")
	}
	if got, want := decode.MatchedKeys, []string{"ab", "ba"}; !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got := len(decode.MissingKeys); got != 0 {
		t.Fatalf("missing keys = %v, want none", decode.MissingKeys)
	}
	if got, want := decode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("optimized decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 2; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 2; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].FirstBlockIndex, 1; got != want {
		t.Fatalf("first cursor window block index = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "key_range_candidate"; got != want {
		t.Fatalf("first cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[1].FirstBlockIndex, 0; got != want {
		t.Fatalf("second cursor window block index = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[1].Reason, "key_range_candidate"; got != want {
		t.Fatalf("second cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.TableSearchHeapCandidates, 2; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 2; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchExactMisses, 0; got != want {
		t.Fatalf("table search exact misses = %d, want %d", got, want)
	}
	wantSamples := []DecodePathCursorOutput{
		{Key: "ab", Type: "mergeset-item", OptimizedValue: "ab", Matches: true},
		{Key: "ba", Type: "mergeset-item", OptimizedValue: "ba", Matches: true},
	}
	if got, want := len(decode.CursorOutputSamples), len(wantSamples); got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range wantSamples {
		if got := decode.CursorOutputSamples[i]; got != want {
			t.Fatalf("cursor output sample[%d] = %+v, want %+v", i, got, want)
		}
	}
	if got, want := string(decode.mergesetSeekResults["ba"].Item), "ba"; got != want {
		t.Fatalf("ba seek result item = %q, want %q", got, want)
	}
	if !decode.mergesetSeekResults["ba"].Matches {
		t.Fatal("expected ba seek result to match exactly")
	}
	if got, want := len(decode.CursorFinalOutputSamples), 2; got != want {
		t.Fatalf("part final output samples = %d, want %d", got, want)
	}
	for i, wantKey := range []string{"ab", "ba"} {
		got := decode.CursorFinalOutputSamples[i]
		if got.Key != wantKey || got.Type != "mergeset-item-search-final-output-item" || got.OptimizedValue != wantKey || !got.Matches {
			t.Fatalf("part final output sample[%d] = %+v, want exact %q", i, got, wantKey)
		}
	}

	fileSetDecode := report.DecodePath
	if fileSetDecode == nil {
		t.Fatal("expected file-set decode path")
	}
	if got, want := fileSetDecode.MatchedKeys, []string{"ab", "ba"}; !equalStrings(got, want) {
		t.Fatalf("file-set matched keys = %v, want %v", got, want)
	}
	if got := len(fileSetDecode.MissingKeys); got != 0 {
		t.Fatalf("file-set missing keys = %v, want none", fileSetDecode.MissingKeys)
	}
	if got, want := len(fileSetDecode.CursorFinalOutputSamples), 2; got != want {
		t.Fatalf("file-set final output samples = %d, want %d", got, want)
	}
	for i, wantKey := range []string{"ab", "ba"} {
		got := fileSetDecode.CursorFinalOutputSamples[i]
		if got.Key != wantKey || got.OptimizedValue != wantKey || !got.Matches {
			t.Fatalf("file-set final output sample[%d] = %+v, want exact %q", i, got, wantKey)
		}
	}
}

func TestAnalyzeMergesetQueryKeySearchAscendingExactMatchDoesNotAdvance(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "4_2_ascendingexact")
	if err := writeTestMergesetPartWithItemBlocks(partPath, [][][]byte{
		{
			[]byte("aa"),
			[]byte("ab"),
		},
		{
			[]byte("az"),
			[]byte("ba"),
		},
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItemBlocks() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"ab"},
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("expected decode path")
	}
	if got, want := decode.MatchedKeys, []string{"ab"}; !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got := len(decode.MissingKeys); got != 0 {
		t.Fatalf("missing keys = %v, want none", decode.MissingKeys)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 1; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "key_range_candidate"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[0].FirstBlockIndex, 0; got != want {
		t.Fatalf("cursor window first block index = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchCursorAdvances, 0; got != want {
		t.Fatalf("table search cursor advances = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	wantSample := DecodePathCursorOutput{
		Key:            "ab",
		Type:           "mergeset-item",
		OptimizedValue: "ab",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != wantSample {
		t.Fatalf("cursor output sample = %+v, want %+v", got, wantSample)
	}
	if got, want := decode.Samples[1].Reason, "key_not_in_block_range"; got != want {
		t.Fatalf("second block sample reason = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetQueryKeySearchAscendingInBlockMissDoesNotAdvance(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "4_2_ascendinginblock")
	if err := writeTestMergesetPartWithItemBlocks(partPath, [][][]byte{
		{
			[]byte("aa"),
			[]byte("az"),
		},
		{
			[]byte("ba"),
			[]byte("bb"),
		},
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItemBlocks() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"ay"},
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
	if got, want := decode.MissingKeys, []string{"ay"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 1; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "key_range_candidate"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[0].FirstBlockIndex, 0; got != want {
		t.Fatalf("cursor window first block index = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchCursorAdvances, 0; got != want {
		t.Fatalf("table search cursor advances = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	wantSample := DecodePathCursorOutput{
		Key:            "ay",
		Type:           "mergeset-item",
		OptimizedValue: "az",
		Matches:        false,
	}
	if got := decode.CursorOutputSamples[0]; got != wantSample {
		t.Fatalf("cursor output sample = %+v, want %+v", got, wantSample)
	}
	if got, want := decode.Samples[1].Reason, "key_not_in_block_range"; got != want {
		t.Fatalf("second block sample reason = %q, want %q", got, want)
	}
}

func TestMergesetSearchPlanDoesNotAdvanceWhenDirectCandidateIsUndecoded(t *testing.T) {
	headers := []mergesetBlockHeader{
		{
			FirstItem:  []byte("aa"),
			ItemsCount: 2,
		},
		{
			FirstItem:  []byte("az"),
			ItemsCount: 2,
		},
	}
	options := Options{
		QueryKeys:        []string{"ay"},
		BlockSampleLimit: 4,
	}
	plan := newMergesetSearchPlan(headers, options, []byte("aa"), []byte("ba"))
	plan.ObserveHeader(0, headers[0])
	plan.ObserveHeader(1, headers[1])
	decodedNext := newMergesetDecodedBlockItems([]byte("az"), 2, plan.QueryKeys, false)
	if err := decodedNext.appendItem([]byte("ba"), 2); err != nil {
		t.Fatal(err)
	}
	plan.ObserveDecodedBlock(1, decodedNext)
	plan.Finish(options)

	decode := plan.DecodePath
	if decode == nil {
		t.Fatal("expected decode path")
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized decode blocks = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 1; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "key_range_candidate"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.TableSearchCursorAdvances, 0; got != want {
		t.Fatalf("table search cursor advances = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 0; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if got, want := decode.MissingKeys, []string{"ay"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
	}
	if got, want := decode.Samples[1].Reason, "key_not_in_block_range"; got != want {
		t.Fatalf("second block sample reason = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetQueryKeySearchAscendingGapAdvancesToNextBlock(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "4_2_ascendinggap")
	if err := writeTestMergesetPartWithItemBlocks(partPath, [][][]byte{
		{
			[]byte("aa"),
			[]byte("ab"),
		},
		{
			[]byte("az"),
			[]byte("ba"),
		},
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItemBlocks() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"ay"},
		BlockSampleLimit: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	fileDecode := report.Files[0].DecodePath
	if fileDecode == nil {
		t.Fatal("expected file decode path")
	}
	if got, want := fileDecode.MatchedKeys, []string(nil); !equalStrings(got, want) {
		t.Fatalf("file matched keys = %v, want %v", got, want)
	}
	if got, want := fileDecode.MissingKeys, []string{"ay"}; !equalStrings(got, want) {
		t.Fatalf("file missing keys = %v, want %v", got, want)
	}
	if got, want := fileDecode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("file optimized decode blocks = %d, want %d", got, want)
	}
	if got, want := fileDecode.CursorWindowCount, 2; got != want {
		t.Fatalf("file cursor window count = %d, want %d", got, want)
	}
	if got, want := len(fileDecode.CursorWindows), 2; got != want {
		t.Fatalf("file cursor windows = %d, want %d", got, want)
	}
	if got, want := fileDecode.CursorWindows[0].Reason, "key_range_candidate"; got != want {
		t.Fatalf("first file cursor window reason = %q, want %q", got, want)
	}
	if got, want := fileDecode.CursorWindows[0].FirstBlockIndex, 0; got != want {
		t.Fatalf("first file cursor window index = %d, want %d", got, want)
	}
	if got, want := fileDecode.CursorWindows[1].Reason, "cursor_advance_candidate"; got != want {
		t.Fatalf("second file cursor window reason = %q, want %q", got, want)
	}
	if got, want := fileDecode.CursorWindows[1].FirstBlockIndex, 1; got != want {
		t.Fatalf("second file cursor window index = %d, want %d", got, want)
	}
	if got, want := fileDecode.TableSearchCursorAdvances, 1; got != want {
		t.Fatalf("file table search cursor advances = %d, want %d", got, want)
	}
	if got, want := fileDecode.TableSearchHeapCandidates, 1; got != want {
		t.Fatalf("file table search heap candidates = %d, want %d", got, want)
	}
	if got, want := fileDecode.TableSearchOutputValues, 1; got != want {
		t.Fatalf("file table search output values = %d, want %d", got, want)
	}
	if got, want := len(fileDecode.CursorOutputSamples), 1; got != want {
		t.Fatalf("file cursor output samples = %d, want %d", got, want)
	}
	wantFileSample := DecodePathCursorOutput{
		Key:            "ay",
		Type:           "mergeset-item",
		OptimizedValue: "az",
		Matches:        false,
	}
	if got := fileDecode.CursorOutputSamples[0]; got != wantFileSample {
		t.Fatalf("file cursor output sample = %+v, want %+v", got, wantFileSample)
	}
	if got, want := len(fileDecode.Samples), 2; got != want {
		t.Fatalf("file block samples = %d, want %d", got, want)
	}
	if got, want := fileDecode.Samples[0].Reason, "key_range_candidate"; got != want {
		t.Fatalf("first file block sample reason = %q, want %q", got, want)
	}
	if got, want := fileDecode.Samples[1].Reason, "cursor_advance_candidate"; got != want {
		t.Fatalf("second file block sample reason = %q, want %q", got, want)
	}
	if !containsString(fileDecode.Recommendations, "advanced 1 local mergeset cursor step") {
		t.Fatalf("file recommendations = %v, want cursor advance recommendation", fileDecode.Recommendations)
	}

	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected file-set decode path")
	}
	if got, want := decode.MissingKeys, []string{"ay"}; !equalStrings(got, want) {
		t.Fatalf("file-set missing keys = %v, want %v", got, want)
	}
	if got, want := decode.TableSearchCursorAdvances, 1; got != want {
		t.Fatalf("file-set cursor advances = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("file-set cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 1; got != want {
		t.Fatalf("file-set cursor windows = %d, want %d", got, want)
	}
	window := decode.CursorWindows[0]
	if got, want := window.Key, "ay"; got != want {
		t.Fatalf("file-set cursor window key = %q, want %q", got, want)
	}
	if got, want := window.Reason, "item_search_exact_miss"; got != want {
		t.Fatalf("file-set cursor window reason = %q, want %q", got, want)
	}
	if got, want := window.Files, []string{partPath}; !equalStrings(got, want) {
		t.Fatalf("file-set cursor window files = %v, want %v", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("file-set cursor output samples = %d, want %d", got, want)
	}
	wantSample := DecodePathCursorOutput{
		Key:            "ay",
		Type:           "mergeset-table-search-item",
		File:           partPath,
		OptimizedValue: "az",
		Matches:        false,
	}
	if got := decode.CursorOutputSamples[0]; got != wantSample {
		t.Fatalf("file-set cursor output sample = %+v, want %+v", got, wantSample)
	}
	if !containsString(decode.Recommendations, "advanced 1 local mergeset part cursor step") {
		t.Fatalf("file-set recommendations = %v, want cursor advance recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "exact-miss TableSearch seek window") {
		t.Fatalf("file-set recommendations = %v, want exact-miss seek window recommendation", decode.Recommendations)
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
	if got, want := decode.CursorWindowCount, 3; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 3; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Key, "0"; got != want {
		t.Fatalf("first cursor window key = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "item_search_exact_miss"; got != want {
		t.Fatalf("first cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[0].Files, []string{partPath1}; !equalStrings(got, want) {
		t.Fatalf("first cursor window files = %v, want %v", got, want)
	}
	if decode.CursorWindows[0].RequiresMerge {
		t.Fatalf("first cursor window = %+v, want no merge for exact miss", decode.CursorWindows[0])
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
	if got, want := decode.CursorOutputSamples[0].File, partPath1; got != want {
		t.Fatalf("first cursor output sample file = %q, want %q", got, want)
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
	if got, want := decode.CursorOutputSamples[1].File, partPath1; got != want {
		t.Fatalf("second cursor output sample file = %q, want %q", got, want)
	}
	if got, want := decode.CursorOutputSamples[2].Key, "za"; got != want {
		t.Fatalf("third cursor output sample key = %q, want %q", got, want)
	}
	if !decode.CursorOutputSamples[2].Matches {
		t.Fatal("expected third cursor output sample to match exactly")
	}
	if got, want := decode.CursorOutputSamples[2].File, partPath2; got != want {
		t.Fatalf("third cursor output sample file = %q, want %q", got, want)
	}
	if got, want := len(decode.CursorFinalOutputSamples), 2; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	for i, want := range []struct {
		key   string
		value string
		file  string
	}{
		{key: "aa", value: "aa", file: partPath1},
		{key: "za", value: "za", file: partPath2},
	} {
		got := decode.CursorFinalOutputSamples[i]
		if got.Key != want.key {
			t.Fatalf("cursor final output sample[%d] key = %q, want %q", i, got.Key, want.key)
		}
		if got.OptimizedValue != want.value {
			t.Fatalf("cursor final output sample[%d] value = %q, want %q", i, got.OptimizedValue, want.value)
		}
		if got.File != want.file {
			t.Fatalf("cursor final output sample[%d] file = %q, want %q", i, got.File, want.file)
		}
		if got.RequiresDedup || got.RequiresMerge || got.MergeFiles != "" {
			t.Fatalf("cursor final output sample[%d] = %+v, want no dedup/merge", i, got)
		}
	}
	if got, want := decode.TableSearchSeekCalls, 6; got != want {
		t.Fatalf("table search seek calls = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapCandidates, 5; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapInserts, 5; got != want {
		t.Fatalf("table search heap inserts = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapPops, 3; got != want {
		t.Fatalf("table search heap pops = %d, want %d", got, want)
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
	if !containsString(decode.Recommendations, "exact-miss TableSearch seek window") {
		t.Fatalf("recommendations = %v, want exact-miss seek window recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetFileSetQueryKeySearchBeyondRangeHasNoExactMissWindow(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "2_1_above")
	if err := writeTestMergesetPartWithItems(partPath, [][]byte{
		[]byte("aa"),
		[]byte("ad"),
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems(%q) error = %v", partPath, err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"zz"},
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected file-set decode path")
	}
	if got, want := decode.MissingKeys, []string{"zz"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
	}
	if got, want := decode.TableSearchExactMisses, 1; got != want {
		t.Fatalf("table search exact misses = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 0; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 0; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 0; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 0; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorFinalOutputSamples), 0; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	if containsString(decode.Recommendations, "exact-miss TableSearch seek window") {
		t.Fatalf("recommendations = %v, want no exact-miss seek window recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "query item key(s) were not found") {
		t.Fatalf("recommendations = %v, want missing-key recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetFileSetQueryKeySearchDescending(t *testing.T) {
	dir := t.TempDir()
	partPath1 := filepath.Join(dir, "2_1_desca")
	if err := writeTestMergesetPartWithItems(partPath1, [][]byte{
		[]byte("aa"),
		[]byte("ad"),
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems(%q) error = %v", partPath1, err)
	}
	partPath2 := filepath.Join(dir, "2_1_descb")
	if err := writeTestMergesetPartWithItems(partPath2, [][]byte{
		[]byte("ac"),
		[]byte("za"),
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems(%q) error = %v", partPath2, err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"ae"},
		BlockSampleLimit: 4,
		CursorDescending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.Mode, "mergeset-file-set-item-search-descending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.MatchedKeys, []string(nil); !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got, want := decode.MissingKeys, []string{"ae"}; !equalStrings(got, want) {
		t.Fatalf("missing keys = %v, want %v", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchSeekCalls, 2; got != want {
		t.Fatalf("table search seek calls = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapCandidates, 2; got != want {
		t.Fatalf("table search heap candidates = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapInserts, 2; got != want {
		t.Fatalf("table search heap inserts = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapPops, 1; got != want {
		t.Fatalf("table search heap pops = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchOutputValues, 1; got != want {
		t.Fatalf("table search output values = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchExactMisses, 1; got != want {
		t.Fatalf("table search exact misses = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindowCount, 1; got != want {
		t.Fatalf("cursor window count = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorWindows), 1; got != want {
		t.Fatalf("cursor windows = %d, want %d", got, want)
	}
	if got, want := decode.CursorWindows[0].Reason, "item_search_exact_miss"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if got, want := decode.CursorWindows[0].Files, []string{partPath1}; !equalStrings(got, want) {
		t.Fatalf("cursor window files = %v, want %v", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	wantSample := DecodePathCursorOutput{
		Key:            "ae",
		Type:           "mergeset-table-search-item",
		File:           partPath1,
		OptimizedValue: "ad",
		Matches:        false,
	}
	if got := decode.CursorOutputSamples[0]; got != wantSample {
		t.Fatalf("cursor output sample = %+v, want %+v", got, wantSample)
	}
	if got, want := len(decode.CursorFinalOutputSamples), 0; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	if !containsString(decode.Recommendations, "exact-miss TableSearch seek window") {
		t.Fatalf("recommendations = %v, want exact-miss seek window recommendation", decode.Recommendations)
	}
}

func TestAnalyzeMergesetFileSetQueryKeySearchDescendingDuplicateExact(t *testing.T) {
	dir := t.TempDir()
	partPath1 := filepath.Join(dir, "2_1_descdup1")
	if err := writeTestMergesetPartWithItems(partPath1, [][]byte{
		[]byte("aa"),
		[]byte("ad"),
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems(%q) error = %v", partPath1, err)
	}
	partPath2 := filepath.Join(dir, "2_1_descdup2")
	if err := writeTestMergesetPartWithItems(partPath2, [][]byte{
		[]byte("ad"),
		[]byte("za"),
	}); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems(%q) error = %v", partPath2, err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:           FormatMergeset,
		QueryKeys:        []string{"ad"},
		BlockSampleLimit: 4,
		CursorDescending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.DecodePath
	if decode == nil {
		t.Fatal("expected top-level decode path")
	}
	if got, want := decode.Mode, "mergeset-file-set-item-search-descending"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.MatchedKeys, []string{"ad"}; !equalStrings(got, want) {
		t.Fatalf("matched keys = %v, want %v", got, want)
	}
	if got := len(decode.MissingKeys); got != 0 {
		t.Fatalf("missing keys = %v, want none", decode.MissingKeys)
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
	if got, want := decode.TableSearchHeapInserts, 2; got != want {
		t.Fatalf("table search heap inserts = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapPops, 1; got != want {
		t.Fatalf("table search heap pops = %d, want %d", got, want)
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
	if got, want := window.Key, "ad"; got != want {
		t.Fatalf("cursor window key = %q, want %q", got, want)
	}
	if !window.RequiresMerge {
		t.Fatal("expected cursor window to require merge")
	}
	if got, want := window.Files, []string{partPath1, partPath2}; !equalStrings(got, want) {
		t.Fatalf("cursor window files = %v, want %v", got, want)
	}
	if got, want := window.Reason, "item_search_exact_match"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	wantSample := DecodePathCursorOutput{
		Key:            "ad",
		Type:           "mergeset-table-search-item",
		File:           partPath1,
		MergeFiles:     newDecodePathStringList([]string{partPath1, partPath2}),
		OptimizedValue: "ad",
		Matches:        true,
		RequiresDedup:  true,
		RequiresMerge:  true,
	}
	if got := decode.CursorOutputSamples[0]; got != wantSample {
		t.Fatalf("cursor output sample = %+v, want %+v", got, wantSample)
	}
	if got, want := len(decode.CursorFinalOutputSamples), 1; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	finalOutput := decode.CursorFinalOutputSamples[0]
	if got, want := finalOutput.Key, "ad"; got != want {
		t.Fatalf("cursor final output key = %q, want %q", got, want)
	}
	if got, want := finalOutput.OptimizedValue, "ad"; got != want {
		t.Fatalf("cursor final output value = %q, want %q", got, want)
	}
	if got, want := finalOutput.File, partPath1; got != want {
		t.Fatalf("cursor final output file = %q, want %q", got, want)
	}
	if got, want := finalOutput.MergeFiles, newDecodePathStringList([]string{partPath1, partPath2}); got != want {
		t.Fatalf("cursor final output merge files = %q, want %q", got, want)
	}
	if !finalOutput.RequiresDedup || !finalOutput.RequiresMerge {
		t.Fatalf("cursor final output = %+v, want dedup and merge", finalOutput)
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
	if got, want := decode.TableSearchHeapInserts, 2; got != want {
		t.Fatalf("table search heap inserts = %d, want %d", got, want)
	}
	if got, want := decode.TableSearchHeapPops, 1; got != want {
		t.Fatalf("table search heap pops = %d, want %d", got, want)
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
	if got, want := window.Reason, "item_search_exact_match"; got != want {
		t.Fatalf("cursor window reason = %q, want %q", got, want)
	}
	if got, want := len(decode.CursorFinalOutputSamples), 1; got != want {
		t.Fatalf("cursor final output samples = %d, want %d", got, want)
	}
	finalOutput := decode.CursorFinalOutputSamples[0]
	if got, want := finalOutput.Key, "aa"; got != want {
		t.Fatalf("cursor final output key = %q, want %q", got, want)
	}
	if got, want := finalOutput.OptimizedValue, "aa"; got != want {
		t.Fatalf("cursor final output value = %q, want %q", got, want)
	}
	if got, want := finalOutput.MergeFiles, newDecodePathStringList([]string{partPath1, partPath2}); got != want {
		t.Fatalf("cursor final output merge files = %q, want %q", got, want)
	}
	if !finalOutput.RequiresDedup || !finalOutput.RequiresMerge {
		t.Fatalf("cursor final output = %+v, want dedup and merge", finalOutput)
	}
	if !containsString(decode.Recommendations, "merge/dedup") {
		t.Fatalf("recommendations = %v, want merge/dedup recommendation", decode.Recommendations)
	}
	if !containsString(decode.Recommendations, "deduplicated exact TableSearch results") {
		t.Fatalf("recommendations = %v, want final output recommendation", decode.Recommendations)
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

func TestAnalyzeMergesetOpenGeminiTSIIndexItems(t *testing.T) {
	items := [][]byte{
		encodeTestOpenGeminiTSIKeyToTSID("cpu", "host", "a", 42),
		encodeTestOpenGeminiTSITSIDToKey(42, "cpu", "host", "a"),
		encodeTestOpenGeminiTSITagToTSID("cpu", "host", "a", 42),
		encodeTestOpenGeminiTSITagValue("cpu", "host", "a"),
	}
	partPath := filepath.Join(t.TempDir(), "4_1_tsi")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   4,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_tsi_index_detected"], "true"; got != want {
		t.Fatalf("TSI index detected = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_key_tsid_mappings"], "1"; got != want {
		t.Fatalf("key->tsid mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tsid_key_mappings"], "1"; got != want {
		t.Fatalf("tsid->key mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tag_tsid_mappings"], "1"; got != want {
		t.Fatalf("tag->tsid mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tag_tsid_values"], "1"; got != want {
		t.Fatalf("tag->tsid values = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tag_value_mappings"], "1"; got != want {
		t.Fatalf("tag value mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_key_tsid_samples"], "cpu,host=a->42"; got != want {
		t.Fatalf("key->tsid samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tsid_key_samples"], "42:cpu,host=a"; got != want {
		t.Fatalf("tsid->key samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tag_tsid_samples"], "cpu:host=a->42"; got != want {
		t.Fatalf("tag->tsid samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tag_value_samples"], "cpu:host=a"; got != want {
		t.Fatalf("tag value samples = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["opengemini-tsi-index-key-tsid"], 1; got != want {
		t.Fatalf("key->tsid block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-tsi-index-tsid-key"], 1; got != want {
		t.Fatalf("tsid->key block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-tsi-index-tag-tsid"], 1; got != want {
		t.Fatalf("tag->tsid block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-tsi-index-tag-tsid-value"], 1; got != want {
		t.Fatalf("tag->tsid value block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-tsi-index-tag-value"], 1; got != want {
		t.Fatalf("tag value block count = %d, want %d", got, want)
	}
}

func TestAnalyzeMergesetOpenGeminiTSIIndexMultiTSIDTagRow(t *testing.T) {
	items := [][]byte{
		encodeTestOpenGeminiTSITagToTSIDs("cpu", "host", "a", 42, 43),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_multitsi")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_tsi_index_tag_tsid_mappings"], "1"; got != want {
		t.Fatalf("tag->tsid mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tag_tsid_values"], "2"; got != want {
		t.Fatalf("tag->tsid values = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tag_tsid_samples"], "cpu:host=a->[42,43]"; got != want {
		t.Fatalf("tag->tsid samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_invalid_items"], "0"; got != want {
		t.Fatalf("invalid TSI index items = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetOpenGeminiTSIIndexInvalidItem(t *testing.T) {
	items := [][]byte{
		append([]byte{opengeminiTSINSPrefixTagToTSIDs}, bytes.Repeat([]byte{0xff}, 12)...),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_badtsi")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_tsi_index_invalid_items"], "1"; got != want {
		t.Fatalf("invalid TSI index items = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["opengemini-tsi-index-invalid-item"], 1; got != want {
		t.Fatalf("invalid TSI index block count = %d, want %d", got, want)
	}
	if !containsString(file.Notices, "openGemini TSI index has 1 invalid") {
		t.Fatalf("notices = %v, want invalid TSI index notice", file.Notices)
	}
}

func TestAnalyzeMergesetOpenGeminiTSIIndexInvalidPrefixTwoNotCLVDictionary(t *testing.T) {
	items := [][]byte{
		{
			opengeminiTSINSPrefixTagToTSIDs,
			0xff, 0xff, 0xff, 0xff,
			opengeminiCLVSuffix,
			'x', 'x', 'x', 'x', 'x', 'x', 'x',
		},
	}
	partPath := filepath.Join(t.TempDir(), "1_1_badtsiclv")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_tsi_index_invalid_items"], "1"; got != want {
		t.Fatalf("invalid TSI index items = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["opengemini-tsi-index-invalid-item"], 1; got != want {
		t.Fatalf("invalid TSI index block count = %d, want %d", got, want)
	}
	if _, ok := file.Extra["opengemini_clv_text_index_detected"]; ok {
		t.Fatalf("unexpected CLV detection for malformed TSI item: %v", file.Extra)
	}
}

func TestAnalyzeMergesetOpenGeminiTSIIndexEscapedTagValues(t *testing.T) {
	tagKey := "ho" + string([]byte{opengeminiTSITagSeparator}) + "st"
	tagValue := "a" + string([]byte{opengeminiTSITagSeparator}) + "b" + string([]byte{opengeminiTSINSSeparator}) + "c" + string([]byte{opengeminiTSIEscape}) + "d"
	items := [][]byte{
		encodeTestOpenGeminiTSITagToTSID("cpu", tagKey, tagValue, 42),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_tsiescape")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_tsi_index_tag_tsid_mappings"], "1"; got != want {
		t.Fatalf("tag->tsid mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_invalid_items"], "0"; got != want {
		t.Fatalf("invalid TSI index items = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_tag_tsid_samples"], fmt.Sprintf("cpu:%s=%s->42", tagKey, tagValue); got != want {
		t.Fatalf("tag->tsid samples = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetOpenGeminiTSIKeyToTSIDWithTabDoesNotDetectCLV(t *testing.T) {
	tagValue := "a\tb"
	items := [][]byte{
		encodeTestOpenGeminiTSIKeyToTSID("cpu", "host", tagValue, 42),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_tsitab")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_tsi_index_key_tsid_mappings"], "1"; got != want {
		t.Fatalf("key->tsid mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_invalid_items"], "0"; got != want {
		t.Fatalf("invalid TSI index items = %q, want %q", got, want)
	}
	if _, ok := file.Extra["opengemini_clv_text_index_detected"]; ok {
		t.Fatalf("unexpected CLV detection for valid TSI item containing tab: %v", file.Extra)
	}
}

func TestAnalyzeMergesetOpenGeminiTSIIndexAllowsEmptyMeasurement(t *testing.T) {
	items := [][]byte{
		encodeTestOpenGeminiTSIKeyToTSID("", "", "", 42),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_emptytsi")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_tsi_index_key_tsid_mappings"], "1"; got != want {
		t.Fatalf("key->tsid mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_tsi_index_invalid_items"], "0"; got != want {
		t.Fatalf("invalid TSI index items = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetOpenGeminiCLVTextIndexItems(t *testing.T) {
	items := [][]byte{
		encodeTestOpenGeminiCLVDocument("get image ", []testCLVSIDPositions{
			{
				SID: 7,
				Rows: []testCLVPosition{
					{RowID: 100, Position: 2},
					{RowID: 120, Position: 4},
				},
			},
		}, []uint32{10, 11}),
		encodeTestOpenGeminiCLVTerm("get"),
		encodeTestOpenGeminiCLVDictionary(2, "get image "),
		encodeTestOpenGeminiCLVDictionaryVersion(2),
	}
	partPath := filepath.Join(t.TempDir(), "4_1_clvtext")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   4,
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_clv_text_index_detected"], "true"; got != want {
		t.Fatalf("CLV text index detected = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_document_rows"], "1"; got != want {
		t.Fatalf("CLV document rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_position_entries"], "2"; got != want {
		t.Fatalf("CLV position entries = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_sid_groups"], "1"; got != want {
		t.Fatalf("CLV sid groups = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_document_ids"], "2"; got != want {
		t.Fatalf("CLV document ids = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_terms"], "1"; got != want {
		t.Fatalf("CLV terms = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_dictionary_rows"], "1"; got != want {
		t.Fatalf("CLV dictionary rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_dictionary_versions"], "1"; got != want {
		t.Fatalf("CLV dictionary versions = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_invalid_items"], "0"; got != want {
		t.Fatalf("CLV invalid items = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_position_samples"], "get image  sid_groups=1 positions=2 ids=2"; got != want {
		t.Fatalf("CLV position samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_term_samples"], "get"; got != want {
		t.Fatalf("CLV term samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_dictionary_samples"], "v2:get image "; got != want {
		t.Fatalf("CLV dictionary samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_version_samples"], "2"; got != want {
		t.Fatalf("CLV version samples = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["opengemini-clv-text-document"], 1; got != want {
		t.Fatalf("CLV document block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-clv-text-position"], 2; got != want {
		t.Fatalf("CLV position block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-clv-text-term"], 1; got != want {
		t.Fatalf("CLV term block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-clv-text-dictionary"], 1; got != want {
		t.Fatalf("CLV dictionary block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-clv-text-dictionary-version"], 1; got != want {
		t.Fatalf("CLV dictionary version block count = %d, want %d", got, want)
	}
	if _, ok := file.Extra["opengemini_tsi_index_detected"]; ok {
		t.Fatalf("unexpected TSI detection for CLV text index items: %v", file.Extra)
	}
}

func TestAnalyzeMergesetOpenGeminiCLVTextIndexInvalidItem(t *testing.T) {
	items := [][]byte{
		{
			opengeminiCLVPrefixPos, 'b', opengeminiCLVSuffix,
			opengeminiCLVPrefixMeta,
			0xff,
			0, 3,
		},
	}
	partPath := filepath.Join(t.TempDir(), "1_1_badclv")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_clv_text_index_invalid_items"], "1"; got != want {
		t.Fatalf("invalid CLV text index items = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["opengemini-clv-text-invalid-item"], 1; got != want {
		t.Fatalf("invalid CLV text index block count = %d, want %d", got, want)
	}
	if !containsString(file.Notices, "openGemini CLV text index has 1 invalid") {
		t.Fatalf("notices = %v, want invalid CLV text index notice", file.Notices)
	}
	if _, ok := file.Extra["opengemini_tsi_index_detected"]; ok {
		t.Fatalf("unexpected TSI detection for malformed CLV document item: %v", file.Extra)
	}
}

func TestAnalyzeMergesetOpenGeminiCLVTextIndexInvalidDocumentWithoutSuffixDoesNotMarkTSIInvalid(t *testing.T) {
	items := [][]byte{
		encodeTestOpenGeminiCLVDocument("a", []testCLVSIDPositions{
			{
				SID: 1,
				Rows: []testCLVPosition{
					{RowID: 1, Position: 1},
				},
			},
		}, nil),
		append([]byte{opengeminiCLVPrefixPos}, bytes.Repeat([]byte{'z'}, 17)...),
	}
	partPath := filepath.Join(t.TempDir(), "2_1_clvbadctx")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_clv_text_index_document_rows"], "1"; got != want {
		t.Fatalf("CLV document rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_invalid_items"], "1"; got != want {
		t.Fatalf("invalid CLV text index items = %q, want %q", got, want)
	}
	if _, ok := file.Extra["opengemini_tsi_index_detected"]; ok {
		t.Fatalf("unexpected TSI detection for malformed CLV document without suffix: %v", file.Extra)
	}
}

func TestAnalyzeMergesetOpenGeminiCLVTextIndexEarlierInvalidDocumentCountsAfterDetection(t *testing.T) {
	items := [][]byte{
		append([]byte{opengeminiCLVPrefixPos}, bytes.Repeat([]byte{'a'}, 17)...),
		encodeTestOpenGeminiCLVDocument("z", []testCLVSIDPositions{
			{
				SID: 1,
				Rows: []testCLVPosition{
					{RowID: 1, Position: 1},
				},
			},
		}, nil),
	}
	partPath := filepath.Join(t.TempDir(), "2_1_clvbadorder")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_clv_text_index_document_rows"], "1"; got != want {
		t.Fatalf("CLV document rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_invalid_items"], "1"; got != want {
		t.Fatalf("invalid CLV text index items = %q, want %q", got, want)
	}
	if _, ok := file.Extra["opengemini_tsi_index_detected"]; ok {
		t.Fatalf("unexpected TSI detection for earlier malformed CLV document: %v", file.Extra)
	}
}

func TestAnalyzeMergesetOpenGeminiCLVTextIndexEarlierInvalidDocumentAcrossBlocks(t *testing.T) {
	blocks := [][][]byte{
		{
			append([]byte{opengeminiCLVPrefixPos}, bytes.Repeat([]byte{'a'}, 17)...),
		},
		{
			encodeTestOpenGeminiCLVDocument("z", []testCLVSIDPositions{
				{
					SID: 1,
					Rows: []testCLVPosition{
						{RowID: 1, Position: 1},
					},
				},
			}, nil),
		},
	}
	partPath := filepath.Join(t.TempDir(), "2_2_clvcross")
	if err := writeTestMergesetPartWithItemBlocks(partPath, blocks); err != nil {
		t.Fatalf("writeTestMergesetPartWithItemBlocks() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_clv_text_index_document_rows"], "1"; got != want {
		t.Fatalf("CLV document rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_invalid_items"], "1"; got != want {
		t.Fatalf("invalid CLV text index items = %q, want %q", got, want)
	}
	if _, ok := file.Extra["opengemini_tsi_index_detected"]; ok {
		t.Fatalf("unexpected TSI detection for cross-block malformed CLV document: %v", file.Extra)
	}
}

func TestAnalyzeMergesetOpenGeminiCLVTextIndexDocumentTokenBytes(t *testing.T) {
	items := [][]byte{
		encodeTestOpenGeminiCLVDocument("a"+string([]byte{1})+"b", []testCLVSIDPositions{
			{
				SID: 1,
				Rows: []testCLVPosition{
					{RowID: 1, Position: 1},
				},
			},
		}, nil),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_clvbytes")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_clv_text_index_document_rows"], "1"; got != want {
		t.Fatalf("CLV document rows = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_invalid_items"], "0"; got != want {
		t.Fatalf("invalid CLV text index items = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_position_samples"], "0x610162 sid_groups=1 positions=1 ids=0"; got != want {
		t.Fatalf("CLV position samples = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetOpenGeminiCLVTextIndexTermOnlyDoesNotMarkTSIInvalid(t *testing.T) {
	items := [][]byte{
		encodeTestOpenGeminiCLVTerm("very long token value"),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_clvterm")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if _, ok := file.Extra["opengemini_tsi_index_detected"]; ok {
		t.Fatalf("unexpected TSI detection for CLV term-only item: %v", file.Extra)
	}
	if _, ok := file.Extra["opengemini_clv_text_index_detected"]; ok {
		t.Fatalf("unexpected CLV detection without document or dictionary-version evidence: %v", file.Extra)
	}
}

func TestAnalyzeMergesetOpenGeminiCLVTextIndexDictionaryVersionOnly(t *testing.T) {
	items := [][]byte{
		encodeTestOpenGeminiCLVDictionaryVersion(7),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_clvversion")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_clv_text_index_detected"], "true"; got != want {
		t.Fatalf("CLV text index detected = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_dictionary_versions"], "1"; got != want {
		t.Fatalf("CLV dictionary versions = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_clv_text_index_version_samples"], "7"; got != want {
		t.Fatalf("CLV version samples = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetOpenGeminiFieldIndexItems(t *testing.T) {
	items := [][]byte{
		encodeTestOpenGeminiFieldIndexTSIDValue(42, "us-east"),
		encodeTestOpenGeminiFieldIndexFieldPID(42, "us-east", 1001),
		encodeTestOpenGeminiFieldIndexMeasurement("cpu", "region"),
	}
	partPath := filepath.Join(t.TempDir(), "3_1_fieldindex")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_field_index_detected"], "true"; got != want {
		t.Fatalf("field index detected = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_field_index_measurements"], "1"; got != want {
		t.Fatalf("field index measurements = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_field_index_tsid_field_values"], "1"; got != want {
		t.Fatalf("field value count = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_field_index_field_pid_mappings"], "1"; got != want {
		t.Fatalf("field pid count = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_field_index_measurement_samples"], "cpu:region"; got != want {
		t.Fatalf("measurement samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_field_index_value_samples"], "42:us-east"; got != want {
		t.Fatalf("field value samples = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_field_index_pid_samples"], "42:us-east->1001"; got != want {
		t.Fatalf("pid samples = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["opengemini-field-index-measurement"], 1; got != want {
		t.Fatalf("field index measurement blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-field-index-tsid-value"], 1; got != want {
		t.Fatalf("field index value blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["opengemini-field-index-field-pid"], 1; got != want {
		t.Fatalf("field index pid blocks = %d, want %d", got, want)
	}
}

func TestAnalyzeMergesetOpenGeminiFieldIndexInvalidItem(t *testing.T) {
	items := [][]byte{
		{opengeminiTSINSMstToFieldKey, 0, 4, 'c'},
	}
	partPath := filepath.Join(t.TempDir(), "1_1_badfieldindex")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_field_index_invalid_items"], "1"; got != want {
		t.Fatalf("invalid field index items = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["opengemini-field-index-invalid-item"], 1; got != want {
		t.Fatalf("invalid field index block count = %d, want %d", got, want)
	}
	if !containsString(file.Notices, "openGemini field index has 1 invalid") {
		t.Fatalf("notices = %v, want invalid field index notice", file.Notices)
	}
}

func TestAnalyzeMergesetOpenGeminiFieldIndexPIDMayContainSeparatorByte(t *testing.T) {
	pid := uint64(0x0202020202020202)
	items := [][]byte{
		encodeTestOpenGeminiFieldIndexFieldPID(42, "us-east", pid),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_fieldpid")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_field_index_field_pid_mappings"], "1"; got != want {
		t.Fatalf("field pid mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_field_index_invalid_items"], "0"; got != want {
		t.Fatalf("invalid field index items = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_field_index_pid_samples"], fmt.Sprintf("42:us-east->%d", pid); got != want {
		t.Fatalf("pid samples = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetOpenGeminiFieldIndexFieldValueMayContainSeparatorByte(t *testing.T) {
	fieldValue := "us" + string([]byte{opengeminiTSINSSeparator}) + "east"
	items := [][]byte{
		encodeTestOpenGeminiFieldIndexFieldPID(42, fieldValue, 1001),
	}
	partPath := filepath.Join(t.TempDir(), "1_1_fieldpidsep")
	if err := writeTestMergesetPartWithItems(partPath, items); err != nil {
		t.Fatalf("writeTestMergesetPartWithItems() error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:           FormatMergeset,
		KeySampleLimit:   1,
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["opengemini_field_index_field_pid_mappings"], "1"; got != want {
		t.Fatalf("field pid mappings = %q, want %q", got, want)
	}
	if got, want := file.Extra["opengemini_field_index_invalid_items"], "0"; got != want {
		t.Fatalf("invalid field index items = %q, want %q", got, want)
	}
	if got := file.BlocksByType["opengemini-field-index-ambiguous-field-pid"]; got != 0 {
		t.Fatalf("ambiguous field-pid count = %d, want 0", got)
	}
	if containsString(file.Notices, "separator bytes inside the field value") {
		t.Fatalf("notices = %v, want no separator-in-field-value notice", file.Notices)
	}
}

func writeTestMergesetPart(path string, metadata mergesetPartMetadata) error {
	return writeTestMergesetPartWithMarshalTypes(path, metadata, nil)
}

func writeTestMergesetPartWithItems(path string, items [][]byte) error {
	if len(items) == 0 {
		return fmt.Errorf("test mergeset items cannot be empty")
	}
	for i := 1; i < len(items); i++ {
		if bytes.Compare(items[i-1], items[i]) >= 0 {
			return fmt.Errorf("test mergeset item %d is not sorted: %x >= %x", i, items[i-1], items[i])
		}
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	metadata := mergesetPartMetadata{
		ItemsCount:  uint64(len(items)),
		BlocksCount: 1,
		FirstItem:   hex.EncodeToString(items[0]),
		LastItem:    hex.EncodeToString(items[len(items)-1]),
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetMetadataFile), data, 0o600); err != nil {
		return err
	}
	itemsData, lensData, err := encodeTestMergesetBlockPayload(items, nil, mergesetMarshalTypePlain)
	if err != nil {
		return err
	}
	header := mergesetBlockHeader{
		FirstItem:        append([]byte(nil), items[0]...),
		MarshalType:      mergesetMarshalTypePlain,
		ItemsCount:       uint32(len(items)),
		ItemsBlockOffset: 0,
		LensBlockOffset:  0,
		ItemsBlockSize:   uint32(len(itemsData)),
		LensBlockSize:    uint32(len(lensData)),
	}
	indexData, err := encodeTestMergesetIndexBlock([]mergesetBlockHeader{header})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetIndexFile), indexData, 0o600); err != nil {
		return err
	}
	metaindex, err := encodeTestMergesetMetaindexRows([]mergesetMetaindexRow{{
		FirstItem:         header.FirstItem,
		BlockHeadersCount: 1,
		IndexBlockOffset:  0,
		IndexBlockSize:    uint32(len(indexData)),
	}})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetMetaindexFile), metaindex, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetItemsFile), itemsData, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(path, mergesetLensFile), lensData, 0o600)
}

func writeTestMergesetPartWithItemBlocks(path string, blocks [][][]byte) error {
	if len(blocks) == 0 {
		return fmt.Errorf("test mergeset item blocks cannot be empty")
	}
	var items [][]byte
	for i, block := range blocks {
		if len(block) == 0 {
			return fmt.Errorf("test mergeset item block %d cannot be empty", i)
		}
		items = append(items, block...)
	}
	for i := 1; i < len(items); i++ {
		if bytes.Compare(items[i-1], items[i]) > 0 {
			return fmt.Errorf("test mergeset item %d is not sorted: %x >= %x", i, items[i-1], items[i])
		}
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	metadata := mergesetPartMetadata{
		ItemsCount:  uint64(len(items)),
		BlocksCount: uint64(len(blocks)),
		FirstItem:   hex.EncodeToString(items[0]),
		LastItem:    hex.EncodeToString(items[len(items)-1]),
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetMetadataFile), data, 0o600); err != nil {
		return err
	}

	headers := make([]mergesetBlockHeader, 0, len(blocks))
	var itemsData []byte
	var lensData []byte
	for _, block := range blocks {
		blockItemsData, blockLensData, err := encodeTestMergesetBlockPayload(block, nil, mergesetMarshalTypePlain)
		if err != nil {
			return err
		}
		headers = append(headers, mergesetBlockHeader{
			FirstItem:        append([]byte(nil), block[0]...),
			MarshalType:      mergesetMarshalTypePlain,
			ItemsCount:       uint32(len(block)),
			ItemsBlockOffset: uint64(len(itemsData)),
			LensBlockOffset:  uint64(len(lensData)),
			ItemsBlockSize:   uint32(len(blockItemsData)),
			LensBlockSize:    uint32(len(blockLensData)),
		})
		itemsData = append(itemsData, blockItemsData...)
		lensData = append(lensData, blockLensData...)
	}
	indexData, err := encodeTestMergesetIndexBlock(headers)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetIndexFile), indexData, 0o600); err != nil {
		return err
	}
	metaindex, err := encodeTestMergesetMetaindexRows([]mergesetMetaindexRow{{
		FirstItem:         headers[0].FirstItem,
		BlockHeadersCount: uint32(len(headers)),
		IndexBlockOffset:  0,
		IndexBlockSize:    uint32(len(indexData)),
	}})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetMetaindexFile), metaindex, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetItemsFile), itemsData, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(path, mergesetLensFile), lensData, 0o600)
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

type testCLVPosition struct {
	RowID    int64
	Position uint16
}

type testCLVSIDPositions struct {
	SID  uint64
	Rows []testCLVPosition
}

func encodeTestOpenGeminiCLVDocument(token string, groups []testCLVSIDPositions, ids []uint32) []byte {
	item := []byte{opengeminiCLVPrefixPos}
	item = append(item, token...)
	item = append(item, opengeminiCLVSuffix)

	sidLens := make([]uint16, 0, len(groups))
	for _, group := range groups {
		item = append(item, opengeminiCLVPrefixSID)
		item = appendTestBigEndianUint64(item, group.SID)
		sidLens = append(sidLens, uint16(len(group.Rows)))
		for _, row := range group.Rows {
			item = appendTestBigEndianInt64(item, row.RowID)
			item = appendTestBigEndianUint16(item, row.Position)
		}
	}
	if len(ids) > 0 {
		item = append(item, opengeminiCLVPrefixID)
		for _, id := range ids {
			item = appendTestBigEndianUint32(item, id)
		}
	}

	metaOffset := len(item)
	item = append(item, opengeminiCLVPrefixMeta)
	flag := byte(0)
	if len(groups) > 0 {
		flag |= opengeminiCLVPosFlag
		item = appendTestBigEndianUint16(item, uint16(len(groups)))
		for _, sidLen := range sidLens {
			item = appendTestBigEndianUint16(item, sidLen)
		}
	}
	if len(ids) > 0 {
		flag |= opengeminiCLVIDFlag
		item = appendTestBigEndianUint16(item, uint16(len(ids)))
	}
	item = append(item, flag)
	return appendTestBigEndianUint16(item, uint16(metaOffset))
}

func encodeTestOpenGeminiCLVTerm(term string) []byte {
	return append([]byte{opengeminiCLVPrefixTerm}, term...)
}

func encodeTestOpenGeminiCLVDictionary(version uint32, token string) []byte {
	item := []byte{opengeminiCLVPrefixDictionary}
	item = appendTestBigEndianUint32(item, version)
	item = append(item, opengeminiCLVSuffix)
	return append(item, token...)
}

func encodeTestOpenGeminiCLVDictionaryVersion(version uint32) []byte {
	item := []byte{opengeminiCLVPrefixVersion, opengeminiCLVSuffix}
	return appendTestBigEndianUint32(item, version)
}

func encodeTestOpenGeminiTSIKeyToTSID(measurement, tagKey, tagValue string, tsid uint64) []byte {
	item := []byte{opengeminiTSINSPrefixKeyToTSID}
	item = append(item, encodeTestOpenGeminiTSIIndexKey(measurement, tagKey, tagValue)...)
	item = append(item, opengeminiTSINSSeparator)
	return appendTestBigEndianUint64(item, tsid)
}

func encodeTestOpenGeminiTSITSIDToKey(tsid uint64, measurement, tagKey, tagValue string) []byte {
	item := []byte{opengeminiTSINSPrefixTSIDToKey}
	item = appendTestBigEndianUint64(item, tsid)
	return append(item, encodeTestOpenGeminiTSIIndexKey(measurement, tagKey, tagValue)...)
}

func encodeTestOpenGeminiTSIIndexKey(measurement, tagKey, tagValue string) []byte {
	tagCount := uint16(0)
	tagsSize := 0
	if tagKey != "" {
		tagCount = 1
		tagsSize = 4 + len(tagKey) + len(tagValue)
	}
	size := 4 + 2 + len(measurement) + 2 + tagsSize
	item := appendTestBigEndianUint32(nil, uint32(size))
	item = appendTestBigEndianUint16(item, uint16(len(measurement)))
	item = append(item, measurement...)
	item = appendTestBigEndianUint16(item, tagCount)
	if tagCount > 0 {
		item = appendTestBigEndianUint16(item, uint16(len(tagKey)))
		item = append(item, tagKey...)
		item = appendTestBigEndianUint16(item, uint16(len(tagValue)))
		item = append(item, tagValue...)
	}
	return item
}

func encodeTestOpenGeminiTSITagToTSID(measurement, tagKey, tagValue string, tsid uint64) []byte {
	return encodeTestOpenGeminiTSITagToTSIDs(measurement, tagKey, tagValue, tsid)
}

func encodeTestOpenGeminiTSITagToTSIDs(measurement, tagKey, tagValue string, tsids ...uint64) []byte {
	item := []byte{opengeminiTSINSPrefixTagToTSIDs}
	item = appendTestOpenGeminiTSITagValue(item, encodeTestOpenGeminiTSICompositeTagKey(measurement, tagKey))
	item = appendTestOpenGeminiTSITagValue(item, []byte(tagValue))
	for _, tsid := range tsids {
		item = appendTestBigEndianUint64(item, tsid)
	}
	return item
}

func encodeTestOpenGeminiTSITagValue(measurement, tagKey, tagValue string) []byte {
	item := []byte{opengeminiTSINSPrefixTagKeysToTagValues}
	item = appendTestOpenGeminiTSITagValue(item, encodeTestOpenGeminiTSICompositeTagKey(measurement, tagKey))
	return appendTestOpenGeminiTSITagValue(item, []byte(tagValue))
}

func encodeTestOpenGeminiTSICompositeTagKey(measurement, tagKey string) []byte {
	item := []byte{opengeminiTSICompositeTagKeyPrefix}
	item = appendTestUvarint(item, uint64(len(measurement)))
	item = append(item, measurement...)
	return append(item, tagKey...)
}

func appendTestOpenGeminiTSITagValue(dst, value []byte) []byte {
	for _, ch := range value {
		switch ch {
		case opengeminiTSIEscape:
			dst = append(dst, opengeminiTSIEscape, '0')
		case opengeminiTSITagSeparator:
			dst = append(dst, opengeminiTSIEscape, '1')
		case opengeminiTSINSSeparator:
			dst = append(dst, opengeminiTSIEscape, '2')
		default:
			dst = append(dst, ch)
		}
	}
	return append(dst, opengeminiTSITagSeparator)
}

func encodeTestOpenGeminiFieldIndexTSIDValue(tsid uint64, value string) []byte {
	item := []byte{opengeminiTSINSPrefixTSIDToField}
	item = appendTestBigEndianUint64(item, tsid)
	item = append(item, byte(len(value)))
	return append(item, value...)
}

func encodeTestOpenGeminiFieldIndexFieldPID(tsid uint64, value string, pid uint64) []byte {
	item := []byte{opengeminiTSINSFieldToPID}
	item = appendTestBigEndianUint64(item, tsid)
	item = append(item, value...)
	item = append(item, opengeminiTSINSSeparator)
	return appendTestBigEndianUint64(item, pid)
}

func encodeTestOpenGeminiFieldIndexMeasurement(measurement, field string) []byte {
	item := []byte{opengeminiTSINSMstToFieldKey}
	item = appendTestBigEndianUint16(item, uint16(len(measurement)))
	item = append(item, measurement...)
	item = appendTestBigEndianUint16(item, uint16(len(field)))
	return append(item, field...)
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

func appendTestUvarint(dst []byte, value uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	return append(dst, buf[:n]...)
}

func appendTestBigEndianUint16(dst []byte, value uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], value)
	return append(dst, buf[:]...)
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

func appendTestBigEndianInt64(dst []byte, value int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	return append(dst, buf[:]...)
}

func appendTestBigEndianUint64(dst []byte, value uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return append(dst, buf[:]...)
}
