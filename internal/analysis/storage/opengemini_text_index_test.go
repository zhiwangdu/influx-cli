package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeOpenGeminiTextIndexTriplet(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000001.tssp.content")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{
		{
			First:      "aa",
			Last:       "az",
			ItemsCount: 3,
			KeysOffset: 0,
			KeysUnpack: 16,
			KeysPack:   10,
			KeysSize:   8,
			PostOffset: 10,
			PostUnpack: 20,
			PostPack:   12,
			PostSize:   12,
		},
		{
			First:      "ba",
			Last:       "bz",
			ItemsCount: 2,
			KeysOffset: 22,
			KeysUnpack: 8,
			KeysPack:   4,
			KeysSize:   4,
			PostOffset: 26,
			PostUnpack: 8,
			PostPack:   4,
			PostSize:   4,
		},
	}, 30)

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexPartSuffix}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatOpenGeminiText; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.KeySamples, []string{"field:content"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if got, want := file.BlockCount, 2; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["text-index-part"], 1; got != want {
		t.Fatalf("text-index-part count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["text-index-block-header"], 2; got != want {
		t.Fatalf("text-index-block-header count = %d, want %d", got, want)
	}
	if file.SecondaryIndex == nil {
		t.Fatalf("secondary index summary is nil")
	}
	if got, want := file.SecondaryIndex.Field, "content"; got != want {
		t.Fatalf("secondary field = %q, want %q", got, want)
	}
	if got, want := file.SecondaryIndex.PartCount, int64(1); got != want {
		t.Fatalf("part count = %d, want %d", got, want)
	}
	if got, want := file.SecondaryIndex.BlockCount, int64(2); got != want {
		t.Fatalf("secondary block count = %d, want %d", got, want)
	}
	if got, want := file.SecondaryIndex.ItemCount, int64(5); got != want {
		t.Fatalf("item count = %d, want %d", got, want)
	}
	if got, want := file.SecondaryIndex.PayloadSizeBytes, int64(30); got != want {
		t.Fatalf("payload size = %d, want %d", got, want)
	}
	if got, want := file.SecondaryIndex.ValidBytes, int64(30); got != want {
		t.Fatalf("valid payload bytes = %d, want %d", got, want)
	}
	if got := file.SecondaryIndex.DataOutOfBoundsBlocks; got != 0 {
		t.Fatalf("data out-of-bounds blocks = %d, want 0", got)
	}
	for key, want := range map[string]string{
		"keys_payload_size_bytes":  "14",
		"post_payload_size_bytes":  "16",
		"payload_size_bytes":       "30",
		"valid_payload_size_bytes": "30",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if got, want := len(file.Blocks), 2; got != want {
		t.Fatalf("sample count = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].SizeBytes, uint32(22); got != want {
		t.Fatalf("first sample size = %d, want %d", got, want)
	}
	if !strings.Contains(file.Blocks[0].Key, "6161..617a") {
		t.Fatalf("first sample key = %q, want hex first/last item", file.Blocks[0].Key)
	}
}

func TestAnalyzeOpenGeminiTextIndexExplicitHeadComponent(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000002.tssp.message")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{{
		First:      "ma",
		Last:       "mz",
		ItemsCount: 1,
		KeysOffset: 0,
		KeysUnpack: 4,
		KeysPack:   3,
		KeysSize:   2,
		PostOffset: 3,
		PostUnpack: 4,
		PostPack:   3,
		PostSize:   2,
	}}, 6)

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexHeadSuffix}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["input_component"], "head"; got != want {
		t.Fatalf("input component = %q, want %q", got, want)
	}
	if got, want := file.SecondaryIndex.Field, "message"; got != want {
		t.Fatalf("secondary field = %q, want %q", got, want)
	}
}

func TestAnalyzeOpenGeminiTextIndexExplicitDataComponent(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000003.tssp.message")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{{
		First:      "ma",
		Last:       "mz",
		ItemsCount: 1,
		KeysOffset: 0,
		KeysUnpack: 4,
		KeysPack:   3,
		KeysSize:   2,
		PostOffset: 3,
		PostUnpack: 4,
		PostPack:   3,
		PostSize:   2,
	}}, 6)

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexDataSuffix}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["input_component"], "data"; got != want {
		t.Fatalf("input component = %q, want %q", got, want)
	}
	if got, want := file.SecondaryIndex.BlockCount, int64(1); got != want {
		t.Fatalf("secondary block count = %d, want %d", got, want)
	}
}

func TestAnalyzeOpenGeminiTextIndexMissingHeadFile(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000004.tssp.content")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{{
		First:      "aa",
		Last:       "az",
		ItemsCount: 1,
		KeysOffset: 0,
		KeysUnpack: 4,
		KeysPack:   2,
		KeysSize:   2,
		PostOffset: 2,
		PostUnpack: 4,
		PostPack:   2,
		PostSize:   2,
	}}, 4)
	if err := os.Remove(base + opengeminiTextIndexHeadSuffix); err != nil {
		t.Fatalf("Remove(.bh) error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexPartSuffix}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.BlockCount, 1; got != want {
		t.Fatalf("declared block count = %d, want %d", got, want)
	}
	if got := file.BlocksByType["text-index-block-header"]; got != 0 {
		t.Fatalf("decoded block header count = %d, want 0", got)
	}
	if got, want := file.Extra["head_file_present"], "false"; got != want {
		t.Fatalf("head_file_present = %q, want %q", got, want)
	}
	if !containsOpenGeminiTextNotice(file.Notices, "sibling .bh file is missing") {
		t.Fatalf("notices %v do not contain missing .bh notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexMissingDataFile(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000005.tssp.content")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{{
		First:      "aa",
		Last:       "az",
		ItemsCount: 1,
		KeysOffset: 0,
		KeysUnpack: 4,
		KeysPack:   2,
		KeysSize:   2,
		PostOffset: 2,
		PostUnpack: 4,
		PostPack:   2,
		PostSize:   2,
	}}, 4)
	if err := os.Remove(base + opengeminiTextIndexDataSuffix); err != nil {
		t.Fatalf("Remove(.pos) error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexPartSuffix}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.BlocksByType["text-index-block-header"], 1; got != want {
		t.Fatalf("decoded block header count = %d, want %d", got, want)
	}
	if got := file.SecondaryIndex.DataOutOfBoundsBlocks; got != 0 {
		t.Fatalf("data out-of-bounds blocks = %d, want 0 when .pos is absent", got)
	}
	if got := file.SecondaryIndex.ValidBytes; got != 0 {
		t.Fatalf("valid payload bytes = %d, want 0 when .pos is absent", got)
	}
	if got, want := file.Extra["data_file_present"], "false"; got != want {
		t.Fatalf("data_file_present = %q, want %q", got, want)
	}
	if got, want := file.Extra["valid_payload_size_bytes"], "0"; got != want {
		t.Fatalf("valid payload bytes extra = %q, want %q", got, want)
	}
	if !containsOpenGeminiTextNotice(file.Notices, "sibling .pos file is missing") {
		t.Fatalf("notices %v do not contain missing .pos notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexHeaderOutOfBounds(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000006.tssp.content")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{{
		First:      "aa",
		Last:       "az",
		ItemsCount: 1,
		KeysOffset: 0,
		KeysUnpack: 4,
		KeysPack:   2,
		KeysSize:   2,
		PostOffset: 2,
		PostUnpack: 4,
		PostPack:   2,
		PostSize:   2,
	}}, 4)
	part := encodeTestOpenGeminiTextPartHeader(t, "aa", "az", 1, 0, 999)
	if err := os.WriteFile(base+opengeminiTextIndexPartSuffix, part, 0o644); err != nil {
		t.Fatalf("WriteFile(.ph) error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexPartSuffix}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.SecondaryIndex.HeaderOutOfBoundsParts, 1; got != want {
		t.Fatalf("header out-of-bounds parts = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["text-index-header-out-of-bounds"], 1; got != want {
		t.Fatalf("header out-of-bounds block type count = %d, want %d", got, want)
	}
	if got := file.BlocksByType["text-index-block-header"]; got != 0 {
		t.Fatalf("decoded block header count = %d, want 0", got)
	}
	if !containsOpenGeminiTextNotice(file.Notices, "outside the .bh file") {
		t.Fatalf("notices %v do not contain .bh range notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexSegmentRangeOverflow(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000007.tssp.content")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{{
		First:      "aa",
		Last:       "az",
		ItemsCount: 1,
		KeysOffset: 0,
		KeysUnpack: 4,
		KeysPack:   2,
		KeysSize:   2,
		PostOffset: 2,
		PostUnpack: 4,
		PostPack:   2,
		PostSize:   2,
	}}, 4)
	partPath := base + opengeminiTextIndexPartSuffix
	part, err := os.ReadFile(partPath)
	if err != nil {
		t.Fatalf("ReadFile(.ph) error = %v", err)
	}
	binary.BigEndian.PutUint32(part[36:40], uint32(opengeminiTextIndexSegmentSize+1))
	if err := os.WriteFile(partPath, part, 0o644); err != nil {
		t.Fatalf("WriteFile(.ph) error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.SecondaryIndex.SegmentRangeOverflows, 1; got != want {
		t.Fatalf("segment range overflows = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["text-index-segment-range-overflow"], 1; got != want {
		t.Fatalf("segment range overflow block type count = %d, want %d", got, want)
	}
	if !containsOpenGeminiTextNotice(file.Notices, "segment range count") {
		t.Fatalf("notices %v do not contain segment range notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexPartRangeIssuesWithoutHead(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000008.tssp.content")
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	part := append([]byte(nil), encodeTestOpenGeminiTextPartHeader(t, "aa", "zz", 1, 0, 0)...)
	part = append(part, encodeTestOpenGeminiTextPartHeader(t, "ab", "ac", 1, 0, 0)...)
	part = append(part, encodeTestOpenGeminiTextPartHeader(t, "zy", "zx", 1, 0, 0)...)
	if err := os.WriteFile(base+opengeminiTextIndexPartSuffix, part, 0o644); err != nil {
		t.Fatalf("WriteFile(.ph) error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexPartSuffix}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["head_file_present"], "false"; got != want {
		t.Fatalf("head file present = %q, want %q", got, want)
	}
	if got, want := file.Extra["invalid_part_ranges"], "1"; got != want {
		t.Fatalf("invalid part ranges = %q, want %q", got, want)
	}
	if got, want := file.Extra["unsorted_part_ranges"], "1"; got != want {
		t.Fatalf("unsorted part ranges = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["text-index-invalid-part-range"], 1; got != want {
		t.Fatalf("invalid part range count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["text-index-unsorted-part-range"], 1; got != want {
		t.Fatalf("unsorted part range count = %d, want %d", got, want)
	}
	for _, want := range []string{"part header(s) with first_item greater than last_item", "adjacent part header range"} {
		if !containsOpenGeminiTextNotice(file.Notices, want) {
			t.Fatalf("notices %v do not contain %q", file.Notices, want)
		}
	}
	if !containsOpenGeminiTextNotice(file.Notices, "sibling .bh file is missing") {
		t.Fatalf("notices %v do not contain missing .bh notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexDetectsOutOfBoundsRanges(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000009.tssp.content")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{{
		First:      "aa",
		Last:       "az",
		ItemsCount: 1,
		KeysOffset: 0,
		KeysUnpack: 4,
		KeysPack:   8,
		KeysSize:   8,
		PostOffset: 30,
		PostUnpack: 2,
		PostPack:   4,
		PostSize:   4,
	}}, 10)

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexPartSuffix}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.SecondaryIndex.DataOutOfBoundsBlocks, 1; got != want {
		t.Fatalf("data out-of-bounds blocks = %d, want %d", got, want)
	}
	if got := file.SecondaryIndex.ValidBytes; got != 0 {
		t.Fatalf("valid payload bytes = %d, want 0 for out-of-bounds payload", got)
	}
	if got, want := file.Extra["valid_payload_size_bytes"], "0"; got != want {
		t.Fatalf("valid payload bytes extra = %q, want %q", got, want)
	}
	if got, want := file.SecondaryIndex.InvalidOffsetBlocks, 1; got != want {
		t.Fatalf("invalid offset blocks = %d, want %d", got, want)
	}
	if got, want := file.SecondaryIndex.InvalidSizeBlocks, 1; got != want {
		t.Fatalf("invalid size blocks = %d, want %d", got, want)
	}
	for _, want := range []string{"data range", "posting-list offset", "unpacked key/post sizes"} {
		if !containsOpenGeminiTextNotice(file.Notices, want) {
			t.Fatalf("notices %v do not contain %q", file.Notices, want)
		}
	}
}

func TestAnalyzeOpenGeminiTextIndexBlockRangeIssues(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000010.tssp.content")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{
		{
			First:      "aa",
			Last:       "zz",
			ItemsCount: 1,
			KeysOffset: 0,
			KeysUnpack: 4,
			KeysPack:   2,
			KeysSize:   2,
			PostOffset: 2,
			PostUnpack: 4,
			PostPack:   2,
			PostSize:   2,
		},
		{
			First:      "ab",
			Last:       "ac",
			ItemsCount: 1,
			KeysOffset: 4,
			KeysUnpack: 4,
			KeysPack:   2,
			KeysSize:   2,
			PostOffset: 6,
			PostUnpack: 4,
			PostPack:   2,
			PostSize:   2,
		},
		{
			First:      "zy",
			Last:       "zx",
			ItemsCount: 1,
			KeysOffset: 8,
			KeysUnpack: 4,
			KeysPack:   2,
			KeysSize:   2,
			PostOffset: 10,
			PostUnpack: 4,
			PostPack:   2,
			PostSize:   2,
		},
	}, 12)

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexPartSuffix}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["invalid_block_ranges"], "1"; got != want {
		t.Fatalf("invalid block ranges = %q, want %q", got, want)
	}
	if got, want := file.Extra["unsorted_block_ranges"], "1"; got != want {
		t.Fatalf("unsorted block ranges = %q, want %q", got, want)
	}
	if got, want := file.Extra["part_boundary_mismatches"], "0"; got != want {
		t.Fatalf("part boundary mismatches = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["text-index-invalid-block-range"], 1; got != want {
		t.Fatalf("invalid block range count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["text-index-unsorted-block-range"], 1; got != want {
		t.Fatalf("unsorted block range count = %d, want %d", got, want)
	}
	if got, want := file.Extra["valid_payload_size_bytes"], "12"; got != want {
		t.Fatalf("valid payload bytes = %q, want %q", got, want)
	}
	for _, want := range []string{"first_item greater than last_item", "not sorted within a part"} {
		if !containsOpenGeminiTextNotice(file.Notices, want) {
			t.Fatalf("notices %v do not contain %q", file.Notices, want)
		}
	}
}

func TestAnalyzeOpenGeminiTextIndexPartBoundaryMismatch(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000011.tssp.content")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{
		{
			First:      "aa",
			Last:       "az",
			ItemsCount: 1,
			KeysOffset: 0,
			KeysUnpack: 4,
			KeysPack:   2,
			KeysSize:   2,
			PostOffset: 2,
			PostUnpack: 4,
			PostPack:   2,
			PostSize:   2,
		},
		{
			First:      "ba",
			Last:       "bz",
			ItemsCount: 1,
			KeysOffset: 4,
			KeysUnpack: 4,
			KeysPack:   2,
			KeysSize:   2,
			PostOffset: 6,
			PostUnpack: 4,
			PostPack:   2,
			PostSize:   2,
		},
	}, 8)
	headInfo, err := os.Stat(base + opengeminiTextIndexHeadSuffix)
	if err != nil {
		t.Fatalf("Stat(.bh) error = %v", err)
	}
	part := encodeTestOpenGeminiTextPartHeader(t, "aa", "by", 2, 0, uint32(headInfo.Size()))
	if err := os.WriteFile(base+opengeminiTextIndexPartSuffix, part, 0o644); err != nil {
		t.Fatalf("WriteFile(.ph) error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{base + opengeminiTextIndexPartSuffix}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["part_boundary_mismatches"], "1"; got != want {
		t.Fatalf("part boundary mismatches = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["text-index-part-boundary-mismatch"], 1; got != want {
		t.Fatalf("part boundary mismatch block type count = %d, want %d", got, want)
	}
	if got, want := file.Extra["decoded_block_headers"], "2"; got != want {
		t.Fatalf("decoded block headers = %q, want %q", got, want)
	}
	if got, want := file.Extra["valid_payload_size_bytes"], "8"; got != want {
		t.Fatalf("valid payload bytes = %q, want %q", got, want)
	}
	if !containsOpenGeminiTextNotice(file.Notices, "part header boundary value") {
		t.Fatalf("notices %v do not contain part boundary mismatch notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexDirectoryExpansionUsesPartHeaderOnly(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000012.tssp.content")
	writeTestOpenGeminiTextIndex(t, base, []testOpenGeminiTextBlockHeader{{
		First:      "aa",
		Last:       "az",
		ItemsCount: 1,
		KeysOffset: 0,
		KeysUnpack: 2,
		KeysPack:   2,
		KeysSize:   2,
		PostOffset: 2,
		PostUnpack: 2,
		PostPack:   2,
		PostSize:   2,
	}}, 4)

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format: FormatAuto,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d; files=%v notices=%v", got, want, report.Files, report.Notices)
	}
	if got, want := report.Files[0].Path, base+opengeminiTextIndexPartSuffix; got != want {
		t.Fatalf("analyzed path = %q, want %q", got, want)
	}
}

type testOpenGeminiTextBlockHeader struct {
	First      string
	Last       string
	Marshal    uint8
	ItemsCount uint32
	KeysOffset uint64
	KeysUnpack uint32
	KeysPack   uint32
	KeysSize   uint32
	PostOffset uint64
	PostUnpack uint32
	PostPack   uint32
	PostSize   uint32
}

func writeTestOpenGeminiTextIndex(t *testing.T, base string, blocks []testOpenGeminiTextBlockHeader, dataSize int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	dataPath := base + opengeminiTextIndexDataSuffix
	headPath := base + opengeminiTextIndexHeadSuffix
	partPath := base + opengeminiTextIndexPartSuffix
	if err := os.WriteFile(dataPath, make([]byte, dataSize), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", dataPath, err)
	}
	head := make([]byte, 0)
	for _, block := range blocks {
		head = encodeTestOpenGeminiTextBlockHeader(head, block)
	}
	if err := os.WriteFile(headPath, head, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", headPath, err)
	}
	part := encodeTestOpenGeminiTextPartHeader(t, blocks[0].First, blocks[len(blocks)-1].Last, uint32(len(blocks)), 0, uint32(len(head)))
	if err := os.WriteFile(partPath, part, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", partPath, err)
	}
}

func encodeTestOpenGeminiTextBlockHeader(dst []byte, header testOpenGeminiTextBlockHeader) []byte {
	dst = appendTestVarBytes(dst, []byte(header.First))
	dst = appendTestVarBytes(dst, []byte(header.Last))
	dst = append(dst, header.Marshal)
	dst = appendTestUint32(dst, header.ItemsCount)
	dst = appendTestUint64(dst, header.KeysOffset)
	dst = appendTestUint32(dst, header.KeysUnpack)
	dst = appendTestUint32(dst, header.KeysPack)
	dst = appendTestUint32(dst, header.KeysSize)
	dst = appendTestUint64(dst, header.PostOffset)
	dst = appendTestUint32(dst, header.PostUnpack)
	dst = appendTestUint32(dst, header.PostPack)
	dst = appendTestUint32(dst, header.PostSize)
	return dst
}

func encodeTestOpenGeminiTextPartHeader(t *testing.T, first, last string, blockCount uint32, blockOffset uint64, blockSize uint32) []byte {
	t.Helper()
	data := make([]byte, 0, opengeminiTextIndexPartSize)
	data = append(data, byte(len(first)))
	data = append(data, byte(len(last)))
	data = appendFixedTestOpenGeminiTextItem(data, []byte(first))
	data = appendFixedTestOpenGeminiTextItem(data, []byte(last))
	data = appendTestUint16(data, 0)
	data = appendTestUint32(data, blockCount)
	data = appendTestUint64(data, blockOffset)
	data = appendTestUint32(data, blockSize)
	data = appendTestUint32(data, opengeminiTextIndexSegmentSize)
	for i := 0; i < opengeminiTextIndexSegmentSize; i++ {
		data = appendTestUint32(data, uint32(i))
	}
	if got, want := len(data), opengeminiTextIndexPartSize; got != want {
		t.Fatalf("part header size = %d, want %d", got, want)
	}
	return data
}

func appendFixedTestOpenGeminiTextItem(dst []byte, value []byte) []byte {
	if len(value) >= opengeminiTextIndexItemPrefix {
		return append(dst, value[:opengeminiTextIndexItemPrefix]...)
	}
	dst = append(dst, value...)
	for len(value) < opengeminiTextIndexItemPrefix {
		dst = append(dst, 0)
		value = append(value, 0)
	}
	return dst
}

func appendTestVarBytes(dst []byte, value []byte) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], uint64(len(value)))
	dst = append(dst, buf[:n]...)
	return append(dst, value...)
}

func appendTestUint16(dst []byte, value uint16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], value)
	return append(dst, buf[:]...)
}

func containsOpenGeminiTextNotice(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}
