package storage

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type testOpenGeminiPKColumn struct {
	Name string
	Type uint32
}

type testOpenGeminiPKMetaBlock struct {
	StartBlockID uint64
	EndBlockID   uint64
	DataOffset   uint32
	DataLength   uint32
	ColumnOffset []uint32
	CorruptCRC   bool
}

func TestAnalyzeOpenGeminiPKMeta(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, opengeminiPKMetaFileName)
	columns := []testOpenGeminiPKColumn{
		{Name: "host", Type: 6},
		{Name: "time", Type: 1},
		{Name: "value", Type: 3},
	}
	blocks := []testOpenGeminiPKMetaBlock{
		{StartBlockID: 10, EndBlockID: 12, DataOffset: 0, DataLength: 80, ColumnOffset: []uint32{0, 16, 48}},
		{StartBlockID: 13, EndBlockID: 13, DataOffset: 80, DataLength: 40, ColumnOffset: []uint32{0, 8, 24}},
	}
	if err := os.WriteFile(path, encodeTestOpenGeminiPKMeta(columns, -1, blocks), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, opengeminiPKDataFileName), make([]byte, 120), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatOpenGeminiPKMeta; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.KeyCount, 3; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.BlockCount, 2; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["primary-key-meta-block"], 2; got != want {
		t.Fatalf("primary key meta blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["primary-key-schema-column"], 3; got != want {
		t.Fatalf("primary key schema columns = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{"host:tag", "time:integer", "value:float"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if file.PrimaryKey == nil {
		t.Fatal("primary key summary is nil")
	}
	if got, want := file.PrimaryKey.RowCount, uint64(4); got != want {
		t.Fatalf("row count = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.MinBlockID, uint64(10); got != want {
		t.Fatalf("min block id = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.MaxBlockID, uint64(13); got != want {
		t.Fatalf("max block id = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.TimeClusterLocation, -1; got != want {
		t.Fatalf("time cluster location = %d, want %d", got, want)
	}
	if !file.PrimaryKey.DataFilePresent {
		t.Fatal("data file present = false, want true")
	}
	if got, want := file.PrimaryKey.DataFileSizeBytes, int64(120); got != want {
		t.Fatalf("data file size = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.DataSizeBytes, int64(120); got != want {
		t.Fatalf("referenced data size = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.ColumnOffsetCount, 6; got != want {
		t.Fatalf("column offset count = %d, want %d", got, want)
	}
	if !file.PrimaryKey.BlockIDRangeSet {
		t.Fatal("block id range set = false, want true")
	}
	if got := file.PrimaryKey.CRCMismatches; got != 0 {
		t.Fatalf("crc mismatches = %d, want 0", got)
	}
	if got, want := file.Blocks[0].Key, "block-id:10-12"; got != want {
		t.Fatalf("block sample key = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].Offset, int64(0); got != want {
		t.Fatalf("block sample offset = %d, want %d", got, want)
	}
	for key, want := range map[string]string{
		"layout":              "opengemini-detached-primary-meta",
		"sidecar":             opengeminiPKMetaFileName,
		"magic":               opengeminiPKMagic,
		"version":             "0",
		"schema_column_count": "3",
		"meta_block_count":    "2",
		"row_count":           "4",
		"data_size_bytes":     "120",
		"column_offset_count": "6",
		"crc_mismatch_count":  "0",
		"data_file_present":   "true",
		"block_id_range_set":  "true",
		"local_only":          "true",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
	}
}

func TestAnalyzeOpenGeminiPKMetaInvalidFirstSpanSeedsRangeFromNextValidBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, opengeminiPKMetaFileName)
	if err := os.WriteFile(path, encodeTestOpenGeminiPKMeta(
		[]testOpenGeminiPKColumn{{Name: "time", Type: 1}},
		-1,
		[]testOpenGeminiPKMetaBlock{
			{StartBlockID: 10, EndBlockID: 9, DataOffset: 0, DataLength: 8, ColumnOffset: []uint32{0}},
			{StartBlockID: 20, EndBlockID: 21, DataOffset: 8, DataLength: 16, ColumnOffset: []uint32{0}},
		},
	), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, opengeminiPKDataFileName), make([]byte, 24), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatOpenGeminiPKMeta,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.BlocksByType["primary-key-invalid-block-id-span"], 1; got != want {
		t.Fatalf("invalid block-id span count = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.MinBlockID, uint64(20); got != want {
		t.Fatalf("min block id = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.MaxBlockID, uint64(21); got != want {
		t.Fatalf("max block id = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.RowCount, uint64(2); got != want {
		t.Fatalf("row count = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.DataSizeBytes, int64(24); got != want {
		t.Fatalf("referenced data size = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.ColumnOffsetCount, 2; got != want {
		t.Fatalf("column offset count = %d, want %d", got, want)
	}
	for key, want := range map[string]string{
		"invalid_block_id_span_blocks": "1",
		"min_block_id":                 "20",
		"max_block_id":                 "21",
		"row_count":                    "2",
		"data_size_bytes":              "24",
		"column_offset_count":          "2",
		"block_id_range_set":           "true",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
	if !containsOpenGeminiPKNotice(file.Notices, "end_block_id before start_block_id") {
		t.Fatalf("notices = %v, want invalid block-id span notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiPKMetaAllInvalidSpansLeaveRangeUnset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, opengeminiPKMetaFileName)
	if err := os.WriteFile(path, encodeTestOpenGeminiPKMeta(
		[]testOpenGeminiPKColumn{{Name: "time", Type: 1}},
		-1,
		[]testOpenGeminiPKMetaBlock{
			{StartBlockID: 10, EndBlockID: 9, DataOffset: 0, DataLength: 8, ColumnOffset: []uint32{0}},
		},
	), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatOpenGeminiPKMeta,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if file.PrimaryKey.BlockIDRangeSet {
		t.Fatal("block id range set = true, want false")
	}
	if _, ok := file.Extra["min_block_id"]; ok {
		t.Fatalf("extra[min_block_id] present for invalid-only spans: %q", file.Extra["min_block_id"])
	}
	if _, ok := file.Extra["max_block_id"]; ok {
		t.Fatalf("extra[max_block_id] present for invalid-only spans: %q", file.Extra["max_block_id"])
	}
	if got, want := file.Extra["block_id_range_set"], "false"; got != want {
		t.Fatalf("extra[block_id_range_set] = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["primary-key-invalid-block-id-span"], 1; got != want {
		t.Fatalf("invalid block-id span count = %d, want %d", got, want)
	}
}

func TestAnalyzeOpenGeminiPKMetaReportsTrailingBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, opengeminiPKMetaFileName)
	data := encodeTestOpenGeminiPKMeta(
		[]testOpenGeminiPKColumn{{Name: "time", Type: 1}},
		-1,
		[]testOpenGeminiPKMetaBlock{{StartBlockID: 1, EndBlockID: 1, DataOffset: 0, DataLength: 8, ColumnOffset: []uint32{0}}},
	)
	data = append(data, 0xff)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatOpenGeminiPKMeta,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.PrimaryKey.TrailingMetaBytes, int64(1); got != want {
		t.Fatalf("trailing meta bytes = %d, want %d", got, want)
	}
	if got, want := file.Extra["trailing_meta_bytes"], "1"; got != want {
		t.Fatalf("extra[trailing_meta_bytes] = %q, want %q", got, want)
	}
	if !containsOpenGeminiPKNotice(file.Notices, "primary.meta has 1 trailing byte") {
		t.Fatalf("notices = %v, want trailing byte warning", file.Notices)
	}
}

func TestAnalyzeOpenGeminiPKMetaDirectoryExpansion(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, opengeminiPKMetaFileName), encodeTestOpenGeminiPKMeta(
		[]testOpenGeminiPKColumn{{Name: "time", Type: 1}},
		-1,
		[]testOpenGeminiPKMetaBlock{{StartBlockID: 1, EndBlockID: 1, DataOffset: 0, DataLength: 8, ColumnOffset: []uint32{0}}},
	), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
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
	if got, want := report.Files[0].Format, FormatOpenGeminiPKMeta; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if report.Files[0].PrimaryKey == nil {
		t.Fatal("primary key summary is nil")
	}
	if report.Files[0].PrimaryKey.DataFilePresent {
		t.Fatal("data file present = true, want false without sibling primary.idx")
	}
}

func TestAnalyzeOpenGeminiPKMetaReportsCRCAndBounds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, opengeminiPKMetaFileName)
	if err := os.WriteFile(path, encodeTestOpenGeminiPKMeta(
		[]testOpenGeminiPKColumn{
			{Name: "host", Type: 6},
			{Name: "time", Type: 1},
		},
		-1,
		[]testOpenGeminiPKMetaBlock{
			{StartBlockID: 3, EndBlockID: 4, DataOffset: 16, DataLength: 20, ColumnOffset: []uint32{24, 8}, CorruptCRC: true},
		},
	), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, opengeminiPKDataFileName), make([]byte, 24), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatOpenGeminiPKMeta,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.PrimaryKey.CRCMismatches, 1; got != want {
		t.Fatalf("crc mismatches = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.DataOutOfBoundsBlocks, 1; got != want {
		t.Fatalf("data out-of-bounds blocks = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.ColumnOutOfBoundsBlocks, 1; got != want {
		t.Fatalf("column out-of-bounds blocks = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.ColumnUnorderedBlocks, 1; got != want {
		t.Fatalf("column unordered blocks = %d, want %d", got, want)
	}
	for key, want := range map[string]int{
		"primary-key-crc-mismatch":         1,
		"primary-key-data-out-of-bounds":   1,
		"primary-key-column-out-of-bounds": 1,
		"primary-key-column-unordered":     1,
	} {
		if got := file.BlocksByType[key]; got != want {
			t.Fatalf("blocksByType[%s] = %d, want %d", key, got, want)
		}
	}
	if !containsOpenGeminiPKNotice(file.Notices, "primary.idx has 1 primary-key data block range") {
		t.Fatalf("notices = %v, want primary.idx bounds warning", file.Notices)
	}
}

func TestAnalyzeOpenGeminiPKMetaRejectsInvalidMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), opengeminiPKMetaFileName)
	if err := os.WriteFile(path, []byte("NOPE\x00\x00\x00\x00\x00\x00\x00\x0c"), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatOpenGeminiPKMeta})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.Files) != 0 {
		t.Fatalf("files = %d, want 0", len(report.Files))
	}
	if !containsOpenGeminiPKNotice(report.Notices, "invalid openGemini primary.meta magic") {
		t.Fatalf("notices = %v, want invalid magic notice", report.Notices)
	}
}

func TestAnalyzeOpenGeminiPKMetaRejectsDetachedPrimaryData(t *testing.T) {
	path := filepath.Join(t.TempDir(), opengeminiPKDataFileName)
	if err := os.WriteFile(path, []byte("detached primary key data"), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatOpenGeminiPKMeta})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.Files) != 0 {
		t.Fatalf("files = %d, want 0", len(report.Files))
	}
	if !containsOpenGeminiPKNotice(report.Notices, "primary.idx is detached primary-key data") {
		t.Fatalf("notices = %v, want detached primary.idx warning", report.Notices)
	}
}

func TestAnalyzeOpenGeminiPKMetaRejectsAttachedIndexName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0000-0000-0001.idx")
	if err := os.WriteFile(path, encodeTestOpenGeminiPKIndex(
		[]testOpenGeminiPKColumn{{Name: "time", Type: 1}},
		1,
		-1,
		nil,
		[]int{8},
	), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatOpenGeminiPKMeta})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.Files) != 0 {
		t.Fatalf("files = %d, want 0", len(report.Files))
	}
	if !containsOpenGeminiPKNotice(report.Notices, "opengemini-pk-meta format requires a primary.meta file") {
		t.Fatalf("notices = %v, want primary.meta filename warning", report.Notices)
	}
}

func encodeTestOpenGeminiPKMeta(columns []testOpenGeminiPKColumn, tcLocation int8, blocks []testOpenGeminiPKMetaBlock) []byte {
	nameBytes := []byte{}
	for _, column := range columns {
		nameBytes = append(nameBytes, column.Name...)
	}
	publicSize := opengeminiPKHeaderSize + 4 + 4 + 1 + len(columns)*4*2 + len(nameBytes)
	data := make([]byte, 0, publicSize+len(blocks)*(opengeminiPKCRCSize+opengeminiPKMetaPrefixSize+len(columns)*4))
	data = append(data, opengeminiPKMagic...)
	data = appendTestUint32(data, 0)
	data = appendTestUint32(data, uint32(publicSize))
	data = appendTestUint32(data, uint32(len(columns)))
	data = append(data, byte(tcLocation))
	for _, column := range columns {
		data = appendTestUint32(data, uint32(len(column.Name)))
	}
	for _, column := range columns {
		data = appendTestUint32(data, column.Type)
	}
	data = append(data, nameBytes...)
	for _, block := range blocks {
		body := make([]byte, 0, opengeminiPKMetaPrefixSize+len(columns)*4)
		body = appendTestUint64(body, block.StartBlockID)
		body = appendTestUint64(body, block.EndBlockID)
		body = appendTestUint32(body, block.DataOffset)
		body = appendTestUint32(body, block.DataLength)
		for _, offset := range block.ColumnOffset {
			body = appendTestUint32(body, offset)
		}
		for len(block.ColumnOffset) < len(columns) {
			body = appendTestUint32(body, 0)
			block.ColumnOffset = append(block.ColumnOffset, 0)
		}
		crc := crc32.ChecksumIEEE(body)
		if block.CorruptCRC {
			crc++
		}
		data = appendTestUint32(data, crc)
		data = append(data, body...)
	}
	return data
}

func appendTestUint32(dst []byte, value uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], value)
	return append(dst, buf[:]...)
}

func appendTestUint64(dst []byte, value uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return append(dst, buf[:]...)
}

func containsOpenGeminiPKNotice(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}
