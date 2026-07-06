package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestAnalyzeOpenGeminiPKIndex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0000-0000-0001.idx")
	columns := []testOpenGeminiPKColumn{
		{Name: "host", Type: 6},
		{Name: "time", Type: 1},
		{Name: "value", Type: 3},
	}
	dataSizes := []int{16, 24, 32}
	data := encodeTestOpenGeminiPKIndex(columns, 7, -1, nil, dataSizes)
	if err := os.WriteFile(path, data, 0o600); err != nil {
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
	if got, want := file.Format, FormatOpenGeminiPKIndex; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.KeyCount, 3; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.BlockCount, 3; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["primary-key-attached-meta"], 1; got != want {
		t.Fatalf("attached meta blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["primary-key-column-data"], 3; got != want {
		t.Fatalf("column data blocks = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{"host:tag", "time:integer", "value:float"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if file.PrimaryKey == nil {
		t.Fatal("primary key summary is nil")
	}
	if got, want := file.PrimaryKey.Type, opengeminiPKIndexLayout; got != want {
		t.Fatalf("primary key type = %q, want %q", got, want)
	}
	if got, want := file.PrimaryKey.RowCount, uint64(7); got != want {
		t.Fatalf("row count = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.TimeClusterLocation, -1; got != want {
		t.Fatalf("time cluster location = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.DataSizeBytes, int64(72); got != want {
		t.Fatalf("data size = %d, want %d", got, want)
	}
	if !file.PrimaryKey.DataInline {
		t.Fatal("data inline = false, want true")
	}
	if file.PrimaryKey.DataFilePresent {
		t.Fatal("data file present = true, want false for attached inline data")
	}
	if got, want := file.PrimaryKey.ColumnOffsetCount, 3; got != want {
		t.Fatalf("column offset count = %d, want %d", got, want)
	}
	firstOffset := int64(opengeminiPKHeaderSize + int(file.PrimaryKey.PublicInfoSizeBytes))
	if got, want := file.PrimaryKey.Schema[0].DataOffset, firstOffset; got != want {
		t.Fatalf("first column offset = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.Schema[0].DataSizeBytes, int64(16); got != want {
		t.Fatalf("first column size = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].Offset, firstOffset; got != want {
		t.Fatalf("block sample offset = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].ValueCount, 7; got != want {
		t.Fatalf("block sample value count = %d, want %d", got, want)
	}
	for key, want := range map[string]string{
		"layout":                opengeminiPKIndexLayout,
		"magic":                 opengeminiPKMagic,
		"version":               "0",
		"schema_column_count":   "3",
		"row_count":             "7",
		"time_cluster_location": "-1",
		"data_inline":           "true",
		"data_size_bytes":       "72",
		"column_offset_count":   "3",
		"local_only":            "true",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
	}
}

func TestAnalyzeOpenGeminiPKIndexDirectoryExpansionIgnoresDetachedDataFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "0000-0000-0001.idx"), encodeTestOpenGeminiPKIndex(
		[]testOpenGeminiPKColumn{{Name: "time", Type: 1}},
		1,
		-1,
		nil,
		[]int{8},
	), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, opengeminiPKDataFileName), []byte("detached primary key data"), 0o600); err != nil {
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
	if got, want := fileBase(report.Files[0].Path), "0000-0000-0001.idx"; got != want {
		t.Fatalf("file path = %q, want %q", got, want)
	}
	if got, want := report.Files[0].Format, FormatOpenGeminiPKIndex; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
}

func TestAnalyzeOpenGeminiPKIndexReportsOffsetIssues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "0000-0000-0002.idx")
	data := encodeTestOpenGeminiPKIndex(
		[]testOpenGeminiPKColumn{
			{Name: "host", Type: 6},
			{Name: "time", Type: 1},
		},
		3,
		-1,
		[]uint32{1000, 20},
		[]int{8, 8},
	)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatOpenGeminiPKIndex,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.PrimaryKey.ColumnOutOfBoundsBlocks, 2; got != want {
		t.Fatalf("column out-of-bounds = %d, want %d", got, want)
	}
	if got, want := file.PrimaryKey.ColumnUnorderedBlocks, 1; got != want {
		t.Fatalf("column unordered = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["primary-key-column-out-of-bounds"], 2; got != want {
		t.Fatalf("blocksByType out-of-bounds = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["primary-key-column-unordered"], 1; got != want {
		t.Fatalf("blocksByType unordered = %d, want %d", got, want)
	}
	if !containsOpenGeminiPKNotice(file.Notices, "primary key index has 2 column data offset") {
		t.Fatalf("notices = %v, want column bounds warning", file.Notices)
	}
	if !containsOpenGeminiPKNotice(file.Notices, "primary key index has 1 unordered column data offset") {
		t.Fatalf("notices = %v, want unordered warning", file.Notices)
	}
}

func TestAnalyzeOpenGeminiPKIndexRejectsPrimaryMeta(t *testing.T) {
	path := filepath.Join(t.TempDir(), opengeminiPKMetaFileName)
	if err := os.WriteFile(path, encodeTestOpenGeminiPKMeta(
		[]testOpenGeminiPKColumn{{Name: "time", Type: 1}},
		-1,
		[]testOpenGeminiPKMetaBlock{{StartBlockID: 1, EndBlockID: 1, DataOffset: 0, DataLength: 8, ColumnOffset: []uint32{0}}},
	), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatOpenGeminiPKIndex})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.Files) != 0 {
		t.Fatalf("files = %d, want 0", len(report.Files))
	}
	if !containsOpenGeminiPKNotice(report.Notices, "primary.meta uses opengemini-pk-meta format") {
		t.Fatalf("notices = %v, want primary.meta format warning", report.Notices)
	}
}

func TestAnalyzeOpenGeminiPKIndexRejectsDetachedPrimaryData(t *testing.T) {
	path := filepath.Join(t.TempDir(), opengeminiPKDataFileName)
	if err := os.WriteFile(path, []byte("detached primary key data"), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatOpenGeminiPKIndex})
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

func TestAnalyzeOpenGeminiPKIndexAutoDoesNotDetectDetachedPrimaryData(t *testing.T) {
	path := filepath.Join(t.TempDir(), opengeminiPKDataFileName)
	data := append([]byte(opengeminiPKMagic), 0, 0, 0, 0)
	data = append(data, []byte("detached primary key data")...)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatAuto})
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

func TestAnalyzeOpenGeminiPKIndexMalformedFiles(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{
			name: "too-small",
			data: []byte("COLX"),
			want: "file too small for openGemini primary key index header",
		},
		{
			name: "bad-magic",
			data: append(append([]byte("NOPE"), make([]byte, 4)...), 0, 0, 0, byte(opengeminiPKIndexMinMetaSize)),
			want: "invalid openGemini primary key index magic",
		},
		{
			name: "meta-too-small",
			data: appendOpenGeminiPKIndexHeader(nil, opengeminiPKIndexMinMetaSize-1),
			want: "invalid openGemini primary key index meta size",
		},
		{
			name: "meta-size-exceeds-file",
			data: appendOpenGeminiPKIndexHeader(nil, 100),
			want: "primary key index meta size 100 exceeds file size",
		},
		{
			name: "schema-arrays-truncated",
			data: appendOpenGeminiPKIndexMeta(nil, 13, 1, 1, -1, nil, nil, nil, nil),
			want: "primary key index meta too small for 1 schema entries",
		},
		{
			name: "name-length-overflow",
			data: appendOpenGeminiPKIndexMeta(nil, 25, 1, 1, -1, []uint32{1}, []uint32{1}, []uint32{33}, nil),
			want: "primary key index field name 0 length 1 exceeds remaining bytes 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bad.idx")
			if err := os.WriteFile(path, tt.data, 0o600); err != nil {
				t.Fatal(err)
			}

			report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatOpenGeminiPKIndex})
			if err != nil {
				t.Fatalf("Analyze() error = %v", err)
			}
			if len(report.Files) != 0 {
				t.Fatalf("files = %d, want 0", len(report.Files))
			}
			if !containsOpenGeminiPKNotice(report.Notices, tt.want) {
				t.Fatalf("notices = %v, want %q", report.Notices, tt.want)
			}
		})
	}
}

func encodeTestOpenGeminiPKIndex(columns []testOpenGeminiPKColumn, rowCount uint32, tcLocation int8, offsets []uint32, dataSizes []int) []byte {
	nameBytes := []byte{}
	for _, column := range columns {
		nameBytes = append(nameBytes, column.Name...)
	}
	metaSize := opengeminiPKIndexMinMetaSize + len(columns)*4*3 + len(nameBytes)
	if offsets == nil {
		offsets = make([]uint32, 0, len(columns))
		nextOffset := uint32(opengeminiPKHeaderSize + metaSize)
		for _, size := range dataSizes {
			offsets = append(offsets, nextOffset)
			nextOffset += uint32(size)
		}
	}
	data := make([]byte, 0, opengeminiPKHeaderSize+metaSize)
	data = append(data, opengeminiPKMagic...)
	data = appendTestUint32(data, 0)
	data = appendTestUint32(data, uint32(metaSize))
	data = appendTestUint32(data, uint32(len(columns)))
	data = appendTestUint32(data, rowCount)
	data = append(data, byte(tcLocation))
	for _, column := range columns {
		data = appendTestUint32(data, uint32(len(column.Name)))
	}
	for _, column := range columns {
		data = appendTestUint32(data, column.Type)
	}
	for i := range columns {
		offset := uint32(0)
		if i < len(offsets) {
			offset = offsets[i]
		}
		data = appendTestUint32(data, offset)
	}
	data = append(data, nameBytes...)
	for _, size := range dataSizes {
		if size < 0 {
			size = 0
		}
		data = append(data, make([]byte, size)...)
	}
	return data
}

func appendOpenGeminiPKIndexHeader(dst []byte, metaSize int) []byte {
	dst = append(dst, opengeminiPKMagic...)
	dst = appendTestUint32(dst, 0)
	return appendTestUint32(dst, uint32(metaSize))
}

func appendOpenGeminiPKIndexMeta(dst []byte, metaSize int, schemaCount uint32, rowCount uint32, tcLocation int8, nameLengths, fieldTypes, offsets []uint32, names []byte) []byte {
	dst = appendOpenGeminiPKIndexHeader(dst, metaSize)
	dst = appendTestUint32(dst, schemaCount)
	dst = appendTestUint32(dst, rowCount)
	dst = append(dst, byte(tcLocation))
	for _, value := range nameLengths {
		dst = appendTestUint32(dst, value)
	}
	for _, value := range fieldTypes {
		dst = appendTestUint32(dst, value)
	}
	for _, value := range offsets {
		dst = appendTestUint32(dst, value)
	}
	return append(dst, names...)
}

func fileBase(path string) string {
	return filepath.Base(path)
}
