package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/snappy"
)

func TestAnalyzeWALSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	if err := writeTestWALSegment(path,
		testWALWriteEntry([]testWALWriteGroup{
			{key: "cpu value", valueType: walIntegerValueType, times: []int64{10, 20}},
			{key: "mem value", valueType: walStringValueType, times: []int64{30}},
		}),
		testWALDeleteRangeEntry(40, 50, "cpu value"),
		testWALDeleteEntry("mem value"),
	); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(15, 25)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
		QueryRange:       queryRange,
		QueryKeys:        []string{"cpu value"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatWAL; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.BlockCount, 3; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["wal-write"], 1; got != want {
		t.Fatalf("wal-write count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["wal-delete-range"], 1; got != want {
		t.Fatalf("wal-delete-range count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["wal-delete"], 1; got != want {
		t.Fatalf("wal-delete count = %d, want %d", got, want)
	}
	if got, want := file.KeyCount, 2; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.MinTime, int64(10); got != want {
		t.Fatalf("min time = %d, want %d", got, want)
	}
	if got, want := file.MaxTime, int64(50); got != want {
		t.Fatalf("max time = %d, want %d", got, want)
	}
	if got, want := file.QueryOverlapBlocks, 1; got != want {
		t.Fatalf("query overlap blocks = %d, want %d", got, want)
	}
	if got, want := file.Extra["entry_count"], "3"; got != want {
		t.Fatalf("entry count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["point_count"], "3"; got != want {
		t.Fatalf("point count extra = %q, want %q", got, want)
	}
	if got, want := len(file.Blocks), 3; got != want {
		t.Fatalf("block samples = %d, want %d", got, want)
	}
	if got, want := file.Blocks[0].Type, "wal-write"; got != want {
		t.Fatalf("first block type = %q, want %q", got, want)
	}
	if got, want := file.Blocks[0].ValueCount, 3; got != want {
		t.Fatalf("first block value count = %d, want %d", got, want)
	}

	decode := file.DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.Mode, "wal-replay-filter"; got != want {
		t.Fatalf("decode mode = %q, want %q", got, want)
	}
	if got, want := decode.BaselineDecodeBlocks, 2; got != want {
		t.Fatalf("baseline blocks = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeBlocks, 1; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedByKeyBlocks, 1; got != want {
		t.Fatalf("skipped by key blocks = %d, want %d", got, want)
	}
	if got, want := decode.SkippedAfterRangeBlocks, 1; got != want {
		t.Fatalf("skipped after range blocks = %d, want %d", got, want)
	}
	if got, want := decode.BaselineDecodeValues, 3; got != want {
		t.Fatalf("baseline values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedDecodeValues, 2; got != want {
		t.Fatalf("optimized values = %d, want %d", got, want)
	}
	if got, want := decode.OptimizedValueOutputPoints, 1; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if !equalStrings(decode.MatchedKeys, []string{"cpu value"}) {
		t.Fatalf("matched keys = %v, want [cpu value]", decode.MatchedKeys)
	}
	if got, want := decode.Samples[0].OutputValues, 2; got != want {
		t.Fatalf("first sample output values = %d, want %d", got, want)
	}
	if got, want := decode.Samples[0].ValueOutputPoints, 1; got != want {
		t.Fatalf("first sample value output points = %d, want %d", got, want)
	}
	if !decode.Samples[0].ValueOutputAvailable {
		t.Fatal("expected first sample value output to be available")
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	wantSample := DecodePathCursorOutput{
		Key:            "cpu value",
		Time:           20,
		Type:           "wal-write-integer",
		OptimizedValue: "2",
		Matches:        true,
	}
	if got := decode.CursorOutputSamples[0]; got != wantSample {
		t.Fatalf("cursor output sample = %+v, want %+v", got, wantSample)
	}
	if !containsString(decode.Recommendations, "sampled local WAL replay candidates") {
		t.Fatalf("recommendations = %v, want replay candidate sample recommendation", decode.Recommendations)
	}
}

func TestAnalyzeDiscoversWALSegmentInDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := writeTestWALSegment(filepath.Join(dir, "_00002.wal"), testWALDeleteEntry("cpu value")); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:         FormatAuto,
		KeySampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got, want := report.Files[0].Format, FormatWAL; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
}

func TestAnalyzeWALEmptySegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatWAL})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.BlockCount, 0; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.Extra["entry_count"], "0"; got != want {
		t.Fatalf("entry count extra = %q, want %q", got, want)
	}
	if file.DecodePath != nil {
		t.Fatalf("decode path = %#v, want nil without query range", file.DecodePath)
	}
}

func TestAnalyzeWALToleratesTornTrailingPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	if err := writeTestWALSegment(path, testWALDeleteEntry("cpu value")); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{walWriteEntryType, 0, 0, 0, 10, 0x01, 0x02}); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:         FormatWAL,
		KeySampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.BlockCount, 1; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if len(file.Notices) != 1 || !strings.Contains(file.Notices[0], "trailing partial WAL entry payload") {
		t.Fatalf("notices = %v, want trailing payload notice", file.Notices)
	}
	if len(report.Notices) != 1 || !strings.Contains(report.Notices[0], "trailing partial WAL entry payload") {
		t.Fatalf("report notices = %v, want propagated trailing payload notice", report.Notices)
	}
}

func TestAnalyzeWALRepeatedWriteKeyMergesCountsAndRanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	if err := writeTestWALSegment(path, testWALWriteEntry([]testWALWriteGroup{
		{key: "cpu value", valueType: walIntegerValueType, times: []int64{10}},
		{key: "cpu value", valueType: walIntegerValueType, times: []int64{30}},
	})); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(1, 40)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatWAL,
		KeySampleLimit:   2,
		BlockSampleLimit: 2,
		QueryRange:       queryRange,
		QueryKeys:        []string{"cpu value"},
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.KeyCount, 1; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.MinTime, int64(10); got != want {
		t.Fatalf("min time = %d, want %d", got, want)
	}
	if got, want := file.MaxTime, int64(30); got != want {
		t.Fatalf("max time = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.OptimizedDecodeValues, 2; got != want {
		t.Fatalf("optimized decode values = %d, want %d", got, want)
	}
	if got, want := file.DecodePath.Samples[0].OutputValues, 2; got != want {
		t.Fatalf("sample output values = %d, want %d", got, want)
	}
}

func TestAnalyzeWALSamplesWritePointValueTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	if err := writeTestWALSegment(path, testWALWriteEntry([]testWALWriteGroup{
		{key: "float value", valueType: walFloat64ValueType, times: []int64{10}},
		{key: "integer value", valueType: walIntegerValueType, times: []int64{10}},
		{key: "unsigned value", valueType: walUnsignedValueType, times: []int64{10}},
		{key: "boolean value", valueType: walBooleanValueType, times: []int64{10}},
		{key: "string value", valueType: walStringValueType, times: []int64{10}},
	})); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(10, 10)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatWAL,
		QueryRange:       queryRange,
		KeySampleLimit:   5,
		BlockSampleLimit: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 5; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	wantSamples := []DecodePathCursorOutput{
		{Key: "float value", Time: 10, Type: "wal-write-float", OptimizedValue: "1.5", Matches: true},
		{Key: "integer value", Time: 10, Type: "wal-write-integer", OptimizedValue: "1", Matches: true},
		{Key: "unsigned value", Time: 10, Type: "wal-write-unsigned", OptimizedValue: "1", Matches: true},
		{Key: "boolean value", Time: 10, Type: "wal-write-boolean", OptimizedValue: "true", Matches: true},
		{Key: "string value", Time: 10, Type: "wal-write-string", OptimizedValue: "v1", Matches: true},
	}
	if got, want := len(decode.CursorOutputSamples), len(wantSamples); got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	for i, want := range wantSamples {
		if got := decode.CursorOutputSamples[i]; got != want {
			t.Fatalf("cursor output sample[%d] = %+v, want %+v", i, got, want)
		}
	}
}

func TestAnalyzeWALWritePointSampleLimitKeepsExactCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	if err := writeTestWALSegment(path,
		testWALWriteEntry([]testWALWriteGroup{
			{key: "cpu value", valueType: walIntegerValueType, times: []int64{10, 20}},
		}),
		testWALWriteEntry([]testWALWriteGroup{
			{key: "cpu value", valueType: walIntegerValueType, times: []int64{30}},
		}),
	); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(1, 40)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatWAL,
		QueryRange:       queryRange,
		QueryKeys:        []string{"cpu value"},
		BlockSampleLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedValueOutputPoints, 3; got != want {
		t.Fatalf("optimized value output points = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 1; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	if got, want := decode.CursorOutputSamples[0].Time, int64(10); got != want {
		t.Fatalf("first cursor output sample time = %d, want %d", got, want)
	}
}

func TestAnalyzeWALSamplesDeleteReplayCandidates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	if err := writeTestWALSegment(path,
		testWALDeleteRangeEntry(15, 25, "cpu value", "mem value"),
		testWALDeleteEntry("cpu value", "disk value"),
	); err != nil {
		t.Fatal(err)
	}
	queryRange, err := NewTimeRange(20, 20)
	if err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatWAL,
		QueryRange:       queryRange,
		QueryKeys:        []string{"cpu value"},
		BlockSampleLimit: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	decode := report.Files[0].DecodePath
	if decode == nil {
		t.Fatal("decode path is nil")
	}
	if got, want := decode.OptimizedDecodeBlocks, 2; got != want {
		t.Fatalf("optimized blocks = %d, want %d", got, want)
	}
	if got, want := len(decode.CursorOutputSamples), 2; got != want {
		t.Fatalf("cursor output samples = %d, want %d", got, want)
	}
	wantSamples := []DecodePathCursorOutput{
		{Key: "cpu value", Time: 15, Type: "wal-delete-range", OptimizedValue: "15..25", Matches: true},
		{Key: "cpu value", Type: "wal-delete", OptimizedValue: "delete-key", Matches: true},
	}
	for i, want := range wantSamples {
		if got := decode.CursorOutputSamples[i]; got != want {
			t.Fatalf("cursor output sample[%d] = %+v, want %+v", i, got, want)
		}
	}
	if !containsString(decode.Recommendations, "sampled local WAL replay candidates") {
		t.Fatalf("recommendations = %v, want replay candidate sample recommendation", decode.Recommendations)
	}
}

func TestAnalyzeWALRejectsCorruptPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	var buf bytes.Buffer
	buf.WriteByte(walWriteEntryType)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], 4)
	buf.Write(length[:])
	buf.Write([]byte{0x00, 0x01, 0x02, 0x03})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := analyzeFile(path, Options{Format: FormatWAL})
	if err == nil || !strings.Contains(err.Error(), "decode WAL entry") {
		t.Fatalf("error = %v, want decode WAL entry", err)
	}
}

func TestAnalyzeWALRejectsUnsupportedWriteValueType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	if err := writeTestWALSegment(path, testWALRecord{
		entryType: walWriteEntryType,
		payload:   testWALUnsupportedWritePayload("cpu value"),
	}); err != nil {
		t.Fatal(err)
	}

	_, err := analyzeFile(path, Options{Format: FormatWAL})
	if err == nil || !strings.Contains(err.Error(), "unsupported write value type") {
		t.Fatalf("error = %v, want unsupported write value type", err)
	}
}

func TestAnalyzeWALRejectsUnknownEntryType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "_00001.wal")
	if err := writeTestWALSegment(path, testWALRecord{
		entryType: 0xff,
		payload:   []byte("payload"),
	}); err != nil {
		t.Fatal(err)
	}

	_, err := analyzeFile(path, Options{Format: FormatWAL})
	if err == nil || !strings.Contains(err.Error(), "unknown WAL entry type") {
		t.Fatalf("error = %v, want unknown WAL entry type", err)
	}
}

func TestAnalyzeWALKeyRequiresRange(t *testing.T) {
	_, err := Analyze(context.Background(), []string{"missing"}, Options{
		Format:    FormatWAL,
		QueryKeys: []string{"cpu value"},
	})
	if err == nil || !strings.Contains(err.Error(), "query key filter requires query range") {
		t.Fatalf("error = %v, want key range requirement", err)
	}
}

type testWALRecord struct {
	entryType byte
	payload   []byte
}

type testWALWriteGroup struct {
	key       string
	valueType byte
	times     []int64
}

func writeTestWALSegment(path string, records ...testWALRecord) error {
	var buf bytes.Buffer
	for _, record := range records {
		compressed := snappy.Encode(nil, record.payload)
		buf.WriteByte(record.entryType)
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(compressed)))
		buf.Write(length[:])
		buf.Write(compressed)
	}
	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func testWALWriteEntry(groups []testWALWriteGroup) testWALRecord {
	var buf bytes.Buffer
	for _, group := range groups {
		buf.WriteByte(group.valueType)
		writeTestWALUint16(&buf, uint16(len(group.key)))
		buf.WriteString(group.key)
		writeTestWALUint32(&buf, uint32(len(group.times)))
		for i, timestamp := range group.times {
			writeTestWALInt64(&buf, timestamp)
			switch group.valueType {
			case walFloat64ValueType:
				writeTestWALUint64(&buf, math.Float64bits(float64(i)+1.5))
			case walIntegerValueType:
				writeTestWALInt64(&buf, int64(i+1))
			case walUnsignedValueType:
				writeTestWALUint64(&buf, uint64(i+1))
			case walBooleanValueType:
				if i%2 == 0 {
					buf.WriteByte(1)
				} else {
					buf.WriteByte(0)
				}
			case walStringValueType:
				value := fmt.Sprintf("v%d", i+1)
				writeTestWALUint32(&buf, uint32(len(value)))
				buf.WriteString(value)
			}
		}
	}
	return testWALRecord{entryType: walWriteEntryType, payload: buf.Bytes()}
}

func testWALDeleteEntry(keys ...string) testWALRecord {
	return testWALRecord{
		entryType: walDeleteEntryType,
		payload:   []byte(strings.Join(keys, "\n")),
	}
}

func testWALDeleteRangeEntry(minTime, maxTime int64, keys ...string) testWALRecord {
	var buf bytes.Buffer
	writeTestWALInt64(&buf, minTime)
	writeTestWALInt64(&buf, maxTime)
	for _, key := range keys {
		writeTestWALUint32(&buf, uint32(len(key)))
		buf.WriteString(key)
	}
	return testWALRecord{entryType: walDeleteRangeEntryType, payload: buf.Bytes()}
}

func testWALUnsupportedWritePayload(key string) []byte {
	var buf bytes.Buffer
	buf.WriteByte(0xfe)
	writeTestWALUint16(&buf, uint16(len(key)))
	buf.WriteString(key)
	writeTestWALUint32(&buf, 1)
	writeTestWALInt64(&buf, 10)
	return buf.Bytes()
}

func writeTestWALUint16(buf *bytes.Buffer, value uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], value)
	buf.Write(b[:])
}

func writeTestWALUint32(buf *bytes.Buffer, value uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], value)
	buf.Write(b[:])
}

func writeTestWALUint64(buf *bytes.Buffer, value uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], value)
	buf.Write(b[:])
}

func writeTestWALInt64(buf *bytes.Buffer, value int64) {
	writeTestWALUint64(buf, uint64(value))
}
