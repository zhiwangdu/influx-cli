package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeTSILogMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "mem", Key: "host", Value: "b"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntryTagValueTombstoneFlag, Name: "cpu", Key: "host", Value: "b"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntryMeasurementTombstoneFlag, Name: "old"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntrySeriesTombstoneFlag, SeriesID: 2})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatAuto,
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
	if got, want := file.Format, FormatTSILog; got != want {
		t.Fatalf("format = %s, want %s", got, want)
	}
	if got, want := file.BlockCount, 5; got != want {
		t.Fatalf("entry count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["series"], 2; got != want {
		t.Fatalf("series entries = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["series-tombstone"], 1; got != want {
		t.Fatalf("series tombstone entries = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Count, int64(1); got != want {
		t.Fatalf("live series id count = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Min, uint64(1); got != want {
		t.Fatalf("min series id = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Max, uint64(1); got != want {
		t.Fatalf("max series id = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{"cpu", "mem", "old"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if file.Index == nil {
		t.Fatal("expected index summary")
	}
	if got, want := file.Index.Type, "tsi1-log"; got != want {
		t.Fatalf("index type = %q, want %q", got, want)
	}
	if got, want := file.Index.MeasurementCount, 3; got != want {
		t.Fatalf("measurement count = %d, want %d", got, want)
	}
	if got, want := file.Index.DeletedMeasurementCount, 1; got != want {
		t.Fatalf("deleted measurement count = %d, want %d", got, want)
	}
	if got, want := file.Index.TagKeyCount, 2; got != want {
		t.Fatalf("tag key count = %d, want %d", got, want)
	}
	if got, want := file.Index.TagValueCount, 3; got != want {
		t.Fatalf("tag value count = %d, want %d", got, want)
	}
	if got, want := file.Index.DeletedTagValueCount, 1; got != want {
		t.Fatalf("deleted tag value count = %d, want %d", got, want)
	}
	if got, want := file.Index.SeriesIDSetCardinality, int64(1); got != want {
		t.Fatalf("series id set cardinality = %d, want %d", got, want)
	}
	if got, want := file.Index.TombstoneSeriesIDSetCardinality, int64(1); got != want {
		t.Fatalf("tombstone series id set cardinality = %d, want %d", got, want)
	}
	if got, want := file.Extra["entry_count"], "5"; got != want {
		t.Fatalf("entry count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["resolved_series_entry_count"], "2"; got != want {
		t.Fatalf("resolved series entry count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["unresolved_series_entry_count"], "0"; got != want {
		t.Fatalf("unresolved series entry count extra = %q, want %q", got, want)
	}
}

func TestAnalyzeTSILogQueryFilters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "mem", Key: "host", Value: "b"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntryMeasurementTombstoneFlag, Name: "old"})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatTSILog,
		QueryMeasurements: []string{"old", "cpu", "disk"},
		QueryTags:         []TagFilter{{Key: "host", Value: "a"}},
		KeySampleLimit:    3,
		BlockSampleLimit:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected query summary")
	}
	if got, want := query.MatchedMeasurements, []string{"cpu", "old"}; !equalStrings(got, want) {
		t.Fatalf("matched measurements = %v, want %v", got, want)
	}
	if got, want := query.MissingMeasurements, []string{"disk"}; !equalStrings(got, want) {
		t.Fatalf("missing measurements = %v, want %v", got, want)
	}
	if got, want := query.MatchedTags, []TagFilter{{Key: "host", Value: "a"}}; !equalTagFilters(got, want) {
		t.Fatalf("matched tags = %+v, want %+v", got, want)
	}
	if got, want := query.CandidateMeasurements, 1; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(1); got != want {
		t.Fatalf("query series refs = %d, want %d", got, want)
	}
	if got, want := query.MeasurementSamples[0].Tags[0].Values[0].SeriesCount, uint64(1); got != want {
		t.Fatalf("query tag series count = %d, want %d", got, want)
	}
}

func TestAnalyzeTSILogQueryFiltersRequireSameSeriesIntersection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "region", Value: "us"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "cpu", Key: "region", Value: "eu"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 3, Name: "cpu", Key: "host", Value: "b"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 3, Name: "cpu", Key: "region", Value: "ap"})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSILog,
		QueryTags:        []TagFilter{{Key: "host", Value: "a"}, {Key: "region", Value: "us"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected query summary")
	}
	if got, want := query.CandidateMeasurements, 1; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(1); got != want {
		t.Fatalf("query series refs = %d, want %d", got, want)
	}
	if got, want := query.MeasurementSamples[0].SeriesCount, uint64(1); got != want {
		t.Fatalf("query sample series count = %d, want %d", got, want)
	}

	report, err = Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSILog,
		QueryTags:        []TagFilter{{Key: "host", Value: "a"}, {Key: "region", Value: "ap"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	query = report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected query summary")
	}
	if got, want := query.MatchedTags, []TagFilter{{Key: "host", Value: "a"}, {Key: "region", Value: "ap"}}; !equalTagFilters(got, want) {
		t.Fatalf("matched tags = %+v, want %+v", got, want)
	}
	if got, want := query.CandidateMeasurements, 0; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(0); got != want {
		t.Fatalf("query series refs = %d, want %d", got, want)
	}
	if len(query.MeasurementSamples) != 0 {
		t.Fatalf("query measurement samples = %+v, want none", query.MeasurementSamples)
	}
}

func TestAnalyzeTSILogSeriesTombstoneRemovesAllTagRefs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "region", Value: "us"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntrySeriesTombstoneFlag, SeriesID: 1})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSILog,
		QueryTags:        []TagFilter{{Key: "host", Value: "a"}, {Key: "region", Value: "us"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.SeriesID.Count, int64(0); got != want {
		t.Fatalf("live series count = %d, want %d", got, want)
	}
	query := file.Index.Query
	if query == nil {
		t.Fatal("expected query summary")
	}
	if got, want := query.CandidateMeasurements, 0; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(0); got != want {
		t.Fatalf("query series refs = %d, want %d", got, want)
	}
}

func TestAnalyzeTSILogTrailingCorruptEntryNotice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "mem"})
	data := buf.Bytes()
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:         FormatTSILog,
		KeySampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.BlockCount, 1; got != want {
		t.Fatalf("valid entry count = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Count, int64(1); got != want {
		t.Fatalf("live series count = %d, want %d", got, want)
	}
	if len(file.Notices) != 1 || !strings.Contains(file.Notices[0], "checksum mismatch") {
		t.Fatalf("notices = %v, want checksum mismatch notice", file.Notices)
	}
	if len(report.Notices) != 1 || !strings.Contains(report.Notices[0], "checksum mismatch") {
		t.Fatalf("report notices = %v, want propagated checksum mismatch notice", report.Notices)
	}
}

func TestAnalyzeTSILogRejectsTSIFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := writeTestTSI(path); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatTSILog})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.Files) != 0 {
		t.Fatalf("files = %d, want 0", len(report.Files))
	}
	if !containsString(report.Notices, "L0-00000001.tsi uses tsi format, not tsi-log") {
		t.Fatalf("notices = %v, want tsi format warning", report.Notices)
	}
}

func TestAnalyzeTSILogRejectsDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatTSILog})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.Files) != 0 {
		t.Fatalf("files = %d, want 0", len(report.Files))
	}
	if !containsString(report.Notices, "tsi-log format requires a .tsl log file, got directory L0-00000001.tsl") {
		t.Fatalf("notices = %v, want directory warning", report.Notices)
	}
}

func appendTestTSILogEntry(buf *bytes.Buffer, entry tsiLogEntry) {
	start := buf.Len()
	buf.WriteByte(entry.Flag)
	buf.Write(binary.AppendUvarint(nil, entry.SeriesID))
	appendTestTSILogBytes(buf, []byte(entry.Name))
	appendTestTSILogBytes(buf, []byte(entry.Key))
	appendTestTSILogBytes(buf, []byte(entry.Value))
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(buf.Bytes()[start:]))
	buf.Write(crc[:])
}

func appendTestTSILogBytes(buf *bytes.Buffer, value []byte) {
	buf.Write(binary.AppendUvarint(nil, uint64(len(value))))
	buf.Write(value)
}
