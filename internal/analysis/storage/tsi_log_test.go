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
	if got, want := uint64PtrValue(file.Index.SeriesIDSetMin), uint64(1); got != want {
		t.Fatalf("series id set min = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.SeriesIDSetMax), uint64(1); got != want {
		t.Fatalf("series id set max = %d, want %d", got, want)
	}
	if got, want := file.Index.TombstoneSeriesIDSetCardinality, int64(1); got != want {
		t.Fatalf("tombstone series id set cardinality = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.TombstoneSeriesIDSetMin), uint64(2); got != want {
		t.Fatalf("tombstone series id set min = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.TombstoneSeriesIDSetMax), uint64(2); got != want {
		t.Fatalf("tombstone series id set max = %d, want %d", got, want)
	}
	if details := indexDetailsText(file.Index); !strings.Contains(details, "series_ids=1 range=1:1") || !strings.Contains(details, "series_ids=1 range=2:2") {
		t.Fatalf("index details = %q, want live and tombstone ranges", details)
	}
	if got, want := file.Extra["entry_count"], "5"; got != want {
		t.Fatalf("entry count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["series_id_set_min"], "1"; got != want {
		t.Fatalf("series id set min extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["series_id_set_max"], "1"; got != want {
		t.Fatalf("series id set max extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["tombstone_series_id_set_min"], "2"; got != want {
		t.Fatalf("tombstone series id set min extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["tombstone_series_id_set_max"], "2"; got != want {
		t.Fatalf("tombstone series id set max extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["resolved_series_entry_count"], "2"; got != want {
		t.Fatalf("resolved series entry count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["unresolved_series_entry_count"], "0"; got != want {
		t.Fatalf("unresolved series entry count extra = %q, want %q", got, want)
	}
}

func TestAnalyzeTSILogSeriesIDFilterWithoutRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "mem", Key: "host", Value: "b"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntrySeriesTombstoneFlag, SeriesID: 2})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSILog,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
		QuerySeriesIDs:   []uint64{42, 2, 1, 2},
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	for key, want := range map[string]string{
		"query_series_id_filter_applied": "true",
		"query_series_ids":               "1,2,42",
		"query_matched_series_ids":       "1",
		"query_tombstone_series_ids":     "2",
		"query_missing_series_ids":       "42",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
}

func TestAnalyzeTSILogAutoSeriesIDFilterWithoutRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntrySeriesTombstoneFlag, SeriesID: 2})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
		QuerySeriesIDs:   []uint64{2, 7, 1},
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatTSILog; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	for key, want := range map[string]string{
		"query_series_id_filter_applied": "true",
		"query_series_ids":               "1,2,7",
		"query_matched_series_ids":       "1",
		"query_tombstone_series_ids":     "2",
		"query_missing_series_ids":       "7",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
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

func TestAnalyzeTSILogQueryMeasurementOnlyEnumeratesTagSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "cpu", Key: "host", Value: "b"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "region", Value: "us"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "cpu", Key: "host", Value: "old"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntryTagValueTombstoneFlag, Name: "cpu", Key: "host", Value: "old"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "zone", Value: "z1"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntryTagKeyTombstoneFlag, Name: "cpu", Key: "zone"})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatTSILog,
		QueryMeasurements: []string{"cpu"},
		KeySampleLimit:    4,
		BlockSampleLimit:  4,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected query summary")
	}
	if !query.MeasurementFilterApplied || query.TagFilterApplied {
		t.Fatalf("query flags = measurement:%t tag:%t, want measurement only", query.MeasurementFilterApplied, query.TagFilterApplied)
	}
	if got, want := query.CandidateMeasurements, 1; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(2); got != want {
		t.Fatalf("query series refs = %d, want %d", got, want)
	}
	if got, want := len(query.MeasurementSamples), 1; got != want {
		t.Fatalf("measurement samples = %d, want %d", got, want)
	}
	tags := query.MeasurementSamples[0].Tags
	if got, want := len(tags), 3; got != want {
		t.Fatalf("sample tags = %d, want %d", got, want)
	}
	if got, want := tags[0].Key, "host"; got != want {
		t.Fatalf("first sample tag key = %q, want %q", got, want)
	}
	if got, want := tags[0].Values[0].Value, "a"; got != want {
		t.Fatalf("first host value = %q, want %q", got, want)
	}
	if got, want := tags[0].Values[1].Value, "b"; got != want {
		t.Fatalf("second host value = %q, want %q", got, want)
	}
	if got, want := tags[0].Values[2].Value, "old"; got != want {
		t.Fatalf("third host value = %q, want %q", got, want)
	}
	if !tags[0].Values[2].Deleted {
		t.Fatalf("host old value should be marked deleted")
	}
	if got, want := tags[0].Values[2].SeriesCount, uint64(0); got != want {
		t.Fatalf("deleted host value series count = %d, want %d", got, want)
	}
	if got, want := tags[1].Key, "region"; got != want {
		t.Fatalf("second sample tag key = %q, want %q", got, want)
	}
	if got, want := tags[1].Values[0].SeriesCount, uint64(1); got != want {
		t.Fatalf("region series count = %d, want %d", got, want)
	}
	if got, want := tags[2].Key, "zone"; got != want {
		t.Fatalf("third sample tag key = %q, want %q", got, want)
	}
	if !tags[2].Deleted {
		t.Fatalf("zone tag key should be marked deleted")
	}
	if got, want := len(tags[2].Values), 1; got != want {
		t.Fatalf("zone values = %d, want %d", got, want)
	}
	if got, want := tags[2].Values[0].SeriesCount, uint64(0); got != want {
		t.Fatalf("deleted zone key value series count = %d, want %d", got, want)
	}
}

func TestAnalyzeTSILogQueryMeasurementOnlyLimitsTagSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "cpu", Key: "host", Value: "b"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 3, Name: "cpu", Key: "host", Value: "c"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 4, Name: "cpu", Key: "rack", Value: "r1"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 5, Name: "cpu", Key: "zone", Value: "z1"})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatTSILog,
		QueryMeasurements: []string{"cpu"},
		KeySampleLimit:    5,
		BlockSampleLimit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected query summary")
	}
	if got, want := len(query.MeasurementSamples), 1; got != want {
		t.Fatalf("measurement samples = %d, want %d", got, want)
	}
	tags := query.MeasurementSamples[0].Tags
	if got, want := len(tags), 2; got != want {
		t.Fatalf("sample tags = %d, want %d", got, want)
	}
	if got, want := tags[0].Key, "host"; got != want {
		t.Fatalf("first sample tag key = %q, want %q", got, want)
	}
	if got, want := len(tags[0].Values), 2; got != want {
		t.Fatalf("host sample values = %d, want %d", got, want)
	}
	if got, want := tags[0].Values[0].Value, "a"; got != want {
		t.Fatalf("first host value = %q, want %q", got, want)
	}
	if got, want := tags[0].Values[1].Value, "b"; got != want {
		t.Fatalf("second host value = %q, want %q", got, want)
	}
	if got, want := tags[1].Key, "rack"; got != want {
		t.Fatalf("second sample tag key = %q, want %q", got, want)
	}
}

func TestAnalyzeTSILogQueryMeasurementOnlyOmitsTagSamplesWhenLimitDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "cpu", Key: "region", Value: "us"})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatTSILog,
		QueryMeasurements: []string{"cpu"},
		KeySampleLimit:    2,
		BlockSampleLimit:  0,
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
	if got, want := query.SeriesRefs, int64(2); got != want {
		t.Fatalf("query series refs = %d, want %d", got, want)
	}
	if got, want := query.TagKeyCount, 2; got != want {
		t.Fatalf("query tag key count = %d, want %d", got, want)
	}
	if got, want := query.TagValueCount, 2; got != want {
		t.Fatalf("query tag value count = %d, want %d", got, want)
	}
	if got, want := len(query.MeasurementSamples), 1; got != want {
		t.Fatalf("measurement samples = %d, want %d", got, want)
	}
	if got := query.MeasurementSamples[0].Tags; len(got) != 0 {
		t.Fatalf("measurement sample tags = %+v, want none when block sample limit is disabled", got)
	}
}

func TestAnalyzeTSILogQueryMeasurementOnlySamplesReflectSeriesTombstone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "cpu", Key: "host", Value: "a"})
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 2, Name: "cpu", Key: "region", Value: "us"})
	appendTestTSILogEntry(&buf, tsiLogEntry{Flag: tsiLogEntrySeriesTombstoneFlag, SeriesID: 2})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatTSILog,
		QueryMeasurements: []string{"cpu"},
		KeySampleLimit:    2,
		BlockSampleLimit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected query summary")
	}
	if got, want := query.SeriesRefs, int64(1); got != want {
		t.Fatalf("query series refs = %d, want %d", got, want)
	}
	tags := query.MeasurementSamples[0].Tags
	if got, want := len(tags), 2; got != want {
		t.Fatalf("sample tags = %d, want %d", got, want)
	}
	if got, want := tags[0].Key, "host"; got != want {
		t.Fatalf("first sample tag key = %q, want %q", got, want)
	}
	if got, want := tags[0].Values[0].SeriesCount, uint64(1); got != want {
		t.Fatalf("host value series count = %d, want %d", got, want)
	}
	if got, want := tags[1].Key, "region"; got != want {
		t.Fatalf("second sample tag key = %q, want %q", got, want)
	}
	if got, want := tags[1].Values[0].SeriesCount, uint64(0); got != want {
		t.Fatalf("region value series count = %d, want %d after series tombstone", got, want)
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
