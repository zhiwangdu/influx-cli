package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeSeriesFileDirectory(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
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
	if got, want := file.Format, FormatSeriesFile; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.KeyCount, 2; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.BlockCount, 4; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Count, int64(2); got != want {
		t.Fatalf("series count = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Min, uint64(1); got != want {
		t.Fatalf("min series id = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Max, uint64(2); got != want {
		t.Fatalf("max series id = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["partition"], 2; got != want {
		t.Fatalf("partition blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["series-segment"], 2; got != want {
		t.Fatalf("segment blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["series-entry"], 3; got != want {
		t.Fatalf("series entry blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["series-tombstone"], 1; got != want {
		t.Fatalf("series tombstone blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["series-index"], 1; got != want {
		t.Fatalf("series index blocks = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{
		"id:1 cpu,host=a,region=us",
		"id:2 cpu,host=b",
	}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if file.Index == nil {
		t.Fatalf("index summary is nil")
	}
	if got, want := file.Index.Type, "series-file"; got != want {
		t.Fatalf("index type = %q, want %q", got, want)
	}
	if got, want := file.Index.MeasurementCount, 1; got != want {
		t.Fatalf("measurement count = %d, want %d", got, want)
	}
	if got, want := file.Index.SeriesRefs, int64(2); got != want {
		t.Fatalf("series refs = %d, want %d", got, want)
	}
	if got, want := file.Index.TagKeyCount, 2; got != want {
		t.Fatalf("tag key count = %d, want %d", got, want)
	}
	if got, want := file.Index.TagValueCount, 3; got != want {
		t.Fatalf("tag value count = %d, want %d", got, want)
	}
	if got, want := file.Index.TombstoneSeriesIDSetCardinality, int64(1); got != want {
		t.Fatalf("tombstone cardinality = %d, want %d", got, want)
	}
	if got, want := len(file.Index.MeasurementSamples), 1; got != want {
		t.Fatalf("measurement samples = %d, want %d", got, want)
	}
	sample := file.Index.MeasurementSamples[0]
	if got, want := sample.Name, "cpu"; got != want {
		t.Fatalf("measurement sample name = %q, want %q", got, want)
	}
	if got, want := sample.SeriesCount, uint64(2); got != want {
		t.Fatalf("measurement sample series count = %d, want %d", got, want)
	}
	if got, want := sample.TagKeyCount, 2; got != want {
		t.Fatalf("measurement sample tag key count = %d, want %d", got, want)
	}
	if got, want := sample.TagValueCount, 3; got != want {
		t.Fatalf("measurement sample tag value count = %d, want %d", got, want)
	}
	for key, want := range map[string]string{
		"layout":                 "series-file",
		"partition_count":        "2",
		"segment_count":          "2",
		"parsed_segment_count":   "2",
		"index_file_count":       "1",
		"entry_count":            "4",
		"insert_entry_count":     "3",
		"tombstone_entry_count":  "1",
		"live_series_count":      "2",
		"tombstone_series_count": "1",
		"max_series_id":          "9",
		"partition_check":        "series-id-modulo",
		"partition_mismatches":   "0",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
}

func TestAnalyzeSeriesFileSeriesIDFilterWithoutRange(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:           FormatSeriesFile,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
		QuerySeriesIDs:   []uint64{42, 2, 9, 2},
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.KeySamples, []string{"id:2 cpu,host=b"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	for key, want := range map[string]string{
		"query_series_id_filter_applied": "true",
		"query_series_ids":               "2,9,42",
		"query_matched_series_ids":       "2",
		"query_tombstone_series_ids":     "9",
		"query_missing_series_ids":       "42",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
}

func TestAnalyzeSeriesFileAutoSeriesIDFilterWithoutRange(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
		QuerySeriesIDs:   []uint64{42, 2, 9, 2},
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatSeriesFile; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	for key, want := range map[string]string{
		"query_series_id_filter_applied": "true",
		"query_series_ids":               "2,9,42",
		"query_matched_series_ids":       "2",
		"query_tombstone_series_ids":     "9",
		"query_missing_series_ids":       "42",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
}

func TestAnalyzeSeriesFileAutoPartitionSeriesIDFilterWithoutRange(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)
	partitionDir := filepath.Join(seriesDir, "00")

	report, err := Analyze(context.Background(), []string{partitionDir}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
		QuerySeriesIDs:   []uint64{42, 2, 9, 2},
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatSeriesFile; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	for key, want := range map[string]string{
		"query_series_id_filter_applied": "true",
		"query_series_ids":               "2,9,42",
		"query_matched_series_ids":       "",
		"query_tombstone_series_ids":     "9",
		"query_missing_series_ids":       "2,42",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
}

func TestSeriesIDFilterRequiresQueryRange(t *testing.T) {
	for _, tc := range []struct {
		name   string
		paths  []string
		format Format
		want   bool
	}{
		{name: "explicit series file", paths: []string{"missing"}, format: FormatSeriesFile, want: false},
		{name: "explicit tssp", paths: []string{"_series"}, format: FormatTSSP, want: true},
		{name: "auto series dir", paths: []string{filepath.Join("shard", "_series")}, format: FormatAuto, want: false},
		{name: "auto series partition", paths: []string{filepath.Join("shard", "_series", "00")}, format: FormatAuto, want: false},
		{name: "auto series segment", paths: []string{filepath.Join("shard", "_series", "00", "0000")}, format: FormatAuto, want: false},
		{name: "auto mixed", paths: []string{filepath.Join("shard", "_series"), "00000001-0001-00000000.tssp"}, format: FormatAuto, want: true},
		{name: "auto unknown", paths: []string{"missing.tssp"}, format: FormatAuto, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SeriesIDFilterRequiresQueryRange(tc.paths, tc.format); got != tc.want {
				t.Fatalf("SeriesIDFilterRequiresQueryRange() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestAnalyzeSeriesFileQueryFilters(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:            FormatSeriesFile,
		QueryMeasurements: []string{"mem", "cpu", "cpu"},
		QueryTags: []TagFilter{
			{Key: " host ", Value: " a "},
			{Key: "host", Value: "a"},
		},
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected series-file query summary")
	}
	if !query.MeasurementFilterApplied || !query.TagFilterApplied {
		t.Fatalf("query flags = measurement:%t tag:%t, want both true", query.MeasurementFilterApplied, query.TagFilterApplied)
	}
	if got, want := query.QueryMeasurements, []string{"cpu", "mem"}; !equalStrings(got, want) {
		t.Fatalf("query measurements = %v, want %v", got, want)
	}
	if got, want := query.QueryTags, []TagFilter{{Key: "host", Value: "a"}}; !equalTagFilters(got, want) {
		t.Fatalf("query tags = %+v, want %+v", got, want)
	}
	if got, want := query.MatchedMeasurements, []string{"cpu"}; !equalStrings(got, want) {
		t.Fatalf("matched measurements = %v, want %v", got, want)
	}
	if got, want := query.MissingMeasurements, []string{"mem"}; !equalStrings(got, want) {
		t.Fatalf("missing measurements = %v, want %v", got, want)
	}
	if got, want := query.MatchedTags, []TagFilter{{Key: "host", Value: "a"}}; !equalTagFilters(got, want) {
		t.Fatalf("matched tags = %+v, want %+v", got, want)
	}
	if len(query.MissingTags) != 0 {
		t.Fatalf("missing tags = %+v, want none", query.MissingTags)
	}
	if got, want := query.CandidateMeasurements, 1; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(1); got != want {
		t.Fatalf("series refs = %d, want %d", got, want)
	}
	if got, want := len(query.MeasurementSamples), 1; got != want {
		t.Fatalf("measurement samples = %d, want %d", got, want)
	}
	sample := query.MeasurementSamples[0]
	if got, want := sample.Name, "cpu"; got != want {
		t.Fatalf("sample name = %q, want %q", got, want)
	}
	if got, want := sample.SeriesCount, uint64(1); got != want {
		t.Fatalf("sample series count = %d, want %d", got, want)
	}
	if got, want := len(sample.Tags), 1; got != want {
		t.Fatalf("sample tags = %d, want %d", got, want)
	}
	if got, want := sample.Tags[0].Key, "host"; got != want {
		t.Fatalf("sample tag key = %q, want %q", got, want)
	}
	if got, want := sample.Tags[0].Values[0].Value, "a"; got != want {
		t.Fatalf("sample tag value = %q, want %q", got, want)
	}
	if got, want := sample.Tags[0].Values[0].SeriesCount, uint64(1); got != want {
		t.Fatalf("sample tag series count = %d, want %d", got, want)
	}
}

func TestAnalyzeSeriesFileQueryFiltersReportMissingTags(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:            FormatSeriesFile,
		QueryMeasurements: []string{"cpu"},
		QueryTags:         []TagFilter{{Key: "host", Value: "missing"}},
		KeySampleLimit:    5,
		BlockSampleLimit:  5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected series-file query summary")
	}
	if got, want := query.MatchedMeasurements, []string{"cpu"}; !equalStrings(got, want) {
		t.Fatalf("matched measurements = %v, want %v", got, want)
	}
	if got, want := query.MissingTags, []TagFilter{{Key: "host", Value: "missing"}}; !equalTagFilters(got, want) {
		t.Fatalf("missing tags = %+v, want %+v", got, want)
	}
	if got, want := query.CandidateMeasurements, 0; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(0); got != want {
		t.Fatalf("series refs = %d, want %d", got, want)
	}
	if len(query.MeasurementSamples) != 0 {
		t.Fatalf("measurement samples = %+v, want none", query.MeasurementSamples)
	}
}

func TestAnalyzeSeriesFileQueryFiltersRequireAllTagsOnSameSeries(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:            FormatSeriesFile,
		QueryMeasurements: []string{"cpu"},
		QueryTags: []TagFilter{
			{Key: "host", Value: "a"},
			{Key: "region", Value: "us"},
		},
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected series-file query summary")
	}
	if got, want := query.CandidateMeasurements, 1; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(1); got != want {
		t.Fatalf("series refs = %d, want %d", got, want)
	}
	if got, want := query.MatchedTags, []TagFilter{{Key: "host", Value: "a"}, {Key: "region", Value: "us"}}; !equalTagFilters(got, want) {
		t.Fatalf("matched tags = %+v, want %+v", got, want)
	}

	report, err = Analyze(context.Background(), []string{seriesDir}, Options{
		Format:            FormatSeriesFile,
		QueryMeasurements: []string{"cpu"},
		QueryTags: []TagFilter{
			{Key: "host", Value: "a"},
			{Key: "region", Value: "missing"},
		},
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	query = report.Files[0].Index.Query
	if got, want := query.CandidateMeasurements, 0; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(0); got != want {
		t.Fatalf("series refs = %d, want %d", got, want)
	}
	if got, want := query.MatchedTags, []TagFilter{{Key: "host", Value: "a"}}; !equalTagFilters(got, want) {
		t.Fatalf("matched tags = %+v, want %+v", got, want)
	}
	if got, want := query.MissingTags, []TagFilter{{Key: "region", Value: "missing"}}; !equalTagFilters(got, want) {
		t.Fatalf("missing tags = %+v, want %+v", got, want)
	}
}

func TestAnalyzeSeriesFileQueryMeasurementOnlyEnumeratesTagSamples(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:            FormatSeriesFile,
		QueryMeasurements: []string{"cpu"},
		KeySampleLimit:    5,
		BlockSampleLimit:  5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected series-file query summary")
	}
	if !query.MeasurementFilterApplied || query.TagFilterApplied {
		t.Fatalf("query flags = measurement:%t tag:%t, want measurement only", query.MeasurementFilterApplied, query.TagFilterApplied)
	}
	if got, want := query.CandidateMeasurements, 1; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(2); got != want {
		t.Fatalf("series refs = %d, want %d", got, want)
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
	if got, want := tags[0].Values[0].Value, "a"; got != want {
		t.Fatalf("first host value = %q, want %q", got, want)
	}
	if got, want := tags[0].Values[1].Value, "b"; got != want {
		t.Fatalf("second host value = %q, want %q", got, want)
	}
	if got, want := tags[1].Key, "region"; got != want {
		t.Fatalf("second sample tag key = %q, want %q", got, want)
	}
	if got, want := tags[1].Values[0].Value, "us"; got != want {
		t.Fatalf("region value = %q, want %q", got, want)
	}
}

func TestAnalyzeSeriesFileQueryTagOnlyScansAllMeasurements(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:           FormatSeriesFile,
		QueryTags:        []TagFilter{{Key: "host", Value: "a"}},
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected series-file query summary")
	}
	if query.MeasurementFilterApplied || !query.TagFilterApplied {
		t.Fatalf("query flags = measurement:%t tag:%t, want tag only", query.MeasurementFilterApplied, query.TagFilterApplied)
	}
	if len(query.MatchedMeasurements) != 0 || len(query.MissingMeasurements) != 0 {
		t.Fatalf("measurement matches = %v/%v, want none without measurement filter", query.MatchedMeasurements, query.MissingMeasurements)
	}
	if got, want := query.CandidateMeasurements, 1; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(1); got != want {
		t.Fatalf("series refs = %d, want %d", got, want)
	}
	if got, want := query.MatchedTags, []TagFilter{{Key: "host", Value: "a"}}; !equalTagFilters(got, want) {
		t.Fatalf("matched tags = %+v, want %+v", got, want)
	}
}

func TestAnalyzeSeriesFileQuerySampleLimitDoesNotCapCounts(t *testing.T) {
	seriesDir := filepath.Join(t.TempDir(), "_series")
	partition0 := filepath.Join(seriesDir, "00")
	partition1 := filepath.Join(seriesDir, "01")
	if err := os.MkdirAll(partition0, 0o755); err != nil {
		t.Fatalf("mkdir partition 00: %v", err)
	}
	if err := os.MkdirAll(partition1, 0o755); err != nil {
		t.Fatalf("mkdir partition 01: %v", err)
	}
	writeTestSeriesSegment(t, filepath.Join(partition0, "0000"), testSeriesInsert(1, "cpu"))
	writeTestSeriesSegment(t, filepath.Join(partition1, "0000"), testSeriesInsert(2, "disk"))

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:            FormatSeriesFile,
		QueryMeasurements: []string{"cpu", "disk"},
		KeySampleLimit:    1,
		BlockSampleLimit:  5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected series-file query summary")
	}
	if got, want := query.CandidateMeasurements, 2; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(2); got != want {
		t.Fatalf("series refs = %d, want %d", got, want)
	}
	if got, want := len(query.MeasurementSamples), 1; got != want {
		t.Fatalf("measurement samples = %d, want %d", got, want)
	}
	if got, want := query.MeasurementSamples[0].Name, "cpu"; got != want {
		t.Fatalf("first capped sample = %q, want %q", got, want)
	}
}

func TestAnalyzeSeriesFileReportsPartitionMismatches(t *testing.T) {
	seriesDir := filepath.Join(t.TempDir(), "_series")
	partition1 := filepath.Join(seriesDir, "01")
	if err := os.MkdirAll(partition1, 0o755); err != nil {
		t.Fatalf("mkdir partition 01: %v", err)
	}
	writeTestSeriesSegment(t, filepath.Join(partition1, "0000"),
		testSeriesInsert(1, "cpu", TagFilter{Key: "host", Value: "a"}),
		testSeriesTombstone(1),
	)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:           FormatSeriesFile,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["partition_check"], "series-id-modulo"; got != want {
		t.Fatalf("partition check = %q, want %q", got, want)
	}
	if got, want := file.Extra["partition_mismatches"], "2"; got != want {
		t.Fatalf("partition mismatches = %q, want %q", got, want)
	}
	if got, want := file.BlocksByType["series-partition-mismatch"], 2; got != want {
		t.Fatalf("partition mismatch blocks = %d, want %d", got, want)
	}
	if got, want := file.Extra["partition_mismatch_samples"], "01/0000 id:1 expected:00 actual:01 flag:insert;01/0000 id:1 expected:00 actual:01 flag:tombstone"; got != want {
		t.Fatalf("partition mismatch samples = %q, want %q", got, want)
	}
	if !containsString(file.Notices, "2 series file entry(s) are stored outside their expected ID partition") {
		t.Fatalf("notices = %v, want partition mismatch notice", file.Notices)
	}
}

func TestAnalyzeSeriesFilePartitionMismatchSampleCapKeepsFullCount(t *testing.T) {
	seriesDir := filepath.Join(t.TempDir(), "_series")
	partition1 := filepath.Join(seriesDir, "01")
	if err := os.MkdirAll(partition1, 0o755); err != nil {
		t.Fatalf("mkdir partition 01: %v", err)
	}
	writeTestSeriesSegment(t, filepath.Join(partition1, "0000"),
		testSeriesInsert(1, "cpu"),
		testSeriesInsert(9, "cpu"),
		testSeriesInsert(17, "cpu"),
		testSeriesInsert(25, "cpu"),
		testSeriesInsert(33, "cpu"),
		testSeriesInsert(41, "cpu"),
		testSeriesInsert(49, "cpu"),
	)

	report, err := Analyze(context.Background(), []string{seriesDir}, Options{
		Format:           FormatSeriesFile,
		KeySampleLimit:   3,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["partition_mismatches"], "7"; got != want {
		t.Fatalf("partition mismatches = %q, want %q", got, want)
	}
	samples := strings.Split(file.Extra["partition_mismatch_samples"], ";")
	if got, want := len(samples), 3; got != want {
		t.Fatalf("partition mismatch samples = %v, want %d samples", samples, want)
	}
	if got, want := samples[0], "01/0000 id:1 expected:00 actual:01 flag:insert"; got != want {
		t.Fatalf("first mismatch sample = %q, want %q", got, want)
	}
	if got, want := samples[2], "01/0000 id:17 expected:00 actual:01 flag:insert"; got != want {
		t.Fatalf("third mismatch sample = %q, want %q", got, want)
	}
	if !containsString(file.Notices, "7 series file entry(s) are stored outside their expected ID partition") {
		t.Fatalf("notices = %v, want full partition mismatch count", file.Notices)
	}
}

func TestAnalyzeSeriesSegmentFile(t *testing.T) {
	seriesDir := writeTestSeriesFile(t)
	segmentPath := filepath.Join(seriesDir, "00", "0000")

	report, err := Analyze(context.Background(), []string{segmentPath}, Options{
		Format:           FormatAuto,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatSeriesFile; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.Extra["layout"], "series-segment"; got != want {
		t.Fatalf("layout = %q, want %q", got, want)
	}
	if got, want := file.BlockCount, 3; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{"id:1 cpu,host=a,region=us"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
}

func writeTestSeriesFile(t *testing.T) string {
	t.Helper()

	seriesDir := filepath.Join(t.TempDir(), "_series")
	partition0 := filepath.Join(seriesDir, "00")
	partition1 := filepath.Join(seriesDir, "01")
	if err := os.MkdirAll(partition0, 0o755); err != nil {
		t.Fatalf("mkdir partition 00: %v", err)
	}
	if err := os.MkdirAll(partition1, 0o755); err != nil {
		t.Fatalf("mkdir partition 01: %v", err)
	}
	writeTestSeriesSegment(t, filepath.Join(partition0, "0000"),
		testSeriesInsert(1, "cpu", TagFilter{Key: "host", Value: "a"}, TagFilter{Key: "region", Value: "us"}),
		testSeriesInsert(9, "mem", TagFilter{Key: "host", Value: "b"}),
		testSeriesTombstone(9),
	)
	writeTestSeriesSegment(t, filepath.Join(partition1, "0000"),
		testSeriesInsert(2, "cpu", TagFilter{Key: "host", Value: "b"}),
	)
	if err := os.WriteFile(filepath.Join(partition0, "index"), []byte("local index placeholder"), 0o644); err != nil {
		t.Fatalf("write series index placeholder: %v", err)
	}
	return seriesDir
}

type testSeriesFileEntry struct {
	Flag        byte
	ID          uint64
	Measurement string
	Tags        []TagFilter
}

func testSeriesInsert(id uint64, measurement string, tags ...TagFilter) testSeriesFileEntry {
	return testSeriesFileEntry{
		Flag:        seriesEntryInsertFlag,
		ID:          id,
		Measurement: measurement,
		Tags:        tags,
	}
}

func testSeriesTombstone(id uint64) testSeriesFileEntry {
	return testSeriesFileEntry{
		Flag: seriesEntryTombstoneFlag,
		ID:   id,
	}
}

func writeTestSeriesSegment(t *testing.T, path string, entries ...testSeriesFileEntry) {
	t.Helper()

	data := []byte(seriesSegmentMagic)
	data = append(data, seriesSegmentVersion)
	for _, entry := range entries {
		data = append(data, entry.Flag)
		var idBuf [8]byte
		binary.BigEndian.PutUint64(idBuf[:], entry.ID)
		data = append(data, idBuf[:]...)
		if entry.Flag == seriesEntryInsertFlag {
			data = append(data, appendTestSeriesKey(nil, []byte(entry.Measurement), entry.Tags)...)
		}
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write series segment: %v", err)
	}
}

func appendTestSeriesKey(dst []byte, name []byte, tags []TagFilter) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	tagCountBuf := make([]byte, binary.MaxVarintLen64)
	tagCountSize := binary.PutUvarint(tagCountBuf, uint64(len(tags)))
	size := 2 + len(name) + tagCountSize
	for _, tag := range tags {
		size += 2 + len(tag.Key) + 2 + len(tag.Value)
	}
	totalSize := binary.PutUvarint(buf, uint64(size))
	dst = append(dst, buf[:totalSize]...)

	binary.BigEndian.PutUint16(buf[:2], uint16(len(name)))
	dst = append(dst, buf[:2]...)
	dst = append(dst, name...)
	dst = append(dst, tagCountBuf[:tagCountSize]...)
	for _, tag := range tags {
		binary.BigEndian.PutUint16(buf[:2], uint16(len(tag.Key)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Key...)
		binary.BigEndian.PutUint16(buf[:2], uint16(len(tag.Value)))
		dst = append(dst, buf[:2]...)
		dst = append(dst, tag.Value...)
	}
	return dst
}
