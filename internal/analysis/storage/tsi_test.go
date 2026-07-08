package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestAnalyzeTSIMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := writeTestTSI(path); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSI,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatTSI; got != want {
		t.Fatalf("format = %s, want %s", got, want)
	}
	if got, want := file.KeyCount, 2; got != want {
		t.Fatalf("measurement count = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Count, int64(3); got != want {
		t.Fatalf("series refs = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Min, uint64(1); got != want {
		t.Fatalf("series id min = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Max, uint64(3); got != want {
		t.Fatalf("series id max = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["measurement"], 2; got != want {
		t.Fatalf("measurement blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["tag-key"], 2; got != want {
		t.Fatalf("tag key blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["tag-value"], 3; got != want {
		t.Fatalf("tag value blocks = %d, want %d", got, want)
	}
	if got, want := file.KeySamples[0], "cpu"; got != want {
		t.Fatalf("first key sample = %q, want %q", got, want)
	}
	if file.Index == nil {
		t.Fatal("expected index summary")
	}
	if got, want := file.Index.Type, "tsi1"; got != want {
		t.Fatalf("index type = %q, want %q", got, want)
	}
	if got, want := file.Index.TagValueCount, 3; got != want {
		t.Fatalf("index tag value count = %d, want %d", got, want)
	}
	if got, want := file.Index.DeletedTagValueCount, 1; got != want {
		t.Fatalf("deleted tag value count = %d, want %d", got, want)
	}
	if got, want := file.Index.SeriesIDSetCardinality, int64(3); got != want {
		t.Fatalf("series id set cardinality = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.SeriesIDSetMin), uint64(1); got != want {
		t.Fatalf("series id set min = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.SeriesIDSetMax), uint64(3); got != want {
		t.Fatalf("series id set max = %d, want %d", got, want)
	}
	if got, want := file.Index.TombstoneSeriesIDSetCardinality, int64(1); got != want {
		t.Fatalf("tombstone series id set cardinality = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.TombstoneSeriesIDSetMin), uint64(4); got != want {
		t.Fatalf("tombstone series id set min = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.TombstoneSeriesIDSetMax), uint64(4); got != want {
		t.Fatalf("tombstone series id set max = %d, want %d", got, want)
	}
	if got, want := len(file.Index.MeasurementSamples), 2; got != want {
		t.Fatalf("measurement samples = %d, want %d", got, want)
	}
	if got, want := file.Index.MeasurementSamples[0].Name, "cpu"; got != want {
		t.Fatalf("first measurement sample = %q, want %q", got, want)
	}
	if got, want := file.Extra["series_id_set_cardinality"], "3"; got != want {
		t.Fatalf("series id set cardinality extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["series_id_set_min"], "1"; got != want {
		t.Fatalf("series id set min extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["series_id_set_max"], "3"; got != want {
		t.Fatalf("series id set max extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["tombstone_series_id_set_cardinality"], "1"; got != want {
		t.Fatalf("tombstone series id set cardinality extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["tombstone_series_id_set_min"], "4"; got != want {
		t.Fatalf("tombstone series id set min extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["tombstone_series_id_set_max"], "4"; got != want {
		t.Fatalf("tombstone series id set max extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["measurement_block_size"], "113"; got != want {
		t.Fatalf("measurement block size = %q, want %q", got, want)
	}
}

func TestAnalyzeTSINoticesCorruptSeriesIDSetCardinality(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := writeTestTSIWithSeriesIDSets(path, []byte{1, 2, 3}, writeTestTSIRoaringSeriesIDSet(4)); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSI,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.SeriesID.Count, int64(3); got != want {
		t.Fatalf("series id count fallback = %d, want %d", got, want)
	}
	if got, want := file.Index.SeriesIDSetCardinality, int64(0); got != want {
		t.Fatalf("series id set cardinality = %d, want %d", got, want)
	}
	if file.Index.SeriesIDSetMin != nil {
		t.Fatalf("series id set min = %d, want nil", *file.Index.SeriesIDSetMin)
	}
	if file.Index.SeriesIDSetMax != nil {
		t.Fatalf("series id set max = %d, want nil", *file.Index.SeriesIDSetMax)
	}
	if got, want := file.Index.TombstoneSeriesIDSetCardinality, int64(1); got != want {
		t.Fatalf("tombstone series id set cardinality = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.TombstoneSeriesIDSetMin), uint64(4); got != want {
		t.Fatalf("tombstone series id set min = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.TombstoneSeriesIDSetMax), uint64(4); got != want {
		t.Fatalf("tombstone series id set max = %d, want %d", got, want)
	}
	if _, ok := file.Extra["series_id_set_min"]; ok {
		t.Fatalf("series id set min extra present for corrupt bitmap: %v", file.Extra)
	}
	if _, ok := file.Extra["series_id_set_max"]; ok {
		t.Fatalf("series id set max extra present for corrupt bitmap: %v", file.Extra)
	}
	if len(file.Notices) != 1 {
		t.Fatalf("notices = %v, want one notice", file.Notices)
	}
	if got, want := file.Notices[0], "series id set cardinality unavailable"; len(got) < len(want) || got[:len(want)] != want {
		t.Fatalf("notice = %q, want prefix %q", got, want)
	}
	if len(report.Notices) != 1 {
		t.Fatalf("report notices = %v, want one propagated notice", report.Notices)
	}
}

func TestAnalyzeTSICorruptSeriesIDSetSingleRefDoesNotInferRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := writeTestTSIWithSingleSeriesRef(path, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSI,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.SeriesID.Count, int64(1); got != want {
		t.Fatalf("series id count fallback = %d, want %d", got, want)
	}
	if file.SeriesID.HasRange {
		t.Fatalf("series id range = %d..%d, want unavailable", file.SeriesID.Min, file.SeriesID.Max)
	}
	if got, want := seriesIDDetailsText("series_id", file.SeriesID), "series_id count=1"; got != want {
		t.Fatalf("series id details = %q, want %q", got, want)
	}
	if file.Index.SeriesIDSetMin != nil || file.Index.SeriesIDSetMax != nil {
		t.Fatalf("series id set range = %v..%v, want nil", file.Index.SeriesIDSetMin, file.Index.SeriesIDSetMax)
	}
}

func TestAnalyzeTSISeriesIDSetRangeIncludesZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := writeTestTSIWithSeriesIDSets(path, writeTestTSIRoaringSeriesIDSet(0, 2), nil); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSI,
		KeySampleLimit:   2,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.SeriesID.Min, uint64(0); got != want {
		t.Fatalf("series id min = %d, want %d", got, want)
	}
	if got, want := file.SeriesID.Max, uint64(2); got != want {
		t.Fatalf("series id max = %d, want %d", got, want)
	}
	if !file.SeriesID.HasRange {
		t.Fatal("series id range unavailable")
	}
	if got, want := seriesIDDetailsText("series_id", file.SeriesID), "series_id count=2 range=0..2"; got != want {
		t.Fatalf("series id details = %q, want %q", got, want)
	}
	if got, want := uint64PtrValue(file.Index.SeriesIDSetMin), uint64(0); got != want {
		t.Fatalf("series id set min = %d, want %d", got, want)
	}
	if got, want := uint64PtrValue(file.Index.SeriesIDSetMax), uint64(2); got != want {
		t.Fatalf("series id set max = %d, want %d", got, want)
	}
	if got, want := file.Extra["series_id_set_min"], "0"; got != want {
		t.Fatalf("series id set min extra = %q, want %q", got, want)
	}

	encoded, err := json.Marshal(file.Index)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"series_id_set_min":0`)) {
		t.Fatalf("index JSON = %s, want explicit zero min", encoded)
	}
}

func TestAnalyzeTSIRejectsTSILogFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsl")
	var buf bytes.Buffer
	appendTestTSILogEntry(&buf, tsiLogEntry{SeriesID: 1, Name: "cpu", Key: "host", Value: "a"})
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatTSI})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.Files) != 0 {
		t.Fatalf("files = %d, want 0", len(report.Files))
	}
	if !containsString(report.Notices, "L0-00000001.tsl uses tsi-log format, not tsi") {
		t.Fatalf("notices = %v, want tsi-log format warning", report.Notices)
	}
}

func TestAnalyzeTSIRejectsDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{Format: FormatTSI})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if len(report.Files) != 0 {
		t.Fatalf("files = %d, want 0", len(report.Files))
	}
	if !containsString(report.Notices, "tsi format requires a .tsi index file, got directory L0-00000001.tsi") {
		t.Fatalf("notices = %v, want directory warning", report.Notices)
	}
}

func TestTSIRoaringCardinalityRunContainer(t *testing.T) {
	var data bytes.Buffer
	writeTSILittleUint32(&data, uint32(tsiRoaringSerialCookie)|uint32(0)<<16)
	data.WriteByte(0x01)
	writeTSILittleUint16(&data, 0)
	writeTSILittleUint16(&data, 4)
	writeTSILittleUint16(&data, 2)
	writeTSILittleUint16(&data, 10)
	writeTSILittleUint16(&data, 2)
	writeTSILittleUint16(&data, 20)
	writeTSILittleUint16(&data, 1)

	stats, err := tsiRoaringStats(data.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := stats.Cardinality, uint64(5); got != want {
		t.Fatalf("cardinality = %d, want %d", got, want)
	}
	ids, err := tsiRoaringSeriesIDs(data.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ids, []uint64{10, 11, 12, 20, 21}; !equalUint64s(got, want) {
		t.Fatalf("series ids = %v, want %v", got, want)
	}
	if !stats.HasRange {
		t.Fatal("stats range unavailable")
	}
	if got, want := stats.Min, uint64(10); got != want {
		t.Fatalf("stats min = %d, want %d", got, want)
	}
	if got, want := stats.Max, uint64(21); got != want {
		t.Fatalf("stats max = %d, want %d", got, want)
	}
}

func TestTSIRoaringCardinalityNoRunMultipleContainers(t *testing.T) {
	data := writeTestTSIRoaringSeriesIDSet(1, 1<<16+2, 2<<16+3)

	stats, err := tsiRoaringStats(data)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := stats.Cardinality, uint64(3); got != want {
		t.Fatalf("cardinality = %d, want %d", got, want)
	}
	ids, err := tsiRoaringSeriesIDs(data)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ids, []uint64{1, 1<<16 + 2, 2<<16 + 3}; !equalUint64s(got, want) {
		t.Fatalf("series ids = %v, want %v", got, want)
	}
	if !stats.HasRange {
		t.Fatal("stats range unavailable")
	}
	if got, want := stats.Min, uint64(1); got != want {
		t.Fatalf("stats min = %d, want %d", got, want)
	}
	if got, want := stats.Max, uint64(2<<16+3); got != want {
		t.Fatalf("stats max = %d, want %d", got, want)
	}
}

func TestTSIRoaringSeriesIDsBitmapContainer(t *testing.T) {
	var data bytes.Buffer
	writeTSILittleUint32(&data, tsiRoaringSerialCookieNoRunContainer)
	writeTSILittleUint32(&data, 1)
	writeTSILittleUint16(&data, 0)
	writeTSILittleUint16(&data, 4096)
	writeTSILittleUint32(&data, 16)
	bitmap := make([]byte, tsiRoaringBitmapContainerBytes)
	for i := 0; i <= tsiRoaringArrayContainerMaxSize; i++ {
		bitmap[i/8] |= 1 << uint(i%8)
	}
	data.Write(bitmap)

	stats, err := tsiRoaringStats(data.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := stats.Cardinality, uint64(4097); got != want {
		t.Fatalf("cardinality = %d, want %d", got, want)
	}
	ids, err := tsiRoaringSeriesIDs(data.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(ids), 4097; got != want {
		t.Fatalf("series id count = %d, want %d", got, want)
	}
	if got, want := ids[0], uint64(0); got != want {
		t.Fatalf("first series id = %d, want %d", got, want)
	}
	if got, want := ids[len(ids)-1], uint64(4096); got != want {
		t.Fatalf("last series id = %d, want %d", got, want)
	}
	if !stats.HasRange {
		t.Fatal("stats range unavailable")
	}
	if got, want := stats.Min, uint64(0); got != want {
		t.Fatalf("stats min = %d, want %d", got, want)
	}
	if got, want := stats.Max, uint64(4096); got != want {
		t.Fatalf("stats max = %d, want %d", got, want)
	}
}

func TestAnalyzeTSIQueryFilters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := writeTestTSI(path); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatTSI,
		QueryMeasurements: []string{"mem", "cpu", "cpu", " "},
		QueryTags: []TagFilter{
			{Key: " host ", Value: " a "},
			{Key: "host", Value: "a"},
		},
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected TSI query summary")
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
	if got, want := query.MatchedMeasurements, []string{"cpu", "mem"}; !equalStrings(got, want) {
		t.Fatalf("matched measurements = %v, want %v", got, want)
	}
	if got, want := query.MatchedTags, []TagFilter{{Key: "host", Value: "a"}}; !equalTagFilters(got, want) {
		t.Fatalf("matched tags = %+v, want %+v", got, want)
	}
	if got, want := query.CandidateMeasurements, 2; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.SeriesRefs, int64(2); got != want {
		t.Fatalf("query series refs = %d, want %d", got, want)
	}
	if got, want := len(query.MeasurementSamples), 2; got != want {
		t.Fatalf("query measurement samples = %d, want %d", got, want)
	}
	if got, want := query.MeasurementSamples[0].Name, "cpu"; got != want {
		t.Fatalf("first query sample = %q, want %q", got, want)
	}
	if got, want := query.MeasurementSamples[0].Tags[0].Key, "host"; got != want {
		t.Fatalf("first query tag key = %q, want %q", got, want)
	}
	if got, want := query.MeasurementSamples[0].Tags[0].Values[0].Value, "a"; got != want {
		t.Fatalf("first query tag value = %q, want %q", got, want)
	}
}

func TestAnalyzeTSIQueryFiltersIntersectTagSeriesIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := writeTestTSIWithIntersectingTags(path); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatTSI,
		QueryMeasurements: []string{"cpu"},
		QueryTags: []TagFilter{
			{Key: "host", Value: "a"},
			{Key: "region", Value: "us"},
			{Key: "zone", Value: "west"},
		},
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
	if got, want := query.MatchedTags, []TagFilter{
		{Key: "host", Value: "a"},
		{Key: "region", Value: "us"},
		{Key: "zone", Value: "west"},
	}; !equalTagFilters(got, want) {
		t.Fatalf("matched tags = %+v, want %+v", got, want)
	}
	if len(query.MissingTags) != 0 {
		t.Fatalf("missing tags = %+v, want none", query.MissingTags)
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
}

func TestAnalyzeTSIQueryFiltersRequireSameSeriesIntersection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := writeTestTSIWithIntersectingTags(path); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatTSI,
		QueryMeasurements: []string{"cpu"},
		QueryTags: []TagFilter{
			{Key: "host", Value: "a"},
			{Key: "region", Value: "eu"},
		},
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
	if got, want := query.MatchedTags, []TagFilter{
		{Key: "host", Value: "a"},
		{Key: "region", Value: "eu"},
	}; !equalTagFilters(got, want) {
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

func TestAnalyzeTSIQueryFiltersIgnoreDeletedTagValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "L0-00000001.tsi")
	if err := writeTestTSI(path); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatTSI,
		QueryTags:        []TagFilter{{Key: "host", Value: "b"}},
		KeySampleLimit:   3,
		BlockSampleLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	query := report.Files[0].Index.Query
	if query == nil {
		t.Fatal("expected TSI query summary")
	}
	if got, want := query.CandidateMeasurements, 0; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := query.MissingTags, []TagFilter{{Key: "host", Value: "b"}}; !equalTagFilters(got, want) {
		t.Fatalf("missing tags = %+v, want %+v", got, want)
	}
	if len(query.MatchedTags) != 0 {
		t.Fatalf("matched tags = %+v, want none", query.MatchedTags)
	}
}

func writeTestTSI(path string) error {
	return writeTestTSIWithSeriesIDSets(
		path,
		writeTestTSIRoaringSeriesIDSet(1, 2, 3),
		writeTestTSIRoaringSeriesIDSet(4),
	)
}

func writeTestTSIWithSeriesIDSets(path string, seriesIDSet, tombstoneSeriesIDSet []byte) error {
	var buf bytes.Buffer
	buf.WriteString(tsiMagic)

	cpuTags := writeTestTSITagBlock([]testTSITagKey{{
		name: "host",
		values: []testTSITagValue{
			{name: "a", seriesIDs: []uint64{1}},
			{name: "b", seriesIDs: []uint64{2}, deleted: true},
		},
	}})
	cpuTagOffset := int64(buf.Len())
	buf.Write(cpuTags)

	memTags := writeTestTSITagBlock([]testTSITagKey{{
		name: "host",
		values: []testTSITagValue{
			{name: "a", seriesIDs: []uint64{3}},
		},
	}})
	memTagOffset := int64(buf.Len())
	buf.Write(memTags)

	measurementBlock := writeTestTSIMeasurementBlock([]testTSIMeasurement{
		{name: "cpu", tagOffset: cpuTagOffset, tagSize: int64(len(cpuTags)), seriesCount: 2},
		{name: "mem", tagOffset: memTagOffset, tagSize: int64(len(memTags)), seriesCount: 1},
	})
	measurementBlockOffset := int64(buf.Len())
	buf.Write(measurementBlock)

	seriesIDSetOffset := int64(buf.Len())
	buf.Write(seriesIDSet)
	tombstoneSeriesIDSetOffset := int64(buf.Len())
	buf.Write(tombstoneSeriesIDSet)

	writeTestTSIIndexTrailer(&buf, tsiIndexTrailer{
		Version: tsiIndexFileVersion,
		MeasurementBlock: tsiRange{
			Offset: measurementBlockOffset,
			Size:   int64(len(measurementBlock)),
		},
		SeriesIDSet: tsiRange{
			Offset: seriesIDSetOffset,
			Size:   int64(len(seriesIDSet)),
		},
		TombstoneSeriesIDSet: tsiRange{
			Offset: tombstoneSeriesIDSetOffset,
			Size:   int64(len(tombstoneSeriesIDSet)),
		},
		SeriesSketch: tsiRange{
			Offset: int64(buf.Len()),
			Size:   0,
		},
		TombstoneSeriesSketch: tsiRange{
			Offset: int64(buf.Len()),
			Size:   0,
		},
	})

	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSIWithSingleSeriesRef(path string, seriesIDSet []byte) error {
	var buf bytes.Buffer
	buf.WriteString(tsiMagic)

	cpuTags := writeTestTSITagBlock([]testTSITagKey{{
		name: "host",
		values: []testTSITagValue{
			{name: "a", seriesIDs: []uint64{1}},
		},
	}})
	cpuTagOffset := int64(buf.Len())
	buf.Write(cpuTags)

	measurementBlock := writeTestTSIMeasurementBlock([]testTSIMeasurement{
		{name: "cpu", tagOffset: cpuTagOffset, tagSize: int64(len(cpuTags)), seriesCount: 1},
	})
	measurementBlockOffset := int64(buf.Len())
	buf.Write(measurementBlock)

	seriesIDSetOffset := int64(buf.Len())
	buf.Write(seriesIDSet)
	tombstoneSeriesIDSetOffset := int64(buf.Len())

	writeTestTSIIndexTrailer(&buf, tsiIndexTrailer{
		Version: tsiIndexFileVersion,
		MeasurementBlock: tsiRange{
			Offset: measurementBlockOffset,
			Size:   int64(len(measurementBlock)),
		},
		SeriesIDSet: tsiRange{
			Offset: seriesIDSetOffset,
			Size:   int64(len(seriesIDSet)),
		},
		TombstoneSeriesIDSet: tsiRange{
			Offset: tombstoneSeriesIDSetOffset,
			Size:   0,
		},
		SeriesSketch: tsiRange{
			Offset: int64(buf.Len()),
			Size:   0,
		},
		TombstoneSeriesSketch: tsiRange{
			Offset: int64(buf.Len()),
			Size:   0,
		},
	})

	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSIWithIntersectingTags(path string) error {
	var buf bytes.Buffer
	buf.WriteString(tsiMagic)

	cpuTags := writeTestTSITagBlock([]testTSITagKey{
		{
			name: "host",
			values: []testTSITagValue{
				{name: "a", seriesIDs: []uint64{1, 2}},
			},
		},
		{
			name: "region",
			values: []testTSITagValue{
				{name: "eu", seriesIDs: []uint64{3}},
				{name: "us", seriesIDs: []uint64{2}},
			},
		},
		{
			name: "zone",
			values: []testTSITagValue{
				{name: "west", seriesIDs: []uint64{2, 3}, legacySeriesIDs: true},
			},
		},
	})
	cpuTagOffset := int64(buf.Len())
	buf.Write(cpuTags)

	measurementBlock := writeTestTSIMeasurementBlock([]testTSIMeasurement{
		{name: "cpu", tagOffset: cpuTagOffset, tagSize: int64(len(cpuTags)), seriesCount: 3},
	})
	measurementBlockOffset := int64(buf.Len())
	buf.Write(measurementBlock)

	seriesIDSet := writeTestTSIRoaringSeriesIDSet(1, 2, 3)
	seriesIDSetOffset := int64(buf.Len())
	buf.Write(seriesIDSet)
	tombstoneSeriesIDSetOffset := int64(buf.Len())

	writeTestTSIIndexTrailer(&buf, tsiIndexTrailer{
		Version: tsiIndexFileVersion,
		MeasurementBlock: tsiRange{
			Offset: measurementBlockOffset,
			Size:   int64(len(measurementBlock)),
		},
		SeriesIDSet: tsiRange{
			Offset: seriesIDSetOffset,
			Size:   int64(len(seriesIDSet)),
		},
		TombstoneSeriesIDSet: tsiRange{
			Offset: tombstoneSeriesIDSetOffset,
			Size:   0,
		},
		SeriesSketch: tsiRange{
			Offset: int64(buf.Len()),
			Size:   0,
		},
		TombstoneSeriesSketch: tsiRange{
			Offset: int64(buf.Len()),
			Size:   0,
		},
	})

	return os.WriteFile(path, buf.Bytes(), 0o600)
}

func writeTestTSIRoaringSeriesIDSet(ids ...uint64) []byte {
	type container struct {
		key    uint16
		values []uint16
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	containers := []container{}
	for _, id := range ids {
		key := uint16(id >> 16)
		value := uint16(id)
		if len(containers) == 0 || containers[len(containers)-1].key != key {
			containers = append(containers, container{key: key})
		}
		last := &containers[len(containers)-1]
		if len(last.values) == 0 || last.values[len(last.values)-1] != value {
			last.values = append(last.values, value)
		}
	}

	var buf bytes.Buffer
	writeTSILittleUint32(&buf, tsiRoaringSerialCookieNoRunContainer)
	writeTSILittleUint32(&buf, uint32(len(containers)))
	for _, container := range containers {
		writeTSILittleUint16(&buf, container.key)
		writeTSILittleUint16(&buf, uint16(len(container.values)-1))
	}
	offset := uint32(8 + len(containers)*8)
	for _, container := range containers {
		writeTSILittleUint32(&buf, offset)
		offset += uint32(len(container.values) * 2)
	}
	for _, container := range containers {
		for _, value := range container.values {
			writeTSILittleUint16(&buf, value)
		}
	}
	return buf.Bytes()
}

func writeTSILittleUint32(buf *bytes.Buffer, value uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], value)
	buf.Write(b[:])
}

func writeTSILittleUint16(buf *bytes.Buffer, value uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], value)
	buf.Write(b[:])
}

type testTSIMeasurement struct {
	name        string
	tagOffset   int64
	tagSize     int64
	seriesCount uint64
	deleted     bool
}

type testTSITagKey struct {
	name    string
	deleted bool
	values  []testTSITagValue
}

type testTSITagValue struct {
	name            string
	seriesCount     uint64
	seriesIDs       []uint64
	legacySeriesIDs bool
	deleted         bool
}

func writeTestTSIMeasurementBlock(measurements []testTSIMeasurement) []byte {
	var data bytes.Buffer
	data.WriteByte(0)
	for _, measurement := range measurements {
		flag := byte(0)
		if measurement.deleted {
			flag |= tsiMeasurementTombstoneFlag
		}
		data.WriteByte(flag)
		writeUint64(&data, uint64(measurement.tagOffset))
		writeUint64(&data, uint64(measurement.tagSize))
		data.Write(binary.AppendUvarint(nil, uint64(len(measurement.name))))
		data.WriteString(measurement.name)
		data.Write(binary.AppendUvarint(nil, measurement.seriesCount))
		data.Write(binary.AppendUvarint(nil, 0))
	}

	var buf bytes.Buffer
	buf.Write(data.Bytes())
	writeTestTSIMeasurementTrailer(&buf, tsiMeasurementBlockTrailer{
		Version: tsiMeasurementBlockVersion,
		Data: tsiRange{
			Offset: 0,
			Size:   int64(data.Len()),
		},
		HashIndex: tsiRange{
			Offset: int64(data.Len()),
			Size:   0,
		},
		Sketch: tsiRange{
			Offset: int64(data.Len()),
			Size:   0,
		},
		TSketch: tsiRange{
			Offset: int64(data.Len()),
			Size:   0,
		},
	})
	return buf.Bytes()
}

func writeTestTSITagBlock(keys []testTSITagKey) []byte {
	var valueData bytes.Buffer
	var keyData bytes.Buffer
	for _, key := range keys {
		valueOffset := uint64(valueData.Len())
		for _, value := range key.values {
			writeTestTSITagValueElem(&valueData, value)
		}
		valueSize := uint64(valueData.Len()) - valueOffset

		flag := byte(0)
		if key.deleted {
			flag |= tsiTagKeyTombstoneFlag
		}
		keyData.WriteByte(flag)
		writeUint64(&keyData, valueOffset)
		writeUint64(&keyData, valueSize)
		writeUint64(&keyData, 0)
		writeUint64(&keyData, 0)
		keyData.Write(binary.AppendUvarint(nil, uint64(len(key.name))))
		keyData.WriteString(key.name)
	}

	var buf bytes.Buffer
	buf.Write(valueData.Bytes())
	keyOffset := int64(buf.Len())
	buf.Write(keyData.Bytes())
	trailer := tsiTagBlockTrailer{
		Version: tsiTagBlockVersion,
		ValueData: tsiRange{
			Offset: 0,
			Size:   int64(valueData.Len()),
		},
		KeyData: tsiRange{
			Offset: keyOffset,
			Size:   int64(keyData.Len()),
		},
		HashIndex: tsiRange{
			Offset: int64(buf.Len()),
			Size:   0,
		},
		Size: int64(buf.Len()) + tsiTagBlockTrailerSize,
	}
	writeTestTSITagTrailer(&buf, trailer)
	return buf.Bytes()
}

func writeTestTSITagValueElem(buf *bytes.Buffer, value testTSITagValue) {
	flag := byte(0)
	if value.deleted {
		flag |= tsiTagValueTombstoneFlag
	}
	var seriesData []byte
	if len(value.seriesIDs) > 0 {
		if value.legacySeriesIDs {
			seriesData = writeTestTSIDeltaSeriesIDs(value.seriesIDs...)
		} else {
			flag |= tsiTagValueSeriesIDSetFlag
			seriesData = writeTestTSIRoaringSeriesIDSet(value.seriesIDs...)
		}
		if value.seriesCount == 0 {
			value.seriesCount = uint64(len(uniqueSortedUint64s(value.seriesIDs)))
		}
	}
	buf.WriteByte(flag)
	buf.Write(binary.AppendUvarint(nil, uint64(len(value.name))))
	buf.WriteString(value.name)
	buf.Write(binary.AppendUvarint(nil, value.seriesCount))
	buf.Write(binary.AppendUvarint(nil, uint64(len(seriesData))))
	buf.Write(seriesData)
}

func writeTestTSIDeltaSeriesIDs(ids ...uint64) []byte {
	ids = uniqueSortedUint64s(ids)
	var buf bytes.Buffer
	var prev uint64
	for _, id := range ids {
		buf.Write(binary.AppendUvarint(nil, id-prev))
		prev = id
	}
	return buf.Bytes()
}

func uniqueSortedUint64s(values []uint64) []uint64 {
	values = append([]uint64(nil), values...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	out := values[:0]
	var prev uint64
	for i, value := range values {
		if i > 0 && value == prev {
			continue
		}
		out = append(out, value)
		prev = value
	}
	return out
}

func uint64PtrValue(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}

func writeTestTSIIndexTrailer(buf *bytes.Buffer, trailer tsiIndexTrailer) {
	writeTestTSIRange(buf, trailer.MeasurementBlock)
	writeTestTSIRange(buf, trailer.SeriesIDSet)
	writeTestTSIRange(buf, trailer.TombstoneSeriesIDSet)
	writeTestTSIRange(buf, trailer.SeriesSketch)
	writeTestTSIRange(buf, trailer.TombstoneSeriesSketch)
	writeUint16(buf, uint16(trailer.Version))
}

func writeTestTSIMeasurementTrailer(buf *bytes.Buffer, trailer tsiMeasurementBlockTrailer) {
	writeTestTSIRange(buf, trailer.Data)
	writeTestTSIRange(buf, trailer.HashIndex)
	writeTestTSIRange(buf, trailer.Sketch)
	writeTestTSIRange(buf, trailer.TSketch)
	writeUint16(buf, uint16(trailer.Version))
}

func writeTestTSITagTrailer(buf *bytes.Buffer, trailer tsiTagBlockTrailer) {
	writeTestTSIRange(buf, trailer.ValueData)
	writeTestTSIRange(buf, trailer.KeyData)
	writeTestTSIRange(buf, trailer.HashIndex)
	writeUint64(buf, uint64(trailer.Size))
	writeUint16(buf, uint16(trailer.Version))
}

func writeTestTSIRange(buf *bytes.Buffer, rng tsiRange) {
	writeUint64(buf, uint64(rng.Offset))
	writeUint64(buf, uint64(rng.Size))
}

func equalTagFilters(a, b []TagFilter) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
