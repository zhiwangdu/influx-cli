package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
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
	if got, want := len(file.Index.MeasurementSamples), 2; got != want {
		t.Fatalf("measurement samples = %d, want %d", got, want)
	}
	if got, want := file.Index.MeasurementSamples[0].Name, "cpu"; got != want {
		t.Fatalf("first measurement sample = %q, want %q", got, want)
	}
	if got, want := file.Extra["measurement_block_size"], "113"; got != want {
		t.Fatalf("measurement block size = %q, want %q", got, want)
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
	if got, want := query.SeriesRefs, int64(3); got != want {
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
	var buf bytes.Buffer
	buf.WriteString(tsiMagic)

	cpuTags := writeTestTSITagBlock([]testTSITagKey{{
		name: "host",
		values: []testTSITagValue{
			{name: "a", seriesCount: 1},
			{name: "b", seriesCount: 1, deleted: true},
		},
	}})
	cpuTagOffset := int64(buf.Len())
	buf.Write(cpuTags)

	memTags := writeTestTSITagBlock([]testTSITagKey{{
		name: "host",
		values: []testTSITagValue{
			{name: "a", seriesCount: 1},
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
	buf.Write([]byte{1, 2, 3})
	tombstoneSeriesIDSetOffset := int64(buf.Len())
	buf.Write([]byte{4})

	writeTestTSIIndexTrailer(&buf, tsiIndexTrailer{
		Version: tsiIndexFileVersion,
		MeasurementBlock: tsiRange{
			Offset: measurementBlockOffset,
			Size:   int64(len(measurementBlock)),
		},
		SeriesIDSet: tsiRange{
			Offset: seriesIDSetOffset,
			Size:   3,
		},
		TombstoneSeriesIDSet: tsiRange{
			Offset: tombstoneSeriesIDSetOffset,
			Size:   1,
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
	name        string
	seriesCount uint64
	deleted     bool
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
	buf.WriteByte(flag)
	buf.Write(binary.AppendUvarint(nil, uint64(len(value.name))))
	buf.WriteString(value.name)
	buf.Write(binary.AppendUvarint(nil, value.seriesCount))
	buf.Write(binary.AppendUvarint(nil, 0))
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
