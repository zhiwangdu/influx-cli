package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeFieldsIndexAppliesChangeLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, fieldsIndexFileName)
	writeTestFieldsIndex(t, path, []fieldsIndexMeasurement{
		{Name: "cpu", Fields: []fieldsIndexField{
			{Name: "value", Type: 1},
			{Name: "temp", Type: 2},
		}},
		{Name: "mem", Fields: []fieldsIndexField{
			{Name: "used", Type: 4},
		}},
	})
	writeTestFieldsIndexLog(t, filepath.Join(dir, fieldsIndexLogFileName), []fieldsIndexChange{
		{Measurement: "cpu", FieldName: "status", FieldType: 3, Change: fieldsIndexChangeAddMeasurementField},
		{Measurement: "disk", FieldName: "free", FieldType: 9, Change: fieldsIndexChangeAddMeasurementField},
		{Measurement: "mem", Change: fieldsIndexChangeDeleteMeasurement},
	})

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
	if got, want := file.Format, FormatFieldsIndex; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.KeyCount, 2; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.BlockCount, 9; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["measurement-fields"], 2; got != want {
		t.Fatalf("measurement blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["field"], 4; got != want {
		t.Fatalf("field blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["fields-index-change-set"], 1; got != want {
		t.Fatalf("change set blocks = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["fields-index-add-field"], 2; got != want {
		t.Fatalf("add field changes = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["fields-index-delete-measurement"], 1; got != want {
		t.Fatalf("delete measurement changes = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{
		"cpu status:string,temp:integer,value:float",
		"disk free:unsigned",
	}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if file.Fields == nil {
		t.Fatalf("fields summary is nil")
	}
	if got, want := file.Fields.MeasurementCount, 2; got != want {
		t.Fatalf("measurement count = %d, want %d", got, want)
	}
	if got, want := file.Fields.FieldCount, 4; got != want {
		t.Fatalf("field count = %d, want %d", got, want)
	}
	if got, want := file.Fields.FieldsByType["float"], 1; got != want {
		t.Fatalf("float fields = %d, want %d", got, want)
	}
	if got, want := file.Fields.FieldsByType["integer"], 1; got != want {
		t.Fatalf("integer fields = %d, want %d", got, want)
	}
	if got, want := file.Fields.FieldsByType["string"], 1; got != want {
		t.Fatalf("string fields = %d, want %d", got, want)
	}
	if got, want := file.Fields.FieldsByType["unsigned"], 1; got != want {
		t.Fatalf("unsigned fields = %d, want %d", got, want)
	}
	if got, want := file.Fields.ChangeSetCount, 1; got != want {
		t.Fatalf("change set count = %d, want %d", got, want)
	}
	if got, want := file.Fields.ChangeCount, 3; got != want {
		t.Fatalf("change count = %d, want %d", got, want)
	}
	if got, want := file.Fields.AddFieldChanges, 2; got != want {
		t.Fatalf("add field changes = %d, want %d", got, want)
	}
	if got, want := file.Fields.DeleteMeasurements, 1; got != want {
		t.Fatalf("delete measurements = %d, want %d", got, want)
	}
	for key, want := range map[string]string{
		"layout":                   "fields-index",
		"encoding":                 "protobuf",
		"main_file_present":        "true",
		"main_measurement_count":   "2",
		"main_field_count":         "3",
		"change_log_present":       "true",
		"change_set_count":         "1",
		"change_count":             "3",
		"add_field_change_count":   "2",
		"delete_measurement_count": "1",
		"field_count":              "4",
		"fields_by_type":           "float:1,integer:1,string:1,unsigned:1",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
	if len(file.Notices) != 0 {
		t.Fatalf("notices = %v, want none", file.Notices)
	}
}

func TestAnalyzeFieldsIndexLogOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), fieldsIndexLogFileName)
	writeTestFieldsIndexLog(t, path, []fieldsIndexChange{
		{Measurement: "cpu", FieldName: "value", FieldType: 1, Change: fieldsIndexChangeAddMeasurementField},
		{Measurement: "mem", FieldName: "used", FieldType: 4, Change: fieldsIndexChangeAddMeasurementField},
	})

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatFieldsIndex,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.Extra["layout"], "fields-index-log"; got != want {
		t.Fatalf("layout = %q, want %q", got, want)
	}
	if got, want := file.Extra["main_file_present"], "false"; got != want {
		t.Fatalf("main file present = %q, want %q", got, want)
	}
	if got, want := file.KeySamples, []string{
		"cpu value:float",
		"mem used:boolean",
	}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
}

func TestAnalyzeFieldsIndexQueryMeasurements(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, fieldsIndexFileName)
	writeTestFieldsIndex(t, path, []fieldsIndexMeasurement{
		{Name: "cpu", Fields: []fieldsIndexField{{Name: "value", Type: 1}}},
		{Name: "mem", Fields: []fieldsIndexField{{Name: "used", Type: 4}}},
	})
	writeTestFieldsIndexLog(t, filepath.Join(dir, fieldsIndexLogFileName), []fieldsIndexChange{
		{Measurement: "disk", FieldName: "free", FieldType: 9, Change: fieldsIndexChangeAddMeasurementField},
		{Measurement: "mem", Change: fieldsIndexChangeDeleteMeasurement},
	})

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:            FormatFieldsIndex,
		KeySampleLimit:    1,
		BlockSampleLimit:  5,
		QueryMeasurements: []string{" disk ", "cpu", "cpu", "mem"},
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if file.Index == nil || file.Index.Query == nil {
		t.Fatalf("index query summary is nil")
	}
	query := file.Index.Query
	if !query.MeasurementFilterApplied {
		t.Fatalf("measurement filter applied = false, want true")
	}
	if got, want := query.QueryMeasurements, []string{"cpu", "disk", "mem"}; !equalStrings(got, want) {
		t.Fatalf("query measurements = %v, want %v", got, want)
	}
	if got, want := query.MatchedMeasurements, []string{"cpu", "disk"}; !equalStrings(got, want) {
		t.Fatalf("matched measurements = %v, want %v", got, want)
	}
	if got, want := query.MissingMeasurements, []string{"mem"}; !equalStrings(got, want) {
		t.Fatalf("missing measurements = %v, want %v", got, want)
	}
	if got, want := query.CandidateMeasurements, 2; got != want {
		t.Fatalf("candidate measurements = %d, want %d", got, want)
	}
	if got, want := len(query.MeasurementSamples), 1; got != want {
		t.Fatalf("query measurement samples = %d, want %d", got, want)
	}
	if got, want := query.MeasurementSamples[0].Name, "cpu"; got != want {
		t.Fatalf("query measurement sample = %q, want %q", got, want)
	}

	details := report.Result().Table.Rows[0][tableColumnIndex(t, report.Result().Table.Columns, "details")].(string)
	for _, want := range []string{
		"index measurements=2",
		"query measurement_filter=true measurements=3/2/1 candidates=2",
		"fields measurements=2 fields=2",
	} {
		if !strings.Contains(details, want) {
			t.Fatalf("details = %q, want %q", details, want)
		}
	}
}

func TestAnalyzeFieldsIndexTruncatedChangeLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, fieldsIndexFileName)
	writeTestFieldsIndex(t, path, []fieldsIndexMeasurement{
		{Name: "cpu", Fields: []fieldsIndexField{{Name: "value", Type: 1}}},
	})
	logPath := filepath.Join(dir, fieldsIndexLogFileName)
	data := encodeTestFieldsIndexLog([]fieldsIndexChange{
		{Measurement: "mem", FieldName: "used", FieldType: 4, Change: fieldsIndexChangeAddMeasurementField},
	})
	var prefix [8]byte
	binary.LittleEndian.PutUint64(prefix[:], 100)
	data = append(data, prefix[:]...)
	data = append(data, []byte{1, 2, 3}...)
	if err := os.WriteFile(logPath, data, 0o644); err != nil {
		t.Fatalf("write truncated fields index log: %v", err)
	}

	report, err := Analyze(context.Background(), []string{path}, Options{
		Format:           FormatFieldsIndex,
		KeySampleLimit:   5,
		BlockSampleLimit: 5,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	file := report.Files[0]
	if got, want := file.KeySamples, []string{
		"cpu value:float",
		"mem used:boolean",
	}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if len(file.Notices) != 1 || !strings.Contains(file.Notices[0], "trailing fields index change set") {
		t.Fatalf("notices = %v, want trailing change set notice", file.Notices)
	}
}

func writeTestFieldsIndex(t *testing.T, path string, measurements []fieldsIndexMeasurement) {
	t.Helper()

	data := append([]byte(nil), fieldsIndexMagicNumber...)
	data = append(data, encodeTestFieldsIndexMeasurementFieldSet(measurements)...)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write fields index: %v", err)
	}
}

func writeTestFieldsIndexLog(t *testing.T, path string, changeSets ...[]fieldsIndexChange) {
	t.Helper()

	if err := os.WriteFile(path, encodeTestFieldsIndexLog(changeSets...), 0o644); err != nil {
		t.Fatalf("write fields index log: %v", err)
	}
}

func encodeTestFieldsIndexMeasurementFieldSet(measurements []fieldsIndexMeasurement) []byte {
	parts := make([][]byte, 0, len(measurements))
	for _, measurement := range measurements {
		parts = append(parts, testProtoBytesField(1, encodeTestFieldsIndexMeasurementFields(measurement)))
	}
	return testProtoMessage(parts...)
}

func encodeTestFieldsIndexMeasurementFields(measurement fieldsIndexMeasurement) []byte {
	parts := [][]byte{testProtoBytesField(1, []byte(measurement.Name))}
	for _, field := range measurement.Fields {
		parts = append(parts, testProtoBytesField(2, encodeTestFieldsIndexField(field.Name, field.Type)))
	}
	return testProtoMessage(parts...)
}

func encodeTestFieldsIndexField(name string, typ int32) []byte {
	return testProtoMessage(
		testProtoBytesField(1, []byte(name)),
		testProtoVarintField(2, uint64(typ)),
	)
}

func encodeTestFieldsIndexLog(changeSets ...[]fieldsIndexChange) []byte {
	var out []byte
	for _, changeSet := range changeSets {
		payload := encodeTestFieldsIndexChangeSet(changeSet)
		var prefix [8]byte
		binary.LittleEndian.PutUint64(prefix[:], uint64(len(payload)))
		out = append(out, prefix[:]...)
		out = append(out, payload...)
	}
	return out
}

func encodeTestFieldsIndexChangeSet(changes []fieldsIndexChange) []byte {
	parts := make([][]byte, 0, len(changes))
	for _, change := range changes {
		parts = append(parts, testProtoBytesField(1, encodeTestFieldsIndexMeasurementFieldChange(change)))
	}
	return testProtoMessage(parts...)
}

func encodeTestFieldsIndexMeasurementFieldChange(change fieldsIndexChange) []byte {
	parts := [][]byte{
		testProtoBytesField(1, []byte(change.Measurement)),
		testProtoVarintField(3, uint64(change.Change)),
	}
	if change.FieldName != "" {
		parts = append(parts, testProtoBytesField(2, encodeTestFieldsIndexField(change.FieldName, change.FieldType)))
	}
	return testProtoMessage(parts...)
}
