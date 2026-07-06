package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	fieldsIndexFileName    = "fields.idx"
	fieldsIndexLogFileName = "fields.idxl"
	fieldsIndexSizeBytes   = 8

	fieldsIndexChangeAddMeasurementField = 0
	fieldsIndexChangeDeleteMeasurement   = 1
)

var fieldsIndexMagicNumber = []byte{0, 6, 1, 3}

type fieldsIndexState struct {
	Measurements map[string]map[string]int32
}

type fieldsIndexLoad struct {
	State                fieldsIndexState
	MainFilePresent      bool
	MainMeasurementCount int
	MainFieldCount       int
	ChangeLogPresent     bool
	ChangeLogPath        string
	ChangeLogSize        int64
	ChangeSetCount       int
	ChangeCount          int
	AddFieldChanges      int
	DeleteMeasurements   int
	ValidChangeBytes     int64
	Notices              []string
}

type fieldsIndexMeasurement struct {
	Name   string
	Fields []fieldsIndexField
}

type fieldsIndexField struct {
	Name string
	Type int32
}

type fieldsIndexChange struct {
	Measurement string
	FieldName   string
	FieldType   int32
	Change      int32
}

func analyzeFieldsIndex(path string, info os.FileInfo, options Options) (FileReport, error) {
	load, err := loadFieldsIndex(path, info)
	if err != nil {
		return FileReport{}, err
	}
	measurements := sortedFieldsIndexMeasurements(load.State)
	fieldCount, fieldsByType := summarizeFieldsIndexFields(measurements)
	fieldSummary := buildFieldIndexSummary(measurements, fieldsByType, load, options)
	keySamples := fieldsIndexKeySamples(measurements, options.KeySampleLimit)
	blocks := fieldsIndexBlockSamples(measurements, options.BlockSampleLimit)
	sizeBytes := info.Size()
	if load.ChangeLogPresent && !strings.EqualFold(filepath.Base(path), fieldsIndexLogFileName) {
		sizeBytes += load.ChangeLogSize
	}

	extra := map[string]string{
		"layout":                   fieldsIndexLayout(path),
		"encoding":                 "protobuf",
		"main_file_present":        fmt.Sprint(load.MainFilePresent),
		"main_measurement_count":   fmt.Sprint(load.MainMeasurementCount),
		"main_field_count":         fmt.Sprint(load.MainFieldCount),
		"change_log_present":       fmt.Sprint(load.ChangeLogPresent),
		"change_set_count":         fmt.Sprint(load.ChangeSetCount),
		"change_count":             fmt.Sprint(load.ChangeCount),
		"add_field_change_count":   fmt.Sprint(load.AddFieldChanges),
		"delete_measurement_count": fmt.Sprint(load.DeleteMeasurements),
		"valid_change_bytes":       fmt.Sprint(load.ValidChangeBytes),
		"field_count":              fmt.Sprint(fieldCount),
		"fields_by_type":           fieldsIndexTypeCountsString(fieldsByType),
	}
	if load.ChangeLogPath != "" {
		extra["change_log_path"] = load.ChangeLogPath
		extra["change_log_size"] = fmt.Sprint(load.ChangeLogSize)
	}

	blocksByType := map[string]int{}
	if len(measurements) > 0 {
		blocksByType["measurement-fields"] = len(measurements)
	}
	if fieldCount > 0 {
		blocksByType["field"] = fieldCount
	}
	if load.ChangeSetCount > 0 {
		blocksByType["fields-index-change-set"] = load.ChangeSetCount
	}
	if load.AddFieldChanges > 0 {
		blocksByType["fields-index-add-field"] = load.AddFieldChanges
	}
	if load.DeleteMeasurements > 0 {
		blocksByType["fields-index-delete-measurement"] = load.DeleteMeasurements
	}

	report := FileReport{
		Path:         path,
		Format:       FormatFieldsIndex,
		SizeBytes:    sizeBytes,
		ModTime:      info.ModTime(),
		KeyCount:     len(measurements),
		KeySamples:   keySamples,
		BlockCount:   len(measurements) + fieldCount + load.ChangeCount,
		BlocksByType: blocksByType,
		Blocks:       blocks,
		Fields:       &fieldSummary,
		Extra:        extra,
		Notices:      load.Notices,
	}
	if len(measurements) > 0 {
		report.MinKey = measurements[0].Name
		report.MaxKey = measurements[len(measurements)-1].Name
	}
	return report, nil
}

func loadFieldsIndex(path string, info os.FileInfo) (fieldsIndexLoad, error) {
	load := fieldsIndexLoad{
		State: fieldsIndexState{Measurements: map[string]map[string]int32{}},
	}
	if info.IsDir() {
		return load, fmt.Errorf("fields-index format requires a fields.idx or fields.idxl file")
	}

	base := filepath.Base(path)
	if strings.EqualFold(base, fieldsIndexLogFileName) {
		load.ChangeLogPresent = true
		load.ChangeLogPath = path
		load.ChangeLogSize = info.Size()
		if err := applyFieldsIndexChangeLog(path, &load); err != nil {
			return load, err
		}
		return load, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return load, err
	}
	if len(data) < len(fieldsIndexMagicNumber) || !bytes.Equal(data[:len(fieldsIndexMagicNumber)], fieldsIndexMagicNumber) {
		return load, fmt.Errorf("unknown field index format")
	}
	measurements, err := parseFieldsIndexMeasurementFieldSet(data[len(fieldsIndexMagicNumber):])
	if err != nil {
		return load, err
	}
	load.MainFilePresent = true
	applyFieldsIndexMeasurements(load.State, measurements)
	load.MainMeasurementCount = len(measurements)
	load.MainFieldCount, _ = summarizeFieldsIndexFields(measurements)

	changePath := filepath.Join(filepath.Dir(path), fieldsIndexLogFileName)
	changeInfo, err := os.Stat(changePath)
	if err == nil {
		load.ChangeLogPresent = true
		load.ChangeLogPath = changePath
		load.ChangeLogSize = changeInfo.Size()
		if err := applyFieldsIndexChangeLog(changePath, &load); err != nil {
			return load, err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return load, err
	}
	return load, nil
}

func parseFieldsIndexMeasurementFieldSet(data []byte) ([]fieldsIndexMeasurement, error) {
	var measurements []fieldsIndexMeasurement
	err := forEachProtoField(data, func(field int, wire int, _ uint64, payload []byte) error {
		if field == 1 && wire == protoWireBytes {
			measurement, err := parseFieldsIndexMeasurementFields(payload)
			if err != nil {
				return err
			}
			measurements = append(measurements, measurement)
		}
		return nil
	})
	return measurements, err
}

func parseFieldsIndexMeasurementFields(data []byte) (fieldsIndexMeasurement, error) {
	var measurement fieldsIndexMeasurement
	err := forEachProtoField(data, func(field int, wire int, _ uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireBytes {
				measurement.Name = string(payload)
			}
		case 2:
			if wire == protoWireBytes {
				field, err := parseFieldsIndexField(payload)
				if err != nil {
					return err
				}
				measurement.Fields = append(measurement.Fields, field)
			}
		}
		return nil
	})
	if err != nil {
		return measurement, err
	}
	sort.Slice(measurement.Fields, func(i, j int) bool {
		return measurement.Fields[i].Name < measurement.Fields[j].Name
	})
	return measurement, nil
}

func parseFieldsIndexField(data []byte) (fieldsIndexField, error) {
	var fieldValue fieldsIndexField
	err := forEachProtoField(data, func(field int, wire int, value uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireBytes {
				fieldValue.Name = string(payload)
			}
		case 2:
			if wire == protoWireVarint {
				fieldValue.Type = int32(value)
			}
		}
		return nil
	})
	return fieldValue, err
}

func applyFieldsIndexChangeLog(path string, load *fieldsIndexLoad) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	offset := 0
	for offset < len(data) {
		if len(data)-offset < fieldsIndexSizeBytes {
			load.Notices = append(load.Notices, fmt.Sprintf("trailing fields index change length at offset %d ignored", offset))
			break
		}
		size := binary.LittleEndian.Uint64(data[offset : offset+fieldsIndexSizeBytes])
		setOffset := offset
		offset += fieldsIndexSizeBytes
		if size > uint64(len(data)-offset) {
			load.Notices = append(load.Notices, fmt.Sprintf("trailing fields index change set at offset %d ignored: declared=%d remaining=%d", setOffset, size, len(data)-offset))
			break
		}
		payload := data[offset : offset+int(size)]
		changes, err := parseFieldsIndexChangeSet(payload)
		if err != nil {
			load.Notices = append(load.Notices, fmt.Sprintf("fields index change set at offset %d ignored: %v", setOffset, err))
			break
		}
		load.ChangeSetCount++
		load.ChangeCount += len(changes)
		load.ValidChangeBytes = int64(offset + int(size))
		applyFieldsIndexChanges(load, changes)
		offset += int(size)
	}
	return nil
}

func parseFieldsIndexChangeSet(data []byte) ([]fieldsIndexChange, error) {
	var changes []fieldsIndexChange
	err := forEachProtoField(data, func(field int, wire int, _ uint64, payload []byte) error {
		if field == 1 && wire == protoWireBytes {
			change, err := parseFieldsIndexMeasurementFieldChange(payload)
			if err != nil {
				return err
			}
			changes = append(changes, change)
		}
		return nil
	})
	return changes, err
}

func parseFieldsIndexMeasurementFieldChange(data []byte) (fieldsIndexChange, error) {
	var change fieldsIndexChange
	err := forEachProtoField(data, func(field int, wire int, value uint64, payload []byte) error {
		switch field {
		case 1:
			if wire == protoWireBytes {
				change.Measurement = string(payload)
			}
		case 2:
			if wire == protoWireBytes {
				fieldValue, err := parseFieldsIndexField(payload)
				if err != nil {
					return err
				}
				change.FieldName = fieldValue.Name
				change.FieldType = fieldValue.Type
			}
		case 3:
			if wire == protoWireVarint {
				change.Change = int32(value)
			}
		}
		return nil
	})
	return change, err
}

func applyFieldsIndexMeasurements(state fieldsIndexState, measurements []fieldsIndexMeasurement) {
	for _, measurement := range measurements {
		fields := state.Measurements[measurement.Name]
		if fields == nil {
			fields = map[string]int32{}
			state.Measurements[measurement.Name] = fields
		}
		for _, field := range measurement.Fields {
			fields[field.Name] = field.Type
		}
	}
}

func applyFieldsIndexChanges(load *fieldsIndexLoad, changes []fieldsIndexChange) {
	for _, change := range changes {
		switch change.Change {
		case fieldsIndexChangeDeleteMeasurement:
			delete(load.State.Measurements, change.Measurement)
			load.DeleteMeasurements++
		case fieldsIndexChangeAddMeasurementField:
			fields := load.State.Measurements[change.Measurement]
			if fields == nil {
				fields = map[string]int32{}
				load.State.Measurements[change.Measurement] = fields
			}
			if existing, ok := fields[change.FieldName]; ok && existing != change.FieldType {
				load.Notices = append(load.Notices, fmt.Sprintf("field type change for %s.%s: %s -> %s", change.Measurement, change.FieldName, fieldsIndexTypeName(existing), fieldsIndexTypeName(change.FieldType)))
			}
			fields[change.FieldName] = change.FieldType
			load.AddFieldChanges++
		default:
			load.Notices = append(load.Notices, fmt.Sprintf("unknown fields index change type %d for measurement %s ignored", change.Change, change.Measurement))
		}
	}
}

func sortedFieldsIndexMeasurements(state fieldsIndexState) []fieldsIndexMeasurement {
	measurements := make([]fieldsIndexMeasurement, 0, len(state.Measurements))
	for name, fields := range state.Measurements {
		measurement := fieldsIndexMeasurement{Name: name}
		for fieldName, fieldType := range fields {
			measurement.Fields = append(measurement.Fields, fieldsIndexField{Name: fieldName, Type: fieldType})
		}
		sort.Slice(measurement.Fields, func(i, j int) bool {
			return measurement.Fields[i].Name < measurement.Fields[j].Name
		})
		measurements = append(measurements, measurement)
	}
	sort.Slice(measurements, func(i, j int) bool {
		return measurements[i].Name < measurements[j].Name
	})
	return measurements
}

func summarizeFieldsIndexFields(measurements []fieldsIndexMeasurement) (int, map[string]int) {
	counts := map[string]int{}
	total := 0
	for _, measurement := range measurements {
		total += len(measurement.Fields)
		for _, field := range measurement.Fields {
			counts[fieldsIndexTypeName(field.Type)]++
		}
	}
	return total, counts
}

func buildFieldIndexSummary(measurements []fieldsIndexMeasurement, fieldsByType map[string]int, load fieldsIndexLoad, options Options) FieldIndexSummary {
	fieldCount := 0
	summary := FieldIndexSummary{
		Type:               "fields-index",
		MeasurementCount:   len(measurements),
		FieldsByType:       fieldsByType,
		ChangeSetCount:     load.ChangeSetCount,
		ChangeCount:        load.ChangeCount,
		AddFieldChanges:    load.AddFieldChanges,
		DeleteMeasurements: load.DeleteMeasurements,
	}
	for _, measurement := range measurements {
		fieldCount += len(measurement.Fields)
		if len(summary.MeasurementSamples) >= options.KeySampleLimit {
			continue
		}
		summary.MeasurementSamples = append(summary.MeasurementSamples, FieldIndexMeasurementReport{
			Name:       measurement.Name,
			FieldCount: len(measurement.Fields),
			Fields:     fieldsIndexFieldReports(measurement.Fields, options.BlockSampleLimit),
		})
	}
	summary.FieldCount = fieldCount
	return summary
}

func fieldsIndexFieldReports(fields []fieldsIndexField, limit int) []FieldIndexFieldReport {
	if limit <= 0 {
		return nil
	}
	reports := make([]FieldIndexFieldReport, 0, minInt(len(fields), limit))
	for _, field := range fields {
		reports = append(reports, FieldIndexFieldReport{
			Name: field.Name,
			Type: fieldsIndexTypeName(field.Type),
		})
		if len(reports) >= limit {
			break
		}
	}
	return reports
}

func fieldsIndexKeySamples(measurements []fieldsIndexMeasurement, limit int) []string {
	if limit <= 0 {
		return nil
	}
	samples := make([]string, 0, minInt(len(measurements), limit))
	for _, measurement := range measurements {
		parts := make([]string, 0, len(measurement.Fields))
		for _, field := range measurement.Fields {
			parts = append(parts, field.Name+":"+fieldsIndexTypeName(field.Type))
		}
		samples = append(samples, measurement.Name+" "+strings.Join(parts, ","))
		if len(samples) >= limit {
			break
		}
	}
	return samples
}

func fieldsIndexBlockSamples(measurements []fieldsIndexMeasurement, limit int) []BlockReport {
	if limit <= 0 {
		return nil
	}
	blocks := make([]BlockReport, 0, minInt(len(measurements), limit))
	for _, measurement := range measurements {
		blocks = append(blocks, BlockReport{
			Key:        measurement.Name,
			Type:       "measurement-fields",
			ValueCount: len(measurement.Fields),
		})
		if len(blocks) >= limit {
			break
		}
	}
	return blocks
}

func fieldsIndexTypeName(value int32) string {
	switch value {
	case 1:
		return "float"
	case 2:
		return "integer"
	case 3:
		return "string"
	case 4:
		return "boolean"
	case 9:
		return "unsigned"
	default:
		return "unknown(" + strconv.FormatInt(int64(value), 10) + ")"
	}
}

func fieldsIndexTypeCountsString(counts map[string]int) string {
	if len(counts) == 0 {
		return ""
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s:%d", name, counts[name]))
	}
	return strings.Join(parts, ",")
}

func fieldsIndexLayout(path string) string {
	if strings.EqualFold(filepath.Base(path), fieldsIndexLogFileName) {
		return "fields-index-log"
	}
	return "fields-index"
}

func isFieldsIndexPath(path string) bool {
	return strings.EqualFold(filepath.Base(path), fieldsIndexFileName)
}
