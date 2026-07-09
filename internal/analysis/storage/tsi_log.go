package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	tsiLogEntrySeriesTombstoneFlag      = 0x01
	tsiLogEntryMeasurementTombstoneFlag = 0x02
	tsiLogEntryTagKeyTombstoneFlag      = 0x04
	tsiLogEntryTagValueTombstoneFlag    = 0x08
)

var errTSILogEntryChecksumMismatch = errors.New("TSI log entry checksum mismatch")

type tsiLogEntry struct {
	Flag     byte
	SeriesID uint64
	Name     string
	Key      string
	Value    string
	Size     int
}

type tsiLogState struct {
	LiveSeries      map[uint64]struct{}
	TombstoneSeries map[uint64]struct{}
	SeriesRefs      map[uint64][]tsiLogSeriesRef
	Measurements    map[string]*tsiLogMeasurement
}

type tsiLogSeriesRef struct {
	Measurement string
	Key         string
	Value       string
}

type tsiLogMeasurement struct {
	Name      string
	Deleted   bool
	SeriesIDs map[uint64]struct{}
	TagKeys   map[string]*tsiLogTagKey
}

type tsiLogTagKey struct {
	Key       string
	Deleted   bool
	TagValues map[string]*tsiLogTagValue
}

type tsiLogTagValue struct {
	Value     string
	Deleted   bool
	SeriesIDs map[uint64]struct{}
}

func analyzeTSILog(path string, info os.FileInfo, options Options) (FileReport, error) {
	if info.IsDir() {
		return FileReport{}, fmt.Errorf("tsi-log format requires a .tsl log file, got directory %s", filepath.Base(path))
	}
	if strings.HasSuffix(strings.ToLower(filepath.Base(path)), ".tsi") {
		return FileReport{}, fmt.Errorf("%s uses tsi format, not tsi-log", filepath.Base(path))
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return FileReport{}, err
	}

	state := newTSILogState()
	entryCount := 0
	validBytes := 0
	resolvedSeriesEntries := 0
	unresolvedSeriesEntries := 0
	blocksByType := map[string]int{}
	notices := []string{}
	for offset := 0; offset < len(data); {
		entry, err := parseTSILogEntry(data[offset:])
		if err != nil {
			if errors.Is(err, io.ErrShortBuffer) || errors.Is(err, errTSILogEntryChecksumMismatch) {
				notices = append(notices, fmt.Sprintf("trailing TSI log entry at offset %d ignored: %v", offset, err))
				break
			}
			return FileReport{}, fmt.Errorf("TSI log entry at offset %d: %w", offset, err)
		}
		entryCount++
		validBytes += entry.Size
		entryType := tsiLogEntryType(entry)
		blocksByType[entryType]++
		if entryType == "series" {
			if entry.Name == "" {
				unresolvedSeriesEntries++
			} else {
				resolvedSeriesEntries++
			}
		}
		state.Apply(entry)
		offset += entry.Size
	}

	index, keySamples := state.IndexSummary(options)
	index.Type = "tsi1-log"
	if query := state.QuerySummary(options); query != nil {
		index.Query = query
	}

	minSeriesID, maxSeriesID := seriesIDMinMax(state.LiveSeries)
	tombstoneMinSeriesID, tombstoneMaxSeriesID := seriesIDMinMax(state.TombstoneSeries)
	if len(state.LiveSeries) > 0 {
		index.SeriesIDSetMin = uint64Ptr(minSeriesID)
		index.SeriesIDSetMax = uint64Ptr(maxSeriesID)
	}
	if len(state.TombstoneSeries) > 0 {
		index.TombstoneSeriesIDSetMin = uint64Ptr(tombstoneMinSeriesID)
		index.TombstoneSeriesIDSetMax = uint64Ptr(tombstoneMaxSeriesID)
	}

	extra := map[string]string{
		"entry_count":                   fmt.Sprint(entryCount),
		"valid_bytes":                   fmt.Sprint(validBytes),
		"series_entry_count":            fmt.Sprint(blocksByType["series"]),
		"resolved_series_entry_count":   fmt.Sprint(resolvedSeriesEntries),
		"unresolved_series_entry_count": fmt.Sprint(unresolvedSeriesEntries),
		"series_tombstone_count":        fmt.Sprint(blocksByType["series-tombstone"]),
		"measurement_tombstone_count":   fmt.Sprint(blocksByType["measurement-tombstone"]),
		"tag_key_tombstone_count":       fmt.Sprint(blocksByType["tag-key-tombstone"]),
		"tag_value_tombstone_count":     fmt.Sprint(blocksByType["tag-value-tombstone"]),
	}
	if len(state.LiveSeries) > 0 {
		extra["series_id_set_min"] = fmt.Sprint(minSeriesID)
		extra["series_id_set_max"] = fmt.Sprint(maxSeriesID)
	}
	if len(state.TombstoneSeries) > 0 {
		extra["tombstone_series_id_set_min"] = fmt.Sprint(tombstoneMinSeriesID)
		extra["tombstone_series_id_set_max"] = fmt.Sprint(tombstoneMaxSeriesID)
	}
	addTSILogIDFilterExtra(extra, state, options)

	report := FileReport{
		Path:         path,
		Format:       FormatTSILog,
		SizeBytes:    info.Size(),
		ModTime:      info.ModTime(),
		KeyCount:     index.MeasurementCount,
		KeySamples:   keySamples,
		BlockCount:   entryCount,
		BlocksByType: blocksByType,
		SeriesID: SeriesIDSummary{
			Min:      minSeriesID,
			Max:      maxSeriesID,
			Count:    int64(len(state.LiveSeries)),
			HasRange: len(state.LiveSeries) > 0,
		},
		Index:   &index,
		Extra:   extra,
		Notices: notices,
	}
	return report, nil
}

func parseTSILogEntry(data []byte) (tsiLogEntry, error) {
	var entry tsiLogEntry
	orig := data
	start := len(data)
	if len(data) < 1 {
		return entry, io.ErrShortBuffer
	}
	entry.Flag, data = data[0], data[1:]

	seriesID, n, err := readTSILogUvarint(data)
	if err != nil {
		return entry, fmt.Errorf("series id: %w", err)
	}
	entry.SeriesID, data = seriesID, data[n:]

	name, n, err := readTSILogBytes(data)
	if err != nil {
		return entry, fmt.Errorf("measurement name: %w", err)
	}
	entry.Name, data = string(name), data[n:]

	key, n, err := readTSILogBytes(data)
	if err != nil {
		return entry, fmt.Errorf("tag key: %w", err)
	}
	entry.Key, data = string(key), data[n:]

	value, n, err := readTSILogBytes(data)
	if err != nil {
		return entry, fmt.Errorf("tag value: %w", err)
	}
	entry.Value, data = string(value), data[n:]

	bodySize := start - len(data)
	if len(data) < 4 {
		return entry, io.ErrShortBuffer
	}
	wantCRC := binary.BigEndian.Uint32(data[:4])
	if gotCRC := crc32.ChecksumIEEE(orig[:bodySize]); gotCRC != wantCRC {
		return entry, errTSILogEntryChecksumMismatch
	}
	entry.Size = bodySize + 4
	return entry, nil
}

func readTSILogBytes(data []byte) ([]byte, int, error) {
	size, n, err := readTSILogUvarint(data)
	if err != nil {
		return nil, 0, err
	}
	if size > uint64(len(data)-n) {
		return nil, 0, io.ErrShortBuffer
	}
	end := n + int(size)
	return data[n:end], end, nil
}

func readTSILogUvarint(data []byte) (uint64, int, error) {
	if len(data) < 1 {
		return 0, 0, io.ErrShortBuffer
	}
	value, n := binary.Uvarint(data)
	if n == 0 || n > len(data) {
		return 0, 0, io.ErrShortBuffer
	}
	if n < 0 {
		return 0, 0, fmt.Errorf("invalid uvarint")
	}
	return value, n, nil
}

func newTSILogState() *tsiLogState {
	return &tsiLogState{
		LiveSeries:      map[uint64]struct{}{},
		TombstoneSeries: map[uint64]struct{}{},
		SeriesRefs:      map[uint64][]tsiLogSeriesRef{},
		Measurements:    map[string]*tsiLogMeasurement{},
	}
}

func (s *tsiLogState) Apply(entry tsiLogEntry) {
	switch entry.Flag {
	case tsiLogEntryMeasurementTombstoneFlag:
		measurement := s.measurement(entry.Name)
		measurement.Deleted = true
		measurement.SeriesIDs = map[uint64]struct{}{}
		measurement.TagKeys = map[string]*tsiLogTagKey{}
	case tsiLogEntryTagKeyTombstoneFlag:
		measurement := s.measurement(entry.Name)
		key := measurement.tagKey(entry.Key)
		key.Deleted = true
	case tsiLogEntryTagValueTombstoneFlag:
		measurement := s.measurement(entry.Name)
		key := measurement.tagKey(entry.Key)
		value := key.tagValue(entry.Value)
		value.Deleted = true
	default:
		s.applySeriesEntry(entry)
	}
}

func (s *tsiLogState) applySeriesEntry(entry tsiLogEntry) {
	if entry.Flag == tsiLogEntrySeriesTombstoneFlag {
		delete(s.LiveSeries, entry.SeriesID)
		s.TombstoneSeries[entry.SeriesID] = struct{}{}
		if refs, ok := s.SeriesRefs[entry.SeriesID]; ok {
			for _, ref := range refs {
				s.removeSeriesFromIndex(entry.SeriesID, ref)
			}
		}
		delete(s.SeriesRefs, entry.SeriesID)
		return
	}

	s.LiveSeries[entry.SeriesID] = struct{}{}
	delete(s.TombstoneSeries, entry.SeriesID)
	if entry.Name == "" {
		return
	}
	ref := tsiLogSeriesRef{
		Measurement: entry.Name,
		Key:         entry.Key,
		Value:       entry.Value,
	}
	if !tsiLogSeriesRefsContain(s.SeriesRefs[entry.SeriesID], ref) {
		s.SeriesRefs[entry.SeriesID] = append(s.SeriesRefs[entry.SeriesID], ref)
	}
	measurement := s.measurement(entry.Name)
	measurement.Deleted = false
	measurement.SeriesIDs[entry.SeriesID] = struct{}{}
	if entry.Key == "" {
		return
	}
	key := measurement.tagKey(entry.Key)
	value := key.tagValue(entry.Value)
	value.SeriesIDs[entry.SeriesID] = struct{}{}
}

func tsiLogSeriesRefsContain(refs []tsiLogSeriesRef, ref tsiLogSeriesRef) bool {
	for _, existing := range refs {
		if existing == ref {
			return true
		}
	}
	return false
}

func (s *tsiLogState) removeSeriesFromIndex(seriesID uint64, ref tsiLogSeriesRef) {
	measurement := s.Measurements[ref.Measurement]
	if measurement == nil {
		return
	}
	delete(measurement.SeriesIDs, seriesID)
	if ref.Key == "" {
		return
	}
	key := measurement.TagKeys[ref.Key]
	if key == nil {
		return
	}
	value := key.TagValues[ref.Value]
	if value == nil {
		return
	}
	delete(value.SeriesIDs, seriesID)
}

func (s *tsiLogState) measurement(name string) *tsiLogMeasurement {
	measurement := s.Measurements[name]
	if measurement == nil {
		measurement = &tsiLogMeasurement{
			Name:      name,
			SeriesIDs: map[uint64]struct{}{},
			TagKeys:   map[string]*tsiLogTagKey{},
		}
		s.Measurements[name] = measurement
	}
	return measurement
}

func (m *tsiLogMeasurement) tagKey(key string) *tsiLogTagKey {
	tagKey := m.TagKeys[key]
	if tagKey == nil {
		tagKey = &tsiLogTagKey{
			Key:       key,
			TagValues: map[string]*tsiLogTagValue{},
		}
		m.TagKeys[key] = tagKey
	}
	return tagKey
}

func (k *tsiLogTagKey) tagValue(value string) *tsiLogTagValue {
	tagValue := k.TagValues[value]
	if tagValue == nil {
		tagValue = &tsiLogTagValue{
			Value:     value,
			SeriesIDs: map[uint64]struct{}{},
		}
		k.TagValues[value] = tagValue
	}
	return tagValue
}

func (s *tsiLogState) IndexSummary(options Options) (IndexSummary, []string) {
	var summary IndexSummary
	names := s.sortedMeasurementNames()
	keySamples := make([]string, 0, options.KeySampleLimit)
	for _, name := range names {
		measurement := s.Measurements[name]
		summary.MeasurementCount++
		summary.SeriesRefs += int64(len(measurement.SeriesIDs))
		if measurement.Deleted {
			summary.DeletedMeasurementCount++
		}
		if len(keySamples) < options.KeySampleLimit {
			keySamples = append(keySamples, name)
		}
		tagKeyCount, deletedTagKeyCount, tagValueCount, deletedTagValueCount := measurement.TagCounts()
		summary.TagKeyCount += tagKeyCount
		summary.DeletedTagKeyCount += deletedTagKeyCount
		summary.TagValueCount += tagValueCount
		summary.DeletedTagValueCount += deletedTagValueCount
		if len(summary.MeasurementSamples) < options.KeySampleLimit {
			summary.MeasurementSamples = append(summary.MeasurementSamples, IndexMeasurementReport{
				Name:                 name,
				Deleted:              measurement.Deleted,
				SeriesCount:          uint64(len(measurement.SeriesIDs)),
				TagKeyCount:          tagKeyCount,
				DeletedTagKeyCount:   deletedTagKeyCount,
				TagValueCount:        tagValueCount,
				DeletedTagValueCount: deletedTagValueCount,
			})
		}
	}
	summary.SeriesRefs = int64(len(s.LiveSeries))
	summary.SeriesIDSetCardinality = int64(len(s.LiveSeries))
	summary.TombstoneSeriesIDSetCardinality = int64(len(s.TombstoneSeries))
	return summary, keySamples
}

func (s *tsiLogState) QuerySummary(options Options) *IndexQuerySummary {
	if len(options.QueryMeasurements) == 0 && len(options.QueryTags) == 0 {
		return nil
	}
	query := &IndexQuerySummary{
		MeasurementFilterApplied: len(options.QueryMeasurements) > 0,
		TagFilterApplied:         len(options.QueryTags) > 0,
		QueryMeasurements:        append([]string(nil), options.QueryMeasurements...),
		QueryTags:                append([]TagFilter(nil), options.QueryTags...),
	}
	measurementSet := queryKeySet(options.QueryMeasurements)
	matchedMeasurements := map[string]struct{}{}
	matchedTags := map[string]TagFilter{}
	for _, name := range s.sortedMeasurementNames() {
		measurement := s.Measurements[name]
		if len(measurementSet) > 0 {
			if _, ok := measurementSet[name]; !ok {
				continue
			}
			matchedMeasurements[name] = struct{}{}
		}
		tagReports, measurementMatchedTags, matchingSeriesIDs, allTagsMatched := measurement.QueryTags(options.QueryTags, options.BlockSampleLimit)
		for id, filter := range measurementMatchedTags {
			matchedTags[id] = filter
		}
		if measurement.Deleted || !allTagsMatched {
			continue
		}
		seriesCount := len(measurement.SeriesIDs)
		if len(options.QueryTags) > 0 {
			seriesCount = len(matchingSeriesIDs)
			if seriesCount == 0 {
				continue
			}
		}
		query.CandidateMeasurements++
		query.SeriesRefs += int64(seriesCount)
		query.TagKeyCount += len(measurement.TagKeys)
		for _, key := range measurement.TagKeys {
			query.TagValueCount += len(key.TagValues)
		}
		if len(query.MeasurementSamples) < options.KeySampleLimit {
			query.MeasurementSamples = append(query.MeasurementSamples, IndexQueryMeasurementReport{
				Name:        name,
				SeriesCount: uint64(seriesCount),
				Tags:        tagReports,
			})
		}
	}
	for _, measurement := range options.QueryMeasurements {
		if _, ok := matchedMeasurements[measurement]; ok {
			query.MatchedMeasurements = append(query.MatchedMeasurements, measurement)
		} else {
			query.MissingMeasurements = append(query.MissingMeasurements, measurement)
		}
	}
	for _, filter := range options.QueryTags {
		id := tagFilterID(filter.Key, filter.Value)
		if matched, ok := matchedTags[id]; ok {
			query.MatchedTags = append(query.MatchedTags, matched)
		} else {
			query.MissingTags = append(query.MissingTags, filter)
		}
	}
	return query
}

func (m *tsiLogMeasurement) QueryTags(filters []TagFilter, sampleLimit int) ([]IndexQueryTagReport, map[string]TagFilter, map[uint64]struct{}, bool) {
	if len(filters) == 0 {
		return m.QueryTagSamples(sampleLimit), nil, nil, true
	}
	matched := map[string]TagFilter{}
	var matchingSeriesIDs map[uint64]struct{}
	reports := []IndexQueryTagReport{}
	for _, filter := range filters {
		tagKey := m.TagKeys[filter.Key]
		if tagKey == nil || tagKey.Deleted {
			continue
		}
		tagValue := tagKey.TagValues[filter.Value]
		if tagValue == nil || tagValue.Deleted {
			continue
		}
		matched[tagFilterID(filter.Key, filter.Value)] = filter
		if matchingSeriesIDs == nil {
			matchingSeriesIDs = cloneTSILogSeriesIDSet(tagValue.SeriesIDs)
		} else {
			matchingSeriesIDs = intersectTSISeriesIDSets(matchingSeriesIDs, tagValue.SeriesIDs)
		}
		if sampleLimit <= 0 || len(reports) >= sampleLimit {
			continue
		}
		reports = append(reports, IndexQueryTagReport{
			Key: tagKey.Key,
			Values: []IndexQueryTagValueReport{{
				Value:       tagValue.Value,
				SeriesCount: uint64(len(tagValue.SeriesIDs)),
			}},
		})
	}
	return reports, matched, matchingSeriesIDs, len(matched) == len(filters)
}

func (m *tsiLogMeasurement) QueryTagSamples(sampleLimit int) []IndexQueryTagReport {
	if m == nil || sampleLimit <= 0 {
		return nil
	}
	keys := make([]string, 0, len(m.TagKeys))
	for key := range m.TagKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	reports := make([]IndexQueryTagReport, 0, minInt(len(keys), sampleLimit))
	for _, keyName := range keys {
		if len(reports) >= sampleLimit {
			break
		}
		tagKey := m.TagKeys[keyName]
		if tagKey == nil {
			continue
		}
		report := IndexQueryTagReport{
			Key:     tagKey.Key,
			Deleted: tagKey.Deleted,
		}
		values := make([]string, 0, len(tagKey.TagValues))
		for value := range tagKey.TagValues {
			values = append(values, value)
		}
		sort.Strings(values)
		for _, valueName := range values {
			if len(report.Values) >= sampleLimit {
				break
			}
			tagValue := tagKey.TagValues[valueName]
			if tagValue == nil {
				continue
			}
			report.Values = append(report.Values, IndexQueryTagValueReport{
				Value:       tagValue.Value,
				Deleted:     tagValue.Deleted,
				SeriesCount: tsiLogTagValueSampleSeriesCount(tagKey, tagValue),
			})
		}
		reports = append(reports, report)
	}
	return reports
}

func tsiLogTagValueSampleSeriesCount(tagKey *tsiLogTagKey, tagValue *tsiLogTagValue) uint64 {
	if tagKey == nil || tagValue == nil || tagKey.Deleted || tagValue.Deleted {
		return 0
	}
	return uint64(len(tagValue.SeriesIDs))
}

func cloneTSILogSeriesIDSet(src map[uint64]struct{}) map[uint64]struct{} {
	dst := make(map[uint64]struct{}, len(src))
	for id := range src {
		dst[id] = struct{}{}
	}
	return dst
}

func addTSILogIDFilterExtra(extra map[string]string, state *tsiLogState, options Options) {
	if len(options.QuerySeriesIDs) == 0 {
		return
	}
	matched := []uint64{}
	tombstoned := []uint64{}
	missing := []uint64{}
	for _, id := range options.QuerySeriesIDs {
		if _, ok := state.LiveSeries[id]; ok {
			matched = append(matched, id)
			continue
		}
		if _, ok := state.TombstoneSeries[id]; ok {
			tombstoned = append(tombstoned, id)
			continue
		}
		missing = append(missing, id)
	}
	extra["query_series_id_filter_applied"] = "true"
	extra["query_series_ids"] = joinStorageUint64s(options.QuerySeriesIDs)
	extra["query_matched_series_ids"] = joinStorageUint64s(matched)
	extra["query_tombstone_series_ids"] = joinStorageUint64s(tombstoned)
	extra["query_missing_series_ids"] = joinStorageUint64s(missing)
}

func (m *tsiLogMeasurement) TagCounts() (tagKeyCount, deletedTagKeyCount, tagValueCount, deletedTagValueCount int) {
	tagKeyCount = len(m.TagKeys)
	for _, key := range m.TagKeys {
		if key.Deleted {
			deletedTagKeyCount++
		}
		tagValueCount += len(key.TagValues)
		for _, value := range key.TagValues {
			if value.Deleted {
				deletedTagValueCount++
			}
		}
	}
	return tagKeyCount, deletedTagKeyCount, tagValueCount, deletedTagValueCount
}

func (s *tsiLogState) sortedMeasurementNames() []string {
	names := make([]string, 0, len(s.Measurements))
	for name := range s.Measurements {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func tsiLogEntryType(entry tsiLogEntry) string {
	switch entry.Flag {
	case tsiLogEntrySeriesTombstoneFlag:
		return "series-tombstone"
	case tsiLogEntryMeasurementTombstoneFlag:
		return "measurement-tombstone"
	case tsiLogEntryTagKeyTombstoneFlag:
		return "tag-key-tombstone"
	case tsiLogEntryTagValueTombstoneFlag:
		return "tag-value-tombstone"
	default:
		return "series"
	}
}

func seriesIDMinMax(seriesIDs map[uint64]struct{}) (uint64, uint64) {
	var minID, maxID uint64
	first := true
	for id := range seriesIDs {
		if first || id < minID {
			minID = id
		}
		if first || id > maxID {
			maxID = id
		}
		first = false
	}
	return minID, maxID
}
