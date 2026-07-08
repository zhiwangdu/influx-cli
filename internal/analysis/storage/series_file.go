package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	seriesFilePartitionN = 8

	seriesSegmentMagic      = "SSEG"
	seriesSegmentVersion    = 1
	seriesSegmentHeaderSize = 4 + 1

	seriesEntryInsertFlag    = 0x01
	seriesEntryTombstoneFlag = 0x02
	seriesEntryHeaderSize    = 1 + 8
)

type seriesFileLayout struct {
	Layout         string
	Segments       []seriesFileSegmentRef
	PartitionCount int
	IndexFileCount int
	SizeBytes      int64
	ModTime        time.Time
}

type seriesFileSegmentRef struct {
	Path      string
	Partition string
	Segment   string
	ID        uint16
	SizeBytes int64
	ModTime   time.Time
}

type seriesFileSegmentAnalysis struct {
	Ref                  seriesFileSegmentRef
	Entries              []seriesFileEntry
	Inserts              int
	Tombstones           int
	ValidBytes           int64
	MaxID                uint64
	PartitionMismatches  int
	PartitionCheckSample []string
	PartitionSampleLimit int
	Notices              []string
}

type seriesFileEntry struct {
	Flag   byte
	ID     uint64
	Key    seriesFileKey
	Offset int64
	Size   int
}

type seriesFileKey struct {
	Measurement string
	Tags        []TagFilter
}

type seriesFileSeries struct {
	ID  uint64
	Key seriesFileKey
}

type seriesFileState struct {
	Live       map[uint64]seriesFileSeries
	Tombstones map[uint64]struct{}
	MaxID      uint64
}

type seriesFileMeasurement struct {
	Name              string
	SeriesIDs         map[uint64]struct{}
	TagValues         map[string]map[string]struct{}
	TagValueSeriesIDs map[string]map[string]map[uint64]struct{}
}

func analyzeSeriesFile(path string, info os.FileInfo, options Options) (FileReport, error) {
	layout, err := collectSeriesFileLayout(path, info)
	if err != nil {
		return FileReport{}, err
	}

	state := newSeriesFileState()
	notices := []string{}
	segmentAnalyses := make([]seriesFileSegmentAnalysis, 0, len(layout.Segments))
	entryCount := 0
	insertCount := 0
	tombstoneCount := 0
	partitionMismatchCount := 0
	partitionMismatchSamples := []string{}
	validBytes := int64(0)
	maxSeriesID := uint64(0)
	for _, ref := range layout.Segments {
		analysis, err := readSeriesFileSegment(ref, options.KeySampleLimit)
		if err != nil {
			if !info.IsDir() {
				return FileReport{}, err
			}
			notices = append(notices, fmt.Sprintf("%s: %v", seriesFileDisplaySegment(ref), err))
			continue
		}
		segmentAnalyses = append(segmentAnalyses, analysis)
		notices = append(notices, analysis.Notices...)
		entryCount += len(analysis.Entries)
		insertCount += analysis.Inserts
		tombstoneCount += analysis.Tombstones
		partitionMismatchCount += analysis.PartitionMismatches
		partitionMismatchSamples = appendLimitedStrings(partitionMismatchSamples, analysis.PartitionCheckSample, options.KeySampleLimit)
		validBytes += analysis.ValidBytes
		if analysis.MaxID > maxSeriesID {
			maxSeriesID = analysis.MaxID
		}
		for _, entry := range analysis.Entries {
			state.Apply(entry)
		}
	}
	if state.MaxID > maxSeriesID {
		maxSeriesID = state.MaxID
	}

	index, measurementNames := buildSeriesFileIndexSummary(state, options)
	index.Type = "series-file"
	keySamples := seriesFileKeySamples(state, options)
	minSeriesID, maxLiveSeriesID := seriesIDMinMax(seriesFileLiveIDSet(state.Live))
	tombstoneSeriesCount := len(state.Tombstones)

	extra := map[string]string{
		"layout":                 layout.Layout,
		"partition_count":        fmt.Sprint(layout.PartitionCount),
		"segment_count":          fmt.Sprint(len(layout.Segments)),
		"parsed_segment_count":   fmt.Sprint(len(segmentAnalyses)),
		"index_file_count":       fmt.Sprint(layout.IndexFileCount),
		"entry_count":            fmt.Sprint(entryCount),
		"insert_entry_count":     fmt.Sprint(insertCount),
		"tombstone_entry_count":  fmt.Sprint(tombstoneCount),
		"live_series_count":      fmt.Sprint(len(state.Live)),
		"tombstone_series_count": fmt.Sprint(tombstoneSeriesCount),
		"valid_bytes":            fmt.Sprint(validBytes),
		"max_series_id":          fmt.Sprint(maxSeriesID),
		"partition_check":        "series-id-modulo",
		"partition_mismatches":   fmt.Sprint(partitionMismatchCount),
	}
	if len(partitionMismatchSamples) > 0 {
		extra["partition_mismatch_samples"] = strings.Join(partitionMismatchSamples, ";")
	}
	if partitionMismatchCount > 0 {
		notices = append(notices, fmt.Sprintf("%d series file entry(s) are stored outside their expected ID partition", partitionMismatchCount))
	}
	addSeriesFileIDFilterExtra(extra, state, options)

	blocksByType := map[string]int{
		"series-segment": len(layout.Segments),
		"series-entry":   insertCount,
	}
	if layout.PartitionCount > 0 {
		blocksByType["partition"] = layout.PartitionCount
	}
	if layout.IndexFileCount > 0 {
		blocksByType["series-index"] = layout.IndexFileCount
	}
	if tombstoneCount > 0 {
		blocksByType["series-tombstone"] = tombstoneCount
	}
	if partitionMismatchCount > 0 {
		blocksByType["series-partition-mismatch"] = partitionMismatchCount
	}

	report := FileReport{
		Path:         path,
		Format:       FormatSeriesFile,
		SizeBytes:    layout.SizeBytes,
		ModTime:      layout.ModTime,
		KeyCount:     len(state.Live),
		KeySamples:   keySamples,
		BlockCount:   entryCount,
		BlocksByType: blocksByType,
		Blocks:       seriesFileBlockSamples(segmentAnalyses, options.BlockSampleLimit),
		SeriesID: SeriesIDSummary{
			Min:      minSeriesID,
			Max:      maxLiveSeriesID,
			Count:    int64(len(state.Live)),
			HasRange: len(state.Live) > 0,
		},
		Index:   &index,
		Extra:   extra,
		Notices: notices,
	}
	if len(measurementNames) > 0 {
		report.MinKey = measurementNames[0]
		report.MaxKey = measurementNames[len(measurementNames)-1]
	}
	return report, nil
}

func collectSeriesFileLayout(path string, info os.FileInfo) (seriesFileLayout, error) {
	layout := seriesFileLayout{
		Layout:    "series-segment",
		SizeBytes: info.Size(),
		ModTime:   info.ModTime(),
	}
	if !info.IsDir() {
		ref, err := newSeriesFileSegmentRef(path, info)
		if err != nil {
			return layout, err
		}
		layout.Segments = []seriesFileSegmentRef{ref}
		if ref.Partition != "" {
			layout.PartitionCount = 1
		}
		return layout, nil
	}

	layout.SizeBytes = 0
	if strings.EqualFold(filepath.Base(path), "_series") {
		layout.Layout = "series-file"
		if err := collectSeriesFileDirectory(path, &layout); err != nil {
			return layout, err
		}
		return layout, nil
	}
	if isSeriesPartitionPath(path) {
		layout.Layout = "series-partition"
		layout.PartitionCount = 1
		if err := collectSeriesFilePartition(path, filepath.Base(path), &layout); err != nil {
			return layout, err
		}
		return layout, nil
	}
	return layout, fmt.Errorf("series-file format requires a _series directory, partition directory, or segment file")
}

func collectSeriesFileDirectory(path string, layout *seriesFileLayout) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !isSeriesPartitionName(entry.Name()) {
			continue
		}
		layout.PartitionCount++
		partitionPath := filepath.Join(path, entry.Name())
		if err := collectSeriesFilePartition(partitionPath, entry.Name(), layout); err != nil {
			return err
		}
	}
	sortSeriesFileSegments(layout.Segments)
	return nil
}

func collectSeriesFilePartition(path, partition string, layout *seriesFileLayout) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		entryPath := filepath.Join(path, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.Name() == "index" {
			layout.IndexFileCount++
			layout.SizeBytes += info.Size()
			if info.ModTime().After(layout.ModTime) {
				layout.ModTime = info.ModTime()
			}
			continue
		}
		if !isSeriesSegmentName(entry.Name()) {
			continue
		}
		ref, err := newSeriesFileSegmentRef(entryPath, info)
		if err != nil {
			return err
		}
		ref.Partition = partition
		layout.Segments = append(layout.Segments, ref)
		layout.SizeBytes += info.Size()
		if info.ModTime().After(layout.ModTime) {
			layout.ModTime = info.ModTime()
		}
	}
	return nil
}

func newSeriesFileSegmentRef(path string, info os.FileInfo) (seriesFileSegmentRef, error) {
	base := filepath.Base(path)
	id, err := strconv.ParseUint(base, 16, 16)
	if err != nil {
		id = 0
	}
	partition := ""
	if isSeriesPartitionName(filepath.Base(filepath.Dir(path))) {
		partition = filepath.Base(filepath.Dir(path))
	}
	return seriesFileSegmentRef{
		Path:      path,
		Partition: partition,
		Segment:   base,
		ID:        uint16(id),
		SizeBytes: info.Size(),
		ModTime:   info.ModTime(),
	}, nil
}

func readSeriesFileSegment(ref seriesFileSegmentRef, partitionSampleLimit int) (seriesFileSegmentAnalysis, error) {
	analysis := seriesFileSegmentAnalysis{
		Ref:                  ref,
		ValidBytes:           seriesSegmentHeaderSize,
		PartitionSampleLimit: partitionSampleLimit,
	}
	f, err := os.Open(ref.Path)
	if err != nil {
		return analysis, err
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 32*1024)
	header := make([]byte, seriesSegmentHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return analysis, fmt.Errorf("short series segment header: %w", err)
	}
	if string(header[:len(seriesSegmentMagic)]) != seriesSegmentMagic {
		return analysis, fmt.Errorf("invalid series segment magic")
	}
	if header[4] != seriesSegmentVersion {
		return analysis, fmt.Errorf("unsupported series segment version %d", header[4])
	}

	offset := int64(seriesSegmentHeaderSize)
	for offset < ref.SizeBytes {
		entryOffset := offset
		flag, err := reader.ReadByte()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return analysis, fmt.Errorf("read series entry flag at offset %d: %w", entryOffset, err)
		}
		if !isValidSeriesFileEntryFlag(flag) {
			if flag != 0 {
				analysis.Notices = append(analysis.Notices, fmt.Sprintf("%s: invalid series entry flag 0x%02x at offset %d; trailing bytes ignored", seriesFileDisplaySegment(ref), flag, entryOffset))
			}
			break
		}

		var idBuf [8]byte
		if _, err := io.ReadFull(reader, idBuf[:]); err != nil {
			analysis.Notices = append(analysis.Notices, fmt.Sprintf("%s: partial series entry header at offset %d ignored: %v", seriesFileDisplaySegment(ref), entryOffset, err))
			break
		}
		id := binary.BigEndian.Uint64(idBuf[:])
		offset += seriesEntryHeaderSize
		entry := seriesFileEntry{
			Flag:   flag,
			ID:     id,
			Offset: entryOffset,
			Size:   seriesEntryHeaderSize,
		}
		if id > analysis.MaxID {
			analysis.MaxID = id
		}

		if flag == seriesEntryInsertFlag {
			key, keySize, err := readSeriesFileKey(reader, ref.SizeBytes-offset)
			if err != nil {
				analysis.Notices = append(analysis.Notices, fmt.Sprintf("%s: partial series key at offset %d ignored: %v", seriesFileDisplaySegment(ref), offset, err))
				break
			}
			parsed, err := parseSeriesFileKey(key)
			if err != nil {
				analysis.Notices = append(analysis.Notices, fmt.Sprintf("%s: invalid series key at offset %d ignored: %v", seriesFileDisplaySegment(ref), offset, err))
				break
			}
			entry.Key = parsed
			entry.Size += int(keySize)
			offset += keySize
			analysis.Inserts++
		} else {
			analysis.Tombstones++
		}

		analysis.Entries = append(analysis.Entries, entry)
		checkSeriesFileEntryPartition(&analysis, entry)
		analysis.ValidBytes = offset
	}
	return analysis, nil
}

func checkSeriesFileEntryPartition(analysis *seriesFileSegmentAnalysis, entry seriesFileEntry) {
	if analysis == nil || analysis.Ref.Partition == "" {
		return
	}
	actual, err := strconv.ParseUint(analysis.Ref.Partition, 16, 8)
	if err != nil {
		return
	}
	expected := seriesFilePartitionIDForSeriesID(entry.ID)
	if int(actual) == expected {
		return
	}
	analysis.PartitionMismatches++
	if analysis.PartitionSampleLimit <= 0 || len(analysis.PartitionCheckSample) >= analysis.PartitionSampleLimit {
		return
	}
	analysis.PartitionCheckSample = append(analysis.PartitionCheckSample, fmt.Sprintf(
		"%s id:%d expected:%02x actual:%s flag:%s",
		seriesFileDisplaySegment(analysis.Ref),
		entry.ID,
		expected,
		analysis.Ref.Partition,
		seriesFileEntryFlagName(entry.Flag),
	))
}

func seriesFilePartitionIDForSeriesID(id uint64) int {
	return int((id - 1) % seriesFilePartitionN)
}

func seriesFileEntryFlagName(flag byte) string {
	switch flag {
	case seriesEntryInsertFlag:
		return "insert"
	case seriesEntryTombstoneFlag:
		return "tombstone"
	default:
		return fmt.Sprintf("unknown(0x%02x)", flag)
	}
}

func appendLimitedStrings(dst, src []string, limit int) []string {
	if limit <= 0 || len(dst) >= limit {
		return dst
	}
	for _, value := range src {
		dst = append(dst, value)
		if len(dst) >= limit {
			break
		}
	}
	return dst
}

func readSeriesFileKey(reader *bufio.Reader, remaining int64) ([]byte, int64, error) {
	var prefix [binary.MaxVarintLen64]byte
	var size uint64
	var shift uint
	for i := 0; i < len(prefix); i++ {
		if int64(i+1) > remaining {
			return nil, 0, io.ErrShortBuffer
		}
		b, err := reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		prefix[i] = b
		if b < 0x80 {
			if i == binary.MaxVarintLen64-1 && b > 1 {
				return nil, 0, fmt.Errorf("invalid series key length")
			}
			size |= uint64(b) << shift
			prefixSize := i + 1
			if size > uint64(remaining-int64(prefixSize)) {
				return nil, 0, io.ErrShortBuffer
			}
			body := make([]byte, int(size))
			if _, err := io.ReadFull(reader, body); err != nil {
				return nil, 0, err
			}
			key := make([]byte, 0, prefixSize+len(body))
			key = append(key, prefix[:prefixSize]...)
			key = append(key, body...)
			return key, int64(len(key)), nil
		}
		size |= uint64(b&0x7f) << shift
		shift += 7
	}
	return nil, 0, fmt.Errorf("invalid series key length")
}

func parseSeriesFileKey(key []byte) (seriesFileKey, error) {
	var parsed seriesFileKey
	size, n := binary.Uvarint(key)
	if n <= 0 {
		return parsed, fmt.Errorf("invalid series key length")
	}
	if size != uint64(len(key)-n) {
		return parsed, fmt.Errorf("series key length %d does not match body size %d", size, len(key)-n)
	}
	body := key[n:]
	if len(body) < 2 {
		return parsed, io.ErrShortBuffer
	}
	nameLen := int(binary.BigEndian.Uint16(body[:2]))
	body = body[2:]
	if len(body) < nameLen {
		return parsed, io.ErrShortBuffer
	}
	parsed.Measurement = string(body[:nameLen])
	body = body[nameLen:]

	tagN, n := binary.Uvarint(body)
	if n <= 0 {
		return parsed, fmt.Errorf("invalid series key tag count")
	}
	body = body[n:]
	for i := uint64(0); i < tagN; i++ {
		if len(body) < 2 {
			return parsed, io.ErrShortBuffer
		}
		keyLen := int(binary.BigEndian.Uint16(body[:2]))
		body = body[2:]
		if len(body) < keyLen {
			return parsed, io.ErrShortBuffer
		}
		tagKey := string(body[:keyLen])
		body = body[keyLen:]

		if len(body) < 2 {
			return parsed, io.ErrShortBuffer
		}
		valueLen := int(binary.BigEndian.Uint16(body[:2]))
		body = body[2:]
		if len(body) < valueLen {
			return parsed, io.ErrShortBuffer
		}
		tagValue := string(body[:valueLen])
		body = body[valueLen:]
		parsed.Tags = append(parsed.Tags, TagFilter{Key: tagKey, Value: tagValue})
	}
	if len(body) != 0 {
		return parsed, fmt.Errorf("series key has %d trailing byte(s)", len(body))
	}
	return parsed, nil
}

func newSeriesFileState() *seriesFileState {
	return &seriesFileState{
		Live:       map[uint64]seriesFileSeries{},
		Tombstones: map[uint64]struct{}{},
	}
}

func (s *seriesFileState) Apply(entry seriesFileEntry) {
	if entry.ID > s.MaxID {
		s.MaxID = entry.ID
	}
	switch entry.Flag {
	case seriesEntryInsertFlag:
		s.Live[entry.ID] = seriesFileSeries{
			ID:  entry.ID,
			Key: entry.Key,
		}
		delete(s.Tombstones, entry.ID)
	case seriesEntryTombstoneFlag:
		delete(s.Live, entry.ID)
		s.Tombstones[entry.ID] = struct{}{}
	}
}

func buildSeriesFileIndexSummary(state *seriesFileState, options Options) (IndexSummary, []string) {
	measurements := map[string]*seriesFileMeasurement{}
	for _, series := range state.Live {
		measurement := measurements[series.Key.Measurement]
		if measurement == nil {
			measurement = &seriesFileMeasurement{
				Name:              series.Key.Measurement,
				SeriesIDs:         map[uint64]struct{}{},
				TagValues:         map[string]map[string]struct{}{},
				TagValueSeriesIDs: map[string]map[string]map[uint64]struct{}{},
			}
			measurements[series.Key.Measurement] = measurement
		}
		measurement.SeriesIDs[series.ID] = struct{}{}
		for _, tag := range series.Key.Tags {
			values := measurement.TagValues[tag.Key]
			if values == nil {
				values = map[string]struct{}{}
				measurement.TagValues[tag.Key] = values
			}
			values[tag.Value] = struct{}{}
			seriesByValue := measurement.TagValueSeriesIDs[tag.Key]
			if seriesByValue == nil {
				seriesByValue = map[string]map[uint64]struct{}{}
				measurement.TagValueSeriesIDs[tag.Key] = seriesByValue
			}
			seriesIDs := seriesByValue[tag.Value]
			if seriesIDs == nil {
				seriesIDs = map[uint64]struct{}{}
				seriesByValue[tag.Value] = seriesIDs
			}
			seriesIDs[series.ID] = struct{}{}
		}
	}

	names := make([]string, 0, len(measurements))
	for name := range measurements {
		names = append(names, name)
	}
	sort.Strings(names)

	var summary IndexSummary
	summary.MeasurementCount = len(names)
	summary.SeriesRefs = int64(len(state.Live))
	summary.SeriesIDSetCardinality = int64(len(state.Live))
	summary.TombstoneSeriesIDSetCardinality = int64(len(state.Tombstones))
	for _, name := range names {
		measurement := measurements[name]
		tagKeyCount, tagValueCount := measurement.TagCounts()
		summary.TagKeyCount += tagKeyCount
		summary.TagValueCount += tagValueCount
		if len(summary.MeasurementSamples) < options.KeySampleLimit {
			summary.MeasurementSamples = append(summary.MeasurementSamples, IndexMeasurementReport{
				Name:          name,
				SeriesCount:   uint64(len(measurement.SeriesIDs)),
				TagKeyCount:   tagKeyCount,
				TagValueCount: tagValueCount,
			})
		}
	}
	if query := buildSeriesFileIndexQuerySummary(measurements, names, options); query != nil {
		summary.Query = query
	}
	return summary, names
}

func (m *seriesFileMeasurement) TagCounts() (tagKeyCount, tagValueCount int) {
	tagKeyCount = len(m.TagValues)
	for _, values := range m.TagValues {
		tagValueCount += len(values)
	}
	return tagKeyCount, tagValueCount
}

func buildSeriesFileIndexQuerySummary(measurements map[string]*seriesFileMeasurement, names []string, options Options) *IndexQuerySummary {
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
	for _, measurement := range query.QueryMeasurements {
		if _, ok := measurements[measurement]; ok {
			query.MatchedMeasurements = append(query.MatchedMeasurements, measurement)
		} else {
			query.MissingMeasurements = append(query.MissingMeasurements, measurement)
		}
	}

	matchedTags := map[string]TagFilter{}
	for _, name := range names {
		measurement := measurements[name]
		if len(measurementSet) > 0 {
			if _, ok := measurementSet[name]; !ok {
				continue
			}
		}
		for _, filter := range query.QueryTags {
			if seriesFileMeasurementHasTagValue(measurement, filter) {
				matchedTags[tagFilterID(filter.Key, filter.Value)] = filter
			}
		}
		matchingSeriesIDs := seriesFileMeasurementMatchingSeriesIDs(measurement, query.QueryTags)
		if len(matchingSeriesIDs) == 0 {
			continue
		}
		tagKeyCount, tagValueCount := measurement.TagCounts()
		query.CandidateMeasurements++
		// The series file has decoded live series keys, so unlike TSI metadata
		// this can report the exact live series matching all requested tag filters.
		query.SeriesRefs += int64(len(matchingSeriesIDs))
		query.TagKeyCount += tagKeyCount
		query.TagValueCount += tagValueCount
		if len(query.MeasurementSamples) < options.KeySampleLimit {
			query.MeasurementSamples = append(query.MeasurementSamples, IndexQueryMeasurementReport{
				Name:        name,
				SeriesCount: uint64(len(matchingSeriesIDs)),
				Tags:        seriesFileQueryTagReports(measurement, query.QueryTags),
			})
		}
	}
	for _, filter := range query.QueryTags {
		id := tagFilterID(filter.Key, filter.Value)
		if matched, ok := matchedTags[id]; ok {
			query.MatchedTags = append(query.MatchedTags, matched)
		} else {
			query.MissingTags = append(query.MissingTags, filter)
		}
	}
	return query
}

func seriesFileMeasurementHasTagValue(measurement *seriesFileMeasurement, filter TagFilter) bool {
	if measurement == nil {
		return false
	}
	values := measurement.TagValueSeriesIDs[filter.Key]
	if len(values) == 0 {
		return false
	}
	seriesIDs := values[filter.Value]
	return len(seriesIDs) > 0
}

func seriesFileMeasurementMatchingSeriesIDs(measurement *seriesFileMeasurement, filters []TagFilter) map[uint64]struct{} {
	if measurement == nil {
		return nil
	}
	matches := make(map[uint64]struct{}, len(measurement.SeriesIDs))
	for id := range measurement.SeriesIDs {
		matches[id] = struct{}{}
	}
	for _, filter := range filters {
		seriesByValue := measurement.TagValueSeriesIDs[filter.Key]
		if len(seriesByValue) == 0 {
			return nil
		}
		filterIDs := seriesByValue[filter.Value]
		if len(filterIDs) == 0 {
			return nil
		}
		for id := range matches {
			if _, ok := filterIDs[id]; !ok {
				delete(matches, id)
			}
		}
		if len(matches) == 0 {
			return nil
		}
	}
	return matches
}

func seriesFileQueryTagReports(measurement *seriesFileMeasurement, filters []TagFilter) []IndexQueryTagReport {
	if measurement == nil {
		return nil
	}
	if len(filters) > 0 {
		reports := make([]IndexQueryTagReport, 0, len(filters))
		for _, filter := range filters {
			valueSeries := measurement.TagValueSeriesIDs[filter.Key][filter.Value]
			if len(valueSeries) == 0 {
				continue
			}
			reports = append(reports, IndexQueryTagReport{
				Key: filter.Key,
				Values: []IndexQueryTagValueReport{{
					Value:       filter.Value,
					SeriesCount: uint64(len(valueSeries)),
				}},
			})
		}
		return reports
	}
	tagKeys := make([]string, 0, len(measurement.TagValues))
	for key := range measurement.TagValues {
		tagKeys = append(tagKeys, key)
	}
	sort.Strings(tagKeys)
	reports := make([]IndexQueryTagReport, 0, len(tagKeys))
	for _, key := range tagKeys {
		values := sortedSeriesFileTagValues(measurement.TagValues[key])
		report := IndexQueryTagReport{Key: key}
		for _, value := range values {
			report.Values = append(report.Values, IndexQueryTagValueReport{
				Value:       value,
				SeriesCount: uint64(len(measurement.TagValueSeriesIDs[key][value])),
			})
		}
		reports = append(reports, report)
	}
	return reports
}

func sortedSeriesFileTagValues(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func seriesFileKeySamples(state *seriesFileState, options Options) []string {
	if options.KeySampleLimit <= 0 {
		return nil
	}
	ids := sortedSeriesFileSampleIDs(state, options)
	samples := make([]string, 0, minInt(len(ids), options.KeySampleLimit))
	for _, id := range ids {
		series, ok := state.Live[id]
		if !ok {
			continue
		}
		samples = append(samples, fmt.Sprintf("id:%d %s", id, formatSeriesFileKey(series.Key)))
		if len(samples) >= options.KeySampleLimit {
			break
		}
	}
	return samples
}

func sortedSeriesFileSampleIDs(state *seriesFileState, options Options) []uint64 {
	if len(options.QuerySeriesIDs) > 0 {
		return append([]uint64(nil), options.QuerySeriesIDs...)
	}
	ids := make([]uint64, 0, len(state.Live))
	for id := range state.Live {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}

func formatSeriesFileKey(key seriesFileKey) string {
	var b strings.Builder
	b.WriteString(key.Measurement)
	for _, tag := range key.Tags {
		b.WriteByte(',')
		b.WriteString(tag.Key)
		b.WriteByte('=')
		b.WriteString(tag.Value)
	}
	return b.String()
}

func addSeriesFileIDFilterExtra(extra map[string]string, state *seriesFileState, options Options) {
	if len(options.QuerySeriesIDs) == 0 {
		return
	}
	matched := []uint64{}
	tombstoned := []uint64{}
	missing := []uint64{}
	for _, id := range options.QuerySeriesIDs {
		if _, ok := state.Live[id]; ok {
			matched = append(matched, id)
			continue
		}
		if _, ok := state.Tombstones[id]; ok {
			tombstoned = append(tombstoned, id)
			continue
		}
		missing = append(missing, id)
	}
	extra["query_series_id_filter_applied"] = "true"
	extra["query_series_ids"] = joinSeriesFileUint64s(options.QuerySeriesIDs)
	extra["query_matched_series_ids"] = joinSeriesFileUint64s(matched)
	extra["query_tombstone_series_ids"] = joinSeriesFileUint64s(tombstoned)
	extra["query_missing_series_ids"] = joinSeriesFileUint64s(missing)
}

func joinSeriesFileUint64s(values []uint64) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.FormatUint(value, 10))
	}
	return strings.Join(parts, ",")
}

func seriesFileBlockSamples(segments []seriesFileSegmentAnalysis, limit int) []BlockReport {
	if limit <= 0 || len(segments) == 0 {
		return nil
	}
	blocks := make([]BlockReport, 0, minInt(len(segments), limit))
	for _, segment := range segments {
		blocks = append(blocks, BlockReport{
			Key:        seriesFileDisplaySegment(segment.Ref),
			Type:       "series-segment",
			Offset:     int64(seriesSegmentHeaderSize),
			SizeBytes:  seriesFileUint32Size(segment.Ref.SizeBytes),
			ValueCount: len(segment.Entries),
		})
		if len(blocks) >= limit {
			break
		}
	}
	return blocks
}

func seriesFileUint32Size(size int64) uint32 {
	if size <= 0 {
		return 0
	}
	if size > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(size)
}

func seriesFileLiveIDSet(live map[uint64]seriesFileSeries) map[uint64]struct{} {
	ids := make(map[uint64]struct{}, len(live))
	for id := range live {
		ids[id] = struct{}{}
	}
	return ids
}

func seriesFileDisplaySegment(ref seriesFileSegmentRef) string {
	if ref.Partition == "" {
		return ref.Segment
	}
	return ref.Partition + "/" + ref.Segment
}

func sortSeriesFileSegments(segments []seriesFileSegmentRef) {
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].Partition == segments[j].Partition {
			return segments[i].ID < segments[j].ID
		}
		return segments[i].Partition < segments[j].Partition
	})
}

func isSeriesFilePath(path string) bool {
	if strings.EqualFold(filepath.Base(path), "_series") {
		return true
	}
	return isSeriesSegmentPath(path)
}

func isSeriesSegmentPath(path string) bool {
	if !isSeriesSegmentName(filepath.Base(path)) {
		return false
	}
	parent := filepath.Dir(path)
	if !isSeriesPartitionName(filepath.Base(parent)) {
		return false
	}
	return strings.EqualFold(filepath.Base(filepath.Dir(parent)), "_series")
}

func isSeriesPartitionPath(path string) bool {
	return isSeriesPartitionName(filepath.Base(path)) && strings.EqualFold(filepath.Base(filepath.Dir(path)), "_series")
}

func isSeriesPartitionName(name string) bool {
	if !isFixedLowerHex(name, 2) {
		return false
	}
	id, err := strconv.ParseUint(name, 16, 8)
	return err == nil && id < seriesFilePartitionN
}

func isSeriesSegmentName(name string) bool {
	return isFixedLowerHex(name, 4)
}

func isFixedLowerHex(name string, size int) bool {
	if len(name) != size {
		return false
	}
	for _, ch := range name {
		if ch >= '0' && ch <= '9' {
			continue
		}
		if ch >= 'a' && ch <= 'f' {
			continue
		}
		return false
	}
	return true
}

func isValidSeriesFileEntryFlag(flag byte) bool {
	return flag == seriesEntryInsertFlag || flag == seriesEntryTombstoneFlag
}
