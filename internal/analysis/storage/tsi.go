package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const (
	tsiMagic = "TSI1"

	tsiIndexFileVersion     = 1
	tsiIndexFileTrailerSize = 82

	tsiMeasurementBlockVersion     = 1
	tsiMeasurementBlockTrailerSize = 66
	tsiMeasurementFillSize         = 1

	tsiTagBlockVersion     = 1
	tsiTagBlockTrailerSize = 58

	tsiMeasurementTombstoneFlag   = 0x01
	tsiMeasurementSeriesIDSetFlag = 0x02
	tsiTagKeyTombstoneFlag        = 0x01
	tsiTagValueTombstoneFlag      = 0x01
	tsiTagValueSeriesIDSetFlag    = 0x02

	tsiRoaringSerialCookieNoRunContainer = 12346
	tsiRoaringSerialCookie               = 12347
	tsiRoaringNoOffsetThreshold          = 4
	tsiRoaringArrayContainerMaxSize      = 4096
	tsiRoaringBitmapContainerBytes       = 1 << 13
)

type tsiRange struct {
	Offset int64
	Size   int64
}

type tsiIndexTrailer struct {
	Version               int
	MeasurementBlock      tsiRange
	SeriesIDSet           tsiRange
	TombstoneSeriesIDSet  tsiRange
	SeriesSketch          tsiRange
	TombstoneSeriesSketch tsiRange
}

type tsiMeasurementBlockTrailer struct {
	Version   int
	Data      tsiRange
	HashIndex tsiRange
	Sketch    tsiRange
	TSketch   tsiRange
}

type tsiTagBlockTrailer struct {
	Version   int
	ValueData tsiRange
	KeyData   tsiRange
	HashIndex tsiRange
	Size      int64
}

type tsiMeasurementElem struct {
	Flag             byte
	Name             string
	TagBlock         tsiRange
	SeriesCount      uint64
	SeriesDataSize   uint64
	SeriesIDSet      bool
	EncodedByteCount int
}

type tsiTagBlockSummary struct {
	TagKeyCount          int
	DeletedTagKeyCount   int
	TagValueCount        int
	DeletedTagValueCount int
}

type tsiTagBlockInspection struct {
	Summary        tsiTagBlockSummary
	Tags           []IndexQueryTagReport
	MatchedFilters map[string]TagFilter
	MatchingSeries map[uint64]struct{}
}

type tsiTagKeyElem struct {
	Flag             byte
	Key              string
	Values           tsiRange
	ValueHashIndex   tsiRange
	EncodedByteCount int
}

type tsiTagValueElem struct {
	Flag             byte
	Value            string
	SeriesCount      uint64
	SeriesDataSize   uint64
	SeriesData       []byte
	SeriesIDSet      bool
	EncodedByteCount int
}

type tsiSeriesIDSetStats struct {
	Cardinality uint64
	Min         uint64
	Max         uint64
	HasRange    bool
}

type tsiRoaringInspectResult struct {
	Cardinality uint64
	IDs         []uint64
	Min         uint64
	Max         uint64
	HasRange    bool
}

func uint64Ptr(value uint64) *uint64 {
	return &value
}

func analyzeTSI(path string, info os.FileInfo, options Options) (FileReport, error) {
	if info.IsDir() {
		return FileReport{}, fmt.Errorf("tsi format requires a .tsi index file, got directory %s", filepath.Base(path))
	}
	if isTSILogPath(path) {
		return FileReport{}, fmt.Errorf("%s uses tsi-log format, not tsi", filepath.Base(path))
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return FileReport{}, err
	}
	if len(data) < len(tsiMagic)+tsiIndexFileTrailerSize {
		return FileReport{}, fmt.Errorf("file too small for TSI header/trailer")
	}
	if string(data[:len(tsiMagic)]) != tsiMagic {
		return FileReport{}, fmt.Errorf("invalid TSI magic")
	}

	trailer, err := parseTSIIndexTrailer(data)
	if err != nil {
		return FileReport{}, err
	}
	measurementBlock, err := sliceTSIRange(data, trailer.MeasurementBlock, "measurement block")
	if err != nil {
		return FileReport{}, err
	}

	index, keySamples, err := summarizeTSIMeasurements(data, measurementBlock, options)
	if err != nil {
		return FileReport{}, err
	}
	index.Type = "tsi1"
	index.SeriesIDSetBytes = trailer.SeriesIDSet.Size
	index.TombstoneSeriesSetBytes = trailer.TombstoneSeriesIDSet.Size
	index.SeriesSketchBytes = trailer.SeriesSketch.Size
	index.TombstoneSketchBytes = trailer.TombstoneSeriesSketch.Size

	notices := []string{}
	seriesIDCount := index.SeriesRefs
	seriesIDSetCardinalityOK := false
	seriesIDRangeOK := false
	if stats, err := tsiSeriesIDSetStatsForRange(data, trailer.SeriesIDSet, "series id set"); err != nil {
		notices = append(notices, fmt.Sprintf("series id set cardinality unavailable: %v", err))
	} else {
		index.SeriesIDSetCardinality = int64(stats.Cardinality)
		seriesIDCount = int64(stats.Cardinality)
		seriesIDSetCardinalityOK = true
		if stats.HasRange {
			index.SeriesIDSetMin = uint64Ptr(stats.Min)
			index.SeriesIDSetMax = uint64Ptr(stats.Max)
			seriesIDRangeOK = true
		}
	}
	tombstoneSeriesIDSetCardinalityOK := false
	tombstoneSeriesIDRangeOK := false
	if stats, err := tsiSeriesIDSetStatsForRange(data, trailer.TombstoneSeriesIDSet, "tombstone series id set"); err != nil {
		notices = append(notices, fmt.Sprintf("tombstone series id set cardinality unavailable: %v", err))
	} else {
		index.TombstoneSeriesIDSetCardinality = int64(stats.Cardinality)
		tombstoneSeriesIDSetCardinalityOK = true
		if stats.HasRange {
			index.TombstoneSeriesIDSetMin = uint64Ptr(stats.Min)
			index.TombstoneSeriesIDSetMax = uint64Ptr(stats.Max)
			tombstoneSeriesIDRangeOK = true
		}
	}

	extra := map[string]string{
		"version":                      fmt.Sprint(trailer.Version),
		"measurement_block_offset":     fmt.Sprint(trailer.MeasurementBlock.Offset),
		"measurement_block_size":       fmt.Sprint(trailer.MeasurementBlock.Size),
		"series_id_set_size":           fmt.Sprint(trailer.SeriesIDSet.Size),
		"tombstone_series_id_set_size": fmt.Sprint(trailer.TombstoneSeriesIDSet.Size),
		"series_sketch_size":           fmt.Sprint(trailer.SeriesSketch.Size),
		"tombstone_series_sketch_size": fmt.Sprint(trailer.TombstoneSeriesSketch.Size),
	}
	if seriesIDSetCardinalityOK {
		extra["series_id_set_cardinality"] = fmt.Sprint(index.SeriesIDSetCardinality)
	}
	if seriesIDRangeOK {
		extra["series_id_set_min"] = fmt.Sprint(*index.SeriesIDSetMin)
		extra["series_id_set_max"] = fmt.Sprint(*index.SeriesIDSetMax)
	}
	if tombstoneSeriesIDSetCardinalityOK {
		extra["tombstone_series_id_set_cardinality"] = fmt.Sprint(index.TombstoneSeriesIDSetCardinality)
	}
	if tombstoneSeriesIDRangeOK {
		extra["tombstone_series_id_set_min"] = fmt.Sprint(*index.TombstoneSeriesIDSetMin)
		extra["tombstone_series_id_set_max"] = fmt.Sprint(*index.TombstoneSeriesIDSetMax)
	}

	seriesID := SeriesIDSummary{Count: seriesIDCount}
	if index.SeriesIDSetMin != nil && index.SeriesIDSetMax != nil {
		seriesID.Min = *index.SeriesIDSetMin
		seriesID.Max = *index.SeriesIDSetMax
		seriesID.HasRange = true
	}

	report := FileReport{
		Path:       path,
		Format:     FormatTSI,
		SizeBytes:  info.Size(),
		ModTime:    info.ModTime(),
		KeyCount:   index.MeasurementCount,
		KeySamples: keySamples,
		BlockCount: index.MeasurementCount + index.TagKeyCount + index.TagValueCount,
		BlocksByType: map[string]int{
			"measurement": index.MeasurementCount,
			"tag-key":     index.TagKeyCount,
			"tag-value":   index.TagValueCount,
		},
		SeriesID: seriesID,
		Index:    &index,
		Extra:    extra,
		Notices:  notices,
	}
	return report, nil
}

func parseTSIIndexTrailer(data []byte) (tsiIndexTrailer, error) {
	var trailer tsiIndexTrailer
	if len(data) < tsiIndexFileTrailerSize {
		return trailer, fmt.Errorf("short TSI index trailer")
	}
	trailer.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if trailer.Version != tsiIndexFileVersion {
		return trailer, fmt.Errorf("unsupported TSI index version %d", trailer.Version)
	}

	buf := data[len(data)-tsiIndexFileTrailerSize:]
	trailer.MeasurementBlock, buf = readTSIRange(buf)
	trailer.SeriesIDSet, buf = readTSIRange(buf)
	trailer.TombstoneSeriesIDSet, buf = readTSIRange(buf)
	trailer.SeriesSketch, buf = readTSIRange(buf)
	trailer.TombstoneSeriesSketch, buf = readTSIRange(buf)
	if len(buf) != 2 {
		return trailer, fmt.Errorf("invalid TSI index trailer length")
	}
	if _, err := sliceTSIRange(data, trailer.MeasurementBlock, "measurement block"); err != nil {
		return trailer, err
	}
	for name, rng := range map[string]tsiRange{
		"series id set":           trailer.SeriesIDSet,
		"tombstone series id set": trailer.TombstoneSeriesIDSet,
		"series sketch":           trailer.SeriesSketch,
		"tombstone series sketch": trailer.TombstoneSeriesSketch,
	} {
		if _, err := sliceTSIRange(data, rng, name); err != nil {
			return trailer, err
		}
	}
	return trailer, nil
}

func summarizeTSIMeasurements(fileData, block []byte, options Options) (IndexSummary, []string, error) {
	var summary IndexSummary
	trailer, err := parseTSIMeasurementBlockTrailer(block)
	if err != nil {
		return summary, nil, err
	}
	data, err := sliceTSIRange(block, trailer.Data, "measurement data")
	if err != nil {
		return summary, nil, err
	}
	if len(data) < tsiMeasurementFillSize {
		return summary, nil, fmt.Errorf("short TSI measurement data")
	}
	data = data[tsiMeasurementFillSize:]

	keySamples := make([]string, 0, options.KeySampleLimit)
	query := newTSIIndexQueryBuilder(options)
	for len(data) > 0 {
		elem, err := parseTSIMeasurementElem(data)
		if err != nil {
			return summary, keySamples, err
		}
		data = data[elem.EncodedByteCount:]

		summary.MeasurementCount++
		summary.SeriesRefs += int64(elem.SeriesCount)
		if elem.Flag&tsiMeasurementTombstoneFlag != 0 {
			summary.DeletedMeasurementCount++
		}
		if len(keySamples) < options.KeySampleLimit {
			keySamples = append(keySamples, elem.Name)
		}

		inspectDetails := query != nil && query.measurementSelected(elem.Name)
		tagInspect, err := inspectTSITagBlock(fileData, elem.TagBlock, queryTagFilters(query), inspectDetails, options.BlockSampleLimit)
		if err != nil {
			return summary, keySamples, fmt.Errorf("measurement %q tag block: %w", elem.Name, err)
		}
		tagSummary := tagInspect.Summary
		summary.TagKeyCount += tagSummary.TagKeyCount
		summary.DeletedTagKeyCount += tagSummary.DeletedTagKeyCount
		summary.TagValueCount += tagSummary.TagValueCount
		summary.DeletedTagValueCount += tagSummary.DeletedTagValueCount
		if query != nil {
			query.observeMeasurement(elem, tagInspect, options)
		}
		if len(summary.MeasurementSamples) < options.KeySampleLimit {
			summary.MeasurementSamples = append(summary.MeasurementSamples, IndexMeasurementReport{
				Name:                 elem.Name,
				Deleted:              elem.Flag&tsiMeasurementTombstoneFlag != 0,
				SeriesCount:          elem.SeriesCount,
				TagKeyCount:          tagSummary.TagKeyCount,
				DeletedTagKeyCount:   tagSummary.DeletedTagKeyCount,
				TagValueCount:        tagSummary.TagValueCount,
				DeletedTagValueCount: tagSummary.DeletedTagValueCount,
			})
		}
	}
	if query != nil {
		summary.Query = query.finish()
	}
	return summary, keySamples, nil
}

type tsiIndexQueryBuilder struct {
	summary             IndexQuerySummary
	measurementSet      map[string]struct{}
	matchedMeasurements map[string]struct{}
	matchedTags         map[string]TagFilter
}

func newTSIIndexQueryBuilder(options Options) *tsiIndexQueryBuilder {
	if len(options.QueryMeasurements) == 0 && len(options.QueryTags) == 0 {
		return nil
	}
	builder := &tsiIndexQueryBuilder{
		summary: IndexQuerySummary{
			MeasurementFilterApplied: len(options.QueryMeasurements) > 0,
			TagFilterApplied:         len(options.QueryTags) > 0,
			QueryMeasurements:        append([]string(nil), options.QueryMeasurements...),
			QueryTags:                append([]TagFilter(nil), options.QueryTags...),
		},
		measurementSet:      queryKeySet(options.QueryMeasurements),
		matchedMeasurements: map[string]struct{}{},
		matchedTags:         map[string]TagFilter{},
	}
	return builder
}

func queryTagFilters(builder *tsiIndexQueryBuilder) []TagFilter {
	if builder == nil {
		return nil
	}
	return builder.summary.QueryTags
}

func (b *tsiIndexQueryBuilder) measurementSelected(name string) bool {
	if b == nil || len(b.measurementSet) == 0 {
		return true
	}
	_, ok := b.measurementSet[name]
	return ok
}

func (b *tsiIndexQueryBuilder) observeMeasurement(elem tsiMeasurementElem, tags tsiTagBlockInspection, options Options) {
	if b == nil || !b.measurementSelected(elem.Name) {
		return
	}
	if len(b.measurementSet) > 0 {
		b.matchedMeasurements[elem.Name] = struct{}{}
	}

	for id, filter := range tags.MatchedFilters {
		b.matchedTags[id] = filter
	}
	if elem.Flag&tsiMeasurementTombstoneFlag != 0 || !b.allTagsMatched(tags) {
		return
	}

	seriesCount := elem.SeriesCount
	if len(b.summary.QueryTags) > 0 {
		seriesCount = uint64(len(tags.MatchingSeries))
		if seriesCount == 0 {
			return
		}
	}

	b.summary.CandidateMeasurements++
	b.summary.SeriesRefs += int64(seriesCount)
	b.summary.TagKeyCount += tags.Summary.TagKeyCount
	b.summary.TagValueCount += tags.Summary.TagValueCount
	if len(b.summary.MeasurementSamples) < options.KeySampleLimit {
		b.summary.MeasurementSamples = append(b.summary.MeasurementSamples, IndexQueryMeasurementReport{
			Name:        elem.Name,
			SeriesCount: seriesCount,
			Tags:        tags.Tags,
		})
	}
}

func (b *tsiIndexQueryBuilder) allTagsMatched(tags tsiTagBlockInspection) bool {
	for _, filter := range b.summary.QueryTags {
		if _, ok := tags.MatchedFilters[tagFilterID(filter.Key, filter.Value)]; !ok {
			return false
		}
	}
	return true
}

func (b *tsiIndexQueryBuilder) finish() *IndexQuerySummary {
	if b == nil {
		return nil
	}
	if len(b.measurementSet) > 0 {
		for _, measurement := range b.summary.QueryMeasurements {
			if _, ok := b.matchedMeasurements[measurement]; ok {
				b.summary.MatchedMeasurements = append(b.summary.MatchedMeasurements, measurement)
			} else {
				b.summary.MissingMeasurements = append(b.summary.MissingMeasurements, measurement)
			}
		}
	}
	for _, filter := range b.summary.QueryTags {
		id := tagFilterID(filter.Key, filter.Value)
		if matched, ok := b.matchedTags[id]; ok {
			b.summary.MatchedTags = append(b.summary.MatchedTags, matched)
		} else {
			b.summary.MissingTags = append(b.summary.MissingTags, filter)
		}
	}
	return &b.summary
}

func parseTSIMeasurementBlockTrailer(data []byte) (tsiMeasurementBlockTrailer, error) {
	var trailer tsiMeasurementBlockTrailer
	if len(data) < tsiMeasurementBlockTrailerSize {
		return trailer, fmt.Errorf("short TSI measurement block trailer")
	}
	trailer.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if trailer.Version != tsiMeasurementBlockVersion {
		return trailer, fmt.Errorf("unsupported TSI measurement block version %d", trailer.Version)
	}
	buf := data[len(data)-tsiMeasurementBlockTrailerSize:]
	trailer.Data, buf = readTSIRange(buf)
	trailer.HashIndex, buf = readTSIRange(buf)
	trailer.Sketch, buf = readTSIRange(buf)
	trailer.TSketch, buf = readTSIRange(buf)
	if _, err := sliceTSIRange(data, trailer.Data, "measurement data"); err != nil {
		return trailer, err
	}
	for name, rng := range map[string]tsiRange{
		"measurement hash index": trailer.HashIndex,
		"measurement sketch":     trailer.Sketch,
		"measurement tsketch":    trailer.TSketch,
	} {
		if _, err := sliceTSIRange(data, rng, name); err != nil {
			return trailer, err
		}
	}
	return trailer, nil
}

func parseTSIMeasurementElem(data []byte) (tsiMeasurementElem, error) {
	var elem tsiMeasurementElem
	start := len(data)
	if len(data) < 1+8+8 {
		return elem, fmt.Errorf("short TSI measurement element")
	}
	elem.Flag, data = data[0], data[1:]
	elem.SeriesIDSet = elem.Flag&tsiMeasurementSeriesIDSetFlag != 0
	elem.TagBlock.Offset, data = int64(binary.BigEndian.Uint64(data[:8])), data[8:]
	elem.TagBlock.Size, data = int64(binary.BigEndian.Uint64(data[:8])), data[8:]

	nameLen, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("measurement name length: %w", err)
	}
	data = data[n:]
	if nameLen > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI measurement name")
	}
	elem.Name, data = string(data[:int(nameLen)]), data[int(nameLen):]

	seriesCount, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("measurement series count: %w", err)
	}
	elem.SeriesCount = seriesCount
	data = data[n:]

	seriesDataSize, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("measurement series data size: %w", err)
	}
	elem.SeriesDataSize = seriesDataSize
	data = data[n:]
	if seriesDataSize > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI measurement series data")
	}
	data = data[int(seriesDataSize):]
	elem.EncodedByteCount = start - len(data)
	return elem, nil
}

func summarizeTSITagBlock(fileData []byte, rng tsiRange) (tsiTagBlockSummary, error) {
	inspection, err := inspectTSITagBlock(fileData, rng, nil, false, 0)
	return inspection.Summary, err
}

func inspectTSITagBlock(fileData []byte, rng tsiRange, filters []TagFilter, includeDetails bool, sampleLimit int) (tsiTagBlockInspection, error) {
	var inspection tsiTagBlockInspection
	if rng.Size == 0 {
		return inspection, nil
	}
	block, err := sliceTSIRange(fileData, rng, "tag block")
	if err != nil {
		return inspection, err
	}
	trailer, err := parseTSITagBlockTrailer(block)
	if err != nil {
		return inspection, err
	}
	if trailer.Size != int64(len(block)) {
		return inspection, fmt.Errorf("tag block size mismatch: trailer=%d actual=%d", trailer.Size, len(block))
	}
	keyData, err := sliceTSIRange(block, trailer.KeyData, "tag key data")
	if err != nil {
		return inspection, err
	}
	filterSet := tagFilterSet(filters)
	for len(keyData) > 0 {
		key, err := parseTSITagKeyElem(keyData, block)
		if err != nil {
			return inspection, err
		}
		keyData = keyData[key.EncodedByteCount:]
		inspection.Summary.TagKeyCount++
		if key.Flag&tsiTagKeyTombstoneFlag != 0 {
			inspection.Summary.DeletedTagKeyCount++
		}
		values, err := sliceTSIRange(block, key.Values, "tag value data")
		if err != nil {
			return inspection, err
		}
		tagReport := IndexQueryTagReport{
			Key:     key.Key,
			Deleted: key.Flag&tsiTagKeyTombstoneFlag != 0,
		}
		for len(values) > 0 {
			value, err := parseTSITagValueElem(values)
			if err != nil {
				return inspection, err
			}
			values = values[value.EncodedByteCount:]
			inspection.Summary.TagValueCount++
			if value.Flag&tsiTagValueTombstoneFlag != 0 {
				inspection.Summary.DeletedTagValueCount++
			}

			id := tagFilterID(key.Key, value.Value)
			live := key.Flag&tsiTagKeyTombstoneFlag == 0 && value.Flag&tsiTagValueTombstoneFlag == 0
			if filter, ok := filterSet[id]; live && ok {
				if inspection.MatchedFilters == nil {
					inspection.MatchedFilters = map[string]TagFilter{}
				}
				inspection.MatchedFilters[id] = filter
				seriesIDs, err := tsiTagValueSeriesIDSet(value)
				if err != nil {
					return inspection, fmt.Errorf("tag %q=%q series id set: %w", key.Key, value.Value, err)
				}
				if inspection.MatchingSeries == nil {
					inspection.MatchingSeries = seriesIDs
				} else {
					inspection.MatchingSeries = intersectTSISeriesIDSets(inspection.MatchingSeries, seriesIDs)
				}
			}
			if !includeDetails || sampleLimit <= 0 {
				continue
			}
			if len(filterSet) > 0 {
				if _, ok := filterSet[id]; !ok {
					continue
				}
			}
			if len(tagReport.Values) >= sampleLimit {
				continue
			}
			tagReport.Values = append(tagReport.Values, IndexQueryTagValueReport{
				Value:       value.Value,
				Deleted:     value.Flag&tsiTagValueTombstoneFlag != 0,
				SeriesCount: value.SeriesCount,
			})
		}
		if includeDetails && sampleLimit > 0 && len(inspection.Tags) < sampleLimit && (len(filterSet) == 0 || len(tagReport.Values) > 0) {
			inspection.Tags = append(inspection.Tags, tagReport)
		}
	}
	return inspection, nil
}

func tagFilterSet(filters []TagFilter) map[string]TagFilter {
	if len(filters) == 0 {
		return nil
	}
	set := make(map[string]TagFilter, len(filters))
	for _, filter := range filters {
		set[tagFilterID(filter.Key, filter.Value)] = filter
	}
	return set
}

func tagFilterID(key, value string) string {
	return key + "\x00" + value
}

func parseTSITagBlockTrailer(data []byte) (tsiTagBlockTrailer, error) {
	var trailer tsiTagBlockTrailer
	if len(data) < tsiTagBlockTrailerSize {
		return trailer, fmt.Errorf("short TSI tag block trailer")
	}
	trailer.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if trailer.Version != tsiTagBlockVersion {
		return trailer, fmt.Errorf("unsupported TSI tag block version %d", trailer.Version)
	}
	buf := data[len(data)-tsiTagBlockTrailerSize:]
	trailer.ValueData, buf = readTSIRange(buf)
	trailer.KeyData, buf = readTSIRange(buf)
	trailer.HashIndex, buf = readTSIRange(buf)
	trailer.Size = int64(binary.BigEndian.Uint64(buf[:8]))
	for name, rng := range map[string]tsiRange{
		"tag value data": trailer.ValueData,
		"tag key data":   trailer.KeyData,
		"tag hash index": trailer.HashIndex,
	} {
		if _, err := sliceTSIRange(data, rng, name); err != nil {
			return trailer, err
		}
	}
	return trailer, nil
}

func parseTSITagKeyElem(data, block []byte) (tsiTagKeyElem, error) {
	var elem tsiTagKeyElem
	start := len(data)
	if len(data) < 1+8+8+8+8 {
		return elem, fmt.Errorf("short TSI tag key element")
	}
	elem.Flag, data = data[0], data[1:]
	elem.Values, data = readTSIRange(data)
	elem.ValueHashIndex, data = readTSIRange(data)
	if _, err := sliceTSIRange(block, elem.Values, "tag key value data"); err != nil {
		return elem, err
	}
	if _, err := sliceTSIRange(block, elem.ValueHashIndex, "tag key value hash index"); err != nil {
		return elem, err
	}
	keyLen, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("tag key length: %w", err)
	}
	data = data[n:]
	if keyLen > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI tag key")
	}
	elem.Key, data = string(data[:int(keyLen)]), data[int(keyLen):]
	elem.EncodedByteCount = start - len(data)
	return elem, nil
}

func parseTSITagValueElem(data []byte) (tsiTagValueElem, error) {
	var elem tsiTagValueElem
	start := len(data)
	if len(data) < 1 {
		return elem, fmt.Errorf("short TSI tag value element")
	}
	elem.Flag, data = data[0], data[1:]
	elem.SeriesIDSet = elem.Flag&tsiTagValueSeriesIDSetFlag != 0
	valueLen, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("tag value length: %w", err)
	}
	data = data[n:]
	if valueLen > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI tag value")
	}
	elem.Value, data = string(data[:int(valueLen)]), data[int(valueLen):]

	seriesCount, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("tag value series count: %w", err)
	}
	elem.SeriesCount = seriesCount
	data = data[n:]

	seriesDataSize, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("tag value series data size: %w", err)
	}
	elem.SeriesDataSize = seriesDataSize
	data = data[n:]
	if seriesDataSize > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI tag value series data")
	}
	elem.SeriesData, data = data[:int(seriesDataSize)], data[int(seriesDataSize):]
	elem.EncodedByteCount = start - len(data)
	return elem, nil
}

func readTSIRange(data []byte) (tsiRange, []byte) {
	return tsiRange{
		Offset: int64(binary.BigEndian.Uint64(data[:8])),
		Size:   int64(binary.BigEndian.Uint64(data[8:16])),
	}, data[16:]
}

func sliceTSIRange(data []byte, rng tsiRange, name string) ([]byte, error) {
	if rng.Offset < 0 || rng.Size < 0 || rng.Offset > int64(len(data)) || rng.Size > int64(len(data))-rng.Offset {
		return nil, fmt.Errorf("invalid TSI %s range offset=%d size=%d", name, rng.Offset, rng.Size)
	}
	return data[rng.Offset : rng.Offset+rng.Size], nil
}

func tsiSeriesIDSetStatsForRange(fileData []byte, rng tsiRange, name string) (tsiSeriesIDSetStats, error) {
	data, err := sliceTSIRange(fileData, rng, name)
	if err != nil {
		return tsiSeriesIDSetStats{}, err
	}
	return tsiRoaringStats(data)
}

func tsiRoaringSeriesIDs(data []byte) ([]uint64, error) {
	result, err := tsiRoaringInspect(data, true, false)
	return result.IDs, err
}

func tsiRoaringStats(data []byte) (tsiSeriesIDSetStats, error) {
	result, err := tsiRoaringInspect(data, false, true)
	if err != nil {
		return tsiSeriesIDSetStats{}, err
	}
	return tsiSeriesIDSetStats{
		Cardinality: result.Cardinality,
		Min:         result.Min,
		Max:         result.Max,
		HasRange:    result.HasRange,
	}, nil
}

func tsiRoaringInspect(data []byte, emitIDs, trackRange bool) (tsiRoaringInspectResult, error) {
	var result tsiRoaringInspectResult
	if len(data) == 0 {
		return result, nil
	}
	if len(data) < 8 {
		return result, fmt.Errorf("short roaring bitmap: %d bytes", len(data))
	}

	pos := 4
	cookie := binary.LittleEndian.Uint32(data[:4])
	var size uint32
	haveRunContainers := false
	var runBitmap []byte
	switch {
	case cookie&0x0000ffff == tsiRoaringSerialCookie:
		haveRunContainers = true
		size = uint32(uint16(cookie>>16) + 1)
		runBitmapSize := (int(size) + 7) / 8
		if runBitmapSize > len(data)-pos {
			return result, fmt.Errorf("roaring run-container bitmap overruns buffer")
		}
		runBitmap = data[pos : pos+runBitmapSize]
		pos += runBitmapSize
	case cookie == tsiRoaringSerialCookieNoRunContainer:
		size = binary.LittleEndian.Uint32(data[pos:])
		pos += 4
	default:
		return result, fmt.Errorf("invalid roaring cookie %d", cookie)
	}
	if size > 1<<16 {
		return result, fmt.Errorf("roaring bitmap has impossible container count %d", size)
	}

	headerSize := int(size) * 4
	if headerSize > len(data)-pos {
		return result, fmt.Errorf("roaring container header overruns buffer")
	}
	keys := make([]uint16, int(size))
	cards := make([]int, int(size))
	for i := 0; i < int(size); i++ {
		keys[i] = binary.LittleEndian.Uint16(data[pos+i*4:])
		card := int(binary.LittleEndian.Uint16(data[pos+i*4+2:])) + 1
		cards[i] = card
		result.Cardinality += uint64(card)
	}
	pos += headerSize

	if !haveRunContainers || size >= tsiRoaringNoOffsetThreshold {
		offsetSize := int(size) * 4
		if offsetSize > len(data)-pos {
			return result, fmt.Errorf("roaring container offsets overrun buffer")
		}
		pos += offsetSize
	}

	if emitIDs {
		result.IDs = make([]uint64, 0, result.Cardinality)
	}
	for i, card := range cards {
		containerBase := uint64(keys[i]) << 16
		if haveRunContainers && runBitmap[i/8]&(1<<uint(i%8)) != 0 {
			if len(data)-pos < 2 {
				return result, fmt.Errorf("short roaring run container")
			}
			runCount := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
			runBytes := runCount * 4
			if runBytes > len(data)-pos {
				return result, fmt.Errorf("roaring run container overruns buffer")
			}
			if emitIDs {
				for run := 0; run < runCount; run++ {
					start := binary.LittleEndian.Uint16(data[pos+run*4:])
					length := binary.LittleEndian.Uint16(data[pos+run*4+2:])
					for offset := uint32(0); offset <= uint32(length); offset++ {
						result.observeSeriesID(containerBase+uint64(start)+uint64(offset), emitIDs, trackRange)
					}
				}
			}
			if trackRange && runCount > 0 {
				start := binary.LittleEndian.Uint16(data[pos:])
				lastStart := binary.LittleEndian.Uint16(data[pos+(runCount-1)*4:])
				lastLength := binary.LittleEndian.Uint16(data[pos+(runCount-1)*4+2:])
				result.observeSeriesIDRange(containerBase+uint64(start), containerBase+uint64(lastStart)+uint64(lastLength))
			}
			pos += runBytes
			continue
		}
		containerBytes := card * 2
		if card > tsiRoaringArrayContainerMaxSize {
			containerBytes = tsiRoaringBitmapContainerBytes
		}
		if containerBytes > len(data)-pos {
			return result, fmt.Errorf("roaring container data overruns buffer")
		}
		if emitIDs {
			if card > tsiRoaringArrayContainerMaxSize {
				for word := 0; word < containerBytes; word++ {
					bits := data[pos+word]
					for bit := 0; bit < 8; bit++ {
						if bits&(1<<uint(bit)) != 0 {
							result.observeSeriesID(containerBase+uint64(word*8+bit), emitIDs, trackRange)
						}
					}
				}
			} else {
				for j := 0; j < card; j++ {
					result.observeSeriesID(containerBase+uint64(binary.LittleEndian.Uint16(data[pos+j*2:])), emitIDs, trackRange)
				}
			}
		}
		if trackRange {
			if card > tsiRoaringArrayContainerMaxSize {
				if first, last, ok := tsiRoaringBitmapContainerMinMax(data[pos : pos+containerBytes]); ok {
					result.observeSeriesIDRange(containerBase+uint64(first), containerBase+uint64(last))
				}
			} else if card > 0 {
				first := binary.LittleEndian.Uint16(data[pos:])
				last := binary.LittleEndian.Uint16(data[pos+(card-1)*2:])
				result.observeSeriesIDRange(containerBase+uint64(first), containerBase+uint64(last))
			}
		}
		pos += containerBytes
	}

	return result, nil
}

func (r *tsiRoaringInspectResult) observeSeriesID(id uint64, emitID, trackRange bool) {
	if emitID {
		r.IDs = append(r.IDs, id)
	}
	if !trackRange {
		return
	}
	if !r.HasRange || id < r.Min {
		r.Min = id
	}
	if !r.HasRange || id > r.Max {
		r.Max = id
	}
	r.HasRange = true
}

func (r *tsiRoaringInspectResult) observeSeriesIDRange(minID, maxID uint64) {
	if !r.HasRange || minID < r.Min {
		r.Min = minID
	}
	if !r.HasRange || maxID > r.Max {
		r.Max = maxID
	}
	r.HasRange = true
}

func tsiRoaringBitmapContainerMinMax(data []byte) (uint16, uint16, bool) {
	var min uint16
	foundMin := false
	for i, b := range data {
		if b == 0 {
			continue
		}
		for bit := 0; bit < 8; bit++ {
			if b&(1<<uint(bit)) != 0 {
				min = uint16(i*8 + bit)
				foundMin = true
				break
			}
		}
		break
	}
	if !foundMin {
		return 0, 0, false
	}

	for i := len(data) - 1; i >= 0; i-- {
		b := data[i]
		if b == 0 {
			continue
		}
		for bit := 7; bit >= 0; bit-- {
			if b&(1<<uint(bit)) != 0 {
				return min, uint16(i*8 + bit), true
			}
		}
	}
	return 0, 0, false
}

func tsiTagValueSeriesIDSet(value tsiTagValueElem) (map[uint64]struct{}, error) {
	var ids []uint64
	var err error
	if value.SeriesIDSet {
		ids, err = tsiRoaringSeriesIDs(value.SeriesData)
	} else {
		ids, err = tsiDeltaSeriesIDs(value.SeriesData)
	}
	if err != nil {
		return nil, err
	}
	set := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set, nil
}

func tsiDeltaSeriesIDs(data []byte) ([]uint64, error) {
	ids := []uint64{}
	var prev uint64
	for len(data) > 0 {
		delta, n, err := readTSIUvarint(data)
		if err != nil {
			return nil, err
		}
		if delta > ^uint64(0)-prev {
			return nil, fmt.Errorf("series id delta overflows uint64")
		}
		seriesID := prev + delta
		ids = append(ids, seriesID)
		prev = seriesID
		data = data[n:]
	}
	return ids, nil
}

func intersectTSISeriesIDSets(a, b map[uint64]struct{}) map[uint64]struct{} {
	if len(a) == 0 || len(b) == 0 {
		return map[uint64]struct{}{}
	}
	if len(b) < len(a) {
		a, b = b, a
	}
	out := make(map[uint64]struct{}, len(a))
	for id := range a {
		if _, ok := b[id]; ok {
			out[id] = struct{}{}
		}
	}
	return out
}

func readTSIUvarint(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("short buffer")
	}
	value, n := binary.Uvarint(data)
	if n == 0 || n > len(data) {
		return 0, 0, fmt.Errorf("short buffer")
	}
	if n < 0 {
		return 0, 0, fmt.Errorf("invalid uvarint")
	}
	return value, n, nil
}
