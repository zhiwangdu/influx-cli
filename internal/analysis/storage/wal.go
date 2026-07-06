package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/snappy"
)

const (
	walFileExtension = ".wal"
	walFilePrefix    = "_"
	walEntryHeader   = 5

	walWriteEntryType       = 0x01
	walDeleteEntryType      = 0x02
	walDeleteRangeEntryType = 0x03

	walFloat64ValueType  = 1
	walIntegerValueType  = 2
	walBooleanValueType  = 3
	walStringValueType   = 4
	walUnsignedValueType = 5
)

type walEntryReport struct {
	Offset               int64
	SizeBytes            uint32
	CompressedBytes      uint32
	PayloadBytes         int
	Type                 string
	Keys                 []string
	WritePointSamples    []walWritePoint
	QueryWritePointCount int
	ValueCountsByKey     map[string]int
	TimeRangesByKey      map[string]walTimeRange
	KeyCount             int
	ValueCount           int
	MinTime              int64
	MaxTime              int64
	HasTime              bool
	QueryOverlaps        bool
}

type walWritePoint struct {
	Key   string
	Time  int64
	Type  string
	Value string
}

type walReadOptions struct {
	QueryKeySet           map[string]struct{}
	QueryRange            TimeRange
	WritePointSampleLimit int
	writePointSamplesLeft *int
}

type walTimeRange struct {
	Min int64
	Max int64
	Set bool
}

func analyzeWAL(path string, info os.FileInfo, options Options) (FileReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return FileReport{}, err
	}
	defer f.Close()

	keySet := queryKeySet(options.QueryKeys)
	entries, notices, err := readWALEntries(f, walReadOptions{
		QueryKeySet:           keySet,
		QueryRange:            options.QueryRange,
		WritePointSampleLimit: options.BlockSampleLimit,
	})
	if err != nil {
		return FileReport{}, err
	}
	walPopulateQueryOverlaps(entries, keySet, options.QueryRange)

	keys := walUniqueKeys(entries)
	minTime, maxTime, hasTime := walTimeBounds(entries)
	report := FileReport{
		Path:              path,
		Format:            FormatWAL,
		SizeBytes:         info.Size(),
		ModTime:           info.ModTime(),
		KeyCount:          len(keys),
		BlockCount:        len(entries),
		BlocksByType:      walBlocksByType(entries),
		QueryOverlapsFile: walAnyQueryOverlap(entries),
		Extra:             walExtra(entries),
		Notices:           notices,
	}
	if hasTime {
		report.MinTime = minTime
		report.MaxTime = maxTime
	}
	if len(keys) > 0 {
		report.MinKey = keys[0]
		report.MaxKey = keys[len(keys)-1]
		report.KeySamples = append(report.KeySamples, keys[:minInt(len(keys), options.KeySampleLimit)]...)
	}
	populateWALBlocks(&report, entries, options.BlockSampleLimit)
	report.DecodePath = buildWALDecodePathSummary(entries, options)
	return report, nil
}

func readWALEntries(r io.Reader, options walReadOptions) ([]walEntryReport, []string, error) {
	br := bufio.NewReader(r)
	entries := make([]walEntryReport, 0)
	notices := []string(nil)
	writePointSamplesLeft := options.WritePointSampleLimit
	options.writePointSamplesLeft = &writePointSamplesLeft
	var offset int64
	for {
		var header [walEntryHeader]byte
		n, err := io.ReadFull(br, header[:])
		if err == io.EOF {
			return entries, notices, nil
		}
		if err == io.ErrUnexpectedEOF {
			notices = append(notices, fmt.Sprintf("trailing partial WAL entry header at offset %d bytes=%d", offset, n))
			return entries, notices, nil
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read WAL entry header at offset %d: %w", offset, err)
		}
		offset += int64(n)

		entryType := header[0]
		compressedLen := binary.BigEndian.Uint32(header[1:])
		compressed := make([]byte, int(compressedLen))
		n, err = io.ReadFull(br, compressed)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			notices = append(notices, fmt.Sprintf("trailing partial WAL entry payload at offset %d bytes=%d want=%d", offset, n, compressedLen))
			return entries, notices, nil
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read WAL entry payload at offset %d length %d: %w", offset, compressedLen, err)
		}
		entryOffset := offset - walEntryHeader
		offset += int64(n)

		payload, err := snappy.Decode(nil, compressed)
		if err != nil {
			return nil, nil, fmt.Errorf("decode WAL entry at offset %d: %w", entryOffset, err)
		}
		entry, err := parseWALEntryPayload(entryType, payload, options)
		if err != nil {
			return nil, nil, fmt.Errorf("parse WAL entry at offset %d: %w", entryOffset, err)
		}
		entry.Offset = entryOffset
		entry.CompressedBytes = compressedLen
		entry.PayloadBytes = len(payload)
		entry.SizeBytes = compressedLen + walEntryHeader
		entries = append(entries, entry)
	}
}

func parseWALEntryPayload(entryType byte, payload []byte, options walReadOptions) (walEntryReport, error) {
	switch entryType {
	case walWriteEntryType:
		return parseWALWriteEntry(payload, options)
	case walDeleteEntryType:
		return parseWALDeleteEntry(payload), nil
	case walDeleteRangeEntryType:
		return parseWALDeleteRangeEntry(payload)
	default:
		return walEntryReport{}, fmt.Errorf("unknown WAL entry type %d", entryType)
	}
}

func parseWALWriteEntry(payload []byte, options walReadOptions) (walEntryReport, error) {
	entry := walEntryReport{
		Type:             "write",
		ValueCountsByKey: map[string]int{},
		TimeRangesByKey:  map[string]walTimeRange{},
	}
	offset := 0
	seen := map[string]struct{}{}
	for offset < len(payload) {
		valueType := payload[offset]
		offset++
		if len(payload)-offset < 2 {
			return entry, fmt.Errorf("short write key length")
		}
		keyLen := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
		offset += 2
		if len(payload)-offset < keyLen {
			return entry, fmt.Errorf("short write key")
		}
		key := string(payload[offset : offset+keyLen])
		offset += keyLen
		walAddUniqueKey(&entry.Keys, seen, key)

		if len(payload)-offset < 4 {
			return entry, fmt.Errorf("short write value count")
		}
		valueCount := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if valueCount <= 0 {
			return entry, fmt.Errorf("invalid write value count %d", valueCount)
		}
		minValueBytes, ok := walMinValueBytes(valueType)
		if !ok {
			return entry, fmt.Errorf("unsupported write value type %d", valueType)
		}
		if valueCount > (len(payload)-offset)/minValueBytes {
			return entry, fmt.Errorf("short write values count=%d type=%d", valueCount, valueType)
		}
		keyRange := walTimeRange{}
		for i := 0; i < valueCount; i++ {
			if len(payload)-offset < 8 {
				return entry, fmt.Errorf("short write timestamp")
			}
			timestamp := int64(binary.BigEndian.Uint64(payload[offset : offset+8]))
			offset += 8
			walAddTime(&entry, timestamp)
			keyRange = walExpandTimeRange(keyRange, timestamp)
			entry.ValueCount++

			selected := walWritePointSelected(key, timestamp, options.QueryKeySet, options.QueryRange)
			if selected {
				entry.QueryWritePointCount++
			}
			if selected && options.consumeWritePointSample() {
				nextOffset, valueTypeName, value, err := readWALValue(payload, offset, valueType)
				if err != nil {
					return entry, err
				}
				offset = nextOffset
				entry.WritePointSamples = append(entry.WritePointSamples, walWritePoint{
					Key:   key,
					Time:  timestamp,
					Type:  valueTypeName,
					Value: value,
				})
				continue
			}
			nextOffset, err := skipWALValue(payload, offset, valueType)
			if err != nil {
				return entry, err
			}
			offset = nextOffset
		}
		entry.ValueCountsByKey[key] += valueCount
		entry.TimeRangesByKey[key] = walMergeTimeRanges(entry.TimeRangesByKey[key], keyRange)
	}
	entry.KeyCount = len(entry.Keys)
	sort.Strings(entry.Keys)
	return entry, nil
}

func parseWALDeleteEntry(payload []byte) walEntryReport {
	entry := walEntryReport{Type: "delete"}
	if len(payload) == 0 {
		return entry
	}
	seen := map[string]struct{}{}
	for _, key := range strings.Split(string(payload), "\n") {
		if key == "" {
			continue
		}
		walAddUniqueKey(&entry.Keys, seen, key)
	}
	entry.KeyCount = len(entry.Keys)
	sort.Strings(entry.Keys)
	return entry
}

func parseWALDeleteRangeEntry(payload []byte) (walEntryReport, error) {
	entry := walEntryReport{Type: "delete-range"}
	if len(payload) < 16 {
		return entry, fmt.Errorf("short delete-range header")
	}
	entry.MinTime = int64(binary.BigEndian.Uint64(payload[:8]))
	entry.MaxTime = int64(binary.BigEndian.Uint64(payload[8:16]))
	entry.HasTime = true
	offset := 16
	seen := map[string]struct{}{}
	for offset < len(payload) {
		if len(payload)-offset < 4 {
			return entry, fmt.Errorf("short delete-range key length")
		}
		keyLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if len(payload)-offset < keyLen {
			return entry, fmt.Errorf("short delete-range key")
		}
		key := string(payload[offset : offset+keyLen])
		offset += keyLen
		walAddUniqueKey(&entry.Keys, seen, key)
	}
	entry.KeyCount = len(entry.Keys)
	sort.Strings(entry.Keys)
	return entry, nil
}

func (o walReadOptions) consumeWritePointSample() bool {
	if o.writePointSamplesLeft == nil || *o.writePointSamplesLeft <= 0 {
		return false
	}
	*o.writePointSamplesLeft = *o.writePointSamplesLeft - 1
	return true
}

func skipWALValue(payload []byte, offset int, valueType byte) (int, error) {
	switch valueType {
	case walFloat64ValueType, walIntegerValueType, walUnsignedValueType:
		if len(payload)-offset < 8 {
			return offset, fmt.Errorf("short numeric write value")
		}
		return offset + 8, nil
	case walBooleanValueType:
		if len(payload)-offset < 1 {
			return offset, fmt.Errorf("short boolean write value")
		}
		return offset + 1, nil
	case walStringValueType:
		if len(payload)-offset < 4 {
			return offset, fmt.Errorf("short string write value length")
		}
		valueLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if len(payload)-offset < valueLen {
			return offset, fmt.Errorf("short string write value")
		}
		return offset + valueLen, nil
	default:
		return offset, fmt.Errorf("unsupported write value type %d", valueType)
	}
}

func readWALValue(payload []byte, offset int, valueType byte) (int, string, string, error) {
	switch valueType {
	case walFloat64ValueType:
		if len(payload)-offset < 8 {
			return offset, "", "", fmt.Errorf("short numeric write value")
		}
		value := math.Float64frombits(binary.BigEndian.Uint64(payload[offset : offset+8]))
		return offset + 8, "float", strconv.FormatFloat(value, 'g', -1, 64), nil
	case walIntegerValueType:
		if len(payload)-offset < 8 {
			return offset, "", "", fmt.Errorf("short numeric write value")
		}
		value := int64(binary.BigEndian.Uint64(payload[offset : offset+8]))
		return offset + 8, "integer", strconv.FormatInt(value, 10), nil
	case walUnsignedValueType:
		if len(payload)-offset < 8 {
			return offset, "", "", fmt.Errorf("short numeric write value")
		}
		value := binary.BigEndian.Uint64(payload[offset : offset+8])
		return offset + 8, "unsigned", strconv.FormatUint(value, 10), nil
	case walBooleanValueType:
		if len(payload)-offset < 1 {
			return offset, "", "", fmt.Errorf("short boolean write value")
		}
		value := "false"
		if payload[offset] != 0 {
			value = "true"
		}
		return offset + 1, "boolean", value, nil
	case walStringValueType:
		if len(payload)-offset < 4 {
			return offset, "", "", fmt.Errorf("short string write value length")
		}
		valueLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if len(payload)-offset < valueLen {
			return offset, "", "", fmt.Errorf("short string write value")
		}
		return offset + valueLen, "string", string(payload[offset : offset+valueLen]), nil
	default:
		return offset, "", "", fmt.Errorf("unsupported write value type %d", valueType)
	}
}

func populateWALBlocks(report *FileReport, entries []walEntryReport, sampleLimit int) {
	for i, entry := range entries {
		if entry.QueryOverlaps {
			report.QueryOverlapBlocks++
		}
		if i >= sampleLimit {
			continue
		}
		block := BlockReport{
			Type:          "wal-" + entry.Type,
			Offset:        entry.Offset,
			SizeBytes:     entry.SizeBytes,
			ValueCount:    entry.ValueCount,
			QueryOverlaps: entry.QueryOverlaps,
		}
		if len(entry.Keys) > 0 {
			block.Key = entry.Keys[0]
		}
		if entry.HasTime {
			block.MinTime = entry.MinTime
			block.MaxTime = entry.MaxTime
		}
		report.Blocks = append(report.Blocks, block)
	}
}

func buildWALDecodePathSummary(entries []walEntryReport, options Options) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}
	keySet := queryKeySet(options.QueryKeys)
	summary := &DecodePathSummary{
		Mode:               "wal-replay-filter",
		QueryRange:         options.QueryRange,
		QueryKeys:          append([]string(nil), options.QueryKeys...),
		KeyFilterApplied:   len(keySet) > 0,
		DecodeBlocksByType: map[string]int{},
	}
	populateWALDecodeKeyMatches(summary, entries)
	for _, entry := range entries {
		selectedKey := walEntryKeySelected(entry, keySet)
		if !selectedKey {
			summary.SkippedByKeyBlocks++
			continue
		}
		summary.BaselineDecodeBlocks++
		summary.BaselineDecodeBytes += int64(entry.SizeBytes)
		if entry.ValueCount > 0 {
			summary.BaselineDecodeValues += entry.ValueCount
		}
		if !entry.QueryOverlaps {
			if entry.HasTime && entry.MaxTime < options.QueryRange.Min {
				summary.SkippedBeforeSeekBlocks++
			} else if entry.HasTime && entry.MinTime > options.QueryRange.Max {
				summary.SkippedAfterRangeBlocks++
			}
			appendWALDecodeSample(summary, entry, keySet, options.BlockSampleLimit)
			continue
		}
		summary.OptimizedDecodeBlocks++
		summary.FilteredDecodeBlocks++
		summary.OptimizedDecodeBytes += int64(entry.SizeBytes)
		if entry.ValueCount > 0 {
			summary.OptimizedDecodeValues += walEntryQueryValueCount(entry, keySet, options.QueryRange)
			outputPoints := walEntryQueryWritePointCount(entry)
			summary.OptimizedValueOutputPoints += outputPoints
		}
		summary.DecodeBlocksByType["wal-"+entry.Type]++
		appendWALDecodeSample(summary, entry, keySet, options.BlockSampleLimit)
		appendWALCursorOutputSamples(summary, entry, options.BlockSampleLimit)
	}
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.BaselineDecodeBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	summary.Recommendations = walDecodeRecommendations(summary)
	return summary
}

func appendWALDecodeSample(summary *DecodePathSummary, entry walEntryReport, keySet map[string]struct{}, sampleLimit int) {
	if len(summary.Samples) >= sampleLimit {
		return
	}
	reason := "outside_query_range"
	if entry.QueryOverlaps {
		reason = "overlaps_query_range"
	}
	sample := DecodePathBlockDecision{
		Type:              "wal-" + entry.Type,
		SizeBytes:         entry.SizeBytes,
		ValueCount:        entry.ValueCount,
		LocationCandidate: true,
		Decoded:           entry.QueryOverlaps,
		Reason:            reason,
	}
	if len(entry.Keys) > 0 {
		sample.Key = entry.Keys[0]
	}
	if entry.HasTime {
		sample.MinTime = entry.MinTime
		sample.MaxTime = entry.MaxTime
	}
	if entry.QueryOverlaps && entry.ValueCount > 0 {
		sample.OutputValues = walEntryQueryValueCount(entry, keySet, summary.QueryRange)
		sample.ValueOutputPoints = walEntryQueryWritePointCount(entry)
		sample.ValueOutputAvailable = true
	}
	summary.Samples = append(summary.Samples, sample)
}

func appendWALCursorOutputSamples(summary *DecodePathSummary, entry walEntryReport, sampleLimit int) {
	if sampleLimit <= 0 || entry.Type != "write" {
		return
	}
	for _, point := range entry.WritePointSamples {
		if len(summary.CursorOutputSamples) >= sampleLimit {
			return
		}
		summary.CursorOutputSamples = append(summary.CursorOutputSamples, DecodePathCursorOutput{
			Key:            point.Key,
			Time:           point.Time,
			Type:           "wal-write-" + point.Type,
			OptimizedValue: point.Value,
			Matches:        true,
		})
	}
}

func populateWALDecodeKeyMatches(summary *DecodePathSummary, entries []walEntryReport) {
	if len(summary.QueryKeys) == 0 {
		return
	}
	seen := map[string]struct{}{}
	for _, entry := range entries {
		for _, key := range entry.Keys {
			seen[key] = struct{}{}
		}
	}
	for _, key := range summary.QueryKeys {
		if _, ok := seen[key]; ok {
			summary.MatchedKeys = append(summary.MatchedKeys, key)
		} else {
			summary.MissingKeys = append(summary.MissingKeys, key)
		}
	}
}

func walDecodeRecommendations(summary *DecodePathSummary) []string {
	recommendations := make([]string, 0, 3)
	if len(summary.MissingKeys) > 0 {
		recommendations = append(recommendations, fmt.Sprintf("%d query key(s) were not found in analyzed WAL entries", len(summary.MissingKeys)))
	}
	if summary.SkippedByKeyBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf("key filter excludes %d WAL entry(s) from replay planning", summary.SkippedByKeyBlocks))
	}
	if summary.SavedDecodeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf("filter %d WAL entry(s) outside the query range before replay", summary.SavedDecodeBlocks))
	}
	if len(recommendations) == 0 && summary.OptimizedDecodeBlocks > 0 {
		recommendations = append(recommendations, "query range maps directly to WAL replay candidates")
	}
	if len(summary.CursorOutputSamples) > 0 {
		recommendations = append(recommendations, "sampled local WAL write points that match the replay key/range filters")
	}
	return recommendations
}

func walPopulateQueryOverlaps(entries []walEntryReport, keySet map[string]struct{}, queryRange TimeRange) {
	if !queryRange.Set {
		return
	}
	for i := range entries {
		if !walEntryKeySelected(entries[i], keySet) {
			continue
		}
		if entries[i].HasTime {
			entries[i].QueryOverlaps = walEntryTimeOverlaps(entries[i], keySet, queryRange)
			continue
		}
		if entries[i].Type == "delete" {
			entries[i].QueryOverlaps = true
		}
	}
}

func walEntryKeySelected(entry walEntryReport, keySet map[string]struct{}) bool {
	if len(keySet) == 0 {
		return true
	}
	for _, key := range entry.Keys {
		if _, ok := keySet[key]; ok {
			return true
		}
	}
	return false
}

func walEntryTimeOverlaps(entry walEntryReport, keySet map[string]struct{}, queryRange TimeRange) bool {
	if !entry.HasTime {
		return false
	}
	if len(keySet) == 0 || entry.Type != "write" {
		return queryRange.Overlaps(entry.MinTime, entry.MaxTime)
	}
	for _, key := range entry.Keys {
		if _, ok := keySet[key]; !ok {
			continue
		}
		if timeRange, ok := entry.TimeRangesByKey[key]; ok && timeRange.Set && queryRange.Overlaps(timeRange.Min, timeRange.Max) {
			return true
		}
	}
	return false
}

func walEntryQueryValueCount(entry walEntryReport, keySet map[string]struct{}, queryRange TimeRange) int {
	if entry.ValueCount == 0 || !walEntryTimeOverlaps(entry, keySet, queryRange) {
		return 0
	}
	if len(keySet) > 0 && len(entry.ValueCountsByKey) > 0 {
		total := 0
		for _, key := range entry.Keys {
			if _, ok := keySet[key]; !ok {
				continue
			}
			timeRange := entry.TimeRangesByKey[key]
			if timeRange.Set && queryRange.Overlaps(timeRange.Min, timeRange.Max) {
				total += entry.ValueCountsByKey[key]
			}
		}
		return total
	}
	if len(keySet) > 0 {
		return 0
	}
	// WAL write entries are summarized at key group granularity. Without
	// retaining every timestamp, report the selected key group's full point count.
	return entry.ValueCount
}

func walEntryQueryWritePointCount(entry walEntryReport) int {
	if entry.Type != "write" {
		return 0
	}
	return entry.QueryWritePointCount
}

func walWritePointSelected(key string, timestamp int64, keySet map[string]struct{}, queryRange TimeRange) bool {
	if len(keySet) > 0 {
		if _, ok := keySet[key]; !ok {
			return false
		}
	}
	return queryRange.Set && timestamp >= queryRange.Min && timestamp <= queryRange.Max
}

func walUniqueKeys(entries []walEntryReport) []string {
	seen := map[string]struct{}{}
	keys := make([]string, 0)
	for _, entry := range entries {
		for _, key := range entry.Keys {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func walTimeBounds(entries []walEntryReport) (int64, int64, bool) {
	minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)
	hasTime := false
	for _, entry := range entries {
		if !entry.HasTime {
			continue
		}
		hasTime = true
		if entry.MinTime < minTime {
			minTime = entry.MinTime
		}
		if entry.MaxTime > maxTime {
			maxTime = entry.MaxTime
		}
	}
	if !hasTime {
		return 0, 0, false
	}
	return minTime, maxTime, true
}

func walBlocksByType(entries []walEntryReport) map[string]int {
	counts := map[string]int{}
	for _, entry := range entries {
		counts["wal-"+entry.Type]++
	}
	return counts
}

func walExtra(entries []walEntryReport) map[string]string {
	writeEntries, deleteEntries, deleteRangeEntries := 0, 0, 0
	pointCount, keyRefs := 0, 0
	compressedBytes, payloadBytes := int64(0), int64(0)
	for _, entry := range entries {
		switch entry.Type {
		case "write":
			writeEntries++
			pointCount += entry.ValueCount
		case "delete":
			deleteEntries++
		case "delete-range":
			deleteRangeEntries++
		}
		keyRefs += entry.KeyCount
		compressedBytes += int64(entry.CompressedBytes)
		payloadBytes += int64(entry.PayloadBytes)
	}
	return map[string]string{
		"entry_count":          fmt.Sprint(len(entries)),
		"write_entries":        fmt.Sprint(writeEntries),
		"delete_entries":       fmt.Sprint(deleteEntries),
		"delete_range_entries": fmt.Sprint(deleteRangeEntries),
		"point_count":          fmt.Sprint(pointCount),
		"key_refs":             fmt.Sprint(keyRefs),
		"compressed_bytes":     fmt.Sprint(compressedBytes),
		"payload_bytes":        fmt.Sprint(payloadBytes),
		"compression":          "snappy",
	}
}

func walAnyQueryOverlap(entries []walEntryReport) bool {
	for _, entry := range entries {
		if entry.QueryOverlaps {
			return true
		}
	}
	return false
}

func walAddTime(entry *walEntryReport, timestamp int64) {
	if !entry.HasTime {
		entry.MinTime = timestamp
		entry.MaxTime = timestamp
		entry.HasTime = true
		return
	}
	if timestamp < entry.MinTime {
		entry.MinTime = timestamp
	}
	if timestamp > entry.MaxTime {
		entry.MaxTime = timestamp
	}
}

func walMinValueBytes(valueType byte) (int, bool) {
	switch valueType {
	case walFloat64ValueType, walIntegerValueType, walUnsignedValueType:
		return 16, true
	case walBooleanValueType:
		return 9, true
	case walStringValueType:
		return 12, true
	default:
		return 0, false
	}
}

func walExpandTimeRange(timeRange walTimeRange, timestamp int64) walTimeRange {
	if !timeRange.Set {
		return walTimeRange{Min: timestamp, Max: timestamp, Set: true}
	}
	if timestamp < timeRange.Min {
		timeRange.Min = timestamp
	}
	if timestamp > timeRange.Max {
		timeRange.Max = timestamp
	}
	return timeRange
}

func walMergeTimeRanges(a, b walTimeRange) walTimeRange {
	if !a.Set {
		return b
	}
	if !b.Set {
		return a
	}
	if b.Min < a.Min {
		a.Min = b.Min
	}
	if b.Max > a.Max {
		a.Max = b.Max
	}
	return a
}

func walAddUniqueKey(keys *[]string, seen map[string]struct{}, key string) {
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	*keys = append(*keys, key)
}

func isWALPath(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	return strings.HasPrefix(base, walFilePrefix) && strings.HasSuffix(base, walFileExtension)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
