package storage

import (
	"fmt"
	"sort"
)

type tsmCursorCandidate struct {
	entry   tsmIndexEntry
	index   int
	decoded bool
}

func buildTSMDecodePathSummary(entries []tsmIndexEntry, tombstones []tsmTombstoneEntry, options Options) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                 tsmCursorMode("tsm-key-cursor", options),
		QueryRange:           options.QueryRange,
		CursorSeekTime:       tsmCursorSeekTime(options),
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	keySet := queryKeySet(options.QueryKeys)
	if len(keySet) > 0 {
		summary.QueryKeys = append([]string(nil), options.QueryKeys...)
		summary.KeyFilterApplied = true
	}

	matchedKeys := map[string]struct{}{}
	locationsByKey := map[string][]tsmCursorCandidate{}
	outputByKey := map[string]map[int64]struct{}{}
	for i, entry := range entries {
		typeName := tsmBlockTypeName(entry.Type)
		selectedKey := tsmQueryKeySelected(entry.Key, keySet)
		if len(keySet) > 0 && selectedKey {
			matchedKeys[entry.Key] = struct{}{}
		}
		queryOverlaps := options.QueryRange.Overlaps(entry.MinTime, entry.MaxTime)
		locationCandidate := true
		decoded := false
		reason := "query_overlap"
		outputTimestamps := []int64(nil)
		outputPoints := []tsmPoint(nil)
		valueOutputAvailable := false

		switch {
		case !selectedKey:
			locationCandidate = false
			summary.SkippedByKeyBlocks++
			reason = "key_not_selected"
		case tsmEntryFullyTombstoned(entry, tombstones):
			locationCandidate = false
			summary.FullyTombstonedBlocks++
			reason = "fully_tombstoned"
		case !options.CursorDescending && entry.MaxTime < options.QueryRange.Min:
			locationCandidate = false
			summary.SkippedBeforeSeekBlocks++
			reason = "before_cursor_seek"
		case options.CursorDescending && entry.MinTime > options.QueryRange.Max:
			locationCandidate = false
			summary.SkippedAfterRangeBlocks++
			reason = "after_cursor_seek"
		case !queryOverlaps:
			summary.LocationBlocks++
			summary.BaselineDecodeBytes += int64(entry.Size)
			if entry.ValueCountAvailable {
				summary.BaselineDecodeValues += entry.ValueCount
			}
			outputTimestamps = tsmOutputTimestamps(entry, tombstones, options.QueryRange)
			summary.BaselineOutputValues += len(outputTimestamps)
			outputPoints, valueOutputAvailable = tsmOutputPoints(entry, tombstones, options.QueryRange)
			if valueOutputAvailable {
				summary.BaselineValueOutputPoints += len(outputPoints)
			}
			summary.LocationBlocksByType[typeName]++
			summary.SkippedAfterRangeBlocks++
			reason = "outside_query_range"
			locationsByKey[entry.Key] = append(locationsByKey[entry.Key], tsmCursorCandidate{
				entry: entry,
				index: i,
			})
		default:
			summary.LocationBlocks++
			summary.FilteredDecodeBlocks++
			summary.BaselineDecodeBytes += int64(entry.Size)
			summary.OptimizedDecodeBytes += int64(entry.Size)
			if entry.ValueCountAvailable {
				summary.BaselineDecodeValues += entry.ValueCount
				summary.OptimizedDecodeValues += entry.ValueCount
			}
			outputTimestamps = tsmOutputTimestamps(entry, tombstones, options.QueryRange)
			summary.BaselineOutputValues += len(outputTimestamps)
			summary.OptimizedOutputValues += len(outputTimestamps)
			addTSMOutputTimestamps(outputByKey, entry.Key, outputTimestamps)
			outputPoints, valueOutputAvailable = tsmOutputPoints(entry, tombstones, options.QueryRange)
			if valueOutputAvailable {
				summary.BaselineValueOutputPoints += len(outputPoints)
				summary.OptimizedValueOutputPoints += len(outputPoints)
			} else {
				summary.ValueOutputUnavailableBlocks++
			}
			summary.LocationBlocksByType[typeName]++
			summary.DecodeBlocksByType[typeName]++
			decoded = true
			locationsByKey[entry.Key] = append(locationsByKey[entry.Key], tsmCursorCandidate{
				entry:   entry,
				index:   i,
				decoded: true,
			})
		}

		if len(summary.Samples) < options.BlockSampleLimit {
			summary.Samples = append(summary.Samples, DecodePathBlockDecision{
				Key:                  entry.Key,
				MinTime:              entry.MinTime,
				MaxTime:              entry.MaxTime,
				Type:                 typeName,
				SizeBytes:            entry.Size,
				ValueCount:           entry.ValueCount,
				OutputValues:         len(outputTimestamps),
				ValueOutputPoints:    len(outputPoints),
				ValueOutputAvailable: valueOutputAvailable,
				LocationCandidate:    locationCandidate,
				Decoded:              decoded,
				Reason:               reason,
			})
		}
	}

	if len(keySet) > 0 {
		for _, key := range options.QueryKeys {
			if _, ok := matchedKeys[key]; ok {
				summary.MatchedKeys = append(summary.MatchedKeys, key)
			} else {
				summary.MissingKeys = append(summary.MissingKeys, key)
			}
		}
	}
	summary.BaselineDecodeBlocks = summary.LocationBlocks
	summary.OptimizedDecodeBlocks = summary.FilteredDecodeBlocks
	summary.SavedDecodeBlocks = summary.LocationBlocks - summary.FilteredDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	summary.DeduplicatedOutputValues = countTSMOutputTimestamps(outputByKey)
	summary.DuplicateOutputValues = summary.OptimizedOutputValues - summary.DeduplicatedOutputValues
	baselineExecution := executeTSMCandidateCursorOutputs(locationsByKey, tombstones, options.QueryRange, false, options.CursorDescending)
	optimizedExecution := executeTSMCandidateCursorOutputs(locationsByKey, tombstones, options.QueryRange, true, options.CursorDescending)
	summarizeTSMCursorOutput(summary, baselineExecution, optimizedExecution, options.BlockSampleLimit)
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	populateTSMCursorWindows(summary, locationsByKey, options.BlockSampleLimit, options.CursorDescending)
	summary.Recommendations = tsmDecodeRecommendations(summary)
	return summary
}

func tsmCursorMode(prefix string, options Options) string {
	if options.CursorDescending {
		return prefix + "-descending"
	}
	return prefix + "-ascending"
}

func tsmCursorSeekTime(options Options) int64 {
	if options.CursorDescending {
		return options.QueryRange.Max
	}
	return options.QueryRange.Min
}

func tsmOutputTimestamps(entry tsmIndexEntry, tombstones []tsmTombstoneEntry, queryRange TimeRange) []int64 {
	if !queryRange.Set || len(entry.Timestamps) == 0 {
		return nil
	}
	timestamps := make([]int64, 0)
	for _, timestamp := range entry.Timestamps {
		if timestamp < queryRange.Min || timestamp > queryRange.Max {
			continue
		}
		if tsmTimestampTombstoned(entry.Key, timestamp, tombstones) {
			continue
		}
		timestamps = append(timestamps, timestamp)
	}
	return timestamps
}

func tsmTimestampTombstoned(key string, timestamp int64, tombstones []tsmTombstoneEntry) bool {
	for _, tombstone := range tombstones {
		if tombstone.Key == key && tombstone.Min <= timestamp && tombstone.Max >= timestamp {
			return true
		}
	}
	return false
}

func tsmOutputPoints(entry tsmIndexEntry, tombstones []tsmTombstoneEntry, queryRange TimeRange) ([]tsmPoint, bool) {
	if !queryRange.Set || !entry.PointsAvailable {
		return nil, entry.PointsAvailable
	}
	points := make([]tsmPoint, 0)
	for _, point := range entry.Points {
		if point.Timestamp < queryRange.Min || point.Timestamp > queryRange.Max {
			continue
		}
		if tsmTimestampTombstoned(entry.Key, point.Timestamp, tombstones) {
			continue
		}
		points = append(points, point)
	}
	return points, true
}

type tsmOutputPointKey struct {
	key       string
	timestamp int64
	typ       byte
}

func addTSMOutputPoints(output map[tsmOutputPointKey]tsmPoint, key string, points []tsmPoint) {
	for _, point := range points {
		output[tsmOutputPointKey{
			key:       key,
			timestamp: point.Timestamp,
			typ:       point.Type,
		}] = point
	}
}

func summarizeTSMCursorOutput(summary *DecodePathSummary, baseline, optimized tsmCursorExecution, sampleLimit int) {
	summary.BaselineCursorOutputPoints = len(baseline.Points)
	summary.OptimizedCursorOutputPoints = len(optimized.Points)
	summary.BaselineCursorReadCalls = baseline.ReadCalls
	summary.OptimizedCursorReadCalls = optimized.ReadCalls
	summary.ComparedValueOutputPoints, summary.ValueOutputMismatches, summary.CursorOutputSamples = compareTSMOutputPoints(baseline.Points, optimized.Points, sampleLimit)
	summary.CursorFinalOutputSamples = sampleTSMFinalOutputPoints(baseline.Points, optimized.Points, sampleLimit)
}

func compareTSMOutputPoints(baseline, optimized map[tsmOutputPointKey]tsmPoint, sampleLimit int) (int, int, []DecodePathCursorOutput) {
	keys := sortedTSMOutputPointKeys(baseline, optimized)
	compared := 0
	mismatches := 0
	samples := make([]DecodePathCursorOutput, 0)
	for _, key := range keys {
		compared++
		baselinePoint, baselineOK := baseline[key]
		optimizedPoint, optimizedOK := optimized[key]
		matches := baselineOK && optimizedOK && optimizedPoint.Value == baselinePoint.Value
		if !matches {
			mismatches++
		}
		if sampleLimit > 0 && len(samples) < sampleLimit {
			sample := DecodePathCursorOutput{
				Key:     key.key,
				Time:    key.timestamp,
				Type:    tsmBlockTypeName(key.typ),
				Matches: matches,
			}
			if baselineOK {
				sample.BaselineValue = baselinePoint.Value
			}
			if optimizedOK {
				sample.OptimizedValue = optimizedPoint.Value
			}
			samples = append(samples, sample)
		}
	}
	return compared, mismatches, samples
}

func sampleTSMFinalOutputPoints(baseline, optimized map[tsmOutputPointKey]tsmPoint, sampleLimit int) []DecodePathCursorOutput {
	if sampleLimit <= 0 || len(optimized) == 0 {
		return nil
	}
	keys := sortedTSMOutputPointKeys(optimized)
	samples := make([]DecodePathCursorOutput, 0, minInt(sampleLimit, len(keys)))
	for _, key := range keys {
		if len(samples) >= sampleLimit {
			break
		}
		point := optimized[key]
		baselinePoint, baselineOK := baseline[key]
		samples = append(samples, DecodePathCursorOutput{
			Key:            key.key,
			Time:           key.timestamp,
			Type:           tsmBlockTypeName(key.typ),
			File:           point.File,
			OptimizedValue: point.Value,
			Matches:        baselineOK && baselinePoint.Value == point.Value,
		})
	}
	return samples
}

func sortedTSMOutputPointKeys(pointSets ...map[tsmOutputPointKey]tsmPoint) []tsmOutputPointKey {
	if len(pointSets) == 1 {
		keys := make([]tsmOutputPointKey, 0, len(pointSets[0]))
		for key := range pointSets[0] {
			keys = append(keys, key)
		}
		sortTSMOutputPointKeys(keys)
		return keys
	}
	total := 0
	for _, points := range pointSets {
		total += len(points)
	}
	keys := make([]tsmOutputPointKey, 0, total)
	seen := map[tsmOutputPointKey]struct{}{}
	for _, points := range pointSets {
		for key := range points {
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	sortTSMOutputPointKeys(keys)
	return keys
}

func sortTSMOutputPointKeys(keys []tsmOutputPointKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].key != keys[j].key {
			return keys[i].key < keys[j].key
		}
		if keys[i].timestamp != keys[j].timestamp {
			return keys[i].timestamp < keys[j].timestamp
		}
		return keys[i].typ < keys[j].typ
	})
}

func addTSMOutputTimestamps(outputByKey map[string]map[int64]struct{}, key string, timestamps []int64) {
	if len(timestamps) == 0 {
		return
	}
	values := outputByKey[key]
	if values == nil {
		values = map[int64]struct{}{}
		outputByKey[key] = values
	}
	for _, timestamp := range timestamps {
		values[timestamp] = struct{}{}
	}
}

func countTSMOutputTimestamps(outputByKey map[string]map[int64]struct{}) int {
	total := 0
	for _, values := range outputByKey {
		total += len(values)
	}
	return total
}

func populateTSMCursorWindows(summary *DecodePathSummary, locationsByKey map[string][]tsmCursorCandidate, sampleLimit int, descending bool) {
	if len(locationsByKey) == 0 {
		return
	}
	mergeKeys := map[string]struct{}{}
	keys := make([]string, 0, len(locationsByKey))
	for key := range locationsByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		locations := locationsByKey[key]
		sort.SliceStable(locations, func(i, j int) bool {
			a, b := locations[i].entry, locations[j].entry
			if rangesOverlap(a.MinTime, a.MaxTime, b.MinTime, b.MaxTime) {
				return locations[i].index < locations[j].index
			}
			if descending {
				return a.MaxTime < b.MaxTime
			}
			return a.MinTime < b.MinTime
		})
		var window tsmCursorWindowBuilder
		if descending {
			for i := len(locations) - 1; i >= 0; i-- {
				addTSMCursorWindowLocation(summary, &window, key, locations[i], mergeKeys, sampleLimit, descending)
			}
		} else {
			for _, location := range locations {
				addTSMCursorWindowLocation(summary, &window, key, location, mergeKeys, sampleLimit, descending)
			}
		}
		if window.started {
			finishTSMCursorWindow(summary, &window, mergeKeys, sampleLimit)
		}
	}
	summary.MergeWindowKeys = len(mergeKeys)
}

func addTSMCursorWindowLocation(summary *DecodePathSummary, window *tsmCursorWindowBuilder, key string, location tsmCursorCandidate, mergeKeys map[string]struct{}, sampleLimit int, descending bool) {
	if !window.started {
		window.start(key, location)
		return
	}
	if tsmCursorWindowOverlaps(window.minTime, window.maxTime, location.entry, descending) {
		window.add(location)
		return
	}
	finishTSMCursorWindow(summary, window, mergeKeys, sampleLimit)
	window.start(key, location)
}

type tsmCursorWindowBuilder struct {
	started         bool
	key             string
	minTime         int64
	maxTime         int64
	locationBlocks  int
	decodedBlocks   int
	firstBlockIndex int
}

func (w *tsmCursorWindowBuilder) start(key string, location tsmCursorCandidate) {
	*w = tsmCursorWindowBuilder{
		started:         true,
		key:             key,
		minTime:         location.entry.MinTime,
		maxTime:         location.entry.MaxTime,
		locationBlocks:  1,
		firstBlockIndex: location.index,
	}
	if location.decoded {
		w.decodedBlocks = 1
	}
}

func (w *tsmCursorWindowBuilder) add(location tsmCursorCandidate) {
	if location.entry.MinTime < w.minTime {
		w.minTime = location.entry.MinTime
	}
	if location.entry.MaxTime > w.maxTime {
		w.maxTime = location.entry.MaxTime
	}
	w.locationBlocks++
	if location.decoded {
		w.decodedBlocks++
	}
}

func tsmCursorWindowOverlaps(minTime, maxTime int64, entry tsmIndexEntry, descending bool) bool {
	if descending {
		return entry.MaxTime >= minTime
	}
	return entry.MinTime <= maxTime
}

func finishTSMCursorWindow(summary *DecodePathSummary, window *tsmCursorWindowBuilder, mergeKeys map[string]struct{}, sampleLimit int) {
	summary.CursorWindowCount++
	requiresMerge := window.decodedBlocks > 1
	if requiresMerge {
		summary.MergeWindowCount++
		summary.MergeWindowBlocks += window.decodedBlocks
		mergeKeys[window.key] = struct{}{}
	}
	if sampleLimit <= 0 || len(summary.CursorWindows) >= sampleLimit {
		return
	}
	reason := "single_decode"
	switch {
	case window.decodedBlocks == 0:
		reason = "outside_query_range"
	case requiresMerge:
		reason = "merge_overlap"
	}
	summary.CursorWindows = append(summary.CursorWindows, DecodePathCursorWindow{
		Key:             window.key,
		MinTime:         window.minTime,
		MaxTime:         window.maxTime,
		LocationBlocks:  window.locationBlocks,
		DecodedBlocks:   window.decodedBlocks,
		SavedBlocks:     window.locationBlocks - window.decodedBlocks,
		RequiresMerge:   requiresMerge,
		Reason:          reason,
		FirstBlockIndex: window.firstBlockIndex,
	})
}

func tsmQueryKeySelected(key string, keySet map[string]struct{}) bool {
	if len(keySet) == 0 {
		return true
	}
	_, ok := keySet[key]
	return ok
}

func tsmEntryFullyTombstoned(entry tsmIndexEntry, tombstones []tsmTombstoneEntry) bool {
	for _, tombstone := range tombstones {
		if tombstone.Key == entry.Key && tombstone.Min <= entry.MinTime && tombstone.Max >= entry.MaxTime {
			return true
		}
	}
	return false
}

func tsmDecodeRecommendations(summary *DecodePathSummary) []string {
	var recommendations []string
	if len(summary.MissingKeys) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"%d query key(s) were not found in this TSM file",
			len(summary.MissingKeys),
		))
	}
	if summary.SkippedByKeyBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"key filter excludes %d block(s) from cursor planning",
			summary.SkippedByKeyBlocks,
		))
	}
	if summary.SkippedAfterRangeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"filter %d cursor location block(s) that do not overlap the query range before decode",
			summary.SkippedAfterRangeBlocks,
		))
	}
	if summary.FullyTombstonedBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"skip %d fully tombstoned block(s) during location planning",
			summary.FullyTombstonedBlocks,
		))
	}
	if summary.MergeWindowKeys > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"%d key(s) have overlapping query blocks and may require merge/dedup work",
			summary.MergeWindowKeys,
		))
	}
	if summary.ValueOutputUnavailableBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"value-output comparison is partial because %d decoded block(s) use unsupported value types or encodings",
			summary.ValueOutputUnavailableBlocks,
		))
	}
	if summary.ValueOutputMismatches > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"value-output comparison found %d optimized/baseline point mismatch(es)",
			summary.ValueOutputMismatches,
		))
	}
	if len(recommendations) == 0 && summary.FilteredDecodeBlocks > 0 {
		recommendations = append(recommendations, "query range already maps directly to overlapping TSM blocks")
	}
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "query range has no decodable TSM block candidates in this file")
	}
	return recommendations
}
