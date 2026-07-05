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
		Mode:                 "tsm-key-cursor-ascending",
		QueryRange:           options.QueryRange,
		CursorSeekTime:       options.QueryRange.Min,
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

		switch {
		case !selectedKey:
			locationCandidate = false
			summary.SkippedByKeyBlocks++
			reason = "key_not_selected"
		case tsmEntryFullyTombstoned(entry, tombstones):
			locationCandidate = false
			summary.FullyTombstonedBlocks++
			reason = "fully_tombstoned"
		case entry.MaxTime < options.QueryRange.Min:
			locationCandidate = false
			summary.SkippedBeforeSeekBlocks++
			reason = "before_cursor_seek"
		case !queryOverlaps:
			summary.LocationBlocks++
			summary.BaselineDecodeBytes += int64(entry.Size)
			if entry.ValueCountAvailable {
				summary.BaselineDecodeValues += entry.ValueCount
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
				Key:               entry.Key,
				MinTime:           entry.MinTime,
				MaxTime:           entry.MaxTime,
				Type:              typeName,
				SizeBytes:         entry.Size,
				ValueCount:        entry.ValueCount,
				LocationCandidate: locationCandidate,
				Decoded:           decoded,
				Reason:            reason,
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
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	populateTSMCursorWindows(summary, locationsByKey, options.BlockSampleLimit)
	summary.Recommendations = tsmDecodeRecommendations(summary)
	return summary
}

func populateTSMCursorWindows(summary *DecodePathSummary, locationsByKey map[string][]tsmCursorCandidate, sampleLimit int) {
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
			if locations[i].entry.MinTime == locations[j].entry.MinTime {
				return locations[i].entry.MaxTime < locations[j].entry.MaxTime
			}
			return locations[i].entry.MinTime < locations[j].entry.MinTime
		})
		var window tsmCursorWindowBuilder
		for _, location := range locations {
			if !window.started {
				window.start(key, location)
				continue
			}
			if location.entry.MinTime <= window.maxTime {
				window.add(location)
				continue
			}
			finishTSMCursorWindow(summary, &window, mergeKeys, sampleLimit)
			window.start(key, location)
		}
		if window.started {
			finishTSMCursorWindow(summary, &window, mergeKeys, sampleLimit)
		}
	}
	summary.MergeWindowKeys = len(mergeKeys)
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
	if len(recommendations) == 0 && summary.FilteredDecodeBlocks > 0 {
		recommendations = append(recommendations, "query range already maps directly to overlapping TSM blocks")
	}
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "query range has no decodable TSM block candidates in this file")
	}
	return recommendations
}
