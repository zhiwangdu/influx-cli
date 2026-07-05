package storage

import "fmt"

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
	overlapByKey := map[string]int{}
	for _, entry := range entries {
		typeName := tsmBlockTypeName(entry.Type)
		fullyTombstoned := tsmEntryFullyTombstoned(entry, tombstones)
		queryOverlaps := options.QueryRange.Overlaps(entry.MinTime, entry.MaxTime)
		locationCandidate := true
		reason := "query_overlap"

		switch {
		case fullyTombstoned:
			locationCandidate = false
			summary.FullyTombstonedBlocks++
			reason = "fully_tombstoned"
		case entry.MaxTime < options.QueryRange.Min:
			locationCandidate = false
			summary.SkippedBeforeSeekBlocks++
			reason = "before_cursor_seek"
		case !queryOverlaps:
			summary.LocationBlocks++
			summary.LocationBlocksByType[typeName]++
			summary.SkippedAfterRangeBlocks++
			reason = "outside_query_range"
		default:
			summary.LocationBlocks++
			summary.FilteredDecodeBlocks++
			summary.LocationBlocksByType[typeName]++
			summary.DecodeBlocksByType[typeName]++
			overlapByKey[entry.Key]++
		}

		if len(summary.Samples) < options.BlockSampleLimit {
			summary.Samples = append(summary.Samples, DecodePathBlockDecision{
				Key:               entry.Key,
				MinTime:           entry.MinTime,
				MaxTime:           entry.MaxTime,
				Type:              typeName,
				LocationCandidate: locationCandidate,
				Decoded:           locationCandidate && queryOverlaps,
				Reason:            reason,
			})
		}
	}

	summary.SavedDecodeBlocks = summary.LocationBlocks - summary.FilteredDecodeBlocks
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	for _, n := range overlapByKey {
		if n > 1 {
			summary.MergeWindowKeys++
			summary.MergeWindowBlocks += n
		}
	}
	summary.Recommendations = tsmDecodeRecommendations(summary)
	return summary
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
