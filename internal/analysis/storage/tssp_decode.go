package storage

import (
	"fmt"
	"sort"
)

const maxTSSPReadAtRangeSamples = 8

func buildTSSPDecodePathSummary(metaIndexes []tsspMetaIndex, chunks []tsspChunkMeta, options Options, dataProbe *tsspAttachedDataProbe) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                 tsspCursorMode("tssp-location-cursor", options),
		QueryRange:           options.QueryRange,
		CursorSeekTime:       tsspCursorSeekTime(options),
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	seriesSet := querySeriesIDSet(options.QuerySeriesIDs)
	if len(seriesSet) > 0 {
		summary.QuerySeriesIDs = append([]uint64(nil), options.QuerySeriesIDs...)
		summary.KeyFilterApplied = true
	}
	chunksBySID := map[uint64][]tsspChunkMeta{}
	for _, chunk := range chunks {
		chunksBySID[chunk.SID] = append(chunksBySID[chunk.SID], chunk)
	}
	populateTSSPDecodeSeriesMatches(summary, metaIndexes)

	for _, meta := range tsspMetaIndexesForCursor(metaIndexes, options.CursorDescending) {
		metaChunks := int(meta.Count)
		if !tsspQuerySeriesSelected(meta.ID, seriesSet) {
			summary.SkippedByKeyBlocks += metaChunks
			continue
		}
		if !options.QueryRange.Overlaps(meta.MinTime, meta.MaxTime) {
			if meta.MaxTime < options.QueryRange.Min {
				summary.SkippedBeforeSeekBlocks += metaChunks
			} else {
				summary.SkippedAfterRangeBlocks += metaChunks
			}
			continue
		}

		summary.IteratorCostFiles = 1
		summary.IteratorCostBlocks += metaChunks
		summary.IteratorCostBytes += int64(meta.Size)
		summary.LocationBlocks += metaChunks
		summary.BaselineDecodeBlocks += metaChunks
		summary.LocationBlocksByType["chunk-meta"] += metaChunks

		sidChunks := chunksBySID[meta.ID]
		if len(sidChunks) == 0 {
			summary.BaselineDecodeBytes += int64(meta.Size)
			summary.OptimizedDecodeBlocks += metaChunks
			summary.FilteredDecodeBlocks += metaChunks
			summary.OptimizedDecodeBytes += int64(meta.Size)
			summary.DecodeBlocksByType["meta-index"] += metaChunks
			appendTSSPMetaIndexDecodeSample(summary, meta, options.BlockSampleLimit)
			continue
		}

		for _, chunk := range tsspChunksForCursor(sidChunks, options.CursorDescending) {
			minTime, maxTime := chunk.minMaxTime()
			segmentCount := len(chunk.TimeRanges)
			outputSegments, outputBytes := tsspChunkOutputSegments(chunk, options.QueryRange)
			baselineBytes := tsspChunkSegmentBytes(chunk)
			baselineReadAtCalls, _ := tsspChunkReadAtPlan(chunk, TimeRange{}, false, 0)
			optimizedReadAtCalls, readAtRanges := tsspChunkReadAtPlan(chunk, options.QueryRange, true, maxTSSPReadAtRangeSamples)
			valueOutputChecked := false
			valueOutputAvailable := false
			valueUnavailableReason := ""
			valueOutputPoints := 0

			summary.BaselineDecodeBytes += int64(baselineBytes)
			summary.BaselineDecodeValues += segmentCount
			summary.BaselineReadSegments += segmentCount
			summary.BaselineReadAtCalls += baselineReadAtCalls
			if outputSegments > 0 {
				summary.OptimizedDecodeBlocks++
				summary.FilteredDecodeBlocks++
				summary.OptimizedDecodeBytes += int64(outputBytes)
				summary.OptimizedDecodeValues += outputSegments
				summary.OptimizedReadSegments += outputSegments
				summary.OptimizedReadAtCalls += optimizedReadAtCalls
				summary.DecodeBlocksByType["chunk-meta"]++
				if available, reason, ok := dataProbe.chunkDataAvailable(chunk); ok {
					valueOutputChecked = true
					valueOutputAvailable = available
					if !valueOutputAvailable {
						valueUnavailableReason = reason
					} else {
						valueOutputPoints = dataProbe.chunkOutputPointsFor(chunk)
					}
				}
				if valueOutputChecked && !valueOutputAvailable {
					summary.ValueOutputUnavailableBlocks++
				} else if valueOutputAvailable {
					summary.OptimizedValueOutputPoints += valueOutputPoints
				}
			} else if maxTime < options.QueryRange.Min {
				summary.SkippedBeforeSeekBlocks++
			} else {
				summary.SkippedAfterRangeBlocks++
			}
			appendTSSPChunkDecodeSample(summary, chunk, minTime, maxTime, segmentCount, outputSegments, baselineBytes,
				baselineReadAtCalls, optimizedReadAtCalls, readAtRanges, valueOutputChecked, valueOutputAvailable, valueOutputPoints, valueUnavailableReason, options.BlockSampleLimit)
			appendTSSPChunkCursorWindow(summary, chunk, minTime, maxTime, segmentCount, outputSegments, options.BlockSampleLimit)
		}
	}

	if dataProbe != nil {
		summary.DataBlockProbeBlocks = dataProbe.BlocksChecked
		summary.DataBlockProbeBytes = dataProbe.BytesRead
		summary.DataBlockProbeFailures = dataProbe.Failures()
		summary.DataBlockProbeValueBlocks = dataProbe.ValueBlocks
		summary.DataBlockProbeValueUnknowns = dataProbe.ValueUnknowns
		summary.DataBlockProbeNullValues = dataProbe.NullValues
		summary.CursorOutputSamples = append(summary.CursorOutputSamples, dataProbe.valueSamples...)
	}
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	summary.SavedReadSegments = summary.BaselineReadSegments - summary.OptimizedReadSegments
	summary.BaselineCursorReadCalls = summary.BaselineReadSegments
	summary.OptimizedCursorReadCalls = summary.OptimizedReadSegments
	summary.SavedReadAtCalls = summary.BaselineReadAtCalls - summary.OptimizedReadAtCalls
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	summary.CursorWindowCount = summary.LocationBlocks
	summary.Recommendations = tsspDecodeRecommendations(summary)
	return summary
}

func buildTSSPFileSetDecodePathSummary(files []FileReport, options Options) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                 tsspCursorMode("tssp-file-set-location-cursor", options),
		QueryRange:           options.QueryRange,
		CursorSeekTime:       tsspCursorSeekTime(options),
		QuerySeriesIDs:       append([]uint64(nil), options.QuerySeriesIDs...),
		KeyFilterApplied:     len(options.QuerySeriesIDs) > 0,
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	matchedSeriesIDs := map[uint64]struct{}{}
	included := false
	for _, file := range tsspFilesForCursor(files, options.CursorDescending) {
		included = true
		addTSSPFileDecodePathSummary(summary, file.DecodePath, file.Path, options.BlockSampleLimit)
		for _, id := range file.DecodePath.MatchedSeriesIDs {
			matchedSeriesIDs[id] = struct{}{}
		}
	}
	if !included {
		return nil
	}

	if len(summary.QuerySeriesIDs) > 0 {
		for _, id := range summary.QuerySeriesIDs {
			if _, ok := matchedSeriesIDs[id]; ok {
				summary.MatchedSeriesIDs = append(summary.MatchedSeriesIDs, id)
			} else {
				summary.MissingSeriesIDs = append(summary.MissingSeriesIDs, id)
			}
		}
	}
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	summary.SavedReadSegments = summary.BaselineReadSegments - summary.OptimizedReadSegments
	summary.SavedReadAtCalls = summary.BaselineReadAtCalls - summary.OptimizedReadAtCalls
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	summary.Recommendations = tsspDecodeRecommendations(summary)
	return summary
}

func tsspCursorMode(prefix string, options Options) string {
	if options.CursorDescending {
		return prefix + "-descending"
	}
	return prefix + "-ascending"
}

func tsspCursorSeekTime(options Options) int64 {
	if options.CursorDescending {
		return options.QueryRange.Max
	}
	return options.QueryRange.Min
}

func tsspMetaIndexesForCursor(metaIndexes []tsspMetaIndex, descending bool) []tsspMetaIndex {
	ordered := append([]tsspMetaIndex(nil), metaIndexes...)
	if !descending {
		return ordered
	}
	for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
		ordered[i], ordered[j] = ordered[j], ordered[i]
	}
	return ordered
}

func tsspChunksForCursor(chunks []tsspChunkMeta, descending bool) []tsspChunkMeta {
	ordered := append([]tsspChunkMeta(nil), chunks...)
	if !descending {
		return ordered
	}
	for i, j := 0, len(ordered)-1; i < j; i, j = i+1, j-1 {
		ordered[i], ordered[j] = ordered[j], ordered[i]
	}
	return ordered
}

func tsspFilesForCursor(files []FileReport, descending bool) []FileReport {
	ordered := make([]FileReport, 0, len(files))
	for _, file := range files {
		if file.Format != FormatTSSP || file.DecodePath == nil {
			continue
		}
		ordered = append(ordered, file)
	}
	if !descending {
		return ordered
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.MaxTime != b.MaxTime {
			return a.MaxTime > b.MaxTime
		}
		return a.Path > b.Path
	})
	return ordered
}

func addTSSPFileDecodePathSummary(dst, src *DecodePathSummary, path string, sampleLimit int) {
	dst.BaselineDecodeBlocks += src.BaselineDecodeBlocks
	dst.OptimizedDecodeBlocks += src.OptimizedDecodeBlocks
	dst.BaselineDecodeBytes += src.BaselineDecodeBytes
	dst.OptimizedDecodeBytes += src.OptimizedDecodeBytes
	dst.BaselineDecodeValues += src.BaselineDecodeValues
	dst.OptimizedDecodeValues += src.OptimizedDecodeValues
	dst.BaselineReadSegments += src.BaselineReadSegments
	dst.OptimizedReadSegments += src.OptimizedReadSegments
	dst.BaselineCursorReadCalls += src.BaselineCursorReadCalls
	dst.OptimizedCursorReadCalls += src.OptimizedCursorReadCalls
	dst.BaselineReadAtCalls += src.BaselineReadAtCalls
	dst.OptimizedReadAtCalls += src.OptimizedReadAtCalls
	dst.OptimizedValueOutputPoints += src.OptimizedValueOutputPoints
	dst.ValueOutputUnavailableBlocks += src.ValueOutputUnavailableBlocks
	dst.DataBlockProbeBlocks += src.DataBlockProbeBlocks
	dst.DataBlockProbeBytes += src.DataBlockProbeBytes
	dst.DataBlockProbeFailures += src.DataBlockProbeFailures
	dst.DataBlockProbeCRCMismatches += src.DataBlockProbeCRCMismatches
	dst.DataBlockProbeValueBlocks += src.DataBlockProbeValueBlocks
	dst.DataBlockProbeValueUnknowns += src.DataBlockProbeValueUnknowns
	dst.DataBlockProbeNullValues += src.DataBlockProbeNullValues
	dst.IteratorCostFiles += src.IteratorCostFiles
	dst.IteratorCostBlocks += src.IteratorCostBlocks
	dst.IteratorCostBytes += src.IteratorCostBytes
	dst.LocationBlocks += src.LocationBlocks
	dst.FilteredDecodeBlocks += src.FilteredDecodeBlocks
	dst.SkippedByKeyBlocks += src.SkippedByKeyBlocks
	dst.SkippedBeforeSeekBlocks += src.SkippedBeforeSeekBlocks
	dst.SkippedAfterRangeBlocks += src.SkippedAfterRangeBlocks
	dst.FullyTombstonedBlocks += src.FullyTombstonedBlocks
	dst.CursorWindowCount += src.CursorWindowCount
	addTSSPDecodePathCounts(dst.LocationBlocksByType, src.LocationBlocksByType)
	addTSSPDecodePathCounts(dst.DecodeBlocksByType, src.DecodeBlocksByType)
	appendTSSPFileDecodePathSamples(dst, src, path, sampleLimit)
}

func addTSSPDecodePathCounts(dst, src map[string]int) {
	for key, count := range src {
		dst[key] += count
	}
}

func appendTSSPFileDecodePathSamples(dst, src *DecodePathSummary, path string, sampleLimit int) {
	if sampleLimit <= 0 {
		return
	}
	for _, sample := range src.Samples {
		if len(dst.Samples) >= sampleLimit {
			break
		}
		if sample.Path == "" {
			sample.Path = path
		}
		dst.Samples = append(dst.Samples, sample)
	}
	for _, window := range src.CursorWindows {
		if len(dst.CursorWindows) >= sampleLimit {
			break
		}
		if len(window.Files) == 0 {
			window.Files = []string{path}
		}
		dst.CursorWindows = append(dst.CursorWindows, window)
	}
	for _, output := range src.CursorOutputSamples {
		if len(dst.CursorOutputSamples) >= sampleLimit {
			break
		}
		dst.CursorOutputSamples = append(dst.CursorOutputSamples, output)
	}
}

func populateTSSPDecodeSeriesMatches(summary *DecodePathSummary, metaIndexes []tsspMetaIndex) {
	if len(summary.QuerySeriesIDs) == 0 {
		return
	}
	known := map[uint64]struct{}{}
	for _, meta := range metaIndexes {
		known[meta.ID] = struct{}{}
	}
	for _, id := range summary.QuerySeriesIDs {
		if _, ok := known[id]; ok {
			summary.MatchedSeriesIDs = append(summary.MatchedSeriesIDs, id)
		} else {
			summary.MissingSeriesIDs = append(summary.MissingSeriesIDs, id)
		}
	}
}

func appendTSSPMetaIndexDecodeSample(summary *DecodePathSummary, meta tsspMetaIndex, sampleLimit int) {
	if sampleLimit <= 0 || len(summary.Samples) >= sampleLimit {
		return
	}
	summary.Samples = append(summary.Samples, DecodePathBlockDecision{
		Key:               fmt.Sprintf("sid:%d", meta.ID),
		SeriesID:          meta.ID,
		MinTime:           meta.MinTime,
		MaxTime:           meta.MaxTime,
		Type:              "meta-index",
		SizeBytes:         meta.Size,
		SegmentCount:      int(meta.Count),
		OutputSegments:    int(meta.Count),
		LocationCandidate: true,
		Decoded:           true,
		Reason:            "chunk_metadata_unexpanded",
	})
}

func appendTSSPChunkDecodeSample(summary *DecodePathSummary, chunk tsspChunkMeta, minTime, maxTime int64, segmentCount, outputSegments int, sizeBytes uint32,
	baselineReadAtCalls, optimizedReadAtCalls int, readAtRanges []DecodePathReadAtRange, valueOutputChecked, valueOutputAvailable bool, valueOutputPoints int, valueUnavailableReason string, sampleLimit int) {
	if sampleLimit <= 0 || len(summary.Samples) >= sampleLimit {
		return
	}
	reason := "outside_query_range"
	decoded := outputSegments > 0
	if decoded {
		reason = "segment_overlap"
		if valueOutputChecked && !valueOutputAvailable {
			reason = valueUnavailableReason
			if reason == "" {
				reason = "segment_overlap_data_unavailable"
			}
		}
	}
	summary.Samples = append(summary.Samples, DecodePathBlockDecision{
		Key:                   fmt.Sprintf("sid:%d", chunk.SID),
		SeriesID:              chunk.SID,
		MinTime:               minTime,
		MaxTime:               maxTime,
		Type:                  "chunk-meta",
		SizeBytes:             sizeBytes,
		SegmentCount:          segmentCount,
		OutputSegments:        outputSegments,
		ValueOutputPoints:     valueOutputPoints,
		ValueOutputAvailable:  valueOutputChecked && valueOutputAvailable,
		BaselineReadAtCalls:   baselineReadAtCalls,
		OptimizedReadAtCalls:  optimizedReadAtCalls,
		LocationCandidate:     true,
		Decoded:               decoded,
		Reason:                reason,
		OptimizedReadAtRanges: readAtRanges,
	})
}

func appendTSSPChunkCursorWindow(summary *DecodePathSummary, chunk tsspChunkMeta, minTime, maxTime int64, segmentCount, outputSegments int, sampleLimit int) {
	if sampleLimit <= 0 || len(summary.CursorWindows) >= sampleLimit {
		return
	}
	reason := "outside_query_range"
	if outputSegments > 0 {
		reason = "segment_overlap"
	}
	summary.CursorWindows = append(summary.CursorWindows, DecodePathCursorWindow{
		Key:             fmt.Sprintf("sid:%d", chunk.SID),
		MinTime:         minTime,
		MaxTime:         maxTime,
		LocationBlocks:  segmentCount,
		DecodedBlocks:   outputSegments,
		SavedBlocks:     segmentCount - outputSegments,
		RequiresMerge:   false,
		Reason:          reason,
		FirstBlockIndex: len(summary.CursorWindows),
	})
}

func tsspChunkOutputSegments(chunk tsspChunkMeta, queryRange TimeRange) (int, uint32) {
	if !queryRange.Set {
		return 0, 0
	}
	segments := 0
	var size uint32
	for i, timeRange := range chunk.TimeRanges {
		if !queryRange.Overlaps(timeRange.Min, timeRange.Max) {
			continue
		}
		segments++
		size += tsspChunkSegmentBytesAt(chunk, i)
	}
	return segments, size
}

func tsspChunkSegmentBytes(chunk tsspChunkMeta) uint32 {
	var size uint32
	for i := range chunk.TimeRanges {
		size += tsspChunkSegmentBytesAt(chunk, i)
	}
	return size
}

func tsspChunkSegmentBytesAt(chunk tsspChunkMeta, segment int) uint32 {
	var size uint32
	for _, column := range chunk.Columns {
		if segment >= 0 && segment < len(column.Segments) {
			size += column.Segments[segment].Size
		}
	}
	return size
}

func tsspChunkReadAtPlan(chunk tsspChunkMeta, queryRange TimeRange, queryOnly bool, sampleLimit int) (int, []DecodePathReadAtRange) {
	calls := 0
	var ranges []DecodePathReadAtRange
	for segment, timeRange := range chunk.TimeRanges {
		if queryOnly && !queryRange.Overlaps(timeRange.Min, timeRange.Max) {
			continue
		}
		for _, column := range chunk.Columns {
			if segment < 0 || segment >= len(column.Segments) {
				continue
			}
			location := column.Segments[segment]
			calls++
			if sampleLimit > 0 && len(ranges) < sampleLimit {
				ranges = append(ranges, DecodePathReadAtRange{
					Segment:   segment,
					Column:    column.Name,
					MinTime:   timeRange.Min,
					MaxTime:   timeRange.Max,
					Offset:    location.Offset,
					SizeBytes: location.Size,
				})
			}
		}
	}
	return calls, ranges
}

func tsspDecodeRecommendations(summary *DecodePathSummary) []string {
	var recommendations []string
	if len(summary.MissingSeriesIDs) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"%d query series id(s) were not found in analyzed TSSP file(s)",
			len(summary.MissingSeriesIDs),
		))
	}
	if summary.SkippedByKeyBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"series-id filter excludes %d TSSP chunk(s) from location planning",
			summary.SkippedByKeyBlocks,
		))
	}
	if summary.SkippedBeforeSeekBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"skip %d TSSP chunk(s) before the cursor seek time",
			summary.SkippedBeforeSeekBlocks,
		))
	}
	if summary.SkippedAfterRangeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"filter %d TSSP chunk/segment candidate(s) outside the query range before ReadAt",
			summary.SkippedAfterRangeBlocks,
		))
	}
	if summary.SavedReadSegments > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"read %d overlapping TSSP segment(s) instead of %d meta-index candidate segment(s)",
			summary.OptimizedReadSegments,
			summary.BaselineReadSegments,
		))
	}
	if summary.SavedReadAtCalls > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"issue %d TSSP ReadAt call(s) instead of %d candidate column-segment ReadAt call(s)",
			summary.OptimizedReadAtCalls,
			summary.BaselineReadAtCalls,
		))
	}
	if summary.DataBlockProbeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"verified %d TSSP data block(s) before value decode",
			summary.DataBlockProbeBlocks-summary.DataBlockProbeFailures,
		))
	}
	if summary.OptimizedValueOutputPoints > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"materialized %d TSSP output point(s) from data block row counts",
			summary.OptimizedValueOutputPoints,
		))
	}
	if len(summary.CursorOutputSamples) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"sampled %d TSSP one-row value output(s) from data blocks",
			len(summary.CursorOutputSamples),
		))
	}
	if summary.DataBlockProbeFailures > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"TSSP data block probe found %d invalid block(s)",
			summary.DataBlockProbeFailures,
		))
	}
	if len(recommendations) == 0 && summary.FilteredDecodeBlocks > 0 {
		recommendations = append(recommendations, "query range maps directly to overlapping TSSP chunk segments")
	}
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "query range has no TSSP chunk segment candidates")
	}
	return recommendations
}
