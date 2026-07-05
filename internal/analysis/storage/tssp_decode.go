package storage

import "fmt"

func buildTSSPDecodePathSummary(metaIndexes []tsspMetaIndex, chunks []tsspChunkMeta, options Options) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                 "tssp-location-cursor-ascending",
		QueryRange:           options.QueryRange,
		CursorSeekTime:       options.QueryRange.Min,
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

	for _, meta := range metaIndexes {
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

		for _, chunk := range sidChunks {
			minTime, maxTime := chunk.minMaxTime()
			segmentCount := len(chunk.TimeRanges)
			outputSegments, outputBytes := tsspChunkOutputSegments(chunk, options.QueryRange)
			baselineBytes := tsspChunkSegmentBytes(chunk)

			summary.BaselineDecodeBytes += int64(baselineBytes)
			summary.BaselineDecodeValues += segmentCount
			summary.BaselineReadSegments += segmentCount
			if outputSegments > 0 {
				summary.OptimizedDecodeBlocks++
				summary.FilteredDecodeBlocks++
				summary.OptimizedDecodeBytes += int64(outputBytes)
				summary.OptimizedDecodeValues += outputSegments
				summary.OptimizedReadSegments += outputSegments
				summary.DecodeBlocksByType["chunk-meta"]++
			} else if maxTime < options.QueryRange.Min {
				summary.SkippedBeforeSeekBlocks++
			} else {
				summary.SkippedAfterRangeBlocks++
			}
			appendTSSPChunkDecodeSample(summary, chunk, minTime, maxTime, segmentCount, outputSegments, baselineBytes, options.BlockSampleLimit)
		}
	}

	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	summary.SavedReadSegments = summary.BaselineReadSegments - summary.OptimizedReadSegments
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
		Mode:                 "tssp-file-set-location-cursor-ascending",
		QueryRange:           options.QueryRange,
		CursorSeekTime:       options.QueryRange.Min,
		QuerySeriesIDs:       append([]uint64(nil), options.QuerySeriesIDs...),
		KeyFilterApplied:     len(options.QuerySeriesIDs) > 0,
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	matchedSeriesIDs := map[uint64]struct{}{}
	included := false
	for _, file := range files {
		if file.Format != FormatTSSP || file.DecodePath == nil {
			continue
		}
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
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	summary.Recommendations = tsspDecodeRecommendations(summary)
	return summary
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
		dst.CursorWindows = append(dst.CursorWindows, window)
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

func appendTSSPChunkDecodeSample(summary *DecodePathSummary, chunk tsspChunkMeta, minTime, maxTime int64, segmentCount, outputSegments int, sizeBytes uint32, sampleLimit int) {
	if sampleLimit <= 0 || len(summary.Samples) >= sampleLimit {
		return
	}
	reason := "outside_query_range"
	decoded := outputSegments > 0
	if decoded {
		reason = "segment_overlap"
	}
	summary.Samples = append(summary.Samples, DecodePathBlockDecision{
		Key:               fmt.Sprintf("sid:%d", chunk.SID),
		SeriesID:          chunk.SID,
		MinTime:           minTime,
		MaxTime:           maxTime,
		Type:              "chunk-meta",
		SizeBytes:         sizeBytes,
		SegmentCount:      segmentCount,
		OutputSegments:    outputSegments,
		LocationCandidate: true,
		Decoded:           decoded,
		Reason:            reason,
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
	if len(recommendations) == 0 && summary.FilteredDecodeBlocks > 0 {
		recommendations = append(recommendations, "query range maps directly to overlapping TSSP chunk segments")
	}
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "query range has no TSSP chunk segment candidates")
	}
	return recommendations
}
