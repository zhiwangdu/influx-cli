package storage

import (
	"fmt"
	"slices"
	"sort"
)

const maxTSSPReadAtRangeSamples = 8

func buildTSSPDecodePathSummary(metaIndexes []tsspMetaIndex, chunks []tsspChunkMeta, options Options, dataProbe *tsspAttachedDataProbe) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                    tsspCursorMode("tssp-location-cursor", options),
		QueryRange:              options.QueryRange,
		CursorSeekTime:          tsspCursorSeekTime(options),
		LocationBlocksByType:    map[string]int{},
		DecodeBlocksByType:      map[string]int{},
		DataBlockProbeFilterOps: map[string]int{},
	}
	populateTSSPColumnProjectionMatches(summary, chunks, options.QueryColumns)
	populateTSSPFieldFilterMatches(summary, chunks, options.QueryFields)
	populateTSSPAnyFieldFilterMatches(summary, chunks, options.QueryAnyFields)
	populateTSSPNoneFieldFilterMatches(summary, chunks, options.QueryNoneFields)
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

	cursorIndex := 0
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
			appendTSSPMetaIndexCursorExecutionSample(summary, meta, cursorIndex, metaChunks, options.BlockSampleLimit)
			cursorIndex += metaChunks
			continue
		}

		for _, chunk := range tsspChunksForCursor(sidChunks, options.CursorDescending) {
			cursorIndexBefore := cursorIndex
			cursorIndex++
			minTime, maxTime := chunk.minMaxTime()
			segmentCount := len(chunk.TimeRanges)
			columnProjection := newTSSPColumnProjection(chunk, options.QueryColumns, options.QueryFields, options.QueryAnyFields, options.QueryNoneFields)
			outputSegments, outputBytes := tsspChunkOutputSegments(chunk, options.QueryRange, columnProjection)
			baselineBytes := tsspChunkSegmentBytes(chunk)
			baselineReadAtCalls, _ := tsspChunkReadAtPlan(chunk, TimeRange{}, false, 0, tsspColumnProjection{})
			optimizedReadAtCalls, readAtRanges := tsspChunkReadAtPlan(chunk, options.QueryRange, true, maxTSSPReadAtRangeSamples, columnProjection)
			projectionMiss := columnProjection.missingAllColumns() && options.QueryRange.Overlaps(minTime, maxTime)
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
			} else if projectionMiss {
				summary.SkippedByProjectionBlocks++
			} else if maxTime < options.QueryRange.Min {
				summary.SkippedBeforeSeekBlocks++
			} else {
				summary.SkippedAfterRangeBlocks++
			}
			appendTSSPChunkDecodeSample(summary, chunk, minTime, maxTime, segmentCount, outputSegments, baselineBytes,
				baselineReadAtCalls, optimizedReadAtCalls, readAtRanges, valueOutputChecked, valueOutputAvailable, valueOutputPoints, valueUnavailableReason, projectionMiss, options.BlockSampleLimit)
			appendTSSPChunkCursorWindow(summary, chunk, minTime, maxTime, segmentCount, outputSegments, projectionMiss, options.BlockSampleLimit)
			appendTSSPLocationCursorExecutionSample(summary, "tssp-location-cursor-step", fmt.Sprintf("sid:%d", chunk.SID),
				minTime, maxTime, segmentCount, outputSegments, projectionMiss, cursorIndexBefore, options.BlockSampleLimit)
		}
	}

	if dataProbe != nil {
		summary.DataBlockProbeBlocks = dataProbe.BlocksChecked
		summary.DataBlockProbeBytes = dataProbe.BytesRead
		summary.DataBlockProbeFailures = dataProbe.Failures()
		summary.DataBlockProbeValueBlocks = dataProbe.ValueBlocks
		summary.DataBlockProbeValueUnknowns = dataProbe.ValueUnknowns
		summary.DataBlockProbeNullValues = dataProbe.NullValues
		summary.DataBlockProbeRecordSamples = dataProbe.RecordSamples
		summary.DataBlockProbeRangeRows = dataProbe.RangeRows
		summary.DataBlockProbeRangeMatches = dataProbe.RangeMatches
		summary.DataBlockProbeRangeRejects = dataProbe.RangeRejects
		summary.DataBlockProbeFilterRows = dataProbe.FilterRows
		summary.DataBlockProbeFilterMatches = dataProbe.FilterMatches
		summary.DataBlockProbeFilterRejects = dataProbe.FilterRejects
		summary.DataBlockProbeFilterEvals = dataProbe.FilterEvaluations
		summary.DataBlockProbeRequiredEvals = dataProbe.FilterRequiredEvals
		summary.DataBlockProbeAnyEvals = dataProbe.FilterAnyEvals
		summary.DataBlockProbeNoneEvals = dataProbe.FilterNoneEvals
		summary.DataBlockProbeFilterEvalHits = dataProbe.FilterEvalMatches
		summary.DataBlockProbeFilterEvalMiss = dataProbe.FilterEvalMisses
		summary.DataBlockProbeFilterSkips = dataProbe.FilterSkippedEvals
		summary.DataBlockProbeRequiredHits = dataProbe.FilterRequiredHits
		summary.DataBlockProbeRequiredMiss = dataProbe.FilterRequiredMiss
		summary.DataBlockProbeRequiredSkips = dataProbe.FilterRequiredSkips
		summary.DataBlockProbeAnyHits = dataProbe.FilterAnyHits
		summary.DataBlockProbeAnyMiss = dataProbe.FilterAnyMiss
		summary.DataBlockProbeAnySkips = dataProbe.FilterAnySkips
		summary.DataBlockProbeNoneHits = dataProbe.FilterNoneHits
		summary.DataBlockProbeNoneMiss = dataProbe.FilterNoneMiss
		summary.DataBlockProbeNoneSkips = dataProbe.FilterNoneSkips
		addTSSPDecodePathCounts(summary.DataBlockProbeFilterOps, dataProbe.FilterOperators)
		summary.CursorOutputSamples = append(summary.CursorOutputSamples, dataProbe.valueSamples...)
		summary.FilterExecutionSamples = append(summary.FilterExecutionSamples, dataProbe.filterExecutionSamples...)
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
	markLastTSSPCursorExecutionSampleExhausted(summary, cursorIndex)
	summary.Recommendations = tsspDecodeRecommendations(summary)
	return summary
}

func buildTSSPFileSetDecodePathSummary(files []FileReport, options Options) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                    tsspCursorMode("tssp-file-set-location-cursor", options),
		QueryRange:              options.QueryRange,
		CursorSeekTime:          tsspCursorSeekTime(options),
		QuerySeriesIDs:          append([]uint64(nil), options.QuerySeriesIDs...),
		KeyFilterApplied:        len(options.QuerySeriesIDs) > 0,
		LocationBlocksByType:    map[string]int{},
		DecodeBlocksByType:      map[string]int{},
		DataBlockProbeFilterOps: map[string]int{},
	}
	matchedSeriesIDs := map[uint64]struct{}{}
	matchedColumns := map[string]struct{}{}
	matchedFields := map[string]struct{}{}
	matchedAnyFields := map[string]struct{}{}
	matchedNoneFields := map[string]struct{}{}
	outputGroups := newTSSPFileSetOutputSampleGroups()
	included := false
	for _, file := range tsspFilesForCursor(files, options.CursorDescending) {
		included = true
		addTSSPFileDecodePathSummary(summary, file.DecodePath, file.Path, options.BlockSampleLimit, outputGroups)
		for _, id := range file.DecodePath.MatchedSeriesIDs {
			matchedSeriesIDs[id] = struct{}{}
		}
		for _, column := range file.DecodePath.MatchedColumns {
			matchedColumns[column] = struct{}{}
		}
		for _, field := range file.DecodePath.MatchedFields {
			matchedFields[field.Key] = struct{}{}
		}
		for _, field := range file.DecodePath.MatchedAnyFields {
			matchedAnyFields[field.Key] = struct{}{}
		}
		for _, field := range file.DecodePath.MatchedNoneFields {
			matchedNoneFields[field.Key] = struct{}{}
		}
	}
	if !included {
		return nil
	}
	populateTSSPFileSetColumnProjectionMatches(summary, options.QueryColumns, matchedColumns)
	populateTSSPFileSetFieldFilterMatches(summary, options.QueryFields, matchedFields)
	populateTSSPFileSetAnyFieldFilterMatches(summary, options.QueryAnyFields, matchedAnyFields)
	populateTSSPFileSetNoneFieldFilterMatches(summary, options.QueryNoneFields, matchedNoneFields)
	populateTSSPFileSetFinalOutputSamples(summary, outputGroups, options.BlockSampleLimit)

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
	markLastTSSPCursorExecutionSampleExhausted(summary, summary.LocationBlocks)
	summary.Recommendations = tsspDecodeRecommendations(summary)
	return summary
}

func buildTSSPDetachedFileSetDecodePathSummary(files []FileReport, options Options) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                    tsspCursorMode("tssp-detached-file-set-location-cursor", options),
		QueryRange:              options.QueryRange,
		CursorSeekTime:          tsspCursorSeekTime(options),
		QueryMetaIndexIDs:       append([]uint64(nil), options.QueryMetaIndexIDs...),
		KeyFilterApplied:        len(options.QueryMetaIndexIDs) > 0,
		LocationBlocksByType:    map[string]int{},
		DecodeBlocksByType:      map[string]int{},
		DataBlockProbeFilterOps: map[string]int{},
	}
	matchedMetaIndexIDs := map[uint64]struct{}{}
	matchedColumns := map[string]struct{}{}
	matchedFields := map[string]struct{}{}
	matchedAnyFields := map[string]struct{}{}
	matchedNoneFields := map[string]struct{}{}
	outputGroups := newTSSPFileSetOutputSampleGroups()
	included := false
	for _, file := range tsspDetachedFilesForCursor(files, options.CursorDescending) {
		included = true
		addTSSPFileDecodePathSummary(summary, file.DecodePath, file.Path, options.BlockSampleLimit, outputGroups)
		for _, id := range file.DecodePath.MatchedMetaIndexIDs {
			matchedMetaIndexIDs[id] = struct{}{}
		}
		for _, column := range file.DecodePath.MatchedColumns {
			matchedColumns[column] = struct{}{}
		}
		for _, field := range file.DecodePath.MatchedFields {
			matchedFields[field.Key] = struct{}{}
		}
		for _, field := range file.DecodePath.MatchedAnyFields {
			matchedAnyFields[field.Key] = struct{}{}
		}
		for _, field := range file.DecodePath.MatchedNoneFields {
			matchedNoneFields[field.Key] = struct{}{}
		}
	}
	if !included {
		return nil
	}
	populateTSSPDetachedFileSetMetaIndexMatches(summary, matchedMetaIndexIDs)
	populateTSSPFileSetColumnProjectionMatches(summary, options.QueryColumns, matchedColumns)
	populateTSSPFileSetFieldFilterMatches(summary, options.QueryFields, matchedFields)
	populateTSSPFileSetAnyFieldFilterMatches(summary, options.QueryAnyFields, matchedAnyFields)
	populateTSSPFileSetNoneFieldFilterMatches(summary, options.QueryNoneFields, matchedNoneFields)
	populateTSSPFileSetFinalOutputSamples(summary, outputGroups, options.BlockSampleLimit)

	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	summary.SavedReadSegments = summary.BaselineReadSegments - summary.OptimizedReadSegments
	summary.SavedReadAtCalls = summary.BaselineReadAtCalls - summary.OptimizedReadAtCalls
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	markLastTSSPCursorExecutionSampleExhausted(summary, summary.LocationBlocks)
	summary.Recommendations = tsspDetachedFileSetRecommendations(summary)
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

func tsspDetachedFilesForCursor(files []FileReport, descending bool) []FileReport {
	ordered := make([]FileReport, 0, len(files))
	for _, file := range files {
		if file.Format != FormatTSSPDetachedIndex || file.DecodePath == nil {
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

func addTSSPFileDecodePathSummary(dst, src *DecodePathSummary, path string, sampleLimit int, outputGroups *tsspFileSetOutputSampleGroups) {
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
	dst.DataBlockProbeRecordSamples += src.DataBlockProbeRecordSamples
	dst.DataBlockProbeRangeRows += src.DataBlockProbeRangeRows
	dst.DataBlockProbeRangeMatches += src.DataBlockProbeRangeMatches
	dst.DataBlockProbeRangeRejects += src.DataBlockProbeRangeRejects
	dst.DataBlockProbeFilterRows += src.DataBlockProbeFilterRows
	dst.DataBlockProbeFilterMatches += src.DataBlockProbeFilterMatches
	dst.DataBlockProbeFilterRejects += src.DataBlockProbeFilterRejects
	dst.DataBlockProbeFilterEvals += src.DataBlockProbeFilterEvals
	dst.DataBlockProbeRequiredEvals += src.DataBlockProbeRequiredEvals
	dst.DataBlockProbeAnyEvals += src.DataBlockProbeAnyEvals
	dst.DataBlockProbeNoneEvals += src.DataBlockProbeNoneEvals
	dst.DataBlockProbeFilterEvalHits += src.DataBlockProbeFilterEvalHits
	dst.DataBlockProbeFilterEvalMiss += src.DataBlockProbeFilterEvalMiss
	dst.DataBlockProbeFilterSkips += src.DataBlockProbeFilterSkips
	dst.DataBlockProbeRequiredHits += src.DataBlockProbeRequiredHits
	dst.DataBlockProbeRequiredMiss += src.DataBlockProbeRequiredMiss
	dst.DataBlockProbeRequiredSkips += src.DataBlockProbeRequiredSkips
	dst.DataBlockProbeAnyHits += src.DataBlockProbeAnyHits
	dst.DataBlockProbeAnyMiss += src.DataBlockProbeAnyMiss
	dst.DataBlockProbeAnySkips += src.DataBlockProbeAnySkips
	dst.DataBlockProbeNoneHits += src.DataBlockProbeNoneHits
	dst.DataBlockProbeNoneMiss += src.DataBlockProbeNoneMiss
	dst.DataBlockProbeNoneSkips += src.DataBlockProbeNoneSkips
	addTSSPDecodePathCounts(dst.DataBlockProbeFilterOps, src.DataBlockProbeFilterOps)
	dst.IteratorCostFiles += src.IteratorCostFiles
	dst.IteratorCostBlocks += src.IteratorCostBlocks
	dst.IteratorCostBytes += src.IteratorCostBytes
	dst.LocationBlocks += src.LocationBlocks
	dst.FilteredDecodeBlocks += src.FilteredDecodeBlocks
	dst.SkippedByKeyBlocks += src.SkippedByKeyBlocks
	dst.SkippedByProjectionBlocks += src.SkippedByProjectionBlocks
	dst.SkippedBeforeSeekBlocks += src.SkippedBeforeSeekBlocks
	dst.SkippedAfterRangeBlocks += src.SkippedAfterRangeBlocks
	dst.FullyTombstonedBlocks += src.FullyTombstonedBlocks
	dst.CursorWindowCount += src.CursorWindowCount
	addTSSPDecodePathCounts(dst.LocationBlocksByType, src.LocationBlocksByType)
	addTSSPDecodePathCounts(dst.DecodeBlocksByType, src.DecodeBlocksByType)
	appendTSSPFileDecodePathSamples(dst, src, path, sampleLimit, outputGroups)
}

func addTSSPDecodePathCounts(dst, src map[string]int) {
	for key, count := range src {
		dst[key] += count
	}
}

func appendTSSPFileDecodePathSamples(dst, src *DecodePathSummary, path string, sampleLimit int, outputGroups *tsspFileSetOutputSampleGroups) {
	if sampleLimit <= 0 {
		return
	}
	cursorIndexBase := dst.LocationBlocks - src.LocationBlocks
	if cursorIndexBase < 0 {
		cursorIndexBase = 0
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
		if output.File == "" {
			output.File = path
		}
		if outputGroups != nil {
			outputGroups.add(output)
		}
		if len(dst.CursorOutputSamples) < sampleLimit {
			dst.CursorOutputSamples = append(dst.CursorOutputSamples, output)
		}
	}
	for _, sample := range src.CursorExecutionSamples {
		if len(dst.CursorExecutionSamples) >= sampleLimit {
			break
		}
		if sample.File == "" {
			sample.File = path
		}
		sample.Step = len(dst.CursorExecutionSamples) + 1
		sample.CursorIndexBefore += cursorIndexBase
		sample.CursorIndexAfter += cursorIndexBase
		sample.CursorExhausted = false
		dst.CursorExecutionSamples = append(dst.CursorExecutionSamples, sample)
	}
	filterIndexBase := 0
	if len(dst.FilterExecutionSamples) > 0 {
		filterIndexBase = dst.FilterExecutionSamples[len(dst.FilterExecutionSamples)-1].CursorIndexAfter
	}
	for _, sample := range src.FilterExecutionSamples {
		if len(dst.FilterExecutionSamples) >= sampleLimit {
			break
		}
		if sample.File == "" {
			sample.File = path
		}
		sample.Step = len(dst.FilterExecutionSamples) + 1
		sample.CursorIndexBefore += filterIndexBase
		sample.CursorIndexAfter += filterIndexBase
		sample.CursorExhausted = false
		dst.FilterExecutionSamples = append(dst.FilterExecutionSamples, sample)
	}
}

type tsspFileSetOutputSampleKey struct {
	key   string
	time  int64
	typ   string
	value string
}

type tsspFileSetOutputSampleGroup struct {
	output DecodePathCursorOutput
	files  []string
	count  int
}

type tsspFileSetOutputSampleGroups struct {
	groups map[tsspFileSetOutputSampleKey]*tsspFileSetOutputSampleGroup
	order  []tsspFileSetOutputSampleKey
}

func newTSSPFileSetOutputSampleGroups() *tsspFileSetOutputSampleGroups {
	return &tsspFileSetOutputSampleGroups{
		groups: map[tsspFileSetOutputSampleKey]*tsspFileSetOutputSampleGroup{},
	}
}

func (g *tsspFileSetOutputSampleGroups) add(output DecodePathCursorOutput) {
	if g == nil {
		return
	}
	if !output.Matches {
		return
	}
	key := tsspFileSetOutputSampleKey{
		key:   output.Key,
		time:  output.Time,
		typ:   output.Type,
		value: output.OptimizedValue,
	}
	group, ok := g.groups[key]
	if !ok {
		output.RequiresDedup = false
		output.RequiresMerge = false
		output.MergeFiles = ""
		group = &tsspFileSetOutputSampleGroup{output: output}
		g.groups[key] = group
		g.order = append(g.order, key)
	}
	group.count++
	if output.File != "" && !slices.Contains(group.files, output.File) {
		group.files = append(group.files, output.File)
	}
}

func populateTSSPFileSetFinalOutputSamples(summary *DecodePathSummary, outputGroups *tsspFileSetOutputSampleGroups, sampleLimit int) {
	if sampleLimit <= 0 || outputGroups == nil || len(outputGroups.order) == 0 {
		return
	}
	for _, key := range outputGroups.order {
		if len(summary.CursorFinalOutputSamples) >= sampleLimit {
			return
		}
		group := outputGroups.groups[key]
		output := group.output
		output.RequiresDedup = group.count > 1
		output.RequiresMerge = len(group.files) > 1
		if len(group.files) > 0 {
			output.File = group.files[0]
		}
		if output.RequiresMerge {
			output.MergeFiles = newDecodePathStringList(group.files)
		}
		summary.CursorFinalOutputSamples = append(summary.CursorFinalOutputSamples, output)
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

func populateTSSPDetachedFileSetMetaIndexMatches(summary *DecodePathSummary, matchedSet map[uint64]struct{}) {
	if summary == nil || len(summary.QueryMetaIndexIDs) == 0 {
		return
	}
	for _, id := range summary.QueryMetaIndexIDs {
		if _, ok := matchedSet[id]; ok {
			summary.MatchedMetaIndexIDs = append(summary.MatchedMetaIndexIDs, id)
		} else {
			summary.MissingMetaIndexIDs = append(summary.MissingMetaIndexIDs, id)
		}
	}
}

func tsspDetachedFileSetRecommendations(summary *DecodePathSummary) []string {
	if summary == nil {
		return nil
	}
	if summary.LocationBlocksByType["detached-chunk-meta"] == 0 && summary.DecodeBlocksByType["detached-chunk-meta"] == 0 {
		return tsspDetachedMetaIndexRecommendations(summary)
	}
	return tsspDetachedChunkDecodeRecommendations(summary)
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

func appendTSSPMetaIndexCursorExecutionSample(summary *DecodePathSummary, meta tsspMetaIndex, cursorIndexBefore, locationBlocks, sampleLimit int) {
	if sampleLimit <= 0 || summary == nil || locationBlocks <= 0 || len(summary.CursorExecutionSamples) >= sampleLimit {
		return
	}
	summary.CursorExecutionSamples = append(summary.CursorExecutionSamples, DecodePathCursorStep{
		Step:              len(summary.CursorExecutionSamples) + 1,
		Type:              "tssp-location-cursor-step",
		Action:            "read_unexpanded_chunk_metadata",
		Key:               fmt.Sprintf("sid:%d", meta.ID),
		CandidateValue:    fmt.Sprintf("time_range=%d:%d chunks=%d", meta.MinTime, meta.MaxTime, locationBlocks),
		CursorIndexBefore: cursorIndexBefore,
		CursorIndexAfter:  cursorIndexBefore + locationBlocks,
		CursorAdvanced:    true,
	})
}

func appendTSSPChunkDecodeSample(summary *DecodePathSummary, chunk tsspChunkMeta, minTime, maxTime int64, segmentCount, outputSegments int, sizeBytes uint32,
	baselineReadAtCalls, optimizedReadAtCalls int, readAtRanges []DecodePathReadAtRange, valueOutputChecked, valueOutputAvailable bool, valueOutputPoints int, valueUnavailableReason string, projectionMiss bool, sampleLimit int) {
	if sampleLimit <= 0 || len(summary.Samples) >= sampleLimit {
		return
	}
	reason := "outside_query_range"
	decoded := outputSegments > 0
	if projectionMiss {
		reason = "projected_columns_unavailable"
	} else if decoded {
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

func appendTSSPLocationCursorExecutionSample(summary *DecodePathSummary, typ, key string, minTime, maxTime int64, segmentCount, outputSegments int, projectionMiss bool, cursorIndexBefore, sampleLimit int) {
	if sampleLimit <= 0 || summary == nil || len(summary.CursorExecutionSamples) >= sampleLimit {
		return
	}
	action := "skip_after_range"
	if projectionMiss {
		action = "skip_projection"
	} else if outputSegments > 0 {
		action = "read_segments"
	} else if maxTime < summary.QueryRange.Min {
		action = "skip_before_seek"
	}
	summary.CursorExecutionSamples = append(summary.CursorExecutionSamples, DecodePathCursorStep{
		Step:              len(summary.CursorExecutionSamples) + 1,
		Type:              typ,
		Action:            action,
		Key:               key,
		CandidateValue:    fmt.Sprintf("time_range=%d:%d segments=%d/%d", minTime, maxTime, outputSegments, segmentCount),
		CursorIndexBefore: cursorIndexBefore,
		CursorIndexAfter:  cursorIndexBefore + 1,
		CursorAdvanced:    true,
	})
}

func markLastTSSPCursorExecutionSampleExhausted(summary *DecodePathSummary, finalCursorIndex int) {
	if summary == nil || finalCursorIndex <= 0 {
		return
	}
	for i := len(summary.CursorExecutionSamples) - 1; i >= 0; i-- {
		if summary.CursorExecutionSamples[i].CursorIndexAfter == finalCursorIndex {
			summary.CursorExecutionSamples[i].CursorExhausted = true
			return
		}
	}
}

func appendTSSPChunkCursorWindow(summary *DecodePathSummary, chunk tsspChunkMeta, minTime, maxTime int64, segmentCount, outputSegments int, projectionMiss bool, sampleLimit int) {
	if sampleLimit <= 0 || len(summary.CursorWindows) >= sampleLimit {
		return
	}
	reason := "outside_query_range"
	if projectionMiss {
		reason = "projected_columns_unavailable"
	} else if outputSegments > 0 {
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

func tsspChunkOutputSegments(chunk tsspChunkMeta, queryRange TimeRange, columnProjection tsspColumnProjection) (int, uint32) {
	if !queryRange.Set {
		return 0, 0
	}
	segments := 0
	var size uint32
	for i, timeRange := range chunk.TimeRanges {
		if !queryRange.Overlaps(timeRange.Min, timeRange.Max) {
			continue
		}
		segmentBytes := tsspChunkSegmentBytesAt(chunk, i, columnProjection)
		if columnProjection.applied && segmentBytes == 0 {
			continue
		}
		segments++
		size += segmentBytes
	}
	return segments, size
}

func tsspChunkSegmentBytes(chunk tsspChunkMeta) uint32 {
	var size uint32
	for i := range chunk.TimeRanges {
		size += tsspChunkSegmentBytesAt(chunk, i, tsspColumnProjection{})
	}
	return size
}

func tsspChunkSegmentBytesAt(chunk tsspChunkMeta, segment int, columnProjection tsspColumnProjection) uint32 {
	var size uint32
	for _, column := range chunk.Columns {
		if !columnProjection.selectedColumn(column.Name) {
			continue
		}
		if segment >= 0 && segment < len(column.Segments) {
			size += column.Segments[segment].Size
		}
	}
	return size
}

func tsspChunkReadAtPlan(chunk tsspChunkMeta, queryRange TimeRange, queryOnly bool, sampleLimit int, columnProjection tsspColumnProjection) (int, []DecodePathReadAtRange) {
	calls := 0
	var ranges []DecodePathReadAtRange
	for segment, timeRange := range chunk.TimeRanges {
		if queryOnly && !queryRange.Overlaps(timeRange.Min, timeRange.Max) {
			continue
		}
		for _, column := range chunk.Columns {
			if !columnProjection.selectedColumn(column.Name) {
				continue
			}
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
	if len(summary.MissingColumns) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"%d query column(s) were not found in analyzed TSSP chunk metadata",
			len(summary.MissingColumns),
		))
	}
	if len(summary.MissingFields) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"%d query field filter(s) were not found in analyzed TSSP chunk metadata",
			len(summary.MissingFields),
		))
	}
	if len(summary.MissingAnyFields) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"%d query OR field filter(s) were not found in analyzed TSSP chunk metadata",
			len(summary.MissingAnyFields),
		))
	}
	if len(summary.MissingNoneFields) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"%d query NOT field filter(s) were not found in analyzed TSSP chunk metadata",
			len(summary.MissingNoneFields),
		))
	}
	if len(summary.QueryColumns) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"column projection requested for %d TSSP column(s) before data ReadAt",
			len(summary.QueryColumns),
		))
	}
	if summary.SkippedByProjectionBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"column projection excludes %d in-range TSSP chunk(s) before data ReadAt",
			summary.SkippedByProjectionBlocks,
		))
	}
	if len(summary.QueryFields) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"applied %d TSSP field filter(s) to decoded record rows",
			len(summary.QueryFields),
		))
	}
	if len(summary.QueryAnyFields) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"applied %d TSSP OR field filter(s) to decoded record rows",
			len(summary.QueryAnyFields),
		))
	}
	if len(summary.QueryNoneFields) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"applied %d TSSP NOT field filter(s) to decoded record rows",
			len(summary.QueryNoneFields),
		))
	}
	if summary.DataBlockProbeFilterRows > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"TSSP field filters matched %d of %d decoded record row(s)",
			summary.DataBlockProbeFilterMatches,
			summary.DataBlockProbeFilterRows,
		))
	}
	if summary.DataBlockProbeRangeRows > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"TSSP query range matched %d of %d decoded row timestamp(s)",
			summary.DataBlockProbeRangeMatches,
			summary.DataBlockProbeRangeRows,
		))
	}
	if summary.DataBlockProbeFilterEvals > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"executed %d TSSP decoded-row field predicate evaluation(s): matches=%d misses=%d required=%d required_matches=%d required_misses=%d any=%d any_matches=%d any_misses=%d none=%d none_matches=%d none_misses=%d",
			summary.DataBlockProbeFilterEvals,
			summary.DataBlockProbeFilterEvalHits,
			summary.DataBlockProbeFilterEvalMiss,
			summary.DataBlockProbeRequiredEvals,
			summary.DataBlockProbeRequiredHits,
			summary.DataBlockProbeRequiredMiss,
			summary.DataBlockProbeAnyEvals,
			summary.DataBlockProbeAnyHits,
			summary.DataBlockProbeAnyMiss,
			summary.DataBlockProbeNoneEvals,
			summary.DataBlockProbeNoneHits,
			summary.DataBlockProbeNoneMiss,
		))
	}
	if summary.DataBlockProbeFilterSkips > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"short-circuited %d TSSP decoded-row field predicate evaluation(s): required_skips=%d any_skips=%d none_skips=%d",
			summary.DataBlockProbeFilterSkips,
			summary.DataBlockProbeRequiredSkips,
			summary.DataBlockProbeAnySkips,
			summary.DataBlockProbeNoneSkips,
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
	if summary.DataBlockProbeRecordSamples > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"materialized %d TSSP record sample(s) from decoded column blocks",
			summary.DataBlockProbeRecordSamples,
		))
	}
	if len(summary.CursorFinalOutputSamples) > 0 {
		recommendations = append(recommendations, "final TSSP file-set output samples include local exact-dedup status")
	}
	recordSamplesInOutput := summary.DataBlockProbeRecordSamples
	if recordSamplesInOutput > len(summary.CursorOutputSamples) {
		recordSamplesInOutput = len(summary.CursorOutputSamples)
	}
	valueSamples := len(summary.CursorOutputSamples) - recordSamplesInOutput
	if valueSamples > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"sampled %d TSSP value output(s) from data blocks",
			valueSamples,
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
	if len(summary.CursorExecutionSamples) > 0 {
		recommendations = append(recommendations, "TSSP location cursor execution samples show local metadata skip/read steps")
	}
	if len(summary.FilterExecutionSamples) > 0 {
		recommendations = append(recommendations, "TSSP filter execution samples show local decoded-row predicate decisions")
	}
	return recommendations
}
