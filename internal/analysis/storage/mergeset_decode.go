package storage

import (
	"bytes"
	"fmt"
	"sort"
)

func buildMergesetFileSetDecodePathSummary(files []FileReport, options Options) *DecodePathSummary {
	if len(options.QueryKeys) > 0 {
		return buildMergesetFileSetSearchSummary(files, options)
	}
	return buildMergesetFileSetScanSummary(files, options)
}

func buildMergesetFileSetSearchSummary(files []FileReport, options Options) *DecodePathSummary {
	if len(options.QueryKeys) == 0 {
		return nil
	}
	summary := &DecodePathSummary{
		Mode:                 mergesetFileSetSearchMode(options),
		QueryKeys:            append([]string(nil), options.QueryKeys...),
		KeyFilterApplied:     true,
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	matchedKeys := map[string]struct{}{}
	matchedKeyFiles := map[string][]string{}
	tableSeekResults := map[string]mergesetSeekResult{}
	included := false
	for _, file := range files {
		if file.Format != FormatMergeset || file.DecodePath == nil {
			continue
		}
		included = true
		addMergesetFileSearchSummary(summary, file.DecodePath, file.Path, options.BlockSampleLimit)
		for _, key := range file.DecodePath.MatchedKeys {
			matchedKeys[key] = struct{}{}
			matchedKeyFiles[key] = append(matchedKeyFiles[key], file.Path)
		}
		for key, result := range file.DecodePath.mergesetSeekResults {
			current, ok := tableSeekResults[key]
			if !ok || bytes.Compare(result.Item, current.Item) < 0 {
				tableSeekResults[key] = result
			}
		}
	}
	if !included {
		return nil
	}
	for _, key := range options.QueryKeys {
		if _, ok := matchedKeys[key]; ok {
			summary.MatchedKeys = append(summary.MatchedKeys, key)
		} else {
			summary.MissingKeys = append(summary.MissingKeys, key)
		}
	}
	summary.BaselineOutputValues = len(options.QueryKeys)
	summary.OptimizedOutputValues = countMergesetMatchedItemFiles(matchedKeyFiles)
	summary.DeduplicatedOutputValues = len(summary.MatchedKeys)
	summary.DuplicateOutputValues = summary.OptimizedOutputValues - summary.DeduplicatedOutputValues
	summary.TableSearchOutputValues = len(tableSeekResults)
	summary.TableSearchExactMisses = len(summary.MissingKeys)
	populateMergesetFileSetCursorWindows(summary, matchedKeyFiles, options)
	populateMergesetFileSetCursorOutputSamples(summary, tableSeekResults, options)
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	if summary.OptimizedDecodeBlocks > 0 {
		summary.Amplification = float64(summary.BaselineDecodeBlocks) / float64(summary.OptimizedDecodeBlocks)
	}
	summary.Recommendations = mergesetFileSetSearchRecommendations(summary, options)
	return summary
}

func buildMergesetFileSetScanSummary(files []FileReport, options Options) *DecodePathSummary {
	if len(options.QueryKeys) > 0 {
		return nil
	}
	summary := &DecodePathSummary{
		Mode:                 mergesetFileSetScanMode(options),
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	outputSamples := []DecodePathCursorOutput{}
	includedParts := 0
	for _, file := range files {
		if file.Format != FormatMergeset || file.DecodePath == nil || !isMergesetTableScanSummary(file.DecodePath) {
			continue
		}
		includedParts++
		addMergesetFileScanSummary(summary, file.DecodePath, file.Path, options.BlockSampleLimit)
		outputSamples = append(outputSamples, file.DecodePath.CursorOutputSamples...)
	}
	if includedParts == 0 {
		return nil
	}
	summary.TableSearchSeekCalls = includedParts
	summary.TableSearchHeapCandidates = includedParts
	summary.TableSearchOutputValues = summary.OptimizedOutputValues
	if includedParts > 1 {
		summary.MergeWindowCount = 1
		summary.MergeWindowBlocks = includedParts
	}
	populateMergesetFileSetScanOutputSamples(summary, outputSamples, options)
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	if summary.OptimizedDecodeBlocks > 0 {
		summary.Amplification = float64(summary.BaselineDecodeBlocks) / float64(summary.OptimizedDecodeBlocks)
	}
	summary.Recommendations = mergesetFileSetScanRecommendations(summary)
	return summary
}

func addMergesetFileSearchSummary(dst, src *DecodePathSummary, path string, sampleLimit int) {
	dst.BaselineDecodeBlocks += src.BaselineDecodeBlocks
	dst.OptimizedDecodeBlocks += src.OptimizedDecodeBlocks
	dst.BaselineDecodeBytes += src.BaselineDecodeBytes
	dst.OptimizedDecodeBytes += src.OptimizedDecodeBytes
	dst.BaselineDecodeValues += src.BaselineDecodeValues
	dst.OptimizedDecodeValues += src.OptimizedDecodeValues
	dst.LocationBlocks += src.LocationBlocks
	dst.FilteredDecodeBlocks += src.FilteredDecodeBlocks
	dst.SkippedByKeyBlocks += src.SkippedByKeyBlocks
	dst.TableSearchSeekCalls += src.TableSearchSeekCalls
	dst.TableSearchHeapCandidates += src.TableSearchHeapCandidates
	addTSSPDecodePathCounts(dst.LocationBlocksByType, src.LocationBlocksByType)
	addTSSPDecodePathCounts(dst.DecodeBlocksByType, src.DecodeBlocksByType)
	appendMergesetFileSearchSamples(dst, src, path, sampleLimit)
}

func addMergesetFileScanSummary(dst, src *DecodePathSummary, path string, sampleLimit int) {
	dst.BaselineDecodeBlocks += src.BaselineDecodeBlocks
	dst.OptimizedDecodeBlocks += src.OptimizedDecodeBlocks
	dst.BaselineDecodeBytes += src.BaselineDecodeBytes
	dst.OptimizedDecodeBytes += src.OptimizedDecodeBytes
	dst.BaselineDecodeValues += src.BaselineDecodeValues
	dst.OptimizedDecodeValues += src.OptimizedDecodeValues
	dst.BaselineOutputValues += src.BaselineOutputValues
	dst.OptimizedOutputValues += src.OptimizedOutputValues
	dst.LocationBlocks += src.LocationBlocks
	dst.FilteredDecodeBlocks += src.FilteredDecodeBlocks
	dst.CursorWindowCount += src.CursorWindowCount
	addTSSPDecodePathCounts(dst.LocationBlocksByType, src.LocationBlocksByType)
	addTSSPDecodePathCounts(dst.DecodeBlocksByType, src.DecodeBlocksByType)
	appendMergesetFileSearchSamples(dst, src, path, sampleLimit)
	appendMergesetFileScanWindows(dst, src, path, sampleLimit)
}

func appendMergesetFileScanWindows(dst, src *DecodePathSummary, path string, sampleLimit int) {
	if sampleLimit <= 0 {
		return
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
}

func populateMergesetFileSetCursorWindows(summary *DecodePathSummary, matchedKeyFiles map[string][]string, options Options) {
	summary.CursorWindowCount = len(matchedKeyFiles)
	for _, key := range options.QueryKeys {
		files := matchedKeyFiles[key]
		if len(files) == 0 {
			continue
		}
		if len(files) > 1 {
			summary.MergeWindowCount++
			summary.MergeWindowBlocks += len(files)
			summary.MergeWindowKeys++
		}
		if options.BlockSampleLimit <= 0 || len(summary.CursorWindows) >= options.BlockSampleLimit {
			continue
		}
		summary.CursorWindows = append(summary.CursorWindows, DecodePathCursorWindow{
			Key:            key,
			Files:          append([]string(nil), files...),
			LocationBlocks: len(files),
			DecodedBlocks:  len(files),
			RequiresMerge:  len(files) > 1,
		})
	}
}

func populateMergesetFileSetCursorOutputSamples(summary *DecodePathSummary, tableSeekResults map[string]mergesetSeekResult, options Options) {
	if options.BlockSampleLimit <= 0 {
		return
	}
	for _, key := range options.QueryKeys {
		if len(summary.CursorOutputSamples) >= options.BlockSampleLimit {
			return
		}
		result, ok := tableSeekResults[key]
		if !ok {
			continue
		}
		summary.CursorOutputSamples = append(summary.CursorOutputSamples, DecodePathCursorOutput{
			Key:            key,
			Type:           "mergeset-table-search-item",
			OptimizedValue: string(result.Item),
			Matches:        result.Matches,
		})
	}
}

func populateMergesetFileSetScanOutputSamples(summary *DecodePathSummary, samples []DecodePathCursorOutput, options Options) {
	if options.BlockSampleLimit <= 0 || len(samples) == 0 {
		return
	}
	sort.SliceStable(samples, func(i, j int) bool {
		cmp := bytes.Compare([]byte(samples[i].OptimizedValue), []byte(samples[j].OptimizedValue))
		if options.CursorDescending {
			return cmp > 0
		}
		return cmp < 0
	})
	limit := options.BlockSampleLimit
	if limit > len(samples) {
		limit = len(samples)
	}
	summary.CursorOutputSamples = append(summary.CursorOutputSamples, samples[:limit]...)
}

func appendMergesetFileSearchSamples(dst, src *DecodePathSummary, path string, sampleLimit int) {
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
}

func mergesetFileSetSearchMode(options Options) string {
	if options.CursorDescending {
		return "mergeset-file-set-item-search-descending"
	}
	return "mergeset-file-set-item-search-ascending"
}

func mergesetFileSetScanMode(options Options) string {
	if options.CursorDescending {
		return "mergeset-file-set-table-scan-descending"
	}
	return "mergeset-file-set-table-scan-ascending"
}

func isMergesetTableScanSummary(summary *DecodePathSummary) bool {
	return summary.Mode == "mergeset-table-scan-ascending" || summary.Mode == "mergeset-table-scan-descending"
}

func mergesetFileSetScanRecommendations(summary *DecodePathSummary) []string {
	recommendations := []string{}
	if summary.OptimizedOutputValues > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"scan %d decoded mergeset item(s) across %d analyzed part block(s)",
			summary.OptimizedOutputValues,
			summary.OptimizedDecodeBlocks,
		))
	}
	if summary.MergeWindowCount > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"merge %d analyzed mergeset part cursor(s) with TableSearch-style heap ordering",
			summary.MergeWindowBlocks,
		))
	}
	if len(summary.CursorOutputSamples) > 0 {
		recommendations = append(recommendations, "file-set scan output samples are ordered by decoded mergeset item bytes")
	}
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "mergeset file-set table scan has no decoded item payload candidates")
	}
	return recommendations
}

func mergesetFileSetSearchRecommendations(summary *DecodePathSummary, options Options) []string {
	recommendations := []string{}
	if len(summary.MissingKeys) > 0 {
		recommendations = append(recommendations, fmt.Sprintf("%d query item key(s) were not found in analyzed mergeset part(s)", len(summary.MissingKeys)))
	}
	if summary.DuplicateOutputValues > 0 {
		recommendations = append(recommendations, fmt.Sprintf("merge/dedup %d duplicate item candidate(s) across mergeset part(s)", summary.DuplicateOutputValues))
	}
	if summary.TableSearchHeapCandidates > summary.TableSearchOutputValues {
		recommendations = append(recommendations, fmt.Sprintf("table search heap compares %d part candidate item(s) for %d table output seek(s)", summary.TableSearchHeapCandidates, summary.TableSearchOutputValues))
	}
	if summary.SavedDecodeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf("sorted item lookup skips %d mergeset block(s) across analyzed part(s)", summary.SavedDecodeBlocks))
	}
	if len(recommendations) == 0 && len(options.QueryKeys) > 0 {
		recommendations = append(recommendations, "all query item keys mapped to decoded mergeset block candidates")
	}
	return recommendations
}

func countMergesetMatchedItemFiles(matchedKeyFiles map[string][]string) int {
	count := 0
	for _, files := range matchedKeyFiles {
		count += len(files)
	}
	return count
}
