package storage

import "fmt"

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
	included := false
	for _, file := range files {
		if file.Format != FormatMergeset || file.DecodePath == nil {
			continue
		}
		included = true
		addMergesetFileSearchSummary(summary, file.DecodePath, file.Path, options.BlockSampleLimit)
		for _, key := range file.DecodePath.MatchedKeys {
			matchedKeys[key] = struct{}{}
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
	summary.OptimizedOutputValues = len(summary.MatchedKeys)
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	if summary.OptimizedDecodeBlocks > 0 {
		summary.Amplification = float64(summary.BaselineDecodeBlocks) / float64(summary.OptimizedDecodeBlocks)
	}
	summary.Recommendations = mergesetFileSetSearchRecommendations(summary, options)
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
	dst.CursorWindowCount += src.CursorWindowCount
	addTSSPDecodePathCounts(dst.LocationBlocksByType, src.LocationBlocksByType)
	addTSSPDecodePathCounts(dst.DecodeBlocksByType, src.DecodeBlocksByType)
	appendMergesetFileSearchSamples(dst, src, path, sampleLimit)
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
	for _, sample := range src.CursorOutputSamples {
		if len(dst.CursorOutputSamples) >= sampleLimit {
			break
		}
		dst.CursorOutputSamples = append(dst.CursorOutputSamples, sample)
	}
}

func mergesetFileSetSearchMode(options Options) string {
	if options.CursorDescending {
		return "mergeset-file-set-item-search-descending"
	}
	return "mergeset-file-set-item-search-ascending"
}

func mergesetFileSetSearchRecommendations(summary *DecodePathSummary, options Options) []string {
	recommendations := []string{}
	if len(summary.MissingKeys) > 0 {
		recommendations = append(recommendations, fmt.Sprintf("%d query item key(s) were not found in analyzed mergeset part(s)", len(summary.MissingKeys)))
	}
	if summary.SavedDecodeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf("sorted item lookup skips %d mergeset block(s) across analyzed part(s)", summary.SavedDecodeBlocks))
	}
	if len(recommendations) == 0 && len(options.QueryKeys) > 0 {
		recommendations = append(recommendations, "all query item keys mapped to decoded mergeset block candidates")
	}
	return recommendations
}
