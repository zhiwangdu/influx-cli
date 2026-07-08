package storage

import (
	"bytes"
	"container/heap"
	"fmt"
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
	tableSeekCandidates := map[string][]mergesetSeekResult{}
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
			result.File = file.Path
			tableSeekCandidates[key] = append(tableSeekCandidates[key], result)
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
	tableSeekResults, heapInserts, heapPops := mergeMergesetFileSetSearchCandidates(summary, tableSeekCandidates, options)
	summary.BaselineOutputValues = len(options.QueryKeys)
	summary.OptimizedOutputValues = countMergesetMatchedItemFiles(matchedKeyFiles)
	summary.DeduplicatedOutputValues = len(summary.MatchedKeys)
	summary.DuplicateOutputValues = summary.OptimizedOutputValues - summary.DeduplicatedOutputValues
	summary.TableSearchHeapCandidates = heapInserts
	summary.TableSearchHeapInserts = heapInserts
	summary.TableSearchHeapPops = heapPops
	summary.TableSearchOutputValues = len(tableSeekResults)
	summary.TableSearchExactMisses = len(summary.MissingKeys)
	populateMergesetFileSetCursorWindows(summary, tableSeekResults, matchedKeyFiles, options)
	populateMergesetFileSetCursorOutputSamples(summary, tableSeekResults, matchedKeyFiles, options)
	populateMergesetFileSetFinalSearchOutputSamples(summary, tableSeekResults, matchedKeyFiles, options)
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	if summary.OptimizedDecodeBlocks > 0 {
		summary.Amplification = float64(summary.BaselineDecodeBlocks) / float64(summary.OptimizedDecodeBlocks)
	}
	populateDecodePathExecutionActionCounts(summary)
	summary.Recommendations = mergesetFileSetSearchRecommendations(summary, options)
	return summary
}

func mergeMergesetFileSetSearchCandidates(summary *DecodePathSummary, candidates map[string][]mergesetSeekResult, options Options) (map[string]mergesetSeekResult, int, int) {
	results := map[string]mergesetSeekResult{}
	heapInserts := 0
	heapPops := 0
	for _, key := range options.QueryKeys {
		group := candidates[key]
		if len(group) == 0 {
			continue
		}
		candidateHeap := mergesetFileSetSearchHeap{
			candidates: append([]mergesetSeekResult(nil), group...),
			descending: options.CursorDescending,
		}
		heapInserts += len(candidateHeap.candidates)
		heap.Init(&candidateHeap)
		if candidateHeap.Len() > 0 {
			heapSizeBefore := candidateHeap.Len()
			result := heap.Pop(&candidateHeap).(mergesetSeekResult)
			heapPops++
			results[key] = result
			appendMergesetFileSetSearchExecutionSample(summary, options.BlockSampleLimit, withMergesetCursorStepHex(DecodePathCursorStep{
				Step:                heapPops,
				Type:                "mergeset-table-search-candidate-heap-step",
				Action:              mergesetFileSetSearchExecutionAction(result.Matches),
				Key:                 key,
				CandidateValue:      string(result.Item),
				File:                result.File,
				HeapSizeBefore:      heapSizeBefore,
				HeapSizeAfterPop:    candidateHeap.Len(),
				HeapSizeAfterAction: candidateHeap.Len(),
				CursorIndexBefore:   -1,
				CursorIndexAfter:    -1,
			}, []byte(key), result.Item))
		}
	}
	return results, heapInserts, heapPops
}

func mergesetFileSetSearchExecutionAction(matches bool) string {
	if matches {
		return "heap_pop_exact_match_candidate"
	}
	return "heap_pop_exact_miss_candidate"
}

func appendMergesetFileSetSearchExecutionSample(summary *DecodePathSummary, sampleLimit int, sample DecodePathCursorStep) {
	if sampleLimit <= 0 || summary == nil || len(summary.CursorExecutionSamples) >= sampleLimit {
		return
	}
	summary.CursorExecutionSamples = append(summary.CursorExecutionSamples, sample)
}

type mergesetFileSetSearchHeap struct {
	candidates []mergesetSeekResult
	descending bool
}

func (h mergesetFileSetSearchHeap) Len() int {
	return len(h.candidates)
}

func (h mergesetFileSetSearchHeap) Less(i, j int) bool {
	left := h.candidates[i]
	right := h.candidates[j]
	cmp := bytes.Compare(left.Item, right.Item)
	if cmp == 0 {
		return left.File < right.File
	}
	if h.descending {
		return cmp > 0
	}
	return cmp < 0
}

func (h mergesetFileSetSearchHeap) Swap(i, j int) {
	h.candidates[i], h.candidates[j] = h.candidates[j], h.candidates[i]
}

func (h *mergesetFileSetSearchHeap) Push(x any) {
	h.candidates = append(h.candidates, x.(mergesetSeekResult))
}

func (h *mergesetFileSetSearchHeap) Pop() any {
	candidates := h.candidates
	candidate := candidates[len(candidates)-1]
	h.candidates = candidates[:len(candidates)-1]
	return candidate
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
	streams := []mergesetFileSetScanStream{}
	includedParts := 0
	for _, file := range files {
		if file.Format != FormatMergeset || file.DecodePath == nil || !isMergesetTableScanSummary(file.DecodePath) {
			continue
		}
		includedParts++
		addMergesetFileScanSummary(summary, file.DecodePath, file.Path, options.BlockSampleLimit)
		if len(file.DecodePath.mergesetScanItems) > 0 {
			streams = append(streams, newMergesetFileSetScanStream(file.Path, file.DecodePath.mergesetScanItems, options))
		}
	}
	if includedParts == 0 {
		return nil
	}
	summary.TableSearchSeekCalls = includedParts
	summary.TableSearchHeapCandidates = len(streams)
	if includedParts > 1 {
		summary.MergeWindowCount = 1
		summary.MergeWindowBlocks = includedParts
	}
	populateMergesetFileSetScanCursor(summary, streams, options)
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	if summary.OptimizedDecodeBlocks > 0 {
		summary.Amplification = float64(summary.BaselineDecodeBlocks) / float64(summary.OptimizedDecodeBlocks)
	}
	populateDecodePathExecutionActionCounts(summary)
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
	dst.TableSearchHeapInserts += src.TableSearchHeapInserts
	dst.TableSearchHeapPops += src.TableSearchHeapPops
	dst.TableSearchCursorAdvances += src.TableSearchCursorAdvances
	dst.TableSearchCursorExhaustions += src.TableSearchCursorExhaustions
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
	dst.TableSearchHeapInserts += src.TableSearchHeapInserts
	dst.TableSearchHeapPops += src.TableSearchHeapPops
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

func populateMergesetFileSetCursorWindows(summary *DecodePathSummary, tableSeekResults map[string]mergesetSeekResult, matchedKeyFiles map[string][]string, options Options) {
	summary.CursorWindowCount = len(tableSeekResults)
	for _, key := range options.QueryKeys {
		result, ok := tableSeekResults[key]
		if !ok {
			continue
		}
		files := uniqueStringsPreserveOrder(matchedKeyFiles[key])
		reason := "item_search_exact_match"
		if !result.Matches {
			files = []string{result.File}
			reason = "item_search_exact_miss"
		} else if len(files) == 0 && result.File != "" {
			files = []string{result.File}
		}
		requiresMerge := result.Matches && len(files) > 1
		if requiresMerge {
			summary.MergeWindowCount++
			summary.MergeWindowBlocks += len(files)
			summary.MergeWindowKeys++
		}
		if options.BlockSampleLimit <= 0 || len(summary.CursorWindows) >= options.BlockSampleLimit {
			continue
		}
		summary.CursorWindows = append(summary.CursorWindows, withMergesetCursorWindowKeyHex(DecodePathCursorWindow{
			Key:            key,
			Files:          append([]string(nil), files...),
			LocationBlocks: len(files),
			DecodedBlocks:  len(files),
			RequiresMerge:  requiresMerge,
			Reason:         reason,
		}, []byte(key)))
	}
}

func populateMergesetFileSetCursorOutputSamples(summary *DecodePathSummary, tableSeekResults map[string]mergesetSeekResult, matchedKeyFiles map[string][]string, options Options) {
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
		files := uniqueStringsPreserveOrder(matchedKeyFiles[key])
		requiresDedup := result.Matches && len(files) > 1
		output := newMergesetCursorOutputSample(key, "mergeset-table-search-item", result.Item, result.Matches)
		output.File = result.File
		output.RequiresDedup = requiresDedup
		output.RequiresMerge = requiresDedup
		if requiresDedup {
			output.MergeFiles = newDecodePathStringList(files)
		}
		summary.CursorOutputSamples = append(summary.CursorOutputSamples, output)
	}
}

func populateMergesetFileSetFinalSearchOutputSamples(summary *DecodePathSummary, tableSeekResults map[string]mergesetSeekResult, matchedKeyFiles map[string][]string, options Options) {
	if options.BlockSampleLimit <= 0 {
		return
	}
	for _, key := range options.QueryKeys {
		if len(summary.CursorFinalOutputSamples) >= options.BlockSampleLimit {
			return
		}
		result, ok := tableSeekResults[key]
		if !ok || !result.Matches {
			continue
		}
		files := uniqueStringsPreserveOrder(matchedKeyFiles[key])
		requiresDedup := len(files) > 1
		output := newMergesetCursorOutputSample(key, "mergeset-table-search-final-output-item", result.Item, true)
		output.File = result.File
		output.RequiresDedup = requiresDedup
		output.RequiresMerge = requiresDedup
		if requiresDedup {
			output.MergeFiles = newDecodePathStringList(files)
		}
		summary.CursorFinalOutputSamples = append(summary.CursorFinalOutputSamples, output)
	}
}

type mergesetFileSetScanStream struct {
	path  string
	items [][]byte
	index int
}

func newMergesetFileSetScanStream(path string, items [][]byte, options Options) mergesetFileSetScanStream {
	index := 0
	if options.CursorDescending {
		index = len(items) - 1
	}
	return mergesetFileSetScanStream{
		path:  path,
		items: items,
		index: index,
	}
}

func (s *mergesetFileSetScanStream) current() []byte {
	if s.index < 0 || s.index >= len(s.items) {
		return nil
	}
	return s.items[s.index]
}

func (s *mergesetFileSetScanStream) advance(descending bool) bool {
	if descending {
		s.index--
		return s.index >= 0
	}
	s.index++
	return s.index < len(s.items)
}

type mergesetFileSetScanHeap struct {
	streams    []*mergesetFileSetScanStream
	descending bool
}

func (h mergesetFileSetScanHeap) Len() int {
	return len(h.streams)
}

func (h mergesetFileSetScanHeap) Less(i, j int) bool {
	left := h.streams[i]
	right := h.streams[j]
	cmp := bytes.Compare(left.current(), right.current())
	if cmp == 0 {
		if left.path == right.path {
			if h.descending {
				return left.index > right.index
			}
			return left.index < right.index
		}
		return left.path < right.path
	}
	if h.descending {
		return cmp > 0
	}
	return cmp < 0
}

func (h mergesetFileSetScanHeap) Swap(i, j int) {
	h.streams[i], h.streams[j] = h.streams[j], h.streams[i]
}

func (h *mergesetFileSetScanHeap) Push(x any) {
	h.streams = append(h.streams, x.(*mergesetFileSetScanStream))
}

func (h *mergesetFileSetScanHeap) Pop() any {
	streams := h.streams
	stream := streams[len(streams)-1]
	h.streams = streams[:len(streams)-1]
	return stream
}

func populateMergesetFileSetScanCursor(summary *DecodePathSummary, streams []mergesetFileSetScanStream, options Options) {
	if len(streams) == 0 {
		summary.TableSearchOutputValues = summary.OptimizedOutputValues
		return
	}
	cursorHeap := mergesetFileSetScanHeap{
		streams:    make([]*mergesetFileSetScanStream, 0, len(streams)),
		descending: options.CursorDescending,
	}
	for i := range streams {
		cursorHeap.streams = append(cursorHeap.streams, &streams[i])
	}
	// Inserts count items placed into the local heap, including the initial streams
	// bulk-loaded before heap.Init and subsequent re-insertions after advance.
	heapInserts := len(cursorHeap.streams)
	heapPops := 0
	cursorAdvances := 0
	cursorExhaustions := 0
	heap.Init(&cursorHeap)

	total := 0
	unique := 0
	duplicates := 0
	duplicateGroups := 0
	groupSize := 0
	var previous []byte
	var groupFiles []string
	var groupSampleIndexes []int
	finishGroup := func() {
		if groupSize == 0 {
			return
		}
		files := uniqueStringsPreserveOrder(groupFiles)
		if groupSize > 1 {
			for _, index := range groupSampleIndexes {
				summary.CursorOutputSamples[index].RequiresDedup = true
			}
		}
		requiresMerge := len(files) > 1
		if requiresMerge {
			duplicateGroups++
			appendMergesetDuplicateMergeWindow(summary, withMergesetCursorWindowKeyHex(DecodePathCursorWindow{
				Key:            string(previous),
				Files:          files,
				LocationBlocks: len(files),
				DecodedBlocks:  len(files),
				RequiresMerge:  true,
				Reason:         "duplicate_item_merge",
			}, previous), options.BlockSampleLimit)
			for _, index := range groupSampleIndexes {
				summary.CursorOutputSamples[index].MergeFiles = newDecodePathStringList(files)
				summary.CursorOutputSamples[index].RequiresMerge = true
			}
		}
		if options.BlockSampleLimit > 0 && len(summary.CursorFinalOutputSamples) < options.BlockSampleLimit {
			output := newMergesetCursorOutputSample(string(previous), "mergeset-table-final-output-item", previous, true)
			output.RequiresDedup = groupSize > 1
			output.RequiresMerge = requiresMerge
			if len(groupFiles) > 0 {
				output.File = groupFiles[0]
			}
			if requiresMerge {
				output.MergeFiles = newDecodePathStringList(files)
			}
			summary.CursorFinalOutputSamples = append(summary.CursorFinalOutputSamples, output)
		}
	}
	for cursorHeap.Len() > 0 {
		heapSizeBefore := cursorHeap.Len()
		stream := heap.Pop(&cursorHeap).(*mergesetFileSetScanStream)
		item := stream.current()
		indexBefore := stream.index
		heapPops++
		total++
		if previous == nil || !bytes.Equal(previous, item) {
			finishGroup()
			unique++
			groupSize = 1
			previous = append(previous[:0], item...)
			groupFiles = groupFiles[:0]
			groupSampleIndexes = groupSampleIndexes[:0]
		} else {
			duplicates++
			groupSize++
		}
		groupFiles = append(groupFiles, stream.path)
		if options.BlockSampleLimit > 0 && len(summary.CursorOutputSamples) < options.BlockSampleLimit {
			sampleIndex := len(summary.CursorOutputSamples)
			output := newMergesetCursorOutputSample(string(item), "mergeset-table-search-item", item, true)
			output.File = stream.path
			summary.CursorOutputSamples = append(summary.CursorOutputSamples, output)
			groupSampleIndexes = append(groupSampleIndexes, sampleIndex)
		}
		advanced := stream.advance(options.CursorDescending)
		exhausted := !advanced
		if advanced {
			heap.Push(&cursorHeap, stream)
			heapInserts++
			cursorAdvances++
		} else {
			cursorExhaustions++
		}
		appendMergesetFileSetScanExecutionSample(summary, options.BlockSampleLimit, withMergesetCursorStepHex(DecodePathCursorStep{
			Step:                total,
			Type:                "mergeset-table-scan-heap-step",
			Action:              mergesetFileSetScanExecutionAction(advanced),
			Key:                 string(item),
			File:                stream.path,
			HeapSizeBefore:      heapSizeBefore,
			HeapSizeAfterPop:    heapSizeBefore - 1,
			HeapSizeAfterAction: cursorHeap.Len(),
			CursorIndexBefore:   indexBefore,
			CursorIndexAfter:    stream.index,
			CursorAdvanced:      advanced,
			CursorExhausted:     exhausted,
		}, item, nil))
	}
	finishGroup()
	summary.TableSearchHeapInserts = heapInserts
	summary.TableSearchHeapPops = heapPops
	summary.TableSearchCursorAdvances = cursorAdvances
	summary.TableSearchCursorExhaustions = cursorExhaustions
	summary.TableSearchOutputValues = total
	summary.DeduplicatedOutputValues = unique
	summary.DuplicateOutputValues = duplicates
	summary.MergeWindowKeys = duplicateGroups
}

func mergesetFileSetScanExecutionAction(advanced bool) string {
	if advanced {
		return "heap_pop_cursor_advance"
	}
	return "heap_pop_cursor_exhaust"
}

func appendMergesetFileSetScanExecutionSample(summary *DecodePathSummary, sampleLimit int, sample DecodePathCursorStep) {
	if sampleLimit <= 0 || summary == nil || len(summary.CursorExecutionSamples) >= sampleLimit {
		return
	}
	summary.CursorExecutionSamples = append(summary.CursorExecutionSamples, sample)
}

func appendMergesetDuplicateMergeWindow(summary *DecodePathSummary, window DecodePathCursorWindow, sampleLimit int) {
	if sampleLimit <= 0 {
		return
	}
	if len(summary.CursorWindows) < sampleLimit {
		summary.CursorWindows = append(summary.CursorWindows, window)
		return
	}
	for i := 0; i < len(summary.CursorWindows); i++ {
		if summary.CursorWindows[i].Reason != "duplicate_item_merge" {
			copy(summary.CursorWindows[i:], summary.CursorWindows[i+1:])
			summary.CursorWindows[len(summary.CursorWindows)-1] = window
			summary.mergesetEvictedCursorWindows++
			return
		}
	}
}

func countMergesetDuplicateMergeWindows(windows []DecodePathCursorWindow) int {
	count := 0
	for _, window := range windows {
		if window.Reason == "duplicate_item_merge" {
			count++
		}
	}
	return count
}

func uniqueStringsPreserveOrder(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
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
	if summary.TableSearchCursorAdvances > 0 || summary.TableSearchCursorExhaustions > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"advanced %d local mergeset part cursor step(s) and exhausted %d part cursor(s)",
			summary.TableSearchCursorAdvances,
			summary.TableSearchCursorExhaustions,
		))
	}
	if summary.DuplicateOutputValues > 0 {
		if summary.MergeWindowKeys > 0 {
			recommendations = append(recommendations, fmt.Sprintf(
				"merge/dedup %d duplicate table-scan item candidate(s) across %d cross-part duplicate group(s)",
				summary.DuplicateOutputValues,
				summary.MergeWindowKeys,
			))
		} else {
			recommendations = append(recommendations, fmt.Sprintf(
				"dedup %d duplicate table-scan item candidate(s) within analyzed part cursor(s)",
				summary.DuplicateOutputValues,
			))
		}
	}
	if summary.mergesetEvictedCursorWindows > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"evicted %d part-level cursor window sample(s) to retain duplicate merge window sample(s)",
			summary.mergesetEvictedCursorWindows,
		))
	}
	if sampled := countMergesetDuplicateMergeWindows(summary.CursorWindows); sampled > 0 && summary.MergeWindowKeys > sampled {
		recommendations = append(recommendations, fmt.Sprintf(
			"sampled %d of %d duplicate merge window(s)",
			sampled,
			summary.MergeWindowKeys,
		))
	}
	if len(summary.CursorOutputSamples) > 0 {
		recommendations = append(recommendations, "file-set scan output samples follow TableSearch heap cursor order")
	}
	if len(summary.CursorExecutionSamples) > 0 {
		recommendations = append(recommendations, "file-set scan execution samples show local heap pop/advance/exhaust steps")
	}
	if len(summary.CursorFinalOutputSamples) > 0 {
		recommendations = append(recommendations, "final file-set scan output samples show deduplicated TableSearch cursor output")
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
	if summary.TableSearchCursorAdvances > 0 {
		recommendations = append(recommendations, fmt.Sprintf("advanced %d local mergeset part cursor step(s) while seeking item candidates", summary.TableSearchCursorAdvances))
	}
	if len(summary.CursorExecutionSamples) > 0 {
		recommendations = append(recommendations, "file-set item-search execution samples show local candidate heap pop steps")
	}
	if exactMissWindows := countMergesetExactMissCursorWindows(summary.CursorWindows); exactMissWindows > 0 {
		recommendations = append(recommendations, fmt.Sprintf("recorded %d exact-miss TableSearch seek window(s) with nearest local item candidates", exactMissWindows))
	}
	if len(summary.CursorFinalOutputSamples) > 0 {
		recommendations = append(recommendations, "final item-search output samples show deduplicated exact TableSearch results")
	}
	if summary.SavedDecodeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf("sorted item lookup skips %d mergeset block(s) across analyzed part(s)", summary.SavedDecodeBlocks))
	}
	if len(recommendations) == 0 && len(options.QueryKeys) > 0 {
		recommendations = append(recommendations, "all query item keys mapped to decoded mergeset block candidates")
	}
	return recommendations
}

func countMergesetExactMissCursorWindows(windows []DecodePathCursorWindow) int {
	count := 0
	for _, window := range windows {
		if window.Reason == "item_search_exact_miss" {
			count++
		}
	}
	return count
}

func countMergesetMatchedItemFiles(matchedKeyFiles map[string][]string) int {
	count := 0
	for _, files := range matchedKeyFiles {
		count += len(files)
	}
	return count
}
