package storage

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type tsspAttachedDataProbe struct {
	Checked                bool
	BlocksChecked          int
	ValidBlocks            int
	BytesRead              int64
	ShortBlocks            int
	UnknownBlockTypes      int
	ReadErrors             int
	FailureReasons         map[string]int
	RowCountBlocks         int
	RowCountUnknowns       int
	RowCountMismatches     int
	OutputPoints           int
	ValueBlocks            int
	ValueUnknowns          int
	ValueUnknownReasons    map[string]int
	NullValues             int
	RecordSamples          int
	RangeRows              int
	RangeMatches           int
	RangeRejects           int
	FilterRows             int
	FilterMatches          int
	FilterRejects          int
	FilterEvaluations      int
	FilterRequiredEvals    int
	FilterAnyEvals         int
	FilterNoneEvals        int
	FilterEvalMatches      int
	FilterEvalMisses       int
	FilterSkippedEvals     int
	FilterRequiredHits     int
	FilterRequiredMiss     int
	FilterRequiredSkips    int
	FilterAnyHits          int
	FilterAnyMiss          int
	FilterAnySkips         int
	FilterNoneHits         int
	FilterNoneMiss         int
	FilterNoneSkips        int
	FilterOperators        map[string]int
	BlockTypes             map[string]int
	chunkAvailable         map[uint64]bool
	chunkFailureReason     map[uint64]string
	chunkOutputPoints      map[uint64]int
	valueSamples           []DecodePathCursorOutput
	rangeExecutionSamples  []DecodePathCursorStep
	recordExecutionSamples []DecodePathCursorStep
	filterExecutionSamples []DecodePathCursorStep
}

func (p *tsspAttachedDataProbe) Failures() int {
	if p == nil {
		return 0
	}
	return p.ShortBlocks + p.UnknownBlockTypes + p.ReadErrors
}

func probeTSSPAttachedDataBlocks(f *os.File, fileSize int64, trailer tsspTrailer, chunks []tsspChunkMeta, options Options) (*tsspAttachedDataProbe, bool, error) {
	if !options.QueryRange.Set || trailer.DataSize == 0 {
		return nil, false, nil
	}
	if trailer.DataSize < 0 || trailer.DataOffset < tsspHeaderSize || trailer.DataOffset > fileSize || trailer.DataSize > fileSize-trailer.DataOffset {
		return nil, true, fmt.Errorf("invalid TSSP data range offset=%d size=%d", trailer.DataOffset, trailer.DataSize)
	}

	probe := &tsspAttachedDataProbe{
		Checked:             true,
		FailureReasons:      map[string]int{},
		ValueUnknownReasons: map[string]int{},
		FilterOperators:     map[string]int{},
		BlockTypes:          map[string]int{},
		chunkAvailable:      map[uint64]bool{},
		chunkFailureReason:  map[uint64]string{},
		chunkOutputPoints:   map[uint64]int{},
	}
	seriesSet := querySeriesIDSet(options.QuerySeriesIDs)
	for _, chunk := range chunks {
		if !tsspQuerySeriesSelected(chunk.SID, seriesSet) {
			continue
		}
		columnProjection := newTSSPColumnProjection(chunk, options.QueryColumns, options.QueryFields, options.QueryAnyFields, options.QueryNoneFields)
		chunkChecked := false
		chunkAvailable := true
		chunkFailureReason := ""
		chunkOutputPoints := 0
		for segment, timeRange := range chunk.TimeRanges {
			if !options.QueryRange.Overlaps(timeRange.Min, timeRange.Max) {
				continue
			}
			segmentChecked := false
			segmentAvailable := true
			segmentRowsKnown := false
			segmentRows := 0
			segmentBlocks := map[string]tsspDetachedDataBlockInfo{}
			for _, column := range chunk.Columns {
				if !columnProjection.selectedColumn(column.Name) {
					continue
				}
				if segment < 0 || segment >= len(column.Segments) {
					continue
				}
				location := column.Segments[segment]
				chunkChecked = true
				segmentChecked = true
				probe.BlocksChecked++
				probe.BytesRead += int64(location.Size)
				if !tsspRangeInDataArea(location.Offset, int64(location.Size), trailer.DataOffset, trailer.DataSize) {
					probe.ReadErrors++
					probe.FailureReasons["segment_overlap_data_range_unavailable"]++
					chunkAvailable = false
					segmentAvailable = false
					chunkFailureReason = "segment_overlap_data_range_unavailable"
					continue
				}
				block := make([]byte, int(location.Size))
				if _, err := f.ReadAt(block, location.Offset); err != nil {
					probe.ReadErrors++
					probe.FailureReasons["segment_overlap_data_read_unavailable"]++
					chunkAvailable = false
					segmentAvailable = false
					chunkFailureReason = "segment_overlap_data_read_unavailable"
					continue
				}
				blockInfo, ok, reason := inspectTSSPDataBlockPayloadForColumn(block, column.Name)
				if !ok {
					chunkAvailable = false
					segmentAvailable = false
					chunkFailureReason = reason
					switch reason {
					case "segment_overlap_data_header_unavailable":
						probe.ShortBlocks++
					default:
						probe.UnknownBlockTypes++
					}
					if reason != "" {
						probe.FailureReasons[reason]++
					}
					continue
				}
				probe.ValidBlocks++
				probe.BlockTypes[blockInfo.Type]++
				segmentBlocks[column.Name] = blockInfo
				if blockInfo.ValueKnown {
					if blockInfo.ValueNull {
						probe.NullValues += blockInfo.Rows
					} else {
						probe.ValueBlocks++
						probe.NullValues += blockInfo.Nulls
					}
				} else {
					probe.ValueUnknowns++
					if blockInfo.ValueReason != "" {
						probe.ValueUnknownReasons[blockInfo.ValueReason]++
						chunkAvailable = false
						segmentAvailable = false
						chunkFailureReason = "segment_overlap_data_value_unavailable"
					}
				}
				if !blockInfo.RowsKnown {
					probe.RowCountUnknowns++
					chunkAvailable = false
					segmentAvailable = false
					chunkFailureReason = "segment_overlap_data_row_count_unavailable"
					continue
				}
				probe.RowCountBlocks++
				if !segmentRowsKnown {
					segmentRowsKnown = true
					segmentRows = blockInfo.Rows
					continue
				}
				if segmentRows != blockInfo.Rows {
					probe.RowCountMismatches++
					chunkAvailable = false
					segmentAvailable = false
					chunkFailureReason = "segment_overlap_data_row_count_mismatch"
				}
			}
			if segmentChecked && segmentAvailable && segmentRowsKnown {
				matchingRows, matchedRows, filterRows, filterStats, ok := tsspDataBlockFilterRows(segmentBlocks, options.QueryFields, options.QueryAnyFields, options.QueryNoneFields, segmentRows, timeRange, options.QueryRange, fmt.Sprintf("sid:%d", chunk.SID), remainingTSSPExecutionSampleLimit(probe.rangeExecutionSamples, options.BlockSampleLimit), remainingTSSPExecutionSampleLimit(probe.filterExecutionSamples, options.BlockSampleLimit))
				if !ok {
					chunkAvailable = false
					chunkFailureReason = "segment_overlap_data_filter_unavailable"
					continue
				}
				probe.RangeRows += filterStats.RangeRows
				probe.RangeMatches += filterStats.RangeMatches
				probe.RangeRejects += filterStats.RangeRejects
				appendTSSPRangeExecutionSamples(&probe.rangeExecutionSamples, filterStats.RangeExecutionSamples, options.BlockSampleLimit)
				if len(options.QueryFields) > 0 || len(options.QueryAnyFields) > 0 || len(options.QueryNoneFields) > 0 {
					probe.FilterRows += filterRows
					probe.FilterMatches += matchedRows
					probe.FilterRejects += filterRows - matchedRows
					probe.FilterEvaluations += filterStats.Evaluations
					probe.FilterRequiredEvals += filterStats.RequiredEvaluations
					probe.FilterAnyEvals += filterStats.AnyEvaluations
					probe.FilterNoneEvals += filterStats.NoneEvaluations
					probe.FilterEvalMatches += filterStats.MatchEvaluations
					probe.FilterEvalMisses += filterStats.MissEvaluations
					probe.FilterSkippedEvals += filterStats.SkippedEvaluations
					probe.FilterRequiredHits += filterStats.RequiredMatches
					probe.FilterRequiredMiss += filterStats.RequiredMisses
					probe.FilterRequiredSkips += filterStats.RequiredSkips
					probe.FilterAnyHits += filterStats.AnyMatches
					probe.FilterAnyMiss += filterStats.AnyMisses
					probe.FilterAnySkips += filterStats.AnySkips
					probe.FilterNoneHits += filterStats.NoneMatches
					probe.FilterNoneMiss += filterStats.NoneMisses
					probe.FilterNoneSkips += filterStats.NoneSkips
					addTSSPFilterOperatorCounts(probe.FilterOperators, filterStats.OperatorEvaluations)
					appendTSSPFilterExecutionSamples(&probe.filterExecutionSamples, filterStats.FilterExecutionSamples, options.BlockSampleLimit)
				}
				chunkOutputPoints += matchedRows
				appendTSSPAttachedDataProbeValueSamples(probe, chunk, timeRange, segmentBlocks, matchingRows, options.QueryRange, options.BlockSampleLimit)
			}
		}
		if chunkChecked {
			probe.chunkAvailable[chunk.SID] = chunkAvailable
			if !chunkAvailable {
				probe.chunkFailureReason[chunk.SID] = chunkFailureReason
			} else {
				probe.chunkOutputPoints[chunk.SID] = chunkOutputPoints
				probe.OutputPoints += chunkOutputPoints
			}
		}
	}
	return probe, true, nil
}

func tsspRangeInDataArea(offset, size, dataOffset, dataSize int64) bool {
	if size < 0 || offset < dataOffset || offset > dataOffset+dataSize {
		return false
	}
	return size <= dataOffset+dataSize-offset
}

func (p *tsspAttachedDataProbe) chunkDataAvailable(chunk tsspChunkMeta) (bool, string, bool) {
	if p == nil || !p.Checked {
		return false, "", false
	}
	available, ok := p.chunkAvailable[chunk.SID]
	if !ok {
		return false, "", false
	}
	return available, p.chunkFailureReason[chunk.SID], true
}

func (p *tsspAttachedDataProbe) chunkOutputPointsFor(chunk tsspChunkMeta) int {
	if p == nil {
		return 0
	}
	return p.chunkOutputPoints[chunk.SID]
}

func remainingTSSPExecutionSampleLimit(samples []DecodePathCursorStep, sampleLimit int) int {
	if sampleLimit <= 0 || len(samples) >= sampleLimit {
		return 0
	}
	return sampleLimit - len(samples)
}

func appendTSSPRangeExecutionSamples(dst *[]DecodePathCursorStep, src []DecodePathCursorStep, sampleLimit int) {
	appendTSSPCursorStepSamples(dst, src, sampleLimit)
}

func appendTSSPRecordExecutionSamples(dst *[]DecodePathCursorStep, src []DecodePathCursorStep, sampleLimit int) {
	appendTSSPCursorStepSamples(dst, src, sampleLimit)
}

func appendTSSPFilterExecutionSamples(dst *[]DecodePathCursorStep, src []DecodePathCursorStep, sampleLimit int) {
	appendTSSPCursorStepSamples(dst, src, sampleLimit)
}

func appendTSSPCursorStepSamples(dst *[]DecodePathCursorStep, src []DecodePathCursorStep, sampleLimit int) {
	if sampleLimit <= 0 || dst == nil {
		return
	}
	cursorIndexBase := 0
	if len(*dst) > 0 {
		cursorIndexBase = (*dst)[len(*dst)-1].CursorIndexAfter
	}
	for _, sample := range src {
		if len(*dst) >= sampleLimit {
			return
		}
		sample.Step = len(*dst) + 1
		sample.CursorIndexBefore += cursorIndexBase
		sample.CursorIndexAfter += cursorIndexBase
		sample.CursorExhausted = false
		*dst = append(*dst, sample)
	}
}

func appendTSSPAttachedDataProbeValueSamples(probe *tsspAttachedDataProbe, chunk tsspChunkMeta, timeRange tsspTimeRange, blocks map[string]tsspDetachedDataBlockInfo, matchingRows []bool, queryRange TimeRange, sampleLimit int) {
	if probe == nil || sampleLimit <= 0 || len(probe.valueSamples) >= sampleLimit {
		return
	}
	var recordSamples int
	var recordExecutionSamples []DecodePathCursorStep
	probe.valueSamples, recordExecutionSamples, recordSamples = appendTSSPDataProbeRecordSamples(probe.valueSamples, "sid", chunk.SID, timeRange, blocks, matchingRows, queryRange, sampleLimit, remainingTSSPExecutionSampleLimit(probe.recordExecutionSamples, sampleLimit))
	appendTSSPRecordExecutionSamples(&probe.recordExecutionSamples, recordExecutionSamples, sampleLimit)
	probe.RecordSamples += recordSamples
	if len(probe.valueSamples) >= sampleLimit {
		return
	}
	columnNames := sortedTSSPDataBlockColumns(blocks)
	for _, columnName := range columnNames {
		block := blocks[columnName]
		if columnName == "time" || !block.ValueKnown || block.ValueNull {
			continue
		}
		timestamps, ok := tsspDataBlockSampleTimes(timeRange, blocks, len(block.Values))
		if !ok {
			continue
		}
		for i, value := range block.Values {
			if !tsspDataBlockRowMatches(matchingRows, i) {
				continue
			}
			if len(block.ValuePresent) > 0 && !block.ValuePresent[i] {
				continue
			}
			timestamp := timestamps[i]
			if queryRange.Set && (timestamp < queryRange.Min || timestamp > queryRange.Max) {
				continue
			}
			probe.valueSamples = append(probe.valueSamples, DecodePathCursorOutput{
				Key:            fmt.Sprintf("sid:%d/%s", chunk.SID, columnName),
				Time:           timestamp,
				Type:           block.Type,
				OptimizedValue: value,
				Matches:        true,
			})
			if len(probe.valueSamples) >= sampleLimit {
				return
			}
		}
	}
}

func sortedTSSPDataBlockColumns(blocks map[string]tsspDetachedDataBlockInfo) []string {
	columnNames := make([]string, 0, len(blocks))
	for columnName := range blocks {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)
	return columnNames
}

type tsspDataBlockFilterStats struct {
	RangeRows              int
	RangeMatches           int
	RangeRejects           int
	Evaluations            int
	RequiredEvaluations    int
	AnyEvaluations         int
	NoneEvaluations        int
	MatchEvaluations       int
	MissEvaluations        int
	SkippedEvaluations     int
	RequiredMatches        int
	RequiredMisses         int
	RequiredSkips          int
	AnyMatches             int
	AnyMisses              int
	AnySkips               int
	NoneMatches            int
	NoneMisses             int
	NoneSkips              int
	OperatorEvaluations    map[string]int
	RangeExecutionSamples  []DecodePathCursorStep
	FilterExecutionSamples []DecodePathCursorStep
}

type tsspDataBlockFilterRowStats struct {
	RequiredEvaluations int
	RequiredMatches     int
	RequiredSkips       int
	AnyEvaluations      int
	AnyMatches          int
	AnySkips            int
	NoneEvaluations     int
	NoneMatches         int
	NoneSkips           int
}

func (s *tsspDataBlockFilterRowStats) observe(clause string, matched bool) {
	switch clause {
	case "required":
		s.RequiredEvaluations++
		if matched {
			s.RequiredMatches++
		}
	case "any":
		s.AnyEvaluations++
		if matched {
			s.AnyMatches++
		}
	case "none":
		s.NoneEvaluations++
		if matched {
			s.NoneMatches++
		}
	}
}

func (s *tsspDataBlockFilterRowStats) skip(clause string, count int) {
	if count <= 0 {
		return
	}
	switch clause {
	case "required":
		s.RequiredSkips += count
	case "any":
		s.AnySkips += count
	case "none":
		s.NoneSkips += count
	}
}

func (s tsspDataBlockFilterRowStats) candidateValue(row int, timestamp int64, timestampKnown bool, values, result string) string {
	parts := []string{fmt.Sprintf("row=%d", row)}
	if timestampKnown {
		parts = append(parts, fmt.Sprintf("time=%d", timestamp))
	}
	parts = append(parts,
		fmt.Sprintf("required=%d/%d", s.RequiredMatches, s.RequiredEvaluations),
		fmt.Sprintf("any=%d/%d", s.AnyMatches, s.AnyEvaluations),
		fmt.Sprintf("none=%d/%d", s.NoneMatches, s.NoneEvaluations),
		fmt.Sprintf("skips=%d/%d/%d", s.RequiredSkips, s.AnySkips, s.NoneSkips),
	)
	if values != "" {
		parts = append(parts, "values="+values)
	}
	parts = append(parts, "result="+result)
	return strings.Join(parts, " ")
}

func (s *tsspDataBlockFilterStats) observe(filter FieldFilter, clause string, matched bool) {
	if s.OperatorEvaluations == nil {
		s.OperatorEvaluations = map[string]int{}
	}
	op := fieldFilterOperator(filter)
	s.Evaluations++
	s.OperatorEvaluations[op]++
	if matched {
		s.MatchEvaluations++
	} else {
		s.MissEvaluations++
	}
	switch clause {
	case "required":
		s.RequiredEvaluations++
		if matched {
			s.RequiredMatches++
		} else {
			s.RequiredMisses++
		}
	case "any":
		s.AnyEvaluations++
		if matched {
			s.AnyMatches++
		} else {
			s.AnyMisses++
		}
	case "none":
		s.NoneEvaluations++
		if matched {
			s.NoneMatches++
		} else {
			s.NoneMisses++
		}
	}
}

func (s *tsspDataBlockFilterStats) skip(clause string, count int) {
	if count <= 0 {
		return
	}
	s.SkippedEvaluations += count
	switch clause {
	case "required":
		s.RequiredSkips += count
	case "any":
		s.AnySkips += count
	case "none":
		s.NoneSkips += count
	}
}

func (s *tsspDataBlockFilterStats) appendExecutionSample(row int, timestamp int64, timestampKnown bool, key string, rowStats tsspDataBlockFilterRowStats, values, result string, sampleLimit int) {
	if sampleLimit <= 0 || len(s.FilterExecutionSamples) >= sampleLimit {
		return
	}
	s.FilterExecutionSamples = append(s.FilterExecutionSamples, DecodePathCursorStep{
		Step:              len(s.FilterExecutionSamples) + 1,
		Type:              "tssp-filter-row-step",
		Action:            "filter_row_" + result,
		Key:               key,
		CandidateValue:    rowStats.candidateValue(row, timestamp, timestampKnown, values, result),
		CursorIndexBefore: row,
		CursorIndexAfter:  row + 1,
		CursorAdvanced:    true,
	})
}

func tsspDataBlockFilterRows(blocks map[string]tsspDetachedDataBlockInfo, filters []FieldFilter, anyFilters []FieldFilter, noneFilters []FieldFilter, rows int, timeRange tsspTimeRange, queryRange TimeRange, sampleKey string, rangeSampleLimit int, filterSampleLimit int) ([]bool, int, int, tsspDataBlockFilterStats, bool) {
	var stats tsspDataBlockFilterStats
	if rows <= 0 {
		return nil, 0, 0, stats, true
	}
	rangeRows, rangeMatched, rangeKnown := tsspDataBlockQueryRangeRows(blocks, timeRange, queryRange, rows)
	if rangeKnown {
		stats.RangeRows = rows
		stats.RangeMatches = rangeMatched
		stats.RangeRejects = rows - rangeMatched
	}
	hasFieldFilters := len(filters) > 0 || len(anyFilters) > 0 || len(noneFilters) > 0
	var sampleTimes []int64
	sampleTimesKnown := false
	if (rangeKnown && rangeSampleLimit > 0) || (filterSampleLimit > 0 && hasFieldFilters) {
		sampleTimes, sampleTimesKnown = tsspDataBlockSampleTimes(timeRange, blocks, rows)
	}
	if rangeKnown {
		stats.appendRangeExecutionSamples(rangeRows, sampleTimes, sampleTimesKnown, queryRange, sampleKey, rangeSampleLimit)
	}
	if !hasFieldFilters {
		return rangeRows, rangeMatched, rangeMatched, stats, true
	}
	filterBlocks, ok := tsspDataBlockRequiredFilterBlocks(blocks, filters, rows)
	if !ok {
		return nil, 0, 0, stats, false
	}
	if filterBlocks == nil && len(filters) > 0 {
		return make([]bool, rows), 0, rangeMatched, stats, true
	}
	anyFilterBlocks, ok := tsspDataBlockAnyFilterBlocks(blocks, anyFilters, rows)
	if !ok {
		return nil, 0, 0, stats, false
	}
	if len(anyFilters) > 0 && len(anyFilterBlocks) == 0 {
		return make([]bool, rows), 0, rangeMatched, stats, true
	}
	noneFilterBlocks, ok := tsspDataBlockAnyFilterBlocks(blocks, noneFilters, rows)
	if !ok {
		return nil, 0, 0, stats, false
	}
	matchingRows := make([]bool, rows)
	matched := 0
	sampleColumns := tsspDataBlockFilterSampleColumns(blocks, filters, anyFilters, noneFilters)
	for row := 0; row < rows; row++ {
		if len(rangeRows) > 0 && !rangeRows[row] {
			continue
		}
		match := true
		result := "match"
		var rowStats tsspDataBlockFilterRowStats
		for i, filterBlock := range filterBlocks {
			predicateMatch := tsspDataBlockValueMatches(filterBlock.block, row, filterBlock.filter)
			stats.observe(filterBlock.filter, "required", predicateMatch)
			rowStats.observe("required", predicateMatch)
			if !predicateMatch {
				requiredSkips := len(filterBlocks) - i - 1
				anySkips := len(anyFilterBlocks)
				noneSkips := len(noneFilterBlocks)
				stats.skip("required", requiredSkips)
				stats.skip("any", anySkips)
				stats.skip("none", noneSkips)
				rowStats.skip("required", requiredSkips)
				rowStats.skip("any", anySkips)
				rowStats.skip("none", noneSkips)
				match = false
				result = "reject_required"
				break
			}
		}
		if match && len(anyFilterBlocks) > 0 {
			match = false
			result = "reject_any"
			for i, filterBlock := range anyFilterBlocks {
				predicateMatch := tsspDataBlockValueMatches(filterBlock.block, row, filterBlock.filter)
				stats.observe(filterBlock.filter, "any", predicateMatch)
				rowStats.observe("any", predicateMatch)
				if predicateMatch {
					anySkips := len(anyFilterBlocks) - i - 1
					stats.skip("any", anySkips)
					rowStats.skip("any", anySkips)
					match = true
					result = "match"
					break
				}
			}
			if !match {
				noneSkips := len(noneFilterBlocks)
				stats.skip("none", noneSkips)
				rowStats.skip("none", noneSkips)
			}
		}
		if match && len(noneFilterBlocks) > 0 {
			for i, filterBlock := range noneFilterBlocks {
				predicateMatch := tsspDataBlockValueMatches(filterBlock.block, row, filterBlock.filter)
				stats.observe(filterBlock.filter, "none", predicateMatch)
				rowStats.observe("none", predicateMatch)
				if predicateMatch {
					noneSkips := len(noneFilterBlocks) - i - 1
					stats.skip("none", noneSkips)
					rowStats.skip("none", noneSkips)
					match = false
					result = "reject_none"
					break
				}
			}
		}
		if match {
			matchingRows[row] = true
			matched++
		}
		timestamp := int64(0)
		if sampleTimesKnown {
			timestamp = sampleTimes[row]
		}
		stats.appendExecutionSample(row, timestamp, sampleTimesKnown, fmt.Sprintf("%s/row:%d", sampleKey, row), rowStats, tsspDataBlockFilterSampleValues(blocks, sampleColumns, row), result, filterSampleLimit)
	}
	return matchingRows, matched, rangeMatched, stats, true
}

func (s *tsspDataBlockFilterStats) appendRangeExecutionSamples(rangeRows []bool, timestamps []int64, timestampsKnown bool, queryRange TimeRange, sampleKey string, sampleLimit int) {
	if sampleLimit <= 0 || len(s.RangeExecutionSamples) >= sampleLimit {
		return
	}
	if !timestampsKnown {
		return
	}
	for row, timestamp := range timestamps {
		if len(s.RangeExecutionSamples) >= sampleLimit {
			return
		}
		result := "match"
		action := "range_row_match"
		if len(rangeRows) > 0 && !rangeRows[row] {
			result = "reject_range"
			action = "range_row_reject"
		}
		s.RangeExecutionSamples = append(s.RangeExecutionSamples, DecodePathCursorStep{
			Step:              len(s.RangeExecutionSamples) + 1,
			Type:              "tssp-range-row-step",
			Action:            action,
			Key:               fmt.Sprintf("%s/row:%d", sampleKey, row),
			CandidateValue:    fmt.Sprintf("row=%d time=%d range=%d:%d result=%s", row, timestamp, queryRange.Min, queryRange.Max, result),
			CursorIndexBefore: row,
			CursorIndexAfter:  row + 1,
			CursorAdvanced:    true,
		})
	}
}

func tsspDataBlockFilterSampleColumns(blocks map[string]tsspDetachedDataBlockInfo, filters ...[]FieldFilter) []string {
	seen := map[string]struct{}{}
	for _, group := range filters {
		for _, filter := range group {
			if _, ok := seen[filter.Key]; ok {
				continue
			}
			block, ok := blocks[filter.Key]
			if !ok || !block.RowsKnown || !block.ValueKnown {
				continue
			}
			seen[filter.Key] = struct{}{}
		}
	}
	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func tsspDataBlockFilterSampleValues(blocks map[string]tsspDetachedDataBlockInfo, columns []string, row int) string {
	if len(columns) == 0 {
		return ""
	}
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, column+"="+tsspDataProbeRecordValue(blocks[column], row))
	}
	return strings.Join(parts, ",")
}

func addTSSPFilterOperatorCounts(dst, src map[string]int) {
	for op, count := range src {
		dst[op] += count
	}
}

func tsspDataBlockQueryRangeRows(blocks map[string]tsspDetachedDataBlockInfo, timeRange tsspTimeRange, queryRange TimeRange, rows int) ([]bool, int, bool) {
	if !queryRange.Set {
		return nil, rows, false
	}
	timestamps, ok := tsspDataBlockSampleTimes(timeRange, blocks, rows)
	if !ok {
		return nil, rows, false
	}
	matchingRows := make([]bool, rows)
	matched := 0
	for row, timestamp := range timestamps {
		if timestamp < queryRange.Min || timestamp > queryRange.Max {
			continue
		}
		matchingRows[row] = true
		matched++
	}
	if matched == rows {
		return nil, rows, true
	}
	return matchingRows, matched, true
}

type tsspDataBlockFieldFilterBlock struct {
	filter FieldFilter
	block  tsspDetachedDataBlockInfo
}

func tsspDataBlockRequiredFilterBlocks(blocks map[string]tsspDetachedDataBlockInfo, filters []FieldFilter, rows int) ([]tsspDataBlockFieldFilterBlock, bool) {
	filterBlocks := make([]tsspDataBlockFieldFilterBlock, 0, len(filters))
	for _, filter := range filters {
		block, ok := blocks[filter.Key]
		if !ok {
			return nil, true
		}
		if !block.RowsKnown || !block.ValueKnown || block.Rows != rows {
			return nil, false
		}
		if !block.ValueNull && len(block.Values) != rows {
			return nil, false
		}
		filterBlocks = append(filterBlocks, tsspDataBlockFieldFilterBlock{filter: filter, block: block})
	}
	return filterBlocks, true
}

func tsspDataBlockAnyFilterBlocks(blocks map[string]tsspDetachedDataBlockInfo, filters []FieldFilter, rows int) ([]tsspDataBlockFieldFilterBlock, bool) {
	filterBlocks := make([]tsspDataBlockFieldFilterBlock, 0, len(filters))
	for _, filter := range filters {
		block, ok := blocks[filter.Key]
		if !ok {
			continue
		}
		if !block.RowsKnown || !block.ValueKnown || block.Rows != rows {
			return nil, false
		}
		if !block.ValueNull && len(block.Values) != rows {
			return nil, false
		}
		filterBlocks = append(filterBlocks, tsspDataBlockFieldFilterBlock{filter: filter, block: block})
	}
	return filterBlocks, true
}

func tsspDataBlockValueMatches(block tsspDetachedDataBlockInfo, row int, filter FieldFilter) bool {
	op := fieldFilterOperator(filter)
	if op == "in" || op == "not-in" {
		values := fieldFilterSetValues(filter.Value)
		if len(values) == 0 {
			return op == "not-in"
		}
		matches := false
		for _, value := range values {
			if tsspDataBlockLiteralMatches(block, row, "=", value) {
				matches = true
				break
			}
		}
		if op == "not-in" {
			return !matches
		}
		return matches
	}
	if op == "between" || op == "not-between" {
		values := fieldFilterSetValues(filter.Value)
		if len(values) != 2 {
			return false
		}
		if !tsspDataBlockSupportsRange(block) {
			return false
		}
		matches := tsspDataBlockLiteralMatches(block, row, ">=", values[0]) && tsspDataBlockLiteralMatches(block, row, "<=", values[1])
		if op == "not-between" {
			return !matches
		}
		return matches
	}
	want := fieldFilterScalarValue(filter.Value)
	return tsspDataBlockLiteralMatches(block, row, op, want)
}

func tsspDataBlockSupportsRange(block tsspDetachedDataBlockInfo) bool {
	return strings.HasPrefix(block.Type, "float") || strings.HasPrefix(block.Type, "integer") || strings.HasPrefix(block.Type, "string")
}

func tsspDataBlockLiteralMatches(block tsspDetachedDataBlockInfo, row int, op, want string) bool {
	got := tsspDataProbeRecordValue(block, row)
	if op == "contains" || op == "not-contains" {
		if !strings.HasPrefix(block.Type, "string") || got == "null" || want == "null" {
			return false
		}
		return compareTSSPStringValues(got, want, op)
	}
	if op == "=~" || op == "!~" {
		matches, err := regexp.MatchString(want, got)
		if err != nil {
			return false
		}
		if op == "!~" {
			return !matches
		}
		return matches
	}
	// null is a reserved decoded-row sentinel even when it came from a quoted literal.
	if got == "null" || want == "null" {
		return compareTSSPEqualValues(got, want, op)
	}
	if op == "=" || op == "!=" {
		if got == want {
			return op == "="
		}
	}
	switch {
	case strings.HasPrefix(block.Type, "float"):
		gotFloat, gotErr := strconv.ParseFloat(got, 64)
		wantFloat, wantErr := strconv.ParseFloat(want, 64)
		if gotErr != nil || wantErr != nil {
			return false
		}
		return compareTSSPFloatValues(gotFloat, wantFloat, op)
	case strings.HasPrefix(block.Type, "integer"):
		gotInt, gotErr := strconv.ParseInt(got, 10, 64)
		wantInt, wantErr := strconv.ParseInt(want, 10, 64)
		if gotErr != nil || wantErr != nil {
			return false
		}
		return compareTSSPIntegerValues(gotInt, wantInt, op)
	case strings.HasPrefix(block.Type, "boolean"):
		gotBool, gotErr := strconv.ParseBool(got)
		wantBool, wantErr := strconv.ParseBool(want)
		if gotErr != nil || wantErr != nil {
			return false
		}
		return compareTSSPEqualValues(gotBool, wantBool, op)
	case strings.HasPrefix(block.Type, "string"):
		return compareTSSPStringValues(got, want, op)
	default:
		if op == "=" || op == "!=" {
			return compareTSSPEqualValues(got, want, op)
		}
		return false
	}
}

func compareTSSPEqualValues[T comparable](got, want T, op string) bool {
	switch op {
	case "=":
		return got == want
	case "!=":
		return got != want
	default:
		return false
	}
}

func compareTSSPFloatValues(got, want float64, op string) bool {
	if math.IsNaN(got) || math.IsNaN(want) {
		return compareTSSPEqualValues(math.IsNaN(got), math.IsNaN(want), op)
	}
	switch op {
	case "=":
		return got == want
	case "!=":
		return got != want
	case ">":
		return got > want
	case ">=":
		return got >= want
	case "<":
		return got < want
	case "<=":
		return got <= want
	default:
		return false
	}
}

func compareTSSPIntegerValues(got, want int64, op string) bool {
	switch op {
	case "=":
		return got == want
	case "!=":
		return got != want
	case ">":
		return got > want
	case ">=":
		return got >= want
	case "<":
		return got < want
	case "<=":
		return got <= want
	default:
		return false
	}
}

func compareTSSPStringValues(got, want, op string) bool {
	switch op {
	case "=":
		return got == want
	case "!=":
		return got != want
	case "contains":
		return strings.Contains(got, want)
	case "not-contains":
		return !strings.Contains(got, want)
	case ">":
		return got > want
	case ">=":
		return got >= want
	case "<":
		return got < want
	case "<=":
		return got <= want
	default:
		return false
	}
}

func tsspDataBlockRowMatches(matchingRows []bool, row int) bool {
	if len(matchingRows) == 0 {
		return true
	}
	return row >= 0 && row < len(matchingRows) && matchingRows[row]
}
