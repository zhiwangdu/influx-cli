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
	Checked             bool
	BlocksChecked       int
	ValidBlocks         int
	BytesRead           int64
	ShortBlocks         int
	UnknownBlockTypes   int
	ReadErrors          int
	RowCountBlocks      int
	RowCountUnknowns    int
	RowCountMismatches  int
	OutputPoints        int
	ValueBlocks         int
	ValueUnknowns       int
	ValueUnknownReasons map[string]int
	NullValues          int
	RecordSamples       int
	FilterRows          int
	FilterMatches       int
	FilterRejects       int
	FilterEvaluations   int
	FilterRequiredEvals int
	FilterAnyEvals      int
	FilterNoneEvals     int
	FilterOperators     map[string]int
	BlockTypes          map[string]int
	chunkAvailable      map[uint64]bool
	chunkFailureReason  map[uint64]string
	chunkOutputPoints   map[uint64]int
	valueSamples        []DecodePathCursorOutput
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
					chunkAvailable = false
					segmentAvailable = false
					chunkFailureReason = "segment_overlap_data_range_unavailable"
					continue
				}
				block := make([]byte, int(location.Size))
				if _, err := f.ReadAt(block, location.Offset); err != nil {
					probe.ReadErrors++
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
				matchingRows, matchedRows, filterRows, filterStats, ok := tsspDataBlockFilterRows(segmentBlocks, options.QueryFields, options.QueryAnyFields, options.QueryNoneFields, segmentRows, timeRange, options.QueryRange)
				if !ok {
					chunkAvailable = false
					chunkFailureReason = "segment_overlap_data_filter_unavailable"
					continue
				}
				if len(options.QueryFields) > 0 || len(options.QueryAnyFields) > 0 || len(options.QueryNoneFields) > 0 {
					probe.FilterRows += filterRows
					probe.FilterMatches += matchedRows
					probe.FilterRejects += filterRows - matchedRows
					probe.FilterEvaluations += filterStats.Evaluations
					probe.FilterRequiredEvals += filterStats.RequiredEvaluations
					probe.FilterAnyEvals += filterStats.AnyEvaluations
					probe.FilterNoneEvals += filterStats.NoneEvaluations
					addTSSPFilterOperatorCounts(probe.FilterOperators, filterStats.OperatorEvaluations)
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

func appendTSSPAttachedDataProbeValueSamples(probe *tsspAttachedDataProbe, chunk tsspChunkMeta, timeRange tsspTimeRange, blocks map[string]tsspDetachedDataBlockInfo, matchingRows []bool, queryRange TimeRange, sampleLimit int) {
	if probe == nil || sampleLimit <= 0 || len(probe.valueSamples) >= sampleLimit {
		return
	}
	var recordSamples int
	probe.valueSamples, recordSamples = appendTSSPDataProbeRecordSamples(probe.valueSamples, "sid", chunk.SID, timeRange, blocks, matchingRows, queryRange, sampleLimit)
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
	Evaluations         int
	RequiredEvaluations int
	AnyEvaluations      int
	NoneEvaluations     int
	OperatorEvaluations map[string]int
}

func (s *tsspDataBlockFilterStats) observe(filter FieldFilter, clause string) {
	if s.OperatorEvaluations == nil {
		s.OperatorEvaluations = map[string]int{}
	}
	op := fieldFilterOperator(filter)
	s.Evaluations++
	s.OperatorEvaluations[op]++
	switch clause {
	case "required":
		s.RequiredEvaluations++
	case "any":
		s.AnyEvaluations++
	case "none":
		s.NoneEvaluations++
	}
}

func tsspDataBlockFilterRows(blocks map[string]tsspDetachedDataBlockInfo, filters []FieldFilter, anyFilters []FieldFilter, noneFilters []FieldFilter, rows int, timeRange tsspTimeRange, queryRange TimeRange) ([]bool, int, int, tsspDataBlockFilterStats, bool) {
	var stats tsspDataBlockFilterStats
	if rows <= 0 {
		return nil, 0, 0, stats, true
	}
	rangeRows, rangeMatched := tsspDataBlockQueryRangeRows(blocks, timeRange, queryRange, rows)
	if len(filters) == 0 && len(anyFilters) == 0 && len(noneFilters) == 0 {
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
	for row := 0; row < rows; row++ {
		if len(rangeRows) > 0 && !rangeRows[row] {
			continue
		}
		match := true
		for _, filterBlock := range filterBlocks {
			stats.observe(filterBlock.filter, "required")
			if !tsspDataBlockValueMatches(filterBlock.block, row, filterBlock.filter) {
				match = false
				break
			}
		}
		if match && len(anyFilterBlocks) > 0 {
			match = false
			for _, filterBlock := range anyFilterBlocks {
				stats.observe(filterBlock.filter, "any")
				if tsspDataBlockValueMatches(filterBlock.block, row, filterBlock.filter) {
					match = true
					break
				}
			}
		}
		if match && len(noneFilterBlocks) > 0 {
			for _, filterBlock := range noneFilterBlocks {
				stats.observe(filterBlock.filter, "none")
				if tsspDataBlockValueMatches(filterBlock.block, row, filterBlock.filter) {
					match = false
					break
				}
			}
		}
		if match {
			matchingRows[row] = true
			matched++
		}
	}
	return matchingRows, matched, rangeMatched, stats, true
}

func addTSSPFilterOperatorCounts(dst, src map[string]int) {
	for op, count := range src {
		dst[op] += count
	}
}

func tsspDataBlockQueryRangeRows(blocks map[string]tsspDetachedDataBlockInfo, timeRange tsspTimeRange, queryRange TimeRange, rows int) ([]bool, int) {
	if !queryRange.Set {
		return nil, rows
	}
	timestamps, ok := tsspDataBlockSampleTimes(timeRange, blocks, rows)
	if !ok {
		return nil, rows
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
		return nil, rows
	}
	return matchingRows, matched
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
	want := fieldFilterScalarValue(filter.Value)
	return tsspDataBlockLiteralMatches(block, row, op, want)
}

func tsspDataBlockLiteralMatches(block tsspDetachedDataBlockInfo, row int, op, want string) bool {
	got := tsspDataProbeRecordValue(block, row)
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
