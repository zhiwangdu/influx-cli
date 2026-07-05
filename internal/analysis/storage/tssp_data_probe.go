package storage

import (
	"fmt"
	"os"
	"sort"
)

type tsspAttachedDataProbe struct {
	Checked            bool
	BlocksChecked      int
	ValidBlocks        int
	BytesRead          int64
	ShortBlocks        int
	UnknownBlockTypes  int
	ReadErrors         int
	RowCountBlocks     int
	RowCountUnknowns   int
	RowCountMismatches int
	OutputPoints       int
	ValueBlocks        int
	ValueUnknowns      int
	NullValues         int
	BlockTypes         map[string]int
	chunkAvailable     map[uint64]bool
	chunkFailureReason map[uint64]string
	chunkOutputPoints  map[uint64]int
	valueSamples       []DecodePathCursorOutput
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
		Checked:            true,
		BlockTypes:         map[string]int{},
		chunkAvailable:     map[uint64]bool{},
		chunkFailureReason: map[uint64]string{},
		chunkOutputPoints:  map[uint64]int{},
	}
	seriesSet := querySeriesIDSet(options.QuerySeriesIDs)
	for _, chunk := range chunks {
		if !tsspQuerySeriesSelected(chunk.SID, seriesSet) {
			continue
		}
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
				blockInfo, ok, reason := inspectTSSPDataBlockPayload(block)
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
					}
				} else {
					probe.ValueUnknowns++
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
				chunkOutputPoints += segmentRows
				appendTSSPAttachedDataProbeValueSamples(probe, chunk, timeRange, segmentBlocks, options.QueryRange, options.BlockSampleLimit)
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

func appendTSSPAttachedDataProbeValueSamples(probe *tsspAttachedDataProbe, chunk tsspChunkMeta, timeRange tsspTimeRange, blocks map[string]tsspDetachedDataBlockInfo, queryRange TimeRange, sampleLimit int) {
	if probe == nil || sampleLimit <= 0 || len(probe.valueSamples) >= sampleLimit {
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
