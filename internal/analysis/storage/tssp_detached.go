package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	gsnappy "github.com/golang/snappy"
	ksnappy "github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	tsspDetachedMetaIndexFileName   = "segment.idx"
	tsspDetachedChunkMetaFileName   = "segment.meta"
	tsspDetachedDataFileName        = "segment.bin"
	tsspDetachedMetaIndexHeaderSize = tsspHeaderSize
	// openGemini MetaIndex.marshalDetached stores id, minTime, maxTime,
	// offset, and size; Count is only present in attached meta-index records.
	tsspDetachedMetaIndexItemSize   = 8 + 8 + 8 + 8 + 4
	tsspDetachedMetaIndexRecordSize = 4 + tsspDetachedMetaIndexItemSize
	tsspDetachedChunkMetaReadNum    = 16 // openGemini immutable.ChunkMetaReadNum
)

func analyzeTSSPDetachedMetaIndex(path string, info os.FileInfo, options Options) (FileReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return FileReport{}, err
	}
	defer f.Close()

	if info.Size() < tsspDetachedMetaIndexHeaderSize {
		return FileReport{}, fmt.Errorf("file too small for detached TSSP meta-index header")
	}
	header := make([]byte, tsspDetachedMetaIndexHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return FileReport{}, err
	}
	if string(header[:len(tsspMagic)]) != tsspMagic {
		return FileReport{}, fmt.Errorf("invalid detached TSSP meta-index magic")
	}
	version := binary.BigEndian.Uint64(header[len(tsspMagic):])

	metaIndexes, err := readTSSPDetachedMetaIndexes(f, info.Size())
	if err != nil {
		return FileReport{}, err
	}
	minID, maxID, minTime, maxTime := summarizeTSSPDetachedMetaIndexes(metaIndexes)

	report := FileReport{
		Path:       path,
		Format:     FormatTSSPDetachedIndex,
		SizeBytes:  info.Size(),
		ModTime:    info.ModTime(),
		MinTime:    minTime,
		MaxTime:    maxTime,
		KeyCount:   len(metaIndexes),
		BlockCount: len(metaIndexes),
		BlocksByType: map[string]int{
			"detached-meta-index": len(metaIndexes),
		},
		MetaIndexID: SeriesIDSummary{
			Min:   minID,
			Max:   maxID,
			Count: int64(len(metaIndexes)),
		},
		QueryOverlapsFile: len(metaIndexes) > 0 && options.QueryRange.Overlaps(minTime, maxTime),
		Extra: map[string]string{
			"version":             fmt.Sprint(version),
			"layout":              "detached",
			"sidecar":             tsspDetachedMetaIndexFileName,
			"header_size":         fmt.Sprint(tsspDetachedMetaIndexHeaderSize),
			"record_size":         fmt.Sprint(tsspDetachedMetaIndexRecordSize),
			"item_size":           fmt.Sprint(tsspDetachedMetaIndexItemSize),
			"count_stored":        "false",
			"crc_algorithm":       "ieee",
			"chunk_meta_expanded": "false",
		},
	}
	for _, meta := range metaIndexes {
		if len(report.KeySamples) >= options.KeySampleLimit {
			break
		}
		report.KeySamples = append(report.KeySamples, fmt.Sprintf("meta-index-id:%d", meta.ID))
	}

	chunkMetas, expanded, err := readTSSPDetachedChunkMetas(filepath.Dir(path), metaIndexes)
	if err != nil {
		report.Notices = append(report.Notices, fmt.Sprintf("detached chunk metadata expansion unavailable: %v", err))
		expanded = false
	}
	if expanded {
		report.BlockCount = len(chunkMetas)
		report.BlocksByType["detached-chunk-meta"] = len(chunkMetas)
		report.Extra["chunk_meta_expanded"] = "true"
		report.Extra["chunk_meta_file"] = tsspDetachedChunkMetaFileName
		report.Extra["chunk_meta_decoded"] = fmt.Sprint(len(chunkMetas))
		dataValidation, checked, err := validateTSSPDetachedDataFile(filepath.Dir(path), chunkMetas)
		report.Extra["data_file_checked"] = fmt.Sprint(checked)
		if checked {
			report.Extra["data_file"] = tsspDetachedDataFileName
			if dataValidation != nil {
				report.Extra["data_file_size"] = fmt.Sprint(dataValidation.FileSize)
			}
		}
		if err != nil {
			report.Notices = append(report.Notices, fmt.Sprintf("detached data file validation unavailable: %v", err))
			dataValidation = nil
		} else if dataValidation != nil {
			report.Extra["data_range_count"] = fmt.Sprint(dataValidation.RangeCount)
			report.Extra["data_invalid_ranges"] = fmt.Sprint(dataValidation.InvalidRanges)
			if dataValidation.InvalidRanges > 0 || dataValidation.InvalidChunks > 0 {
				report.Notices = append(report.Notices, fmt.Sprintf(
					"detached data file has %d invalid chunk range(s) and %d invalid column segment range(s)",
					dataValidation.InvalidChunks,
					dataValidation.InvalidRanges,
				))
			}
		}
		dataProbe, probed, err := probeTSSPDetachedDataFile(filepath.Dir(path), chunkMetas, options, dataValidation)
		report.Extra["data_block_probe_checked"] = fmt.Sprint(probed)
		if err != nil {
			report.Notices = append(report.Notices, fmt.Sprintf("detached data block probe unavailable: %v", err))
			dataProbe = nil
		} else if dataProbe != nil {
			report.Extra["data_block_probe_blocks"] = fmt.Sprint(dataProbe.BlocksChecked)
			report.Extra["data_block_probe_bytes"] = fmt.Sprint(dataProbe.BytesRead)
			report.Extra["data_block_probe_valid_blocks"] = fmt.Sprint(dataProbe.ValidBlocks)
			report.Extra["data_block_probe_failures"] = fmt.Sprint(dataProbe.Failures())
			report.Extra["data_block_probe_crc_mismatches"] = fmt.Sprint(dataProbe.CRCMismatches)
			report.Extra["data_block_probe_row_count_blocks"] = fmt.Sprint(dataProbe.RowCountBlocks)
			report.Extra["data_block_probe_row_count_unknowns"] = fmt.Sprint(dataProbe.RowCountUnknowns)
			report.Extra["data_block_probe_row_count_mismatches"] = fmt.Sprint(dataProbe.RowCountMismatches)
			report.Extra["data_block_probe_output_points"] = fmt.Sprint(dataProbe.OutputPoints)
			report.Extra["data_block_probe_value_blocks"] = fmt.Sprint(dataProbe.ValueBlocks)
			report.Extra["data_block_probe_value_unknowns"] = fmt.Sprint(dataProbe.ValueUnknowns)
			report.Extra["data_block_probe_null_values"] = fmt.Sprint(dataProbe.NullValues)
			if len(dataProbe.BlockTypes) > 0 {
				report.Extra["data_block_probe_types"] = tsspDetachedDataProbeTypeSummary(dataProbe.BlockTypes)
			}
			if len(dataProbe.ValueUnknownReasons) > 0 {
				reasonSummary := tsspDetachedDataProbeTypeSummary(dataProbe.ValueUnknownReasons)
				report.Extra["data_block_probe_value_unknown_reasons"] = reasonSummary
				report.Notices = append(report.Notices, fmt.Sprintf("detached data block probe found %d block(s) with unavailable value samples: %s", dataProbe.ValueUnknowns, reasonSummary))
			}
			if dataProbe.Failures() > 0 {
				report.Notices = append(report.Notices, fmt.Sprintf(
					"detached data block probe found %d invalid block(s), including %d crc mismatch(es)",
					dataProbe.Failures(),
					dataProbe.CRCMismatches,
				))
			}
		}
		if options.QueryRange.Set {
			report.Extra["query_overlap_precision"] = "detached-chunk-meta"
		}
		populateTSSPDetachedChunkReports(&report, chunkMetas, options)
		report.DecodePath = buildTSSPDetachedChunkDecodePathSummary(metaIndexes, chunkMetas, options, dataValidation, dataProbe)
	} else {
		populateTSSPDetachedMetaIndexReports(&report, metaIndexes, options)
		report.DecodePath = buildTSSPDetachedMetaIndexDecodePathSummary(metaIndexes, options)
	}
	return report, nil
}

func readTSSPDetachedMetaIndexes(f *os.File, size int64) ([]tsspMetaIndex, error) {
	payloadSize := size - tsspDetachedMetaIndexHeaderSize
	if payloadSize == 0 {
		return nil, nil
	}
	if payloadSize%tsspDetachedMetaIndexRecordSize != 0 {
		return nil, fmt.Errorf("detached meta-index payload size %d is not a multiple of %d", payloadSize, tsspDetachedMetaIndexRecordSize)
	}
	buf := make([]byte, payloadSize)
	if _, err := f.ReadAt(buf, tsspDetachedMetaIndexHeaderSize); err != nil {
		return nil, fmt.Errorf("read detached meta-index payload at offset %d: %w", tsspDetachedMetaIndexHeaderSize, err)
	}

	items := make([]tsspMetaIndex, 0, int(payloadSize)/tsspDetachedMetaIndexRecordSize)
	for i := 0; len(buf) > 0; i++ {
		record := buf[:tsspDetachedMetaIndexRecordSize]
		wantCRC := binary.BigEndian.Uint32(record[:4])
		payload := record[4:]
		if gotCRC := crc32.ChecksumIEEE(payload); gotCRC != wantCRC {
			return nil, fmt.Errorf("detached meta-index record %d crc mismatch", i)
		}
		items = append(items, tsspMetaIndex{
			ID:      binary.BigEndian.Uint64(payload[:8]),
			MinTime: decodeGeminiInt64(payload[8:16]),
			MaxTime: decodeGeminiInt64(payload[16:24]),
			Offset:  decodeGeminiInt64(payload[24:32]),
			Size:    binary.BigEndian.Uint32(payload[32:36]),
		})
		buf = buf[tsspDetachedMetaIndexRecordSize:]
	}
	return items, nil
}

func summarizeTSSPDetachedMetaIndexes(metaIndexes []tsspMetaIndex) (uint64, uint64, int64, int64) {
	if len(metaIndexes) == 0 {
		return 0, 0, 0, 0
	}
	minID, maxID := metaIndexes[0].ID, metaIndexes[0].ID
	minTime, maxTime := metaIndexes[0].MinTime, metaIndexes[0].MaxTime
	for _, meta := range metaIndexes[1:] {
		if meta.ID < minID {
			minID = meta.ID
		}
		if meta.ID > maxID {
			maxID = meta.ID
		}
		if meta.MinTime < minTime {
			minTime = meta.MinTime
		}
		if meta.MaxTime > maxTime {
			maxTime = meta.MaxTime
		}
	}
	return minID, maxID, minTime, maxTime
}

func populateTSSPDetachedMetaIndexReports(report *FileReport, metaIndexes []tsspMetaIndex, options Options) {
	idSet := queryMetaIndexIDSet(options.QueryMetaIndexIDs)
	for i, meta := range metaIndexes {
		overlaps := tsspQueryMetaIndexSelected(meta.ID, idSet) && options.QueryRange.Overlaps(meta.MinTime, meta.MaxTime)
		if overlaps {
			report.QueryOverlapBlocks++
		}
		if i < options.BlockSampleLimit {
			report.Blocks = append(report.Blocks, BlockReport{
				MetaIndexID:   meta.ID,
				MinTime:       meta.MinTime,
				MaxTime:       meta.MaxTime,
				Type:          "detached-meta-index",
				Offset:        meta.Offset,
				SizeBytes:     meta.Size,
				QueryOverlaps: overlaps,
			})
		}
	}
}

func readTSSPDetachedChunkMetas(dir string, metaIndexes []tsspMetaIndex) ([]tsspChunkMeta, bool, error) {
	path := filepath.Join(dir, tsspDetachedChunkMetaFileName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	if info.Size() < tsspHeaderSize {
		return nil, true, fmt.Errorf("file too small for detached TSSP chunk metadata header")
	}
	header := make([]byte, tsspHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, true, err
	}
	if string(header[:len(tsspMagic)]) != tsspMagic {
		return nil, true, fmt.Errorf("invalid detached TSSP chunk metadata magic")
	}

	chunks := []tsspChunkMeta{}
	for i, meta := range metaIndexes {
		metas, err := readTSSPDetachedChunkMetaRange(f, info.Size(), meta)
		if err != nil {
			return nil, true, fmt.Errorf("meta-index %d id %d: %w", i, meta.ID, err)
		}
		chunks = append(chunks, metas...)
	}
	sort.SliceStable(chunks, func(i, j int) bool {
		if chunks[i].Offset != chunks[j].Offset {
			return chunks[i].Offset < chunks[j].Offset
		}
		return chunks[i].SID < chunks[j].SID
	})
	return chunks, true, nil
}

func readTSSPDetachedChunkMetaRange(f *os.File, fileSize int64, meta tsspMetaIndex) ([]tsspChunkMeta, error) {
	if meta.Size == 0 {
		return nil, nil
	}
	size := int64(meta.Size)
	if meta.Offset < tsspHeaderSize || meta.Offset > fileSize || size > fileSize-meta.Offset {
		return nil, fmt.Errorf("invalid detached chunk metadata range offset=%d size=%d", meta.Offset, meta.Size)
	}
	buf := make([]byte, int(meta.Size))
	if _, err := f.ReadAt(buf, meta.Offset); err != nil {
		return nil, err
	}
	if len(buf) < crc32.Size+tsspChunkMetaFixedLen {
		return nil, fmt.Errorf("short detached chunk metadata record")
	}
	wantCRC := binary.BigEndian.Uint32(buf[:crc32.Size])
	payload := buf[crc32.Size:]
	if gotCRC := crc32.ChecksumIEEE(payload); gotCRC != wantCRC {
		return nil, fmt.Errorf("detached chunk metadata crc mismatch")
	}
	chunk, consumed, err := parseTSSPChunkMetaBlockWithConsumed(payload)
	if err != nil {
		return nil, err
	}
	if consumed <= 0 || consumed > len(payload) {
		return nil, fmt.Errorf("invalid detached chunk metadata record size %d", consumed)
	}
	return []tsspChunkMeta{chunk}, nil
}

func populateTSSPDetachedChunkReports(report *FileReport, chunks []tsspChunkMeta, options Options) {
	idSet := queryMetaIndexIDSet(options.QueryMetaIndexIDs)
	for i, chunk := range chunks {
		overlaps := tsspQueryMetaIndexSelected(chunk.SID, idSet) && chunk.queryOverlaps(options.QueryRange)
		if overlaps {
			report.QueryOverlapBlocks++
		}
		if i < options.BlockSampleLimit {
			minTime, maxTime := chunk.minMaxTime()
			report.Blocks = append(report.Blocks, BlockReport{
				MetaIndexID:   chunk.SID,
				MinTime:       minTime,
				MaxTime:       maxTime,
				Type:          "detached-chunk-meta",
				Offset:        chunk.Offset,
				SizeBytes:     chunk.Size,
				ColumnCount:   int(chunk.ColumnCount),
				SegmentCount:  int(chunk.SegmentCount),
				QueryOverlaps: overlaps,
			})
		}
	}
}

type tsspDetachedDataValidation struct {
	Checked       bool
	FileSize      int64
	RangeCount    int
	InvalidChunks int
	InvalidRanges int
	chunkValid    map[uint64]bool
}

type tsspDetachedDataProbe struct {
	Checked             bool
	BlocksChecked       int
	ValidBlocks         int
	BytesRead           int64
	CRCMismatches       int
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
	BlockTypes          map[string]int
	chunkAvailable      map[uint64]bool
	chunkFailureReason  map[uint64]string
	chunkOutputPoints   map[uint64]int
	valueSamples        []DecodePathCursorOutput
}

func (p *tsspDetachedDataProbe) Failures() int {
	if p == nil {
		return 0
	}
	return p.CRCMismatches + p.ShortBlocks + p.UnknownBlockTypes + p.ReadErrors
}

func validateTSSPDetachedDataFile(dir string, chunks []tsspChunkMeta) (*tsspDetachedDataValidation, bool, error) {
	path := filepath.Join(dir, tsspDetachedDataFileName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, true, err
	}
	validation := &tsspDetachedDataValidation{
		Checked:    true,
		FileSize:   info.Size(),
		chunkValid: make(map[uint64]bool, len(chunks)),
	}
	if info.Size() < tsspHeaderSize {
		return validation, true, fmt.Errorf("file too small for detached TSSP data header")
	}
	header := make([]byte, tsspHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return validation, true, err
	}
	if string(header[:len(tsspMagic)]) != tsspMagic {
		return validation, true, fmt.Errorf("invalid detached TSSP data magic")
	}
	for _, chunk := range chunks {
		valid := true
		if !tsspRangeInFile(chunk.Offset, int64(chunk.Size), info.Size()) {
			validation.InvalidChunks++
			valid = false
		}
		for _, column := range chunk.Columns {
			for _, segment := range column.Segments {
				validation.RangeCount++
				if !tsspRangeInFile(segment.Offset, int64(segment.Size), info.Size()) {
					validation.InvalidRanges++
					valid = false
				}
			}
		}
		validation.chunkValid[chunk.SID] = valid
	}
	return validation, true, nil
}

func tsspRangeInFile(offset, size, fileSize int64) bool {
	if offset < tsspHeaderSize || size < 0 || offset > fileSize {
		return false
	}
	return size <= fileSize-offset
}

func (v *tsspDetachedDataValidation) chunkDataAvailable(chunk tsspChunkMeta) bool {
	if v == nil || !v.Checked {
		return false
	}
	return v.chunkValid[chunk.SID]
}

func probeTSSPDetachedDataFile(dir string, chunks []tsspChunkMeta, options Options, validation *tsspDetachedDataValidation) (*tsspDetachedDataProbe, bool, error) {
	if !options.QueryRange.Set || validation == nil || !validation.Checked {
		return nil, false, nil
	}
	path := filepath.Join(dir, tsspDetachedDataFileName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer f.Close()
	header := make([]byte, tsspHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, true, err
	}
	if string(header[:len(tsspMagic)]) != tsspMagic {
		return nil, true, fmt.Errorf("invalid detached TSSP data magic")
	}

	probe := &tsspDetachedDataProbe{
		Checked:             true,
		ValueUnknownReasons: map[string]int{},
		BlockTypes:          map[string]int{},
		chunkAvailable:      map[uint64]bool{},
		chunkFailureReason:  map[uint64]string{},
		chunkOutputPoints:   map[uint64]int{},
	}
	idSet := queryMetaIndexIDSet(options.QueryMetaIndexIDs)
	for _, chunk := range chunks {
		if !tsspQueryMetaIndexSelected(chunk.SID, idSet) {
			continue
		}
		if !validation.chunkDataAvailable(chunk) {
			probe.chunkAvailable[chunk.SID] = false
			probe.chunkFailureReason[chunk.SID] = "segment_overlap_data_range_unavailable"
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
				if !tsspRangeInFile(location.Offset, int64(location.Size), validation.FileSize) {
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
				blockInfo, ok, reason := inspectTSSPDetachedDataBlock(block)
				if !ok {
					chunkAvailable = false
					segmentAvailable = false
					chunkFailureReason = reason
					switch reason {
					case "segment_overlap_data_crc_unavailable":
						probe.CRCMismatches++
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
				chunkOutputPoints += segmentRows
				appendTSSPDetachedDataProbeValueSamples(probe, chunk, timeRange, segmentBlocks, options.QueryRange, options.BlockSampleLimit)
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

type tsspDetachedDataBlockInfo struct {
	Type        string
	Rows        int
	RowsKnown   bool
	Value       string
	Values      []string
	ValueKnown  bool
	ValueReason string
	ValueNull   bool
}

func inspectTSSPDetachedDataBlock(block []byte) (tsspDetachedDataBlockInfo, bool, string) {
	if len(block) < crc32.Size+1 {
		return tsspDetachedDataBlockInfo{}, false, "segment_overlap_data_header_unavailable"
	}
	wantCRC := binary.BigEndian.Uint32(block[:crc32.Size])
	payload := block[crc32.Size:]
	if gotCRC := crc32.ChecksumIEEE(payload); gotCRC != wantCRC {
		return tsspDetachedDataBlockInfo{}, false, "segment_overlap_data_crc_unavailable"
	}
	return inspectTSSPDataBlockPayload(payload)
}

func inspectTSSPDataBlockPayload(payload []byte) (tsspDetachedDataBlockInfo, bool, string) {
	var info tsspDetachedDataBlockInfo
	if len(payload) < 1 {
		return info, false, "segment_overlap_data_header_unavailable"
	}
	blockType, ok := tsspDataBlockTypeName(payload[0])
	if !ok {
		return info, false, "segment_overlap_data_header_unavailable"
	}
	info.Type = blockType
	switch {
	case tsspDataBlockTypeIsOne(payload[0]):
		info.Rows = 1
		info.RowsKnown = true
		if value, ok := decodeTSSPDetachedOneBlockValue(payload); ok {
			info.Value = value
			info.Values = []string{value}
			info.ValueKnown = true
		}
	case tsspDataBlockTypeIsFullOrEmpty(payload[0]):
		if len(payload) < 5 {
			return info, false, "segment_overlap_data_header_unavailable"
		}
		info.Rows = int(binary.BigEndian.Uint32(payload[1:5]))
		info.RowsKnown = true
		if tsspDataBlockTypeIsEmpty(payload[0]) {
			info.ValueKnown = true
			info.ValueNull = true
		} else if payload[0] == 31 {
			if values, ok := decodeTSSPFloatFullValues(payload[5:], info.Rows); ok {
				info.Values = values
				if len(values) > 0 {
					info.Value = values[0]
				}
				info.ValueKnown = true
			}
		} else if payload[0] == 32 {
			if values, ok := decodeTSSPIntegerFullValues(payload[5:], info.Rows); ok {
				info.Values = values
				if len(values) > 0 {
					info.Value = values[0]
				}
				info.ValueKnown = true
			}
		} else if payload[0] == 33 {
			if values, ok := decodeTSSPBooleanFullBitpackValues(payload[5:], info.Rows); ok {
				info.Values = values
				if len(values) > 0 {
					info.Value = values[0]
				}
				info.ValueKnown = true
			}
		} else if payload[0] == 34 {
			if values, ok := decodeTSSPStringFullValues(payload[5:], info.Rows); ok {
				info.Values = values
				if len(values) > 0 {
					info.Value = values[0]
				}
				info.ValueKnown = true
			}
		}
		if !info.ValueKnown && !info.ValueNull {
			info.ValueReason = tsspDataBlockValueUnknownReason(payload[0], payload[5:])
		}
	default:
		if !validTSSPRegularDataBlockHeader(payload) {
			return info, false, "segment_overlap_data_header_unavailable"
		}
	}
	return info, true, ""
}

func tsspDataBlockValueUnknownReason(blockType byte, encoded []byte) string {
	if len(encoded) == 0 {
		return ""
	}
	codec := encoded[0] >> 4
	if codec == 0 {
		return ""
	}
	switch blockType {
	case 31:
		return fmt.Sprintf("float-full-codec-%d", codec)
	case 32:
		return fmt.Sprintf("integer-full-codec-%d", codec)
	case 33:
		return fmt.Sprintf("boolean-full-codec-%d", codec)
	case 34:
		return fmt.Sprintf("string-full-codec-%d", codec)
	default:
		return "value-codec-unavailable"
	}
}

func decodeTSSPDetachedOneBlockValue(payload []byte) (string, bool) {
	if len(payload) < 2 {
		return "", false
	}
	// Block*One appends record.ColVal.Val directly; openGemini's supported
	// targets store numeric ColVal bytes in little-endian native layout.
	value := payload[1:]
	switch payload[0] {
	case 17:
		if len(value) != 8 {
			return "", false
		}
		f := math.Float64frombits(binary.LittleEndian.Uint64(value))
		return strconv.FormatFloat(f, 'f', -1, 64), true
	case 18:
		if len(value) != 8 {
			return "", false
		}
		return strconv.FormatInt(int64(binary.LittleEndian.Uint64(value)), 10), true
	case 19:
		if len(value) != 1 {
			return "", false
		}
		return strconv.FormatBool(value[0] != 0), true
	case 20:
		return string(value), true
	default:
		return "", false
	}
}

func decodeTSSPFloatFullValues(encoded []byte, rows int) ([]string, bool) {
	if rows < 0 || len(encoded) < 1 {
		return nil, false
	}
	switch encoded[0] >> 4 {
	case 0:
		return decodeTSSPFloatFullRawValues(encoded[1:], rows)
	case 1:
		return decodeTSSPFloatFullOldGorillaValues(encoded[1:], rows)
	case 2:
		decoded, err := gsnappy.Decode(nil, encoded[1:])
		if err != nil {
			return nil, false
		}
		return decodeTSSPFloatFullRawValues(decoded, rows)
	case 3:
		values, err := decodeTSMFloatValues(encoded[1:])
		if err != nil || len(values) != rows {
			return nil, false
		}
		return formatTSSPFloatValues(values), true
	case 4:
		values, ok := decodeTSSPFloatFullSameValues(encoded[1:], rows)
		if !ok {
			return nil, false
		}
		return formatTSSPFloatValues(values), true
	case 5:
		values, ok := decodeTSSPFloatFullRLEValues(encoded[1:], rows)
		if !ok {
			return nil, false
		}
		return formatTSSPFloatValues(values), true
	case 6:
		values, ok := decodeTSSPFloatFullMLFValues(encoded[1:], rows)
		if !ok {
			return nil, false
		}
		return formatTSSPFloatValues(values), true
	default:
		return nil, false
	}
}

func decodeTSSPFloatFullOldGorillaValues(encoded []byte, rows int) ([]string, bool) {
	if rows < 0 || len(encoded) < 4 {
		return nil, false
	}
	count := int(binary.BigEndian.Uint32(encoded[:4]))
	if count != rows {
		return nil, false
	}
	tsmEncoded := make([]byte, 1, len(encoded)-3)
	tsmEncoded[0] = tsmFloatCompressedGorilla << 4
	tsmEncoded = append(tsmEncoded, encoded[4:]...)
	values, err := decodeTSMFloatValues(tsmEncoded)
	if err != nil || len(values) != rows {
		return nil, false
	}
	return formatTSSPFloatValues(values), true
}

func decodeTSSPFloatFullRawValues(raw []byte, rows int) ([]string, bool) {
	values, ok := decodeTSSPFloatFullRawFloatValues(raw, rows)
	if !ok {
		return nil, false
	}
	return formatTSSPFloatValues(values), true
}

func decodeTSSPFloatFullRawFloatValues(raw []byte, rows int) ([]float64, bool) {
	if len(raw) != rows*8 {
		return nil, false
	}
	values := make([]float64, rows)
	for offset := 0; offset < len(raw); offset += 8 {
		values[offset/8] = math.Float64frombits(binary.LittleEndian.Uint64(raw[offset : offset+8]))
	}
	return values, true
}

func decodeTSSPFloatFullSameValues(encoded []byte, rows int) ([]float64, bool) {
	if rows < 0 || len(encoded) < 2 {
		return nil, false
	}
	count := int(binary.BigEndian.Uint16(encoded[:2]))
	if count != rows {
		return nil, false
	}
	value := 0.0
	if len(encoded) > 2 {
		if len(encoded) < 10 {
			return nil, false
		}
		value = math.Float64frombits(binary.LittleEndian.Uint64(encoded[2:10]))
	}
	values := make([]float64, count)
	for i := range values {
		values[i] = value
	}
	return values, true
}

func decodeTSSPFloatFullRLEValues(encoded []byte, rows int) ([]float64, bool) {
	if rows < 0 {
		return nil, false
	}
	values := make([]float64, 0, rows)
	for len(encoded) > 0 {
		if len(encoded) < 2 {
			return nil, false
		}
		countBits := binary.BigEndian.Uint16(encoded[:2])
		encoded = encoded[2:]
		zero := countBits>>15 == 1
		count := int(countBits &^ (uint16(1) << 15))
		if count == 0 || len(values)+count > rows {
			return nil, false
		}
		value := 0.0
		if !zero {
			if len(encoded) < 8 {
				return nil, false
			}
			value = math.Float64frombits(binary.LittleEndian.Uint64(encoded[:8]))
			encoded = encoded[8:]
		}
		for i := 0; i < count; i++ {
			values = append(values, value)
		}
	}
	if len(values) != rows {
		return nil, false
	}
	return values, true
}

const (
	tsspMLFCompressModeNone    = 0xF0
	tsspMLFCompressModeSame    = 0xF1
	tsspMLFCompressModeAllZero = 0xF2

	tsspMLFBitmapEmpty  = 0
	tsspMLFBitmapNormal = 1

	tsspMLFFlagZero     = 1
	tsspMLFFlagNegative = 2
	tsspMLFFlagSkip     = 3

	tsspMLFMantissaBits  = 52
	tsspMLFMiddleNumber  = 1023
	tsspMLFMaxFactorBits = 50
)

var tsspMLFPow10 = [...]float64{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000}

func decodeTSSPFloatFullMLFValues(encoded []byte, rows int) ([]float64, bool) {
	if rows < 0 || len(encoded) < 3 {
		return nil, false
	}
	count := int(binary.BigEndian.Uint16(encoded[:2]))
	if count != rows {
		return nil, false
	}
	mode := encoded[2]
	data := encoded[3:]
	switch mode {
	case tsspMLFCompressModeNone:
		return decodeTSSPFloatFullRawFloatValues(data, rows)
	case tsspMLFCompressModeAllZero:
		return make([]float64, rows), true
	case tsspMLFCompressModeSame:
		if len(data) < 8 {
			return nil, false
		}
		value := math.Float64frombits(binary.BigEndian.Uint64(data[:8]))
		values := make([]float64, rows)
		for i := range values {
			values[i] = value
		}
		return values, true
	default:
		if int(mode) >= len(tsspMLFPow10) {
			return nil, false
		}
		return decodeTSSPFloatFullMLFFactors(data, rows, int(mode))
	}
}

func decodeTSSPFloatFullMLFFactors(data []byte, rows int, precisionSize int) ([]float64, bool) {
	if len(data) < 3 {
		return nil, false
	}
	uncompressedCount := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	if uncompressedCount < 0 || uncompressedCount > rows || len(data) < uncompressedCount*8+1 {
		return nil, false
	}
	uncompressed := data[:uncompressedCount*8]
	data = data[uncompressedCount*8:]

	bitmapFlag := data[0]
	data = data[1:]
	var bitmap []byte
	switch bitmapFlag {
	case tsspMLFBitmapEmpty:
	case tsspMLFBitmapNormal:
		size := tsspMLFBitmapSize(rows)
		if len(data) < size {
			return nil, false
		}
		bitmap = data[:size]
		data = data[size:]
	default:
		return nil, false
	}

	var multiplicand float64
	var bitSize, publicPrefixSize int
	if len(data) > 0 {
		if len(data) < 10 {
			return nil, false
		}
		multiplicand = math.Float64frombits(binary.BigEndian.Uint64(data[:8]))
		bitSize = int(data[8])
		publicPrefixSize = int(data[9])
		data = data[10:]
		if bitSize <= 0 || bitSize >= tsspMLFMaxFactorBits || publicPrefixSize < 0 || bitSize+publicPrefixSize > tsspMLFMantissaBits {
			return nil, false
		}
	}

	values := make([]float64, rows)
	bitPos := 0
	uncompressedOffset := 0
	precision := tsspMLFPow10[precisionSize]
	for i := range values {
		flag := uint8(0)
		if bitmap != nil {
			var ok bool
			flag, ok = tsspMLFBitmapFlag(bitmap, i)
			if !ok {
				return nil, false
			}
		}
		switch flag {
		case tsspMLFFlagZero:
			values[i] = 0
		case tsspMLFFlagSkip:
			if len(uncompressed)-uncompressedOffset < 8 {
				return nil, false
			}
			values[i] = math.Float64frombits(binary.BigEndian.Uint64(uncompressed[uncompressedOffset : uncompressedOffset+8]))
			uncompressedOffset += 8
		case tsspMLFFlagNegative, 0:
			if bitSize == 0 {
				return nil, false
			}
			coefficient, ok := tsspReadBits(data, bitPos, bitSize)
			if !ok {
				return nil, false
			}
			bitPos += bitSize
			value, ok := decodeTSSPFloatFullMLFCoefficient(coefficient, bitSize, publicPrefixSize, precision, multiplicand)
			if !ok {
				return nil, false
			}
			if flag == tsspMLFFlagNegative {
				value = -value
			}
			values[i] = value
		default:
			return nil, false
		}
	}
	if uncompressedOffset != len(uncompressed) {
		return nil, false
	}
	return values, true
}

func decodeTSSPFloatFullMLFCoefficient(coefficient uint64, bitSize int, publicPrefixSize int, precision float64, multiplicand float64) (float64, bool) {
	left := tsspMLFMantissaBits - bitSize - publicPrefixSize
	if left < 0 {
		return 0, false
	}
	prefix := uint64(0)
	if publicPrefixSize > 0 {
		prefix = (uint64(1)<<uint(publicPrefixSize) - 1) << uint(tsspMLFMantissaBits-publicPrefixSize)
	}
	base := prefix | (uint64(tsspMLFMiddleNumber) << tsspMLFMantissaBits)
	factor := math.Float64frombits(base|(coefficient<<uint(left))) - 1
	return math.Floor(multiplicand*factor*precision) / precision, true
}

func tsspMLFBitmapSize(rows int) int {
	return 2 * ((rows + 7) / 8)
}

func tsspMLFBitmapFlag(bitmap []byte, pos int) (uint8, bool) {
	index := pos / 4
	if index < 0 || index >= len(bitmap) {
		return 0, false
	}
	shift := uint(6 - 2*(pos%4))
	return (bitmap[index] >> shift) & 3, true
}

func tsspReadBits(data []byte, bitPos int, bitSize int) (uint64, bool) {
	if bitSize < 0 || bitSize > 64 || bitPos < 0 || len(data)*8-bitPos < bitSize {
		return 0, false
	}
	var value uint64
	for i := 0; i < bitSize; i++ {
		index := bitPos + i
		bit := (data[index/8] >> uint(7-index%8)) & 1
		value = (value << 1) | uint64(bit)
	}
	return value, true
}

func formatTSSPFloatValues(values []float64) []string {
	formatted := make([]string, len(values))
	for i, value := range values {
		formatted[i] = strconv.FormatFloat(value, 'f', -1, 64)
	}
	return formatted
}

func decodeTSSPIntegerFullValues(encoded []byte, rows int) ([]string, bool) {
	if rows < 0 || len(encoded) < 1 {
		return nil, false
	}
	switch encoded[0] >> 4 {
	case 1:
		return decodeTSSPIntegerFullConstDeltaValues(encoded[1:], rows)
	case 2:
		return decodeTSSPIntegerFullSimple8bValues(encoded[1:], rows)
	case 3:
		return decodeTSSPIntegerFullZSTDValues(encoded[1:], rows)
	case 4:
		return decodeTSSPIntegerFullUncompressedValues(encoded, rows)
	default:
		return nil, false
	}
}

func decodeTSSPIntegerFullUncompressedValues(encoded []byte, rows int) ([]string, bool) {
	if rows < 0 || len(encoded) < 5 || encoded[0]>>4 != 4 {
		return nil, false
	}
	rawLen := int(binary.BigEndian.Uint32(encoded[1:5]))
	if rawLen%8 != 0 || len(encoded)-5 < rawLen {
		return nil, false
	}
	if rows != rawLen/8 {
		return nil, false
	}
	values := make([]string, 0, rows)
	raw := encoded[5 : 5+rawLen]
	for offset := 0; offset < len(raw); offset += 8 {
		values = append(values, strconv.FormatInt(decodeGeminiInt64(raw[offset:offset+8]), 10))
	}
	return values, true
}

func decodeTSSPIntegerFullConstDeltaValues(encoded []byte, rows int) ([]string, bool) {
	if rows <= 0 || len(encoded) < 8 {
		return nil, false
	}
	first := decodeGeminiZigZagUint64(binary.BigEndian.Uint64(encoded[:8]))
	encoded = encoded[8:]
	delta, n := binary.Uvarint(encoded)
	if n <= 0 {
		return nil, false
	}
	encoded = encoded[n:]
	deltaCount, n := binary.Uvarint(encoded)
	if n <= 0 || deltaCount != uint64(rows-1) {
		return nil, false
	}
	step := decodeGeminiZigZagUint64(delta)
	values := make([]string, rows)
	value := first
	for i := 0; i < rows; i++ {
		values[i] = strconv.FormatInt(value, 10)
		value += step
	}
	return values, true
}

func decodeTSSPIntegerFullSimple8bValues(encoded []byte, rows int) ([]string, bool) {
	if rows <= 0 || len(encoded) < 16 {
		return nil, false
	}
	encodedCount := int(binary.BigEndian.Uint32(encoded[:4]))
	sourceCount := int(binary.BigEndian.Uint32(encoded[4:8]))
	encoded = encoded[8:]
	if sourceCount != rows || encodedCount < 1 || len(encoded) < encodedCount*8 {
		return nil, false
	}
	encoded = encoded[:encodedCount*8]
	first := decodeGeminiZigZagUint64(binary.BigEndian.Uint64(encoded[:8]))
	if rows == 1 {
		if encodedCount != 1 {
			return nil, false
		}
		return []string{strconv.FormatInt(first, 10)}, true
	}
	deltas, err := decodeSimple8bValues(encoded[8:])
	if err != nil || len(deltas) != rows-1 {
		return nil, false
	}
	values := make([]string, rows)
	value := first
	values[0] = strconv.FormatInt(value, 10)
	for i, delta := range deltas {
		value += decodeGeminiZigZagUint64(delta)
		values[i+1] = strconv.FormatInt(value, 10)
	}
	return values, true
}

func decodeTSSPIntegerFullZSTDValues(encoded []byte, rows int) ([]string, bool) {
	if rows < 0 || len(encoded) < 8 {
		return nil, false
	}
	sourceLen := binary.BigEndian.Uint32(encoded[:4])
	compressedLen := binary.BigEndian.Uint32(encoded[4:8])
	encoded = encoded[8:]
	if sourceLen%8 != 0 || uint64(sourceLen/8) != uint64(rows) || uint64(compressedLen) > uint64(len(encoded)) {
		return nil, false
	}
	sourceLenInt := int(sourceLen)
	compressedLenInt := int(compressedLen)
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, false
	}
	defer decoder.Close()
	raw, err := decoder.DecodeAll(encoded[:compressedLenInt], make([]byte, 0, sourceLenInt))
	if err != nil || len(raw) != sourceLenInt {
		return nil, false
	}
	values := make([]string, 0, rows)
	for offset := 0; offset < len(raw); offset += 8 {
		values = append(values, strconv.FormatInt(int64(binary.LittleEndian.Uint64(raw[offset:offset+8])), 10))
	}
	return values, true
}

func decodeTSSPBooleanFullBitpackValues(encoded []byte, rows int) ([]string, bool) {
	if rows < 0 || len(encoded) < 5 || encoded[0]>>4 != 1 {
		return nil, false
	}
	count := int(binary.BigEndian.Uint32(encoded[1:5]))
	if count != rows {
		return nil, false
	}
	bitBytes := (count + 7) / 8
	if len(encoded)-5 < bitBytes {
		return nil, false
	}
	values := make([]string, count)
	bits := encoded[5 : 5+bitBytes]
	for i := 0; i < count; i++ {
		if bits[i/8]&(0x80>>uint(i%8)) != 0 {
			values[i] = strconv.FormatBool(true)
		} else {
			values[i] = strconv.FormatBool(false)
		}
	}
	return values, true
}

func decodeTSSPStringFullValues(encoded []byte, rows int) ([]string, bool) {
	if rows < 0 || len(encoded) < 9 {
		return nil, false
	}
	codec := encoded[0] >> 4
	sourceLen := int(binary.BigEndian.Uint32(encoded[1:5]))
	compressedLen := int(binary.BigEndian.Uint32(encoded[5:9]))
	if sourceLen < 0 || compressedLen < 0 || len(encoded)-9 < compressedLen {
		return nil, false
	}
	compressed := encoded[9 : 9+compressedLen]

	var raw []byte
	switch codec {
	case 0:
		if sourceLen != compressedLen {
			return nil, false
		}
		raw = compressed
	case 1:
		decoded, err := ksnappy.Decode(nil, compressed)
		if err != nil {
			return nil, false
		}
		raw = decoded
	case 2:
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, false
		}
		defer decoder.Close()
		decoded, err := decoder.DecodeAll(compressed, make([]byte, 0, sourceLen))
		if err != nil {
			return nil, false
		}
		raw = decoded
	case 3:
		decoded := make([]byte, sourceLen)
		n, err := lz4.UncompressBlock(compressed, decoded)
		if err != nil || n != sourceLen {
			return nil, false
		}
		raw = decoded[:n]
	default:
		return nil, false
	}
	if len(raw) != sourceLen {
		return nil, false
	}
	return decodeTSSPPackedStringValues(raw, rows)
}

const (
	tsspStringEncodingV1  = ^uint32(0)
	tsspStringEncodingV2  = tsspStringEncodingV1 - 1
	tsspStringEncodingEnd = tsspStringEncodingV1 - 2
)

func decodeTSSPPackedStringValues(src []byte, rows int) ([]string, bool) {
	if len(src) < 4 {
		return nil, false
	}
	version := binary.BigEndian.Uint32(src[:4])
	switch {
	case version == tsspStringEncodingV2:
		return decodeTSSPPackedStringV2Values(src[4:], rows)
	case version == tsspStringEncodingV1 || version < tsspStringEncodingEnd:
		return decodeTSSPPackedStringV1Values(src, rows)
	default:
		return nil, false
	}
}

func decodeTSSPPackedStringV1Values(src []byte, rows int) ([]string, bool) {
	if len(src) < 4 {
		return nil, false
	}
	byteLen := int(binary.BigEndian.Uint32(src[:4]))
	src = src[4:]
	if len(src) < byteLen+4 {
		return nil, false
	}
	data := src[:byteLen]
	src = src[byteLen:]
	offsetBytes := int(binary.BigEndian.Uint32(src[:4]))
	src = src[4:]
	if offsetBytes%4 != 0 || len(src) < offsetBytes {
		return nil, false
	}
	offsetCount := offsetBytes / 4
	if offsetCount != rows {
		return nil, false
	}
	offsets := make([]uint32, offsetCount)
	for i := range offsets {
		offsets[i] = binary.BigEndian.Uint32(src[i*4:])
	}
	return materializeTSSPStringValues(data, offsets)
}

func decodeTSSPPackedStringV2Values(src []byte, rows int) ([]string, bool) {
	if len(src) < 4 {
		return nil, false
	}
	byteLen := int(binary.BigEndian.Uint32(src[:4]))
	src = src[4:]
	if len(src) < byteLen+4 {
		return nil, false
	}
	data := src[:byteLen]
	src = src[byteLen:]
	offsetCount := int(binary.BigEndian.Uint32(src[:4]))
	src = src[4:]
	if offsetCount != rows || len(src) < offsetCount*4 {
		return nil, false
	}
	offsets := make([]uint32, offsetCount)
	var pos uint32
	byteLenU := uint32(byteLen)
	for i := 0; i < offsetCount; i++ {
		length := binary.BigEndian.Uint32(src[i*4:])
		offsets[i] = pos
		if length > byteLenU-pos {
			return nil, false
		}
		pos += length
	}
	if pos != byteLenU {
		return nil, false
	}
	return materializeTSSPStringValues(data, offsets)
}

func materializeTSSPStringValues(data []byte, offsets []uint32) ([]string, bool) {
	if len(offsets) > 0 && offsets[0] != 0 {
		return nil, false
	}
	values := make([]string, 0, len(offsets))
	for i, offset := range offsets {
		if offset > uint32(len(data)) {
			return nil, false
		}
		end := uint32(len(data))
		if i+1 < len(offsets) {
			end = offsets[i+1]
		}
		if end < offset || end > uint32(len(data)) {
			return nil, false
		}
		values = append(values, string(data[offset:end]))
	}
	return values, true
}

func tsspDataBlockTypeName(blockType byte) (string, bool) {
	switch blockType {
	case 1:
		return "integer", true
	case 3:
		return "float", true
	case 4:
		return "string", true
	case 5:
		return "boolean", true
	case 17:
		return "float-one", true
	case 18:
		return "integer-one", true
	case 19:
		return "boolean-one", true
	case 20:
		return "string-one", true
	case 31:
		return "float-full", true
	case 32:
		return "integer-full", true
	case 33:
		return "boolean-full", true
	case 34:
		return "string-full", true
	case 41:
		return "float-empty", true
	case 42:
		return "integer-empty", true
	case 43:
		return "boolean-empty", true
	case 44:
		return "string-empty", true
	default:
		return "", false
	}
}

func tsspDataBlockTypeIsOne(blockType byte) bool {
	return blockType > 16 && blockType < 21
}

func tsspDataBlockTypeIsFullOrEmpty(blockType byte) bool {
	return (blockType > 30 && blockType < 35) || (blockType > 40 && blockType < 45)
}

func tsspDataBlockTypeIsEmpty(blockType byte) bool {
	return blockType > 40 && blockType < 45
}

func validTSSPRegularDataBlockHeader(payload []byte) bool {
	if len(payload) < 13 {
		return false
	}
	nilBitmapLen := binary.BigEndian.Uint32(payload[1:5])
	headerLen := uint64(1 + 4 + 8)
	headerLen += uint64(nilBitmapLen)
	return headerLen <= uint64(len(payload))
}

func tsspDetachedDataProbeTypeSummary(types map[string]int) string {
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s:%d", name, types[name]))
	}
	return strings.Join(parts, ",")
}

func (p *tsspDetachedDataProbe) chunkDataAvailable(chunk tsspChunkMeta) (bool, string, bool) {
	if p == nil || !p.Checked {
		return false, "", false
	}
	available, ok := p.chunkAvailable[chunk.SID]
	if !ok {
		return false, "", false
	}
	return available, p.chunkFailureReason[chunk.SID], true
}

func (p *tsspDetachedDataProbe) chunkOutputPointsFor(chunk tsspChunkMeta) int {
	if p == nil {
		return 0
	}
	return p.chunkOutputPoints[chunk.SID]
}

func appendTSSPDetachedDataProbeValueSamples(probe *tsspDetachedDataProbe, chunk tsspChunkMeta, timeRange tsspTimeRange, blocks map[string]tsspDetachedDataBlockInfo, queryRange TimeRange, sampleLimit int) {
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
				Key:            fmt.Sprintf("meta-index-id:%d/%s", chunk.SID, columnName),
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

func tsspDataBlockSampleTimes(timeRange tsspTimeRange, blocks map[string]tsspDetachedDataBlockInfo, rows int) ([]int64, bool) {
	if rows <= 0 {
		return nil, false
	}
	if block, ok := blocks["time"]; ok && block.ValueKnown && !block.ValueNull {
		if len(block.Values) != rows {
			return nil, false
		}
		timestamps := make([]int64, 0, rows)
		for _, raw := range block.Values {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return nil, false
			}
			timestamps = append(timestamps, value)
		}
		return timestamps, true
	}
	if timeRange.Min == timeRange.Max {
		timestamps := make([]int64, rows)
		for i := range timestamps {
			timestamps[i] = timeRange.Min
		}
		return timestamps, true
	}
	return nil, false
}

func buildTSSPDetachedChunkDecodePathSummary(metaIndexes []tsspMetaIndex, chunks []tsspChunkMeta, options Options, dataValidation *tsspDetachedDataValidation, dataProbe *tsspDetachedDataProbe) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                 tsspCursorMode("tssp-detached-location-cursor", options),
		QueryRange:           options.QueryRange,
		CursorSeekTime:       tsspCursorSeekTime(options),
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	idSet := queryMetaIndexIDSet(options.QueryMetaIndexIDs)
	if len(idSet) > 0 {
		summary.QueryMetaIndexIDs = append([]uint64(nil), options.QueryMetaIndexIDs...)
		summary.KeyFilterApplied = true
	}
	populateTSSPDecodeMetaIndexMatches(summary, metaIndexes)

	chunksByID := make(map[uint64]tsspChunkMeta, len(chunks))
	for _, chunk := range chunks {
		chunksByID[chunk.SID] = chunk
	}

	selectedMetas := []tsspMetaIndex{}
	overlapMetas := []tsspMetaIndex{}
	for _, meta := range tsspMetaIndexesForCursor(metaIndexes, options.CursorDescending) {
		if !tsspQueryMetaIndexSelected(meta.ID, idSet) {
			summary.SkippedByKeyBlocks++
			continue
		}
		selectedMetas = append(selectedMetas, meta)
		if options.QueryRange.Overlaps(meta.MinTime, meta.MaxTime) {
			overlapMetas = append(overlapMetas, meta)
			if summary.IteratorCostFiles == 0 {
				summary.IteratorCostFiles = 1
			}
			summary.IteratorCostBlocks++
			summary.IteratorCostBytes += int64(meta.Size)
		}

		chunk, ok := chunksByID[meta.ID]
		if !ok {
			continue
		}
		minTime, maxTime := chunk.minMaxTime()
		segmentCount := len(chunk.TimeRanges)
		outputSegments, outputBytes := tsspChunkOutputSegments(chunk, options.QueryRange)
		baselineBytes := tsspChunkSegmentBytes(chunk)
		baselineReadAtCalls, _ := tsspChunkReadAtPlan(chunk, TimeRange{}, false, 0)
		optimizedReadAtCalls, readAtRanges := tsspChunkReadAtPlan(chunk, options.QueryRange, true, maxTSSPReadAtRangeSamples)
		valueOutputAvailable := false
		valueOutputChecked := false
		valueUnavailableReason := ""
		valueOutputPoints := 0

		summary.LocationBlocks++
		summary.LocationBlocksByType["detached-chunk-meta"]++
		summary.BaselineDecodeBlocks++
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
			summary.DecodeBlocksByType["detached-chunk-meta"]++
			if dataValidation != nil && dataValidation.Checked {
				valueOutputChecked = true
				valueOutputAvailable = dataValidation.chunkDataAvailable(chunk)
				if !valueOutputAvailable {
					valueUnavailableReason = "segment_overlap_data_range_unavailable"
				}
			}
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
		appendTSSPDetachedChunkDecodeSample(summary, chunk, minTime, maxTime, segmentCount, outputSegments, baselineBytes,
			baselineReadAtCalls, optimizedReadAtCalls, readAtRanges, valueOutputChecked, valueOutputAvailable, valueOutputPoints, valueUnavailableReason, options.BlockSampleLimit)
	}

	if dataProbe != nil {
		summary.DataBlockProbeBlocks = dataProbe.BlocksChecked
		summary.DataBlockProbeBytes = dataProbe.BytesRead
		summary.DataBlockProbeFailures = dataProbe.Failures()
		summary.DataBlockProbeCRCMismatches = dataProbe.CRCMismatches
		summary.DataBlockProbeValueBlocks = dataProbe.ValueBlocks
		summary.DataBlockProbeValueUnknowns = dataProbe.ValueUnknowns
		summary.DataBlockProbeNullValues = dataProbe.NullValues
		summary.CursorOutputSamples = append(summary.CursorOutputSamples, dataProbe.valueSamples...)
	}
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	summary.SavedReadSegments = summary.BaselineReadSegments - summary.OptimizedReadSegments
	summary.BaselineCursorReadCalls = tsspDetachedChunkMetaBatchCount(len(selectedMetas))
	summary.OptimizedCursorReadCalls = tsspDetachedChunkMetaBatchCount(len(overlapMetas))
	summary.SavedReadAtCalls = summary.BaselineReadAtCalls - summary.OptimizedReadAtCalls
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	populateTSSPDetachedChunkMetaBatches(summary, selectedMetas, overlapMetas, options)
	summary.Recommendations = tsspDetachedChunkDecodeRecommendations(summary)
	return summary
}

func appendTSSPDetachedChunkDecodeSample(summary *DecodePathSummary, chunk tsspChunkMeta, minTime, maxTime int64, segmentCount, outputSegments int, sizeBytes uint32,
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
		Key:                   fmt.Sprintf("meta-index-id:%d", chunk.SID),
		MetaIndexID:           chunk.SID,
		MinTime:               minTime,
		MaxTime:               maxTime,
		Type:                  "detached-chunk-meta",
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

func buildTSSPDetachedMetaIndexDecodePathSummary(metaIndexes []tsspMetaIndex, options Options) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                 tsspCursorMode("tssp-detached-meta-index", options),
		QueryRange:           options.QueryRange,
		CursorSeekTime:       tsspCursorSeekTime(options),
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	idSet := queryMetaIndexIDSet(options.QueryMetaIndexIDs)
	if len(idSet) > 0 {
		summary.QueryMetaIndexIDs = append([]uint64(nil), options.QueryMetaIndexIDs...)
		summary.KeyFilterApplied = true
	}
	populateTSSPDecodeMetaIndexMatches(summary, metaIndexes)

	selectedMetas := []tsspMetaIndex{}
	overlapMetas := []tsspMetaIndex{}
	for _, meta := range tsspMetaIndexesForCursor(metaIndexes, options.CursorDescending) {
		if !tsspQueryMetaIndexSelected(meta.ID, idSet) {
			summary.SkippedByKeyBlocks++
			continue
		}
		selectedMetas = append(selectedMetas, meta)

		summary.LocationBlocks++
		summary.LocationBlocksByType["detached-meta-index"]++
		summary.BaselineDecodeBlocks++
		summary.BaselineDecodeBytes += int64(meta.Size)
		summary.BaselineReadAtCalls++

		overlaps := options.QueryRange.Overlaps(meta.MinTime, meta.MaxTime)
		if overlaps {
			overlapMetas = append(overlapMetas, meta)
			if summary.IteratorCostFiles == 0 {
				summary.IteratorCostFiles = 1
			}
			summary.IteratorCostBlocks++
			summary.IteratorCostBytes += int64(meta.Size)
			summary.OptimizedDecodeBlocks++
			summary.FilteredDecodeBlocks++
			summary.OptimizedDecodeBytes += int64(meta.Size)
			summary.OptimizedReadAtCalls++
			summary.DecodeBlocksByType["detached-meta-index"]++
		} else if meta.MaxTime < options.QueryRange.Min {
			summary.SkippedBeforeSeekBlocks++
		} else {
			summary.SkippedAfterRangeBlocks++
		}
		appendTSSPDetachedMetaIndexDecodeSample(summary, meta, overlaps, options.BlockSampleLimit)
	}

	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedReadAtCalls = summary.BaselineReadAtCalls - summary.OptimizedReadAtCalls
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	populateTSSPDetachedChunkMetaBatches(summary, selectedMetas, overlapMetas, options)
	summary.Recommendations = tsspDetachedMetaIndexRecommendations(summary)
	return summary
}

func populateTSSPDetachedChunkMetaBatches(summary *DecodePathSummary, selected, overlapping []tsspMetaIndex, options Options) {
	summary.BaselineCursorReadCalls = tsspDetachedChunkMetaBatchCount(len(selected))
	summary.OptimizedCursorReadCalls = tsspDetachedChunkMetaBatchCount(len(overlapping))
	summary.CursorWindowCount = summary.BaselineCursorReadCalls
	if options.BlockSampleLimit <= 0 {
		return
	}
	overlapIDs := map[uint64]struct{}{}
	for _, meta := range overlapping {
		overlapIDs[meta.ID] = struct{}{}
	}
	for start := 0; start < len(selected) && len(summary.CursorWindows) < options.BlockSampleLimit; start += tsspDetachedChunkMetaReadNum {
		end := start + tsspDetachedChunkMetaReadNum
		if end > len(selected) {
			end = len(selected)
		}
		batch := selected[start:end]
		decoded := 0
		minTime, maxTime := batch[0].MinTime, batch[0].MaxTime
		for _, meta := range batch {
			if meta.MinTime < minTime {
				minTime = meta.MinTime
			}
			if meta.MaxTime > maxTime {
				maxTime = meta.MaxTime
			}
			if _, ok := overlapIDs[meta.ID]; ok {
				decoded++
			}
		}
		reason := "detached_chunk_meta_batch_overlap"
		if decoded == 0 {
			reason = "outside_query_range"
		} else if decoded < len(batch) {
			reason = "detached_chunk_meta_batch_filtered"
		}
		summary.CursorWindows = append(summary.CursorWindows, DecodePathCursorWindow{
			MinTime:         minTime,
			MaxTime:         maxTime,
			LocationBlocks:  len(batch),
			DecodedBlocks:   decoded,
			SavedBlocks:     len(batch) - decoded,
			Reason:          reason,
			FirstBlockIndex: start,
		})
	}
}

func tsspDetachedChunkMetaBatchCount(records int) int {
	if records <= 0 {
		return 0
	}
	return (records + tsspDetachedChunkMetaReadNum - 1) / tsspDetachedChunkMetaReadNum
}

func appendTSSPDetachedMetaIndexDecodeSample(summary *DecodePathSummary, meta tsspMetaIndex, overlaps bool, sampleLimit int) {
	if len(summary.Samples) >= sampleLimit {
		return
	}
	reason := "outside_query_range"
	optimizedReadAtCalls := 0
	readAtRanges := []DecodePathReadAtRange(nil)
	if overlaps {
		reason = "overlaps_query_range"
		optimizedReadAtCalls = 1
		readAtRanges = append(readAtRanges, DecodePathReadAtRange{
			MinTime:   meta.MinTime,
			MaxTime:   meta.MaxTime,
			Offset:    meta.Offset,
			SizeBytes: meta.Size,
		})
	}
	summary.Samples = append(summary.Samples, DecodePathBlockDecision{
		MetaIndexID:           meta.ID,
		MinTime:               meta.MinTime,
		MaxTime:               meta.MaxTime,
		Type:                  "detached-meta-index",
		SizeBytes:             meta.Size,
		BaselineReadAtCalls:   1,
		OptimizedReadAtCalls:  optimizedReadAtCalls,
		LocationCandidate:     true,
		Decoded:               overlaps,
		Reason:                reason,
		OptimizedReadAtRanges: readAtRanges,
	})
}

func populateTSSPDecodeMetaIndexMatches(summary *DecodePathSummary, metaIndexes []tsspMetaIndex) {
	if len(summary.QueryMetaIndexIDs) == 0 {
		return
	}
	seen := map[uint64]struct{}{}
	for _, meta := range metaIndexes {
		seen[meta.ID] = struct{}{}
	}
	for _, id := range summary.QueryMetaIndexIDs {
		if _, ok := seen[id]; ok {
			summary.MatchedMetaIndexIDs = append(summary.MatchedMetaIndexIDs, id)
		} else {
			summary.MissingMetaIndexIDs = append(summary.MissingMetaIndexIDs, id)
		}
	}
}

func queryMetaIndexIDSet(ids []uint64) map[uint64]struct{} {
	return querySeriesIDSet(ids)
}

func tsspQueryMetaIndexSelected(id uint64, idSet map[uint64]struct{}) bool {
	if len(idSet) == 0 {
		return true
	}
	_, ok := idSet[id]
	return ok
}

func tsspDetachedMetaIndexRecommendations(summary *DecodePathSummary) []string {
	if summary == nil {
		return nil
	}
	recommendations := make([]string, 0, 4)
	if summary.SkippedByKeyBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"skip %d detached TSSP meta-index record(s) outside the requested ID set",
			summary.SkippedByKeyBlocks,
		))
	}
	if summary.SavedDecodeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"filter %d detached TSSP meta-index record(s) outside the query range before loading detached chunk metadata",
			summary.SavedDecodeBlocks,
		))
	}
	if summary.SavedReadAtCalls > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"avoid %d detached chunk metadata ReadAt call(s) after meta-index filtering",
			summary.SavedReadAtCalls,
		))
	}
	if summary.BaselineCursorReadCalls > summary.OptimizedCursorReadCalls {
		recommendations = append(recommendations, fmt.Sprintf(
			"reduce detached chunk metadata batch read(s) from %d to %d after query filtering",
			summary.BaselineCursorReadCalls,
			summary.OptimizedCursorReadCalls,
		))
	}
	return recommendations
}

func tsspDetachedChunkDecodeRecommendations(summary *DecodePathSummary) []string {
	if summary == nil {
		return nil
	}
	recommendations := make([]string, 0, 6)
	if len(summary.MissingMetaIndexIDs) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"%d query meta-index id(s) were not found in analyzed detached TSSP metadata",
			len(summary.MissingMetaIndexIDs),
		))
	}
	if summary.SkippedByKeyBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"skip %d detached TSSP chunk(s) outside the requested meta-index ID set",
			summary.SkippedByKeyBlocks,
		))
	}
	if summary.SavedDecodeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"filter %d detached TSSP chunk candidate(s) outside the query range before data ReadAt",
			summary.SavedDecodeBlocks,
		))
	}
	if summary.SavedReadSegments > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"read %d overlapping detached TSSP segment(s) instead of %d candidate segment(s)",
			summary.OptimizedReadSegments,
			summary.BaselineReadSegments,
		))
	}
	if summary.SavedReadAtCalls > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"issue %d detached TSSP data ReadAt call(s) instead of %d candidate column-segment ReadAt call(s)",
			summary.OptimizedReadAtCalls,
			summary.BaselineReadAtCalls,
		))
	}
	if summary.DataBlockProbeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"verified %d detached TSSP data block(s) with CRC before value decode",
			summary.DataBlockProbeBlocks-summary.DataBlockProbeFailures,
		))
	}
	if summary.OptimizedValueOutputPoints > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"materialized %d detached TSSP output point(s) from data block row counts",
			summary.OptimizedValueOutputPoints,
		))
	}
	if len(summary.CursorOutputSamples) > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"sampled %d detached TSSP value output(s) from data blocks",
			len(summary.CursorOutputSamples),
		))
	}
	if summary.DataBlockProbeFailures > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"detached TSSP data block probe found %d invalid block(s), including %d crc mismatch(es)",
			summary.DataBlockProbeFailures,
			summary.DataBlockProbeCRCMismatches,
		))
	}
	if summary.BaselineCursorReadCalls > summary.OptimizedCursorReadCalls {
		recommendations = append(recommendations, fmt.Sprintf(
			"reduce detached chunk metadata batch read(s) from %d to %d after query filtering",
			summary.BaselineCursorReadCalls,
			summary.OptimizedCursorReadCalls,
		))
	}
	if len(recommendations) == 0 && summary.FilteredDecodeBlocks > 0 {
		recommendations = append(recommendations, "query range maps directly to detached TSSP chunk segments")
	}
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "query range has no detached TSSP chunk segment candidates")
	}
	return recommendations
}

func isTSSPDetachedMetaIndexPath(path string) bool {
	return strings.EqualFold(filepath.Base(path), tsspDetachedMetaIndexFileName)
}
