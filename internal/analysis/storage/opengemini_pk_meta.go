package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	opengeminiPKMetaFileName      = "primary.meta"
	opengeminiPKDataFileName      = "primary.idx"
	opengeminiPKMagic             = "COLX"
	opengeminiPKHeaderSize        = 8
	opengeminiPKPublicSizeOffset  = opengeminiPKHeaderSize
	opengeminiPKPublicSizeLength  = 12
	opengeminiPKMinPublicInfoSize = opengeminiPKHeaderSize + 4 + 4 + 1
	opengeminiPKMetaPrefixSize    = 8 + 8 + 4 + 4
	opengeminiPKCRCSize           = 4
)

type opengeminiPKMetaInfo struct {
	Version             uint32
	PublicInfoSizeBytes int64
	TCLocation          int
	Schema              []opengeminiPKColumn
}

type opengeminiPKColumn struct {
	Name string
	Type uint32
}

type opengeminiPKMetaBlock struct {
	MetaOffset         int64
	StartBlockID       uint64
	EndBlockID         uint64
	DataOffset         uint32
	DataLength         uint32
	ColumnOffsets      []uint32
	CRCWritten         uint32
	CRCComputed        uint32
	CRCOK              bool
	DataOutOfBounds    bool
	ColumnOutOfBounds  bool
	ColumnUnordered    bool
	InvalidBlockIDSpan bool
}

type opengeminiPKMetaAnalysis struct {
	Info                    opengeminiPKMetaInfo
	Blocks                  []opengeminiPKMetaBlock
	MetaRecordSizeBytes     int
	ValidMetaBytes          int64
	TrailingMetaBytes       int64
	RowCount                uint64
	DataSizeBytes           int64
	ColumnOffsetCount       int
	MinBlockID              uint64
	MaxBlockID              uint64
	BlockIDRangeSet         bool
	CRCMismatches           int
	DataFilePresent         bool
	DataFilePath            string
	DataFileSizeBytes       int64
	DataOutOfBoundsBlocks   int
	ColumnOutOfBoundsBlocks int
	ColumnUnorderedBlocks   int
	InvalidBlockIDSpans     int
	Notices                 []string
}

func analyzeOpenGeminiPKMeta(path string, info os.FileInfo, options Options) (FileReport, error) {
	if info.IsDir() {
		return FileReport{}, fmt.Errorf("opengemini-pk-meta format requires a primary.meta file")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return FileReport{}, err
	}
	analysis, err := parseOpenGeminiPKMeta(data)
	if err != nil {
		return FileReport{}, err
	}
	validateOpenGeminiPKDataFile(filepath.Dir(path), &analysis)

	columns := openGeminiPKColumnReports(analysis.Info.Schema)
	columnSamples, minKey, maxKey := openGeminiPKColumnSamples(analysis.Info.Schema, options.KeySampleLimit)
	blockSamples := openGeminiPKBlockSamples(analysis.Blocks, options.BlockSampleLimit)
	blocksByType := map[string]int{
		"primary-key-meta-block":    len(analysis.Blocks),
		"primary-key-schema-column": len(analysis.Info.Schema),
	}
	if analysis.CRCMismatches > 0 {
		blocksByType["primary-key-crc-mismatch"] = analysis.CRCMismatches
	}
	if analysis.DataOutOfBoundsBlocks > 0 {
		blocksByType["primary-key-data-out-of-bounds"] = analysis.DataOutOfBoundsBlocks
	}
	if analysis.ColumnOutOfBoundsBlocks > 0 {
		blocksByType["primary-key-column-out-of-bounds"] = analysis.ColumnOutOfBoundsBlocks
	}
	if analysis.ColumnUnorderedBlocks > 0 {
		blocksByType["primary-key-column-unordered"] = analysis.ColumnUnorderedBlocks
	}
	if analysis.InvalidBlockIDSpans > 0 {
		blocksByType["primary-key-invalid-block-id-span"] = analysis.InvalidBlockIDSpans
	}

	extra := map[string]string{
		"layout":                       "opengemini-detached-primary-meta",
		"sidecar":                      opengeminiPKMetaFileName,
		"magic":                        opengeminiPKMagic,
		"version":                      fmt.Sprint(analysis.Info.Version),
		"public_info_size":             fmt.Sprint(analysis.Info.PublicInfoSizeBytes),
		"schema_column_count":          fmt.Sprint(len(analysis.Info.Schema)),
		"time_cluster_location":        fmt.Sprint(analysis.Info.TCLocation),
		"meta_record_size":             fmt.Sprint(analysis.MetaRecordSizeBytes),
		"meta_block_count":             fmt.Sprint(len(analysis.Blocks)),
		"row_count":                    fmt.Sprint(analysis.RowCount),
		"data_size_bytes":              fmt.Sprint(analysis.DataSizeBytes),
		"column_offset_count":          fmt.Sprint(analysis.ColumnOffsetCount),
		"valid_meta_bytes":             fmt.Sprint(analysis.ValidMetaBytes),
		"trailing_meta_bytes":          fmt.Sprint(analysis.TrailingMetaBytes),
		"crc_algorithm":                "ieee",
		"crc_mismatch_count":           fmt.Sprint(analysis.CRCMismatches),
		"data_file_present":            fmt.Sprint(analysis.DataFilePresent),
		"data_out_of_bounds_blocks":    fmt.Sprint(analysis.DataOutOfBoundsBlocks),
		"column_out_of_bounds_blocks":  fmt.Sprint(analysis.ColumnOutOfBoundsBlocks),
		"column_unordered_blocks":      fmt.Sprint(analysis.ColumnUnorderedBlocks),
		"invalid_block_id_span_blocks": fmt.Sprint(analysis.InvalidBlockIDSpans),
		"block_id_range_set":           fmt.Sprint(analysis.BlockIDRangeSet),
		"local_only":                   "true",
	}
	if analysis.DataFilePath != "" {
		extra["data_file"] = analysis.DataFilePath
		extra["data_file_size"] = fmt.Sprint(analysis.DataFileSizeBytes)
	}
	if analysis.BlockIDRangeSet {
		extra["min_block_id"] = fmt.Sprint(analysis.MinBlockID)
		extra["max_block_id"] = fmt.Sprint(analysis.MaxBlockID)
	}

	report := FileReport{
		Path:         path,
		Format:       FormatOpenGeminiPKMeta,
		SizeBytes:    info.Size(),
		ModTime:      info.ModTime(),
		MinKey:       minKey,
		MaxKey:       maxKey,
		KeyCount:     len(analysis.Info.Schema),
		KeySamples:   columnSamples,
		BlockCount:   len(analysis.Blocks),
		BlocksByType: blocksByType,
		Blocks:       blockSamples,
		PrimaryKey: &PrimaryKeySummary{
			Type:                    "opengemini-detached-primary-meta",
			Version:                 analysis.Info.Version,
			Schema:                  columns,
			ColumnCount:             len(columns),
			TimeClusterLocation:     analysis.Info.TCLocation,
			MetaBlockCount:          len(analysis.Blocks),
			RowCount:                analysis.RowCount,
			DataSizeBytes:           analysis.DataSizeBytes,
			DataFilePresent:         analysis.DataFilePresent,
			DataFileSizeBytes:       analysis.DataFileSizeBytes,
			CRCMismatches:           analysis.CRCMismatches,
			DataOutOfBoundsBlocks:   analysis.DataOutOfBoundsBlocks,
			ColumnOutOfBoundsBlocks: analysis.ColumnOutOfBoundsBlocks,
			ColumnUnorderedBlocks:   analysis.ColumnUnorderedBlocks,
			BlockIDRangeSet:         analysis.BlockIDRangeSet,
			MinBlockID:              analysis.MinBlockID,
			MaxBlockID:              analysis.MaxBlockID,
			PublicInfoSizeBytes:     analysis.Info.PublicInfoSizeBytes,
			ValidMetaBytes:          analysis.ValidMetaBytes,
			TrailingMetaBytes:       analysis.TrailingMetaBytes,
			MetaRecordSizeBytes:     analysis.MetaRecordSizeBytes,
			ColumnOffsetCount:       analysis.ColumnOffsetCount,
		},
		Extra:   extra,
		Notices: analysis.Notices,
	}
	return report, nil
}

func parseOpenGeminiPKMeta(data []byte) (opengeminiPKMetaAnalysis, error) {
	var analysis opengeminiPKMetaAnalysis
	if len(data) < opengeminiPKPublicSizeLength {
		return analysis, fmt.Errorf("file too small for openGemini primary.meta public header")
	}
	info, err := parseOpenGeminiPKMetaInfo(data)
	if err != nil {
		return analysis, err
	}
	analysis.Info = info
	recordSize := opengeminiPKCRCSize + opengeminiPKMetaPrefixSize + len(info.Schema)*4
	analysis.MetaRecordSizeBytes = recordSize
	if recordSize <= opengeminiPKCRCSize+opengeminiPKMetaPrefixSize {
		analysis.Notices = append(analysis.Notices, "primary key meta has no schema columns")
	}

	offset := int(info.PublicInfoSizeBytes)
	if offset > len(data) {
		return analysis, fmt.Errorf("primary.meta public info size %d exceeds file size %d", offset, len(data))
	}
	fullRecords := (len(data) - offset) / recordSize
	analysis.TrailingMetaBytes = int64((len(data) - offset) % recordSize)
	if analysis.TrailingMetaBytes > 0 {
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("primary.meta has %d trailing byte(s) after full meta records", analysis.TrailingMetaBytes))
	}
	analysis.ValidMetaBytes = int64(offset + fullRecords*recordSize)
	analysis.Blocks = make([]opengeminiPKMetaBlock, 0, fullRecords)
	for i := 0; i < fullRecords; i++ {
		recordOffset := offset + i*recordSize
		block, err := parseOpenGeminiPKMetaBlock(data[recordOffset:recordOffset+recordSize], int64(recordOffset), len(info.Schema))
		if err != nil {
			return analysis, err
		}
		observeOpenGeminiPKMetaBlock(&analysis, &block)
		analysis.Blocks = append(analysis.Blocks, block)
	}
	return analysis, nil
}

func parseOpenGeminiPKMetaInfo(data []byte) (opengeminiPKMetaInfo, error) {
	var info opengeminiPKMetaInfo
	if !bytes.Equal(data[:len(opengeminiPKMagic)], []byte(opengeminiPKMagic)) {
		return info, fmt.Errorf("invalid openGemini primary.meta magic %q", string(data[:len(opengeminiPKMagic)]))
	}
	info.Version = binary.BigEndian.Uint32(data[len(opengeminiPKMagic):opengeminiPKHeaderSize])
	publicInfoSize := int64(binary.BigEndian.Uint32(data[opengeminiPKPublicSizeOffset:opengeminiPKPublicSizeLength]))
	info.PublicInfoSizeBytes = publicInfoSize
	if publicInfoSize < opengeminiPKMinPublicInfoSize {
		return info, fmt.Errorf("invalid primary.meta public info size %d", publicInfoSize)
	}
	if publicInfoSize > int64(len(data)) {
		return info, fmt.Errorf("primary.meta public info size %d exceeds file size %d", publicInfoSize, len(data))
	}
	public := data[:publicInfoSize]
	schemaCount := int(binary.BigEndian.Uint32(public[opengeminiPKPublicSizeLength : opengeminiPKPublicSizeLength+4]))
	info.TCLocation = int(int8(public[opengeminiPKPublicSizeLength+4]))
	if schemaCount < 0 {
		return info, fmt.Errorf("invalid primary.meta schema count %d", schemaCount)
	}
	schemaBytes := schemaCount * 4
	namesOffset := opengeminiPKPublicSizeLength + 4 + 1
	typesOffset := namesOffset + schemaBytes
	fieldsOffset := typesOffset + schemaBytes
	if fieldsOffset > len(public) {
		return info, fmt.Errorf("primary.meta public info too small for %d schema entries", schemaCount)
	}
	nameLengths := make([]uint32, schemaCount)
	fieldTypes := make([]uint32, schemaCount)
	for i := 0; i < schemaCount; i++ {
		nameLengths[i] = binary.BigEndian.Uint32(public[namesOffset+i*4 : namesOffset+(i+1)*4])
		fieldTypes[i] = binary.BigEndian.Uint32(public[typesOffset+i*4 : typesOffset+(i+1)*4])
	}
	names := public[fieldsOffset:]
	info.Schema = make([]opengeminiPKColumn, 0, schemaCount)
	for i, nameLength := range nameLengths {
		if uint64(nameLength) > uint64(len(names)) {
			return info, fmt.Errorf("primary.meta field name %d length %d exceeds remaining bytes %d", i, nameLength, len(names))
		}
		name := string(names[:nameLength])
		names = names[nameLength:]
		info.Schema = append(info.Schema, opengeminiPKColumn{Name: name, Type: fieldTypes[i]})
	}
	if len(names) > 0 {
		return info, fmt.Errorf("primary.meta public info has %d unused field-name byte(s)", len(names))
	}
	return info, nil
}

func parseOpenGeminiPKMetaBlock(data []byte, metaOffset int64, columnCount int) (opengeminiPKMetaBlock, error) {
	var block opengeminiPKMetaBlock
	recordSize := opengeminiPKCRCSize + opengeminiPKMetaPrefixSize + columnCount*4
	if len(data) < recordSize {
		return block, fmt.Errorf("primary.meta block at offset=%d too small: got %d, want %d", metaOffset, len(data), recordSize)
	}
	block.MetaOffset = metaOffset
	block.CRCWritten = binary.BigEndian.Uint32(data[:opengeminiPKCRCSize])
	block.CRCComputed = crc32.ChecksumIEEE(data[opengeminiPKCRCSize:recordSize])
	block.CRCOK = block.CRCWritten == block.CRCComputed
	body := data[opengeminiPKCRCSize:]
	block.StartBlockID = binary.BigEndian.Uint64(body[:8])
	block.EndBlockID = binary.BigEndian.Uint64(body[8:16])
	block.DataOffset = binary.BigEndian.Uint32(body[16:20])
	block.DataLength = binary.BigEndian.Uint32(body[20:24])
	block.ColumnOffsets = make([]uint32, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		columnOffset := binary.BigEndian.Uint32(body[24+i*4 : 24+(i+1)*4])
		block.ColumnOffsets = append(block.ColumnOffsets, columnOffset)
		if uint64(columnOffset)+opengeminiPKCRCSize > uint64(block.DataLength) {
			block.ColumnOutOfBounds = true
		}
		if i > 0 && columnOffset < block.ColumnOffsets[i-1] {
			block.ColumnUnordered = true
		}
	}
	block.InvalidBlockIDSpan = block.EndBlockID < block.StartBlockID
	return block, nil
}

func observeOpenGeminiPKMetaBlock(analysis *opengeminiPKMetaAnalysis, block *opengeminiPKMetaBlock) {
	analysis.DataSizeBytes += int64(block.DataLength)
	analysis.ColumnOffsetCount += len(block.ColumnOffsets)
	if !block.CRCOK {
		analysis.CRCMismatches++
	}
	if block.ColumnOutOfBounds {
		analysis.ColumnOutOfBoundsBlocks++
	}
	if block.ColumnUnordered {
		analysis.ColumnUnorderedBlocks++
	}
	if block.InvalidBlockIDSpan {
		analysis.InvalidBlockIDSpans++
		return
	}
	if !analysis.BlockIDRangeSet {
		analysis.MinBlockID = block.StartBlockID
		analysis.MaxBlockID = block.EndBlockID
		analysis.BlockIDRangeSet = true
	} else {
		if block.StartBlockID < analysis.MinBlockID {
			analysis.MinBlockID = block.StartBlockID
		}
		if block.EndBlockID > analysis.MaxBlockID {
			analysis.MaxBlockID = block.EndBlockID
		}
	}
	analysis.RowCount += block.EndBlockID - block.StartBlockID + 1
}

func validateOpenGeminiPKDataFile(dir string, analysis *opengeminiPKMetaAnalysis) {
	path := filepath.Join(dir, opengeminiPKDataFileName)
	info, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			analysis.Notices = append(analysis.Notices, fmt.Sprintf("primary.idx validation unavailable: %v", err))
		}
		return
	}
	if info.IsDir() {
		analysis.Notices = append(analysis.Notices, "primary.idx validation unavailable: sibling path is a directory")
		return
	}
	analysis.DataFilePresent = true
	analysis.DataFilePath = path
	analysis.DataFileSizeBytes = info.Size()
	for i := range analysis.Blocks {
		block := &analysis.Blocks[i]
		if uint64(block.DataOffset)+uint64(block.DataLength) > uint64(info.Size()) {
			block.DataOutOfBounds = true
			analysis.DataOutOfBoundsBlocks++
		}
	}
	if analysis.DataOutOfBoundsBlocks > 0 {
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("primary.idx has %d primary-key data block range(s) outside file bounds", analysis.DataOutOfBoundsBlocks))
	}
}

func openGeminiPKColumnReports(schema []opengeminiPKColumn) []PrimaryKeyColumnReport {
	reports := make([]PrimaryKeyColumnReport, 0, len(schema))
	for _, column := range schema {
		reports = append(reports, PrimaryKeyColumnReport{
			Name: column.Name,
			Type: opengeminiPKFieldTypeName(column.Type),
		})
	}
	return reports
}

func openGeminiPKColumnSamples(schema []opengeminiPKColumn, limit int) ([]string, string, string) {
	if len(schema) == 0 {
		return nil, "", ""
	}
	names := make([]string, 0, len(schema))
	samples := make([]string, 0, len(schema))
	for _, column := range schema {
		names = append(names, column.Name)
		samples = append(samples, fmt.Sprintf("%s:%s", column.Name, opengeminiPKFieldTypeName(column.Type)))
	}
	sort.Strings(names)
	sort.Strings(samples)
	return sampleStrings(samples, limit), names[0], names[len(names)-1]
}

func openGeminiPKBlockSamples(blocks []opengeminiPKMetaBlock, limit int) []BlockReport {
	if limit <= 0 || len(blocks) == 0 {
		return nil
	}
	count := minInt(limit, len(blocks))
	reports := make([]BlockReport, 0, count)
	for i := 0; i < count; i++ {
		block := blocks[i]
		reports = append(reports, BlockReport{
			Key:         fmt.Sprintf("block-id:%d-%d", block.StartBlockID, block.EndBlockID),
			MetaIndexID: uint64(i),
			Type:        "primary-key-meta-block",
			Offset:      int64(block.DataOffset),
			SizeBytes:   block.DataLength,
			ColumnCount: len(block.ColumnOffsets),
			ValueCount:  openGeminiPKBlockValueCount(block),
		})
	}
	return reports
}

func openGeminiPKBlockValueCount(block opengeminiPKMetaBlock) int {
	if block.EndBlockID < block.StartBlockID {
		return 0
	}
	count := block.EndBlockID - block.StartBlockID + 1
	if count > uint64(^uint(0)>>1) {
		return 0
	}
	return int(count)
}

func opengeminiPKFieldTypeName(value uint32) string {
	switch value {
	case 0:
		return "unknown"
	case 1:
		return "integer"
	case 2:
		return "unsigned"
	case 3:
		return "float"
	case 4:
		return "string"
	case 5:
		return "boolean"
	case 6:
		return "tag"
	default:
		return "unknown(" + strconv.FormatUint(uint64(value), 10) + ")"
	}
}
