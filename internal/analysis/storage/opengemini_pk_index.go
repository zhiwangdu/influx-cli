package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	opengeminiPKIndexMinMetaSize = 4 + 4 + 4 + 1
	opengeminiPKIndexLayout      = "opengemini-attached-primary-index"
)

type opengeminiPKIndexInfo struct {
	Version              uint32
	MetaSizeBytes        int64
	RowCount             uint64
	TCLocation           int
	Schema               []opengeminiPKColumn
	ColumnOffsets        []uint32
	DataSectionOffset    int64
	DataSizeBytes        int64
	ColumnOutOfBounds    int
	ColumnUnordered      int
	ColumnOffsetCount    int
	PublicInfoValidBytes int64
	Notices              []string
}

func analyzeOpenGeminiPKIndex(path string, info os.FileInfo, options Options) (FileReport, error) {
	if info.IsDir() {
		return FileReport{}, fmt.Errorf("opengemini-pk-index format requires an attached primary key .idx file")
	}
	if strings.EqualFold(filepath.Base(path), opengeminiPKMetaFileName) {
		return FileReport{}, fmt.Errorf("primary.meta uses opengemini-pk-meta format, not opengemini-pk-index")
	}
	if strings.EqualFold(filepath.Base(path), opengeminiPKDataFileName) {
		return FileReport{}, fmt.Errorf("primary.idx is detached primary-key data; analyze primary.meta with opengemini-pk-meta")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return FileReport{}, err
	}
	index, err := parseOpenGeminiPKIndex(data)
	if err != nil {
		return FileReport{}, err
	}

	columns := openGeminiPKIndexColumnReports(index, int64(len(data)))
	columnSamples, minKey, maxKey := openGeminiPKColumnSamples(index.Schema, options.KeySampleLimit)
	blockSamples := openGeminiPKIndexBlockSamples(index, int64(len(data)), options.BlockSampleLimit)
	blocksByType := map[string]int{
		"primary-key-attached-meta": 1,
		"primary-key-column-data":   len(index.Schema),
		"primary-key-schema-column": len(index.Schema),
	}
	if index.ColumnOutOfBounds > 0 {
		blocksByType["primary-key-column-out-of-bounds"] = index.ColumnOutOfBounds
	}
	if index.ColumnUnordered > 0 {
		blocksByType["primary-key-column-unordered"] = index.ColumnUnordered
	}

	extra := map[string]string{
		"layout":                      opengeminiPKIndexLayout,
		"magic":                       opengeminiPKMagic,
		"version":                     fmt.Sprint(index.Version),
		"meta_size":                   fmt.Sprint(index.MetaSizeBytes),
		"schema_column_count":         fmt.Sprint(len(index.Schema)),
		"row_count":                   fmt.Sprint(index.RowCount),
		"time_cluster_location":       fmt.Sprint(index.TCLocation),
		"data_inline":                 "true",
		"data_section_offset":         fmt.Sprint(index.DataSectionOffset),
		"data_size_bytes":             fmt.Sprint(index.DataSizeBytes),
		"column_offset_count":         fmt.Sprint(index.ColumnOffsetCount),
		"column_out_of_bounds_blocks": fmt.Sprint(index.ColumnOutOfBounds),
		"column_unordered_blocks":     fmt.Sprint(index.ColumnUnordered),
		"local_only":                  "true",
	}

	report := FileReport{
		Path:         path,
		Format:       FormatOpenGeminiPKIndex,
		SizeBytes:    info.Size(),
		ModTime:      info.ModTime(),
		MinKey:       minKey,
		MaxKey:       maxKey,
		KeyCount:     len(index.Schema),
		KeySamples:   columnSamples,
		BlockCount:   len(index.Schema),
		BlocksByType: blocksByType,
		Blocks:       blockSamples,
		PrimaryKey: &PrimaryKeySummary{
			Type:                    opengeminiPKIndexLayout,
			Version:                 index.Version,
			Schema:                  columns,
			ColumnCount:             len(columns),
			TimeClusterLocation:     index.TCLocation,
			RowCount:                index.RowCount,
			DataSizeBytes:           index.DataSizeBytes,
			DataInline:              true,
			ColumnOutOfBoundsBlocks: index.ColumnOutOfBounds,
			ColumnUnorderedBlocks:   index.ColumnUnordered,
			PublicInfoSizeBytes:     index.MetaSizeBytes,
			ValidMetaBytes:          index.PublicInfoValidBytes,
			MetaRecordSizeBytes:     int(index.MetaSizeBytes),
			ColumnOffsetCount:       index.ColumnOffsetCount,
		},
		Extra:   extra,
		Notices: index.Notices,
	}
	return report, nil
}

func parseOpenGeminiPKIndex(data []byte) (opengeminiPKIndexInfo, error) {
	var index opengeminiPKIndexInfo
	if len(data) < opengeminiPKHeaderSize+4 {
		return index, fmt.Errorf("file too small for openGemini primary key index header")
	}
	if string(data[:len(opengeminiPKMagic)]) != opengeminiPKMagic {
		return index, fmt.Errorf("invalid openGemini primary key index magic %q", string(data[:len(opengeminiPKMagic)]))
	}
	index.Version = binary.BigEndian.Uint32(data[len(opengeminiPKMagic):opengeminiPKHeaderSize])
	metaSize := int(binary.BigEndian.Uint32(data[opengeminiPKHeaderSize : opengeminiPKHeaderSize+4]))
	index.MetaSizeBytes = int64(metaSize)
	if metaSize < opengeminiPKIndexMinMetaSize {
		return index, fmt.Errorf("invalid openGemini primary key index meta size %d", metaSize)
	}
	metaEnd := opengeminiPKHeaderSize + metaSize
	if metaEnd > len(data) {
		return index, fmt.Errorf("primary key index meta size %d exceeds file size %d", metaSize, len(data))
	}
	meta := data[opengeminiPKHeaderSize:metaEnd]
	schemaCount := int(binary.BigEndian.Uint32(meta[4:8]))
	rowCount := binary.BigEndian.Uint32(meta[8:12])
	index.RowCount = uint64(rowCount)
	index.TCLocation = int(int8(meta[12]))
	schemaBytes := schemaCount * 4
	namesOffset := 13
	typesOffset := namesOffset + schemaBytes
	dataOffsetsOffset := typesOffset + schemaBytes
	fieldsOffset := dataOffsetsOffset + schemaBytes
	if fieldsOffset > len(meta) {
		return index, fmt.Errorf("primary key index meta too small for %d schema entries", schemaCount)
	}

	nameLengths := make([]uint32, schemaCount)
	fieldTypes := make([]uint32, schemaCount)
	index.ColumnOffsets = make([]uint32, schemaCount)
	for i := 0; i < schemaCount; i++ {
		nameLengths[i] = binary.BigEndian.Uint32(meta[namesOffset+i*4 : namesOffset+(i+1)*4])
		fieldTypes[i] = binary.BigEndian.Uint32(meta[typesOffset+i*4 : typesOffset+(i+1)*4])
		index.ColumnOffsets[i] = binary.BigEndian.Uint32(meta[dataOffsetsOffset+i*4 : dataOffsetsOffset+(i+1)*4])
	}

	names := meta[fieldsOffset:]
	index.Schema = make([]opengeminiPKColumn, 0, schemaCount)
	for i, nameLength := range nameLengths {
		if uint64(nameLength) > uint64(len(names)) {
			return index, fmt.Errorf("primary key index field name %d length %d exceeds remaining bytes %d", i, nameLength, len(names))
		}
		name := string(names[:nameLength])
		names = names[nameLength:]
		index.Schema = append(index.Schema, opengeminiPKColumn{Name: name, Type: fieldTypes[i]})
	}
	if len(names) > 0 {
		return index, fmt.Errorf("primary key index meta has %d unused field-name byte(s)", len(names))
	}

	index.DataSectionOffset = int64(metaEnd)
	index.DataSizeBytes = int64(len(data) - metaEnd)
	index.PublicInfoValidBytes = int64(metaEnd)
	validateOpenGeminiPKIndexColumnOffsets(&index, int64(len(data)))
	return index, nil
}

func validateOpenGeminiPKIndexColumnOffsets(index *opengeminiPKIndexInfo, fileSize int64) {
	index.ColumnOffsetCount = len(index.ColumnOffsets)
	for i, offset := range index.ColumnOffsets {
		if _, outOfBounds := openGeminiPKIndexColumnDataSize(index.ColumnOffsets, i, index.DataSectionOffset, fileSize); outOfBounds {
			index.ColumnOutOfBounds++
		}
		if i > 0 && offset < index.ColumnOffsets[i-1] {
			index.ColumnUnordered++
		}
	}
	if index.ColumnOutOfBounds > 0 {
		index.Notices = append(index.Notices, fmt.Sprintf("primary key index has %d column data offset(s) outside file bounds", index.ColumnOutOfBounds))
	}
	if index.ColumnUnordered > 0 {
		index.Notices = append(index.Notices, fmt.Sprintf("primary key index has %d unordered column data offset(s)", index.ColumnUnordered))
	}
}

func openGeminiPKIndexColumnReports(index opengeminiPKIndexInfo, fileSize int64) []PrimaryKeyColumnReport {
	reports := make([]PrimaryKeyColumnReport, 0, len(index.Schema))
	for i, column := range index.Schema {
		offset := int64(0)
		if i < len(index.ColumnOffsets) {
			offset = int64(index.ColumnOffsets[i])
		}
		size, outOfBounds := openGeminiPKIndexColumnDataSize(index.ColumnOffsets, i, index.DataSectionOffset, fileSize)
		reports = append(reports, PrimaryKeyColumnReport{
			Name:            column.Name,
			Type:            opengeminiPKFieldTypeName(column.Type),
			DataOffset:      offset,
			DataSizeBytes:   size,
			DataOutOfBounds: outOfBounds,
		})
	}
	return reports
}

func openGeminiPKIndexBlockSamples(index opengeminiPKIndexInfo, fileSize int64, limit int) []BlockReport {
	if limit <= 0 || len(index.Schema) == 0 {
		return nil
	}
	count := minInt(limit, len(index.Schema))
	reports := make([]BlockReport, 0, count)
	for i := 0; i < count; i++ {
		column := index.Schema[i]
		offset := int64(0)
		if i < len(index.ColumnOffsets) {
			offset = int64(index.ColumnOffsets[i])
		}
		size, _ := openGeminiPKIndexColumnDataSize(index.ColumnOffsets, i, index.DataSectionOffset, fileSize)
		reports = append(reports, BlockReport{
			Key:         column.Name,
			Type:        "primary-key-column-data",
			Offset:      offset,
			SizeBytes:   clampUint32(size),
			ColumnCount: 1,
			ValueCount:  int(minUint64(uint64(^uint(0)>>1), index.RowCount)),
		})
	}
	return reports
}

func openGeminiPKIndexColumnDataSize(offsets []uint32, index int, dataSectionOffset, fileSize int64) (int64, bool) {
	if index < 0 || index >= len(offsets) {
		return 0, true
	}
	offset := int64(offsets[index])
	if offset < dataSectionOffset || offset+opengeminiPKCRCSize > fileSize {
		return 0, true
	}
	next := fileSize
	if index < len(offsets)-1 {
		next = int64(offsets[index+1])
	}
	if next < offset || next > fileSize {
		return 0, true
	}
	return next - offset, false
}

func clampUint32(value int64) uint32 {
	if value <= 0 {
		return 0
	}
	if value > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(value)
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
