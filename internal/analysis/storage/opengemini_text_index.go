package storage

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	opengeminiTextIndexType        = "opengemini-text-index"
	opengeminiTextIndexLayout      = "attached-text-index"
	opengeminiTextIndexDataSuffix  = ".pos"
	opengeminiTextIndexHeadSuffix  = ".bh"
	opengeminiTextIndexPartSuffix  = ".ph"
	opengeminiTextIndexPartSize    = 104
	opengeminiTextIndexItemPrefix  = 8
	opengeminiTextIndexSegmentSize = 16
)

type opengeminiTextIndexPaths struct {
	Base           string
	Field          string
	InputComponent string
	DataPath       string
	HeadPath       string
	PartPath       string
}

type opengeminiTextIndexPartHeader struct {
	Index             int
	Offset            int64
	FirstItem         []byte
	LastItem          []byte
	Flag              uint16
	BlockHeaderCount  uint32
	BlockHeaderOffset uint64
	BlockHeaderSize   uint32
	SegmentRangeCount uint32
	SegmentRanges     []uint32
	HeaderOutOfBounds bool
}

type opengeminiTextIndexBlockHeader struct {
	PartIndex       int
	HeaderIndex     int
	HeadOffset      int64
	FirstItem       []byte
	LastItem        []byte
	MarshalType     uint8
	ItemsCount      uint32
	KeysOffset      uint64
	KeysUnpackSize  uint32
	KeysPackSize    uint32
	KeysSize        uint32
	PostOffset      uint64
	PostUnpackSize  uint32
	PostPackSize    uint32
	PostSize        uint32
	DataOutOfBounds bool
	InvalidOffsets  bool
	InvalidSizes    bool
}

type opengeminiTextIndexAnalysis struct {
	Paths                 opengeminiTextIndexPaths
	DataFilePresent       bool
	HeadFilePresent       bool
	DataSizeBytes         int64
	HeadSizeBytes         int64
	PartSizeBytes         int64
	SizeBytes             int64
	PartHeaders           []opengeminiTextIndexPartHeader
	BlockHeaders          []opengeminiTextIndexBlockHeader
	DeclaredBlockCount    int64
	ItemCount             int64
	KeysPayloadSizeBytes  int64
	PostPayloadSizeBytes  int64
	PayloadSizeBytes      int64
	PartTrailingBytes     int64
	HeaderOutOfBounds     int
	DataOutOfBounds       int
	InvalidOffsetBlocks   int
	InvalidSizeBlocks     int
	SegmentRangeOverflows int
	BlocksByType          map[string]int
	Notices               []string
}

func analyzeOpenGeminiTextIndex(path string, info os.FileInfo, options Options) (FileReport, error) {
	if info.IsDir() {
		return FileReport{}, fmt.Errorf("opengemini-text-index format requires a .pos, .bh, or .ph file")
	}
	paths, err := openGeminiTextIndexPaths(path)
	if err != nil {
		return FileReport{}, err
	}
	analysis, err := parseOpenGeminiTextIndex(paths)
	if err != nil {
		return FileReport{}, err
	}

	keySamples := openGeminiTextIndexKeySamples(paths, options.KeySampleLimit)
	blocks := openGeminiTextIndexBlockSamples(analysis, options.BlockSampleLimit)
	extra := map[string]string{
		"layout":                  opengeminiTextIndexLayout,
		"field":                   paths.Field,
		"input_component":         paths.InputComponent,
		"data_path":               paths.DataPath,
		"head_path":               paths.HeadPath,
		"part_path":               paths.PartPath,
		"data_file_present":       fmt.Sprint(analysis.DataFilePresent),
		"head_file_present":       fmt.Sprint(analysis.HeadFilePresent),
		"data_size_bytes":         fmt.Sprint(analysis.DataSizeBytes),
		"head_size_bytes":         fmt.Sprint(analysis.HeadSizeBytes),
		"part_size_bytes":         fmt.Sprint(analysis.PartSizeBytes),
		"part_header_record_size": fmt.Sprint(opengeminiTextIndexPartSize),
		"segments_per_part":       fmt.Sprint(opengeminiTextIndexSegmentSize),
		"declared_block_headers":  fmt.Sprint(analysis.DeclaredBlockCount),
		"decoded_block_headers":   fmt.Sprint(len(analysis.BlockHeaders)),
		"item_count":              fmt.Sprint(analysis.ItemCount),
		"keys_payload_size_bytes": fmt.Sprint(analysis.KeysPayloadSizeBytes),
		"post_payload_size_bytes": fmt.Sprint(analysis.PostPayloadSizeBytes),
		"payload_size_bytes":      fmt.Sprint(analysis.PayloadSizeBytes),
		"local_only":              "true",
	}

	report := FileReport{
		Path:         path,
		Format:       FormatOpenGeminiText,
		SizeBytes:    analysis.SizeBytes,
		ModTime:      info.ModTime(),
		MinKey:       paths.Field,
		MaxKey:       paths.Field,
		KeyCount:     openGeminiTextIndexKeyCount(paths),
		KeySamples:   keySamples,
		BlockCount:   int(analysis.DeclaredBlockCount),
		BlocksByType: analysis.BlocksByType,
		Blocks:       blocks,
		SecondaryIndex: &SecondaryIndexSummary{
			Type:                   opengeminiTextIndexType,
			Layout:                 opengeminiTextIndexLayout,
			Field:                  paths.Field,
			BlockCount:             analysis.DeclaredBlockCount,
			PartCount:              int64(len(analysis.PartHeaders)),
			ItemCount:              analysis.ItemCount,
			PayloadSizeBytes:       analysis.PayloadSizeBytes,
			DataSizeBytes:          analysis.DataSizeBytes,
			HeaderSizeBytes:        analysis.HeadSizeBytes,
			PartHeaderSizeBytes:    analysis.PartSizeBytes,
			TrailingBytes:          analysis.PartTrailingBytes,
			HeaderOutOfBoundsParts: analysis.HeaderOutOfBounds,
			DataOutOfBoundsBlocks:  analysis.DataOutOfBounds,
			InvalidOffsetBlocks:    analysis.InvalidOffsetBlocks,
			InvalidSizeBlocks:      analysis.InvalidSizeBlocks,
			SegmentRangeOverflows:  analysis.SegmentRangeOverflows,
		},
		Extra:   extra,
		Notices: analysis.Notices,
	}
	if report.KeyCount == 0 {
		report.MinKey = ""
		report.MaxKey = ""
	}
	return report, nil
}

func parseOpenGeminiTextIndex(paths opengeminiTextIndexPaths) (opengeminiTextIndexAnalysis, error) {
	analysis := opengeminiTextIndexAnalysis{
		Paths:        paths,
		BlocksByType: map[string]int{},
	}

	partInfo, err := os.Stat(paths.PartPath)
	if err != nil {
		if os.IsNotExist(err) {
			return analysis, fmt.Errorf("openGemini text index requires sibling part header file %s", paths.PartPath)
		}
		return analysis, err
	}
	if partInfo.IsDir() {
		return analysis, fmt.Errorf("openGemini text index part header path is a directory: %s", paths.PartPath)
	}
	analysis.PartSizeBytes = partInfo.Size()
	analysis.SizeBytes += partInfo.Size()

	headInfo, err := os.Stat(paths.HeadPath)
	if err != nil && !os.IsNotExist(err) {
		return analysis, err
	}
	if err == nil {
		if headInfo.IsDir() {
			return analysis, fmt.Errorf("openGemini text index block header path is a directory: %s", paths.HeadPath)
		}
		analysis.HeadFilePresent = true
		analysis.HeadSizeBytes = headInfo.Size()
		analysis.SizeBytes += headInfo.Size()
	}

	dataInfo, err := os.Stat(paths.DataPath)
	if err != nil && !os.IsNotExist(err) {
		return analysis, err
	}
	if err == nil {
		if dataInfo.IsDir() {
			return analysis, fmt.Errorf("openGemini text index data path is a directory: %s", paths.DataPath)
		}
		analysis.DataFilePresent = true
		analysis.DataSizeBytes = dataInfo.Size()
		analysis.SizeBytes += dataInfo.Size()
	}

	partData, err := os.ReadFile(paths.PartPath)
	if err != nil {
		return analysis, err
	}
	parts, trailing, err := parseOpenGeminiTextPartHeaders(partData, analysis.HeadSizeBytes, analysis.HeadFilePresent)
	if err != nil {
		return analysis, err
	}
	analysis.PartHeaders = parts
	analysis.PartTrailingBytes = trailing
	analysis.BlocksByType["text-index-part"] = len(parts)
	if trailing > 0 {
		analysis.BlocksByType["text-index-part-trailing-bytes"] = 1
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("openGemini text index part header file has %d trailing byte(s) after complete part headers", trailing))
	}
	for _, part := range parts {
		analysis.DeclaredBlockCount += int64(part.BlockHeaderCount)
		if part.HeaderOutOfBounds {
			analysis.HeaderOutOfBounds++
		}
		if part.SegmentRangeCount > opengeminiTextIndexSegmentSize {
			analysis.SegmentRangeOverflows++
		}
	}
	if analysis.HeaderOutOfBounds > 0 {
		analysis.BlocksByType["text-index-header-out-of-bounds"] = analysis.HeaderOutOfBounds
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("openGemini text index has %d part header range(s) outside the .bh file", analysis.HeaderOutOfBounds))
	}
	if analysis.SegmentRangeOverflows > 0 {
		analysis.BlocksByType["text-index-segment-range-overflow"] = analysis.SegmentRangeOverflows
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("openGemini text index has %d part header(s) with segment range count over %d", analysis.SegmentRangeOverflows, opengeminiTextIndexSegmentSize))
	}
	if len(parts) == 0 {
		analysis.Notices = append(analysis.Notices, "openGemini text index part header file has no complete part headers")
		return analysis, nil
	}
	if !analysis.HeadFilePresent {
		analysis.Notices = append(analysis.Notices, "openGemini text index sibling .bh file is missing; block headers were not decoded")
		return analysis, nil
	}

	headData, err := os.ReadFile(paths.HeadPath)
	if err != nil {
		return analysis, err
	}
	blocks, notices := parseOpenGeminiTextBlockHeaders(headData, parts, analysis.DataSizeBytes, analysis.DataFilePresent)
	analysis.BlockHeaders = blocks
	analysis.Notices = append(analysis.Notices, notices...)
	if len(blocks) > 0 {
		analysis.BlocksByType["text-index-block-header"] = len(blocks)
	}
	for _, block := range blocks {
		analysis.ItemCount += int64(block.ItemsCount)
		analysis.KeysPayloadSizeBytes += int64(block.KeysPackSize)
		analysis.PostPayloadSizeBytes += int64(block.PostPackSize)
		analysis.PayloadSizeBytes += int64(block.KeysPackSize) + int64(block.PostPackSize)
		if block.DataOutOfBounds {
			analysis.DataOutOfBounds++
		}
		if block.InvalidOffsets {
			analysis.InvalidOffsetBlocks++
		}
		if block.InvalidSizes {
			analysis.InvalidSizeBlocks++
		}
	}
	if !analysis.DataFilePresent {
		analysis.Notices = append(analysis.Notices, "openGemini text index sibling .pos file is missing; data ranges were not bounds-checked")
	}
	if analysis.DataOutOfBounds > 0 {
		analysis.BlocksByType["text-index-data-out-of-bounds"] = analysis.DataOutOfBounds
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("openGemini text index has %d block header data range(s) outside the .pos file", analysis.DataOutOfBounds))
	}
	if analysis.InvalidOffsetBlocks > 0 {
		analysis.BlocksByType["text-index-invalid-offset"] = analysis.InvalidOffsetBlocks
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("openGemini text index has %d block header(s) whose posting-list offset does not follow keys payload", analysis.InvalidOffsetBlocks))
	}
	if analysis.InvalidSizeBlocks > 0 {
		analysis.BlocksByType["text-index-invalid-size"] = analysis.InvalidSizeBlocks
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("openGemini text index has %d block header(s) with unpacked key/post sizes smaller than item bytes", analysis.InvalidSizeBlocks))
	}
	return analysis, nil
}

func parseOpenGeminiTextPartHeaders(data []byte, headSize int64, hasHead bool) ([]opengeminiTextIndexPartHeader, int64, error) {
	count := len(data) / opengeminiTextIndexPartSize
	trailing := len(data) % opengeminiTextIndexPartSize
	parts := make([]opengeminiTextIndexPartHeader, 0, count)
	for i := 0; i < count; i++ {
		start := i * opengeminiTextIndexPartSize
		part, err := parseOpenGeminiTextPartHeader(data[start : start+opengeminiTextIndexPartSize])
		if err != nil {
			return nil, 0, fmt.Errorf("part header %d: %w", i, err)
		}
		part.Index = i
		part.Offset = int64(start)
		if hasHead && !openGeminiTextRangeInFile(part.BlockHeaderOffset, part.BlockHeaderSize, headSize) {
			part.HeaderOutOfBounds = true
		}
		parts = append(parts, part)
	}
	return parts, int64(trailing), nil
}

func parseOpenGeminiTextPartHeader(data []byte) (opengeminiTextIndexPartHeader, error) {
	var part opengeminiTextIndexPartHeader
	if len(data) < opengeminiTextIndexPartSize {
		return part, fmt.Errorf("cannot unmarshal part header from %d bytes; need at least %d bytes", len(data), opengeminiTextIndexPartSize)
	}
	firstLen := int(data[0])
	lastLen := int(data[1])
	if firstLen > opengeminiTextIndexItemPrefix {
		return part, fmt.Errorf("first item length %d exceeds fixed prefix length %d", firstLen, opengeminiTextIndexItemPrefix)
	}
	if lastLen > opengeminiTextIndexItemPrefix {
		return part, fmt.Errorf("last item length %d exceeds fixed prefix length %d", lastLen, opengeminiTextIndexItemPrefix)
	}
	part.FirstItem = append([]byte(nil), data[2:2+firstLen]...)
	part.LastItem = append([]byte(nil), data[10:10+lastLen]...)
	pos := 18
	part.Flag = binary.BigEndian.Uint16(data[pos : pos+2])
	pos += 2
	part.BlockHeaderCount = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	part.BlockHeaderOffset = binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8
	part.BlockHeaderSize = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	part.SegmentRangeCount = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4
	part.SegmentRanges = make([]uint32, 0, opengeminiTextIndexSegmentSize)
	for i := 0; i < opengeminiTextIndexSegmentSize; i++ {
		part.SegmentRanges = append(part.SegmentRanges, binary.BigEndian.Uint32(data[pos:pos+4]))
		pos += 4
	}
	return part, nil
}

func parseOpenGeminiTextBlockHeaders(headData []byte, parts []opengeminiTextIndexPartHeader, dataSize int64, hasData bool) ([]opengeminiTextIndexBlockHeader, []string) {
	blocks := make([]opengeminiTextIndexBlockHeader, 0)
	notices := make([]string, 0)
	for _, part := range parts {
		if part.HeaderOutOfBounds {
			continue
		}
		start := int(part.BlockHeaderOffset)
		end := start + int(part.BlockHeaderSize)
		if start < 0 || end < start || end > len(headData) {
			notices = append(notices, fmt.Sprintf("openGemini text index part %d header range offset=%d size=%d is outside loaded .bh bytes", part.Index, part.BlockHeaderOffset, part.BlockHeaderSize))
			continue
		}
		data := headData[start:end]
		headOffset := int64(start)
		decoded := uint32(0)
		for decoded < part.BlockHeaderCount {
			header, consumed, err := parseOpenGeminiTextBlockHeader(data)
			if err != nil {
				notices = append(notices, fmt.Sprintf("openGemini text index part %d block header %d: %v", part.Index, decoded, err))
				break
			}
			header.PartIndex = part.Index
			header.HeaderIndex = int(decoded)
			header.HeadOffset = headOffset
			validateOpenGeminiTextBlockData(&header, dataSize, hasData)
			blocks = append(blocks, header)
			data = data[consumed:]
			headOffset += int64(consumed)
			decoded++
		}
		if decoded < part.BlockHeaderCount {
			notices = append(notices, fmt.Sprintf("openGemini text index part %d decoded %d of %d declared block header(s)", part.Index, decoded, part.BlockHeaderCount))
			continue
		}
		if len(data) > 0 {
			notices = append(notices, fmt.Sprintf("openGemini text index part %d has %d unparsed byte(s) after declared block headers", part.Index, len(data)))
		}
	}
	return blocks, notices
}

func parseOpenGeminiTextBlockHeader(data []byte) (opengeminiTextIndexBlockHeader, int, error) {
	var header opengeminiTextIndexBlockHeader
	originalLen := len(data)
	var err error
	header.FirstItem, data, err = parseMergesetBytes(data, "firstItem")
	if err != nil {
		return header, 0, err
	}
	header.LastItem, data, err = parseMergesetBytes(data, "lastItem")
	if err != nil {
		return header, 0, err
	}
	if len(data) < 1 {
		return header, 0, fmt.Errorf("cannot unmarshal marshalType from zero bytes")
	}
	header.MarshalType = data[0]
	data = data[1:]
	if len(data) < 4 {
		return header, 0, fmt.Errorf("cannot unmarshal itemsCount from %d bytes; need at least 4 bytes", len(data))
	}
	header.ItemsCount = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	if len(data) < 20 {
		return header, 0, fmt.Errorf("cannot unmarshal keys info from %d bytes; need at least 20 bytes", len(data))
	}
	header.KeysOffset = binary.BigEndian.Uint64(data[:8])
	data = data[8:]
	header.KeysUnpackSize = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	header.KeysPackSize = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	header.KeysSize = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	if len(data) < 20 {
		return header, 0, fmt.Errorf("cannot unmarshal post info from %d bytes; need at least 20 bytes", len(data))
	}
	header.PostOffset = binary.BigEndian.Uint64(data[:8])
	data = data[8:]
	header.PostUnpackSize = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	header.PostPackSize = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	header.PostSize = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	return header, originalLen - len(data), nil
}

func validateOpenGeminiTextBlockData(header *opengeminiTextIndexBlockHeader, dataSize int64, hasData bool) {
	expectedPostOffset, ok := openGeminiTextRangeEnd(header.KeysOffset, header.KeysPackSize)
	if !ok || header.PostOffset != expectedPostOffset {
		header.InvalidOffsets = true
	}
	if header.KeysSize > header.KeysUnpackSize || header.PostSize > header.PostUnpackSize {
		header.InvalidSizes = true
	}
	if !hasData {
		return
	}
	if !openGeminiTextRangeInFile(header.KeysOffset, header.KeysPackSize, dataSize) || !openGeminiTextRangeInFile(header.PostOffset, header.PostPackSize, dataSize) {
		header.DataOutOfBounds = true
	}
}

func openGeminiTextIndexBlockSamples(analysis opengeminiTextIndexAnalysis, limit int) []BlockReport {
	if limit <= 0 || len(analysis.BlockHeaders) == 0 {
		return nil
	}
	if limit > len(analysis.BlockHeaders) {
		limit = len(analysis.BlockHeaders)
	}
	reports := make([]BlockReport, 0, limit)
	for i := 0; i < limit; i++ {
		header := analysis.BlockHeaders[i]
		sizeBytes := int64(header.KeysPackSize) + int64(header.PostPackSize)
		reports = append(reports, BlockReport{
			Key:        openGeminiTextIndexBlockKey(analysis.Paths.Field, header),
			Type:       "text-index-block-header",
			Offset:     int64(header.KeysOffset),
			SizeBytes:  clampUint32(sizeBytes),
			ValueCount: int(header.ItemsCount),
		})
	}
	return reports
}

func openGeminiTextIndexBlockKey(field string, header opengeminiTextIndexBlockHeader) string {
	prefix := ""
	if field != "" {
		prefix = "field:" + field + " "
	}
	return prefix + hex.EncodeToString(header.FirstItem) + ".." + hex.EncodeToString(header.LastItem)
}

func openGeminiTextIndexKeySamples(paths opengeminiTextIndexPaths, limit int) []string {
	if limit <= 0 || paths.Field == "" {
		return nil
	}
	return []string{"field:" + paths.Field}
}

func openGeminiTextIndexKeyCount(paths opengeminiTextIndexPaths) int {
	if paths.Field == "" {
		return 0
	}
	return 1
}

func openGeminiTextIndexPaths(path string) (opengeminiTextIndexPaths, error) {
	suffix, component, ok := openGeminiTextIndexSuffix(path)
	if !ok {
		return opengeminiTextIndexPaths{}, fmt.Errorf("opengemini-text-index format requires a .pos, .bh, or .ph file")
	}
	base := path[:len(path)-len(suffix)]
	field := openGeminiTextIndexField(base)
	return opengeminiTextIndexPaths{
		Base:           base,
		Field:          field,
		InputComponent: component,
		DataPath:       base + opengeminiTextIndexDataSuffix,
		HeadPath:       base + opengeminiTextIndexHeadSuffix,
		PartPath:       base + opengeminiTextIndexPartSuffix,
	}, nil
}

func openGeminiTextIndexSuffix(path string) (suffix string, component string, ok bool) {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, opengeminiTextIndexDataSuffix):
		return path[len(path)-len(opengeminiTextIndexDataSuffix):], "data", true
	case strings.HasSuffix(lower, opengeminiTextIndexHeadSuffix):
		return path[len(path)-len(opengeminiTextIndexHeadSuffix):], "head", true
	case strings.HasSuffix(lower, opengeminiTextIndexPartSuffix):
		return path[len(path)-len(opengeminiTextIndexPartSuffix):], "part", true
	default:
		return "", "", false
	}
}

func openGeminiTextIndexField(base string) string {
	stem := filepath.Base(base)
	if idx := strings.LastIndex(stem, "."); idx >= 0 && idx < len(stem)-1 {
		return stem[idx+1:]
	}
	return ""
}

func isOpenGeminiTextIndexPath(path string) bool {
	_, _, ok := openGeminiTextIndexSuffix(path)
	return ok
}

func isOpenGeminiTextIndexPartPath(path string) bool {
	lower := strings.ToLower(filepath.Base(path))
	return strings.HasSuffix(lower, opengeminiTextIndexPartSuffix)
}

func openGeminiTextRangeInFile(offset uint64, size uint32, fileSize int64) bool {
	end, ok := openGeminiTextRangeEnd(offset, size)
	if !ok || fileSize < 0 {
		return false
	}
	return end <= uint64(fileSize)
}

func openGeminiTextRangeEnd(offset uint64, size uint32) (uint64, bool) {
	end := offset + uint64(size)
	if end < offset {
		return 0, false
	}
	return end, true
}
