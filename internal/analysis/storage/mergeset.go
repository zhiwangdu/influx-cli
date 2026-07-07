package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

const (
	mergesetMetadataFile  = "metadata.json"
	mergesetMetaindexFile = "metaindex.bin"
	mergesetIndexFile     = "index.bin"
	mergesetItemsFile     = "items.bin"
	mergesetLensFile      = "lens.bin"

	mergesetMaxIndexBlockSize    = 64 * 1024
	mergesetMaxInmemoryBlockSize = 64 * 1024
	mergesetMinBlockHeaderSize   = 31

	mergesetMarshalTypePlain byte = 0
	mergesetMarshalTypeZSTD  byte = 1
)

type mergesetPartName struct {
	ItemsCount  uint64
	BlocksCount uint64
	Suffix      string
}

type mergesetPartMetadata struct {
	ItemsCount  uint64 `json:"ItemsCount"`
	BlocksCount uint64 `json:"BlocksCount"`
	FirstItem   string `json:"FirstItem"`
	LastItem    string `json:"LastItem"`
}

type mergesetMetaindexSummary struct {
	Rows             []mergesetMetaindexRow
	UncompressedSize int
}

type mergesetMetaindexRow struct {
	FirstItem         []byte
	BlockHeadersCount uint32
	IndexBlockOffset  uint64
	IndexBlockSize    uint32
}

type mergesetIndexSummary struct {
	Headers          []mergesetBlockHeader
	IndexBlocks      int
	DecodedBlocks    int
	UncompressedSize int
}

type mergesetItemPayloadSummary struct {
	Blocks        int
	DecodedBlocks int
	ItemsDecoded  uint64
	FirstItem     []byte
	LastItem      []byte
	Samples       [][]byte
	TSIIndex      mergesetTSIIndexSummary
	FieldIndex    mergesetFieldIndexSummary
	CLVText       mergesetCLVTextIndexSummary
	DecodePath    *DecodePathSummary
}

type mergesetTSIIndexSummary struct {
	Detected            bool
	KeyToTSIDCount      int
	TSIDToKeyCount      int
	TagToTSIDCount      int
	TagToTSIDValueCount int
	TagValueCount       int
	InvalidItems        int
	DeferredCLVInvalids int
	KeyToTSIDSamples    []string
	TSIDToKeySamples    []string
	TagToTSIDSamples    []string
	TagValueSamples     []string
	measurements        map[string]*mergesetTSIMeasurement
}

type mergesetFieldIndexSummary struct {
	Detected                bool
	MeasurementFieldKeys    map[string]string
	TSIDToFieldValueCount   int
	FieldToPIDCount         int
	InvalidItems            int
	MeasurementSamples      []string
	FieldValueSamples       []string
	FieldToPIDSamples       []string
	DuplicateMeasurementKey int
}

type mergesetCLVTextIndexSummary struct {
	Detected              bool
	DocumentRows          int
	PositionEntries       int
	PositionSIDGroups     int
	DocumentIDs           int
	TermRows              int
	DictionaryRows        int
	DictionaryVersionRows int
	InvalidItems          int
	DeferredInvalidItems  int
	PositionSamples       []string
	TermSamples           []string
	DictionarySamples     []string
	VersionSamples        []string
}

type mergesetBlockHeader struct {
	CommonPrefix     []byte
	FirstItem        []byte
	MarshalType      byte
	ItemsCount       uint32
	ItemsBlockOffset uint64
	LensBlockOffset  uint64
	ItemsBlockSize   uint32
	LensBlockSize    uint32
}

type mergesetTSIMeasurement struct {
	Name              string
	SeriesIDs         map[uint64]struct{}
	TagValues         map[string]map[string]struct{}
	TagValueSeriesIDs map[string]map[string]map[uint64]struct{}
}

func analyzeMergesetPart(path string, info os.FileInfo, options Options) (FileReport, error) {
	if !info.IsDir() {
		return FileReport{}, fmt.Errorf("mergeset part must be a directory")
	}
	name, err := parseMergesetPartName(filepath.Base(path))
	if err != nil {
		return FileReport{}, err
	}
	metadata, err := readMergesetPartMetadata(path)
	if err != nil {
		return FileReport{}, err
	}
	if metadata.ItemsCount != name.ItemsCount {
		return FileReport{}, fmt.Errorf("invalid mergeset ItemsCount in metadata: got %d, want %d", metadata.ItemsCount, name.ItemsCount)
	}
	notices := []string{}
	if metadata.BlocksCount != name.BlocksCount {
		notices = append(notices, fmt.Sprintf("mergeset part name blocks_count=%d differs from metadata blocks_count=%d", name.BlocksCount, metadata.BlocksCount))
	}

	componentSizes, totalSize, err := mergesetComponentSizes(path)
	if err != nil {
		return FileReport{}, err
	}
	firstItem, err := decodeMergesetHexItem(metadata.FirstItem, "FirstItem")
	if err != nil {
		return FileReport{}, err
	}
	lastItem, err := decodeMergesetHexItem(metadata.LastItem, "LastItem")
	if err != nil {
		return FileReport{}, err
	}
	metaindex, metaindexErr := readMergesetMetaindex(filepath.Join(path, mergesetMetaindexFile))
	if metaindexErr != nil {
		notices = append(notices, fmt.Sprintf("mergeset metaindex decode unavailable: %v", metaindexErr))
	}

	keySamples := make([]string, 0, 2)
	if options.KeySampleLimit > 0 {
		keySamples = append(keySamples, "first:"+metadata.FirstItem)
		if options.KeySampleLimit > 1 && metadata.LastItem != metadata.FirstItem {
			keySamples = append(keySamples, "last:"+metadata.LastItem)
		}
	}

	report := FileReport{
		Path:       path,
		Format:     FormatMergeset,
		SizeBytes:  totalSize,
		ModTime:    info.ModTime(),
		KeyCount:   uint64ToInt(metadata.ItemsCount),
		KeySamples: keySamples,
		BlockCount: uint64ToInt(metadata.BlocksCount),
		BlocksByType: map[string]int{
			"mergeset-block": uint64ToInt(metadata.BlocksCount),
		},
		Extra: map[string]string{
			"layout":             "part",
			"items_count":        fmt.Sprint(metadata.ItemsCount),
			"blocks_count":       fmt.Sprint(metadata.BlocksCount),
			"part_name_items":    fmt.Sprint(name.ItemsCount),
			"part_name_blocks":   fmt.Sprint(name.BlocksCount),
			"part_suffix":        name.Suffix,
			"first_item_hex":     metadata.FirstItem,
			"last_item_hex":      metadata.LastItem,
			"first_item_bytes":   fmt.Sprint(len(firstItem)),
			"last_item_bytes":    fmt.Sprint(len(lastItem)),
			"metadata_json_size": fmt.Sprint(componentSizes[mergesetMetadataFile]),
			"metaindex_size":     fmt.Sprint(componentSizes[mergesetMetaindexFile]),
			"index_size":         fmt.Sprint(componentSizes[mergesetIndexFile]),
			"items_size":         fmt.Sprint(componentSizes[mergesetItemsFile]),
			"lens_size":          fmt.Sprint(componentSizes[mergesetLensFile]),
		},
		Notices: notices,
	}
	if metaindexErr == nil {
		addMergesetMetaindexSummary(&report, metaindex, componentSizes[mergesetIndexFile], metadata.BlocksCount)
		indexSummary, indexNotices := readMergesetIndexBlocks(path, metaindex.Rows, componentSizes)
		report.Notices = append(report.Notices, indexNotices...)
		addMergesetIndexSummary(&report, indexSummary, componentSizes, metaindex, metadata.ItemsCount, options)
		payloadSummary, payloadNotices := readMergesetItemPayloads(path, indexSummary.Headers, componentSizes, options, firstItem, lastItem)
		report.Notices = append(report.Notices, payloadNotices...)
		addMergesetItemPayloadSummary(&report, payloadSummary, firstItem, lastItem, metadata.ItemsCount, options)
	}
	return report, nil
}

func isMergesetPartPath(path string) bool {
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return false
	}
	if _, err := parseMergesetPartName(filepath.Base(path)); err != nil {
		return false
	}
	if _, err := os.Stat(filepath.Join(path, mergesetMetadataFile)); err != nil {
		return false
	}
	return true
}

func parseMergesetPartName(name string) (mergesetPartName, error) {
	var part mergesetPartName
	fields := strings.Split(name, "_")
	if len(fields) != 3 {
		return part, fmt.Errorf("invalid mergeset part name %q: expected items_blocks_suffix", name)
	}
	itemsCount, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return part, fmt.Errorf("invalid mergeset items count in part name %q: %w", name, err)
	}
	if itemsCount == 0 {
		return part, fmt.Errorf("mergeset part %q cannot contain zero items", name)
	}
	blocksCount, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return part, fmt.Errorf("invalid mergeset blocks count in part name %q: %w", name, err)
	}
	if blocksCount == 0 {
		return part, fmt.Errorf("mergeset part %q cannot contain zero blocks", name)
	}
	if blocksCount > itemsCount {
		return part, fmt.Errorf("mergeset part %q has blocks_count=%d greater than items_count=%d", name, blocksCount, itemsCount)
	}
	part.ItemsCount = itemsCount
	part.BlocksCount = blocksCount
	part.Suffix = fields[2]
	return part, nil
}

func readMergesetPartMetadata(path string) (mergesetPartMetadata, error) {
	var metadata mergesetPartMetadata
	data, err := os.ReadFile(filepath.Join(path, mergesetMetadataFile))
	if err != nil {
		return metadata, err
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, fmt.Errorf("parse mergeset metadata: %w", err)
	}
	return metadata, nil
}

func mergesetComponentSizes(path string) (map[string]int64, int64, error) {
	names := []string{
		mergesetMetadataFile,
		mergesetMetaindexFile,
		mergesetIndexFile,
		mergesetItemsFile,
		mergesetLensFile,
	}
	sizes := make(map[string]int64, len(names))
	var total int64
	for _, name := range names {
		info, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			return nil, 0, fmt.Errorf("stat mergeset component %s: %w", name, err)
		}
		if info.IsDir() {
			return nil, 0, fmt.Errorf("mergeset component %s is a directory", name)
		}
		sizes[name] = info.Size()
		total += info.Size()
	}
	return sizes, total, nil
}

func decodeMergesetHexItem(value, field string) ([]byte, error) {
	item, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("invalid mergeset %s hex item: %w", field, err)
	}
	return item, nil
}

func readMergesetMetaindex(path string) (mergesetMetaindexSummary, error) {
	var summary mergesetMetaindexSummary
	compressed, err := os.ReadFile(path)
	if err != nil {
		return summary, err
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return summary, fmt.Errorf("create zstd decoder: %w", err)
	}
	defer decoder.Close()
	data, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return summary, fmt.Errorf("decompress zstd: %w", err)
	}
	rows, err := parseMergesetMetaindexRows(data)
	if err != nil {
		return summary, err
	}
	summary.Rows = rows
	summary.UncompressedSize = len(data)
	return summary, nil
}

func parseMergesetMetaindexRows(data []byte) ([]mergesetMetaindexRow, error) {
	rows := []mergesetMetaindexRow{}
	for len(data) > 0 {
		row, consumed, err := parseMergesetMetaindexRow(data)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal metaindex row #%d: %w", len(rows)+1, err)
		}
		rows = append(rows, row)
		data = data[consumed:]
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("expecting non-zero metaindex rows; got zero")
	}
	for i := 1; i < len(rows); i++ {
		if bytes.Compare(rows[i-1].FirstItem, rows[i].FirstItem) > 0 {
			return nil, fmt.Errorf("metaindex %d rows aren't sorted by firstItem", len(rows))
		}
	}
	return rows, nil
}

func parseMergesetMetaindexRow(data []byte) (mergesetMetaindexRow, int, error) {
	var row mergesetMetaindexRow
	originalLen := len(data)
	firstItem, tail, err := parseMergesetBytes(data, "firstItem")
	if err != nil {
		return row, 0, err
	}
	row.FirstItem = firstItem
	data = tail

	if len(data) < 4 {
		return row, 0, fmt.Errorf("cannot unmarshal blockHeadersCount from %d bytes; need at least 4 bytes", len(data))
	}
	row.BlockHeadersCount = binary.BigEndian.Uint32(data[:4])
	data = data[4:]

	if len(data) < 8 {
		return row, 0, fmt.Errorf("cannot unmarshal indexBlockOffset from %d bytes; need at least 8 bytes", len(data))
	}
	row.IndexBlockOffset = binary.BigEndian.Uint64(data[:8])
	data = data[8:]

	if len(data) < 4 {
		return row, 0, fmt.Errorf("cannot unmarshal indexBlockSize from %d bytes; need at least 4 bytes", len(data))
	}
	row.IndexBlockSize = binary.BigEndian.Uint32(data[:4])
	data = data[4:]

	if row.BlockHeadersCount == 0 {
		return row, 0, fmt.Errorf("blockHeadersCount must be bigger than 0; got 0")
	}
	if row.IndexBlockSize > 3*mergesetMaxIndexBlockSize {
		return row, 0, fmt.Errorf("too big indexBlockSize: %d; cannot exceed %d", row.IndexBlockSize, 3*mergesetMaxIndexBlockSize)
	}
	return row, originalLen - len(data), nil
}

func addMergesetMetaindexSummary(report *FileReport, metaindex mergesetMetaindexSummary, indexSize int64, metadataBlockCount uint64) {
	rows := metaindex.Rows
	report.BlocksByType["mergeset-metaindex-row"] = len(rows)
	report.Extra["metaindex_row_count"] = fmt.Sprint(len(rows))
	report.Extra["metaindex_uncompressed_size"] = fmt.Sprint(metaindex.UncompressedSize)
	report.Extra["metaindex_first_item_hex"] = hex.EncodeToString(rows[0].FirstItem)
	report.Extra["metaindex_last_item_hex"] = hex.EncodeToString(rows[len(rows)-1].FirstItem)

	var totalHeaders uint64
	var totalIndexBytes uint64
	outOfBounds := 0
	indexSizeUint := uint64(0)
	if indexSize > 0 {
		indexSizeUint = uint64(indexSize)
	}
	for _, row := range rows {
		totalHeaders += uint64(row.BlockHeadersCount)
		totalIndexBytes += uint64(row.IndexBlockSize)
		if row.IndexBlockOffset > indexSizeUint || uint64(row.IndexBlockSize) > indexSizeUint-row.IndexBlockOffset {
			outOfBounds++
		}
	}
	report.Extra["metaindex_block_headers"] = fmt.Sprint(totalHeaders)
	report.Extra["metaindex_index_bytes"] = fmt.Sprint(totalIndexBytes)
	if totalHeaders != metadataBlockCount {
		report.Notices = append(report.Notices, fmt.Sprintf("mergeset metaindex block_headers_count total=%d differs from metadata blocks_count=%d", totalHeaders, metadataBlockCount))
	}
	if outOfBounds > 0 {
		report.Notices = append(report.Notices, fmt.Sprintf("mergeset metaindex has %d row(s) outside index.bin bounds", outOfBounds))
	}
}

func readMergesetIndexBlocks(path string, rows []mergesetMetaindexRow, componentSizes map[string]int64) (mergesetIndexSummary, []string) {
	summary := mergesetIndexSummary{
		IndexBlocks: len(rows),
	}
	indexSize := componentSizes[mergesetIndexFile]
	indexSizeUint := uint64(0)
	if indexSize > 0 {
		indexSizeUint = uint64(indexSize)
	}
	indexFile, err := os.Open(filepath.Join(path, mergesetIndexFile))
	if err != nil {
		return summary, []string{fmt.Sprintf("mergeset index block decode unavailable: %v", err)}
	}
	defer indexFile.Close()

	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return summary, []string{fmt.Sprintf("mergeset index block decode unavailable: create zstd decoder: %v", err)}
	}
	defer decoder.Close()

	notices := []string{}
	for i, row := range rows {
		if row.IndexBlockOffset > indexSizeUint || uint64(row.IndexBlockSize) > indexSizeUint-row.IndexBlockOffset {
			continue
		}
		compressed := make([]byte, row.IndexBlockSize)
		if _, err := indexFile.ReadAt(compressed, int64(row.IndexBlockOffset)); err != nil {
			notices = append(notices, fmt.Sprintf("mergeset index block decode unavailable at offset=%d size=%d: %v", row.IndexBlockOffset, row.IndexBlockSize, err))
			continue
		}
		data, err := decoder.DecodeAll(compressed, nil)
		if err != nil {
			notices = append(notices, fmt.Sprintf("mergeset index block decode unavailable at offset=%d size=%d: decompress zstd: %v", row.IndexBlockOffset, row.IndexBlockSize, err))
			continue
		}
		headers, err := parseMergesetBlockHeaders(data, int(row.BlockHeadersCount))
		if err != nil {
			notices = append(notices, fmt.Sprintf("mergeset index block decode unavailable at row=%d offset=%d size=%d: %v", i+1, row.IndexBlockOffset, row.IndexBlockSize, err))
			continue
		}
		summary.DecodedBlocks++
		summary.UncompressedSize += len(data)
		summary.Headers = append(summary.Headers, headers...)
	}
	return summary, notices
}

func parseMergesetBlockHeaders(data []byte, count int) ([]mergesetBlockHeader, error) {
	if count <= 0 {
		return nil, fmt.Errorf("blockHeadersCount must be greater than 0; got %d", count)
	}
	if count > len(data)/mergesetMinBlockHeaderSize {
		return nil, fmt.Errorf("blockHeadersCount=%d exceeds maximum possible headers for %d index bytes", count, len(data))
	}
	headers := make([]mergesetBlockHeader, 0, count)
	for i := 0; i < count; i++ {
		header, consumed, err := parseMergesetBlockHeader(data)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal blockHeader #%d: %w", i+1, err)
		}
		headers = append(headers, header)
		data = data[consumed:]
	}
	if len(data) > 0 {
		return nil, fmt.Errorf("unexpected non-empty tail left after unmarshaling block headers; len(tail)=%d", len(data))
	}
	for i := 1; i < len(headers); i++ {
		if bytes.Compare(headers[i-1].FirstItem, headers[i].FirstItem) > 0 {
			return nil, fmt.Errorf("block headers must be sorted by firstItem")
		}
	}
	return headers, nil
}

func parseMergesetBlockHeader(data []byte) (mergesetBlockHeader, int, error) {
	var header mergesetBlockHeader
	originalLen := len(data)
	var err error
	header.CommonPrefix, data, err = parseMergesetBytes(data, "commonPrefix")
	if err != nil {
		return header, 0, err
	}
	header.FirstItem, data, err = parseMergesetBytes(data, "firstItem")
	if err != nil {
		return header, 0, err
	}
	if len(data) == 0 {
		return header, 0, fmt.Errorf("cannot unmarshal marshalType from zero bytes")
	}
	header.MarshalType = data[0]
	data = data[1:]
	if header.MarshalType != mergesetMarshalTypePlain && header.MarshalType != mergesetMarshalTypeZSTD {
		return header, 0, fmt.Errorf("marshalType must be in the range [0..1]; got %d", header.MarshalType)
	}
	if len(data) < 4 {
		return header, 0, fmt.Errorf("cannot unmarshal itemsCount from %d bytes; need at least 4 bytes", len(data))
	}
	header.ItemsCount = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	if len(data) < 8 {
		return header, 0, fmt.Errorf("cannot unmarshal itemsBlockOffset from %d bytes; need at least 8 bytes", len(data))
	}
	header.ItemsBlockOffset = binary.BigEndian.Uint64(data[:8])
	data = data[8:]
	if len(data) < 8 {
		return header, 0, fmt.Errorf("cannot unmarshal lensBlockOffset from %d bytes; need at least 8 bytes", len(data))
	}
	header.LensBlockOffset = binary.BigEndian.Uint64(data[:8])
	data = data[8:]
	if len(data) < 4 {
		return header, 0, fmt.Errorf("cannot unmarshal itemsBlockSize from %d bytes; need at least 4 bytes", len(data))
	}
	header.ItemsBlockSize = binary.BigEndian.Uint32(data[:4])
	data = data[4:]
	if len(data) < 4 {
		return header, 0, fmt.Errorf("cannot unmarshal lensBlockSize from %d bytes; need at least 4 bytes", len(data))
	}
	header.LensBlockSize = binary.BigEndian.Uint32(data[:4])
	data = data[4:]

	if header.ItemsCount == 0 {
		return header, 0, fmt.Errorf("itemsCount must be bigger than 0; got 0")
	}
	if header.ItemsBlockSize > 2*mergesetMaxInmemoryBlockSize {
		return header, 0, fmt.Errorf("too big itemsBlockSize; got %d; cannot exceed %d", header.ItemsBlockSize, 2*mergesetMaxInmemoryBlockSize)
	}
	if header.LensBlockSize > 2*8*mergesetMaxInmemoryBlockSize {
		return header, 0, fmt.Errorf("too big lensBlockSize; got %d; cannot exceed %d", header.LensBlockSize, 2*8*mergesetMaxInmemoryBlockSize)
	}
	return header, originalLen - len(data), nil
}

func parseMergesetBytes(data []byte, field string) ([]byte, []byte, error) {
	valueLen, n := binary.Uvarint(data)
	if n == 0 {
		return nil, data, fmt.Errorf("cannot unmarshal %s", field)
	}
	if n < 0 {
		return nil, data, fmt.Errorf("%s length varuint overflows uint64", field)
	}
	data = data[n:]
	if valueLen > uint64(len(data)) {
		return nil, data, fmt.Errorf("%s length %d exceeds remaining %d bytes", field, valueLen, len(data))
	}
	valueLenInt := int(valueLen)
	value := append([]byte(nil), data[:valueLenInt]...)
	return value, data[valueLenInt:], nil
}

func addMergesetIndexSummary(report *FileReport, summary mergesetIndexSummary, componentSizes map[string]int64, metaindex mergesetMetaindexSummary, metadataItemsCount uint64, options Options) {
	report.Extra["index_block_count"] = fmt.Sprint(summary.IndexBlocks)
	report.Extra["index_blocks_decoded"] = fmt.Sprint(summary.DecodedBlocks)
	report.Extra["index_block_headers"] = fmt.Sprint(len(summary.Headers))
	report.Extra["index_uncompressed_size"] = fmt.Sprint(summary.UncompressedSize)
	if len(summary.Headers) == 0 {
		return
	}

	var itemCount uint64
	var itemsBytes uint64
	var lensBytes uint64
	var plainBlocks int
	var zstdBlocks int
	var itemsOutOfBounds int
	var lensOutOfBounds int
	itemsSize := uint64(0)
	if componentSizes[mergesetItemsFile] > 0 {
		itemsSize = uint64(componentSizes[mergesetItemsFile])
	}
	lensSize := uint64(0)
	if componentSizes[mergesetLensFile] > 0 {
		lensSize = uint64(componentSizes[mergesetLensFile])
	}
	for _, header := range summary.Headers {
		itemCount += uint64(header.ItemsCount)
		itemsBytes += uint64(header.ItemsBlockSize)
		lensBytes += uint64(header.LensBlockSize)
		switch header.MarshalType {
		case mergesetMarshalTypePlain:
			plainBlocks++
		case mergesetMarshalTypeZSTD:
			zstdBlocks++
		}
		if header.ItemsBlockOffset > itemsSize || uint64(header.ItemsBlockSize) > itemsSize-header.ItemsBlockOffset {
			itemsOutOfBounds++
		}
		if header.LensBlockOffset > lensSize || uint64(header.LensBlockSize) > lensSize-header.LensBlockOffset {
			lensOutOfBounds++
		}
	}
	report.Extra["index_first_block_item_hex"] = hex.EncodeToString(summary.Headers[0].FirstItem)
	report.Extra["index_last_block_item_hex"] = hex.EncodeToString(summary.Headers[len(summary.Headers)-1].FirstItem)
	report.Extra["item_count_from_blocks"] = fmt.Sprint(itemCount)
	report.Extra["items_block_bytes"] = fmt.Sprint(itemsBytes)
	report.Extra["lens_block_bytes"] = fmt.Sprint(lensBytes)
	report.Extra["plain_block_headers"] = fmt.Sprint(plainBlocks)
	report.Extra["zstd_block_headers"] = fmt.Sprint(zstdBlocks)
	if uint64(len(summary.Headers)) != metaindexBlockHeaders(metaindex.Rows) {
		report.Notices = append(report.Notices, fmt.Sprintf("mergeset decoded index block headers=%d differs from metaindex block_headers=%d", len(summary.Headers), metaindexBlockHeaders(metaindex.Rows)))
	}
	if itemCount != metadataItemsCount {
		report.Notices = append(report.Notices, fmt.Sprintf("mergeset index item count total=%d differs from metadata items_count=%d", itemCount, metadataItemsCount))
	}
	if itemsOutOfBounds > 0 {
		report.Notices = append(report.Notices, fmt.Sprintf("mergeset index has %d item block header(s) outside items.bin bounds", itemsOutOfBounds))
	}
	if lensOutOfBounds > 0 {
		report.Notices = append(report.Notices, fmt.Sprintf("mergeset index has %d lens block header(s) outside lens.bin bounds", lensOutOfBounds))
	}

	for i, header := range summary.Headers {
		if i >= options.BlockSampleLimit {
			break
		}
		report.Blocks = append(report.Blocks, BlockReport{
			Key:        hex.EncodeToString(header.FirstItem),
			Type:       "mergeset-block",
			Offset:     uint64ToNonNegativeInt64(header.ItemsBlockOffset),
			SizeBytes:  header.ItemsBlockSize,
			ValueCount: uint64ToInt(uint64(header.ItemsCount)),
		})
	}
}

func readMergesetItemPayloads(path string, headers []mergesetBlockHeader, componentSizes map[string]int64, options Options, firstItem, lastItem []byte) (mergesetItemPayloadSummary, []string) {
	summary := mergesetItemPayloadSummary{
		Blocks: len(headers),
	}
	search := newMergesetSearchPlan(headers, options, firstItem, lastItem)
	scan := newMergesetScanPlan(options)
	summary.DecodePath = search.DecodePath
	if summary.DecodePath == nil {
		summary.DecodePath = scan.DecodePath
	}
	if len(headers) == 0 {
		return summary, nil
	}
	itemsFile, err := os.Open(filepath.Join(path, mergesetItemsFile))
	if err != nil {
		return summary, []string{fmt.Sprintf("mergeset item payload decode unavailable: %v", err)}
	}
	defer itemsFile.Close()
	lensFile, err := os.Open(filepath.Join(path, mergesetLensFile))
	if err != nil {
		return summary, []string{fmt.Sprintf("mergeset item payload decode unavailable: %v", err)}
	}
	defer lensFile.Close()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return summary, []string{fmt.Sprintf("mergeset item payload decode unavailable: create zstd decoder: %v", err)}
	}
	defer decoder.Close()

	itemsSize := uint64(0)
	if componentSizes[mergesetItemsFile] > 0 {
		itemsSize = uint64(componentSizes[mergesetItemsFile])
	}
	lensSize := uint64(0)
	if componentSizes[mergesetLensFile] > 0 {
		lensSize = uint64(componentSizes[mergesetLensFile])
	}
	trackTSIQueryState := len(options.QueryMeasurements) > 0 || len(options.QueryTags) > 0
	notices := []string{}
	for i, header := range headers {
		search.ObserveHeader(i, header)
		scan.ObserveHeader(i, header)
		if header.ItemsBlockOffset > itemsSize || uint64(header.ItemsBlockSize) > itemsSize-header.ItemsBlockOffset {
			continue
		}
		if header.LensBlockOffset > lensSize || uint64(header.LensBlockSize) > lensSize-header.LensBlockOffset {
			continue
		}
		itemsData := make([]byte, header.ItemsBlockSize)
		if _, err := itemsFile.ReadAt(itemsData, int64(header.ItemsBlockOffset)); err != nil {
			notices = append(notices, fmt.Sprintf("mergeset item payload decode unavailable at block=%d items_offset=%d items_size=%d: %v", i+1, header.ItemsBlockOffset, header.ItemsBlockSize, err))
			continue
		}
		lensData := make([]byte, header.LensBlockSize)
		if _, err := lensFile.ReadAt(lensData, int64(header.LensBlockOffset)); err != nil {
			notices = append(notices, fmt.Sprintf("mergeset item payload decode unavailable at block=%d lens_offset=%d lens_size=%d: %v", i+1, header.LensBlockOffset, header.LensBlockSize, err))
			continue
		}
		payloadSampleLimit := options.KeySampleLimit - len(summary.Samples)
		decodeSampleLimit := payloadSampleLimit
		if scan.DecodePath != nil {
			scanSampleLimit := options.BlockSampleLimit - len(scan.DecodePath.CursorOutputSamples)
			if scanSampleLimit > decodeSampleLimit {
				decodeSampleLimit = scanSampleLimit
			}
		}
		decoded, err := decodeMergesetBlockItems(header, itemsData, lensData, decoder, decodeSampleLimit, search.QueryKeys, options.CursorDescending)
		if err != nil {
			notices = append(notices, fmt.Sprintf("mergeset item payload decode unavailable at block=%d first_item=%s: %v", i+1, hex.EncodeToString(header.FirstItem), err))
			continue
		}
		search.ObserveDecodedBlock(i, decoded)
		scan.ObserveDecodedBlock(i, header, decoded)
		summary.DecodedBlocks++
		summary.ItemsDecoded += decoded.Count
		if len(summary.FirstItem) == 0 {
			summary.FirstItem = append(summary.FirstItem[:0], decoded.FirstItem...)
		}
		summary.LastItem = append(summary.LastItem[:0], decoded.LastItem...)
		observeMergesetCLVTextIndexItems(&summary.CLVText, decoded.Items, options.BlockSampleLimit)
		observeMergesetTSIIndexItems(&summary.TSIIndex, decoded.Items, options.BlockSampleLimit, summary.CLVText.Detected, trackTSIQueryState)
		observeMergesetFieldIndexItems(&summary.FieldIndex, decoded.Items, options.BlockSampleLimit)
		if payloadSampleLimit > len(decoded.Samples) {
			payloadSampleLimit = len(decoded.Samples)
		}
		if payloadSampleLimit > 0 {
			summary.Samples = append(summary.Samples, decoded.Samples[:payloadSampleLimit]...)
		}
	}
	search.Finish(options)
	scan.Finish(options)
	finalizeMergesetItemNamespaceSummaries(&summary.CLVText, &summary.TSIIndex)
	return summary, notices
}

type mergesetDecodedBlockItems struct {
	Count       uint64
	FirstItem   []byte
	LastItem    []byte
	Samples     [][]byte
	Items       [][]byte
	QueryKeys   []string
	Descending  bool
	SeekResults map[string]mergesetSeekResult
}

type mergesetSeekResult struct {
	Item    []byte
	File    string
	Matches bool
}

func decodeMergesetBlockItems(header mergesetBlockHeader, itemsData, lensData []byte, decoder *zstd.Decoder, sampleLimit int, queryKeys []string, descending bool) (mergesetDecodedBlockItems, error) {
	if !bytes.HasPrefix(header.FirstItem, header.CommonPrefix) {
		return mergesetDecodedBlockItems{}, fmt.Errorf("firstItem does not start with commonPrefix")
	}
	switch header.MarshalType {
	case mergesetMarshalTypePlain:
		return decodeMergesetPlainBlockItems(header, itemsData, lensData, sampleLimit, queryKeys, descending)
	case mergesetMarshalTypeZSTD:
		return decodeMergesetZSTDBlockItems(header, itemsData, lensData, decoder, sampleLimit, queryKeys, descending)
	default:
		return mergesetDecodedBlockItems{}, fmt.Errorf("unknown marshalType=%d", header.MarshalType)
	}
}

func decodeMergesetPlainBlockItems(header mergesetBlockHeader, itemsData, lensData []byte, sampleLimit int, queryKeys []string, descending bool) (mergesetDecodedBlockItems, error) {
	itemsCount := int(header.ItemsCount)
	expectedLensBytes := uint64(itemsCount-1) * 8
	if uint64(len(lensData)) != expectedLensBytes {
		return mergesetDecodedBlockItems{}, fmt.Errorf("plain lensData size=%d; want %d", len(lensData), expectedLensBytes)
	}
	lengths := make([]uint64, itemsCount)
	lengths[0] = uint64(len(header.FirstItem) - len(header.CommonPrefix))
	lensTail := lensData
	for i := 1; i < itemsCount; i++ {
		lengths[i] = binary.BigEndian.Uint64(lensTail[:8])
		lensTail = lensTail[8:]
	}

	decoded := newMergesetDecodedBlockItems(header.FirstItem, sampleLimit, queryKeys, descending)
	itemsTail := itemsData
	totalBytes := len(header.FirstItem)
	for i := 1; i < itemsCount; i++ {
		itemLen := lengths[i]
		if itemLen > uint64(len(itemsTail)) {
			return mergesetDecodedBlockItems{}, fmt.Errorf("not enough data for item #%d from itemsData; want %d bytes; remained %d bytes", i+1, itemLen, len(itemsTail))
		}
		if uint64(totalBytes)+uint64(len(header.CommonPrefix))+itemLen > mergesetMaxInmemoryBlockSize {
			return mergesetDecodedBlockItems{}, fmt.Errorf("decoded data exceeds max inmemory block size %d", mergesetMaxInmemoryBlockSize)
		}
		itemLenInt := int(itemLen)
		item := make([]byte, 0, len(header.CommonPrefix)+itemLenInt)
		item = append(item, header.CommonPrefix...)
		item = append(item, itemsTail[:itemLenInt]...)
		if err := decoded.appendItem(item, sampleLimit); err != nil {
			return mergesetDecodedBlockItems{}, err
		}
		totalBytes += len(item)
		itemsTail = itemsTail[itemLenInt:]
	}
	if len(itemsTail) > 0 {
		return mergesetDecodedBlockItems{}, fmt.Errorf("unexpected tail left after itemsData with len %d", len(itemsTail))
	}
	return decoded, nil
}

func decodeMergesetZSTDBlockItems(header mergesetBlockHeader, itemsData, lensData []byte, decoder *zstd.Decoder, sampleLimit int, queryKeys []string, descending bool) (mergesetDecodedBlockItems, error) {
	lensPayload, err := decoder.DecodeAll(lensData, nil)
	if err != nil {
		return mergesetDecodedBlockItems{}, fmt.Errorf("cannot decompress lensData: %w", err)
	}
	valueCount := int(header.ItemsCount) - 1
	if valueCount > len(lensPayload)/2 {
		return mergesetDecodedBlockItems{}, fmt.Errorf("zstd lensData has %d bytes, too small for %d prefix/length values", len(lensPayload), valueCount*2)
	}
	prefixXORs, tail, err := parseMergesetVarUint64s(lensPayload, valueCount)
	if err != nil {
		return mergesetDecodedBlockItems{}, fmt.Errorf("cannot unmarshal prefixLens from lensData: %w", err)
	}
	lengthXORs, tail, err := parseMergesetVarUint64s(tail, valueCount)
	if err != nil {
		return mergesetDecodedBlockItems{}, fmt.Errorf("cannot unmarshal lens from lensData: %w", err)
	}
	if len(tail) > 0 {
		return mergesetDecodedBlockItems{}, fmt.Errorf("unexpected tail left unmarshaling %d lens; tail size=%d", header.ItemsCount, len(tail))
	}
	itemsPayload, err := decoder.DecodeAll(itemsData, nil)
	if err != nil {
		return mergesetDecodedBlockItems{}, fmt.Errorf("cannot decompress itemsData: %w", err)
	}

	prefixLens := make([]uint64, int(header.ItemsCount))
	lengths := make([]uint64, int(header.ItemsCount))
	lengths[0] = uint64(len(header.FirstItem) - len(header.CommonPrefix))
	dataLen := len(header.FirstItem)
	for i, xLen := range prefixXORs {
		prefixLens[i+1] = xLen ^ prefixLens[i]
	}
	for i, xLen := range lengthXORs {
		lengths[i+1] = xLen ^ lengths[i]
		if uint64(dataLen)+uint64(len(header.CommonPrefix))+lengths[i+1] > mergesetMaxInmemoryBlockSize {
			return mergesetDecodedBlockItems{}, fmt.Errorf("decoded data exceeds max inmemory block size %d", mergesetMaxInmemoryBlockSize)
		}
		dataLen += len(header.CommonPrefix) + int(lengths[i+1])
	}

	decoded := newMergesetDecodedBlockItems(header.FirstItem, sampleLimit, queryKeys, descending)
	itemsTail := itemsPayload
	prevItemSuffix := header.FirstItem[len(header.CommonPrefix):]
	for i := 1; i < int(header.ItemsCount); i++ {
		itemLen := lengths[i]
		prefixLen := prefixLens[i]
		if prefixLen > itemLen {
			return mergesetDecodedBlockItems{}, fmt.Errorf("prefixLen=%d exceeds itemLen=%d", prefixLen, itemLen)
		}
		if prefixLen > uint64(len(prevItemSuffix)) {
			return mergesetDecodedBlockItems{}, fmt.Errorf("prefixLen=%d exceeds previous item suffix length=%d", prefixLen, len(prevItemSuffix))
		}
		suffixLen := itemLen - prefixLen
		if suffixLen > uint64(len(itemsTail)) {
			return mergesetDecodedBlockItems{}, fmt.Errorf("not enough data for item #%d from itemsData; want %d bytes; remained %d bytes", i+1, suffixLen, len(itemsTail))
		}
		prefixLenInt := int(prefixLen)
		suffixLenInt := int(suffixLen)
		item := make([]byte, 0, len(header.CommonPrefix)+int(itemLen))
		item = append(item, header.CommonPrefix...)
		item = append(item, prevItemSuffix[:prefixLenInt]...)
		item = append(item, itemsTail[:suffixLenInt]...)
		if err := decoded.appendItem(item, sampleLimit); err != nil {
			return mergesetDecodedBlockItems{}, err
		}
		prevItemSuffix = item[len(header.CommonPrefix):]
		itemsTail = itemsTail[suffixLenInt:]
	}
	if len(itemsTail) > 0 {
		return mergesetDecodedBlockItems{}, fmt.Errorf("unexpected tail left after itemsData with len %d", len(itemsTail))
	}
	return decoded, nil
}

func newMergesetDecodedBlockItems(firstItem []byte, sampleLimit int, queryKeys []string, descending bool) mergesetDecodedBlockItems {
	item := append([]byte(nil), firstItem...)
	decoded := mergesetDecodedBlockItems{
		Count:       1,
		FirstItem:   item,
		LastItem:    item,
		Items:       [][]byte{item},
		QueryKeys:   queryKeys,
		Descending:  descending,
		SeekResults: map[string]mergesetSeekResult{},
	}
	decoded.observeQuerySeek(firstItem)
	if sampleLimit > 0 {
		decoded.Samples = append(decoded.Samples, item)
	}
	return decoded
}

func (decoded *mergesetDecodedBlockItems) appendItem(item []byte, sampleLimit int) error {
	if bytes.Compare(decoded.LastItem, item) > 0 {
		return fmt.Errorf("decoded data block contains unsorted items")
	}
	item = append([]byte(nil), item...)
	decoded.Count++
	decoded.LastItem = item
	decoded.Items = append(decoded.Items, item)
	decoded.observeQuerySeek(item)
	if len(decoded.Samples) < sampleLimit {
		decoded.Samples = append(decoded.Samples, item)
	}
	return nil
}

func (decoded *mergesetDecodedBlockItems) observeQuerySeek(item []byte) {
	if len(decoded.QueryKeys) == 0 {
		return
	}
	for _, key := range decoded.QueryKeys {
		cmp := bytes.Compare(item, []byte(key))
		if decoded.Descending {
			if cmp > 0 {
				continue
			}
			decoded.SeekResults[key] = mergesetSeekResult{
				Item:    append([]byte(nil), item...),
				Matches: cmp == 0,
			}
			continue
		}
		if _, ok := decoded.SeekResults[key]; ok {
			continue
		}
		if cmp < 0 {
			continue
		}
		decoded.SeekResults[key] = mergesetSeekResult{
			Item:    append([]byte(nil), item...),
			Matches: cmp == 0,
		}
	}
}

func parseMergesetVarUint64s(src []byte, count int) ([]uint64, []byte, error) {
	values := make([]uint64, count)
	for i := 0; i < count; i++ {
		value, n := binary.Uvarint(src)
		if n == 0 {
			return nil, src, fmt.Errorf("cannot unmarshal varuint #%d from empty data", i+1)
		}
		if n < 0 {
			return nil, src, fmt.Errorf("varuint #%d overflows uint64", i+1)
		}
		values[i] = value
		src = src[n:]
	}
	return values, src, nil
}

type mergesetSearchPlan struct {
	QueryKeys             []string
	CandidateBlocks       map[int]struct{}
	CandidateBlockByKey   map[string]int
	AdvanceBlockByKey     map[string]int
	CandidateDecodedByKey map[string]struct{}
	MatchedKeys           map[string]struct{}
	SeekResults           map[string]mergesetSeekResult
	SampleLimit           int
	DecodePath            *DecodePathSummary
}

type mergesetScanPlan struct {
	SampleLimit int
	DecodePath  *DecodePathSummary
}

func newMergesetSearchPlan(headers []mergesetBlockHeader, options Options, firstItem, lastItem []byte) *mergesetSearchPlan {
	plan := &mergesetSearchPlan{}
	if len(options.QueryKeys) == 0 {
		return plan
	}
	plan.QueryKeys = append([]string(nil), options.QueryKeys...)
	plan.CandidateBlocks = map[int]struct{}{}
	plan.CandidateBlockByKey = map[string]int{}
	plan.AdvanceBlockByKey = map[string]int{}
	plan.CandidateDecodedByKey = map[string]struct{}{}
	plan.MatchedKeys = map[string]struct{}{}
	plan.SeekResults = map[string]mergesetSeekResult{}
	plan.SampleLimit = options.BlockSampleLimit
	for _, key := range options.QueryKeys {
		queryItem := []byte(key)
		var idx int
		if options.CursorDescending {
			if bytes.Compare(queryItem, firstItem) < 0 {
				continue
			}
			idx = len(headers) - 1
			if bytes.Compare(queryItem, lastItem) < 0 {
				idx = sort.Search(len(headers), func(i int) bool {
					return bytes.Compare(headers[i].FirstItem, queryItem) > 0
				}) - 1
			}
		} else {
			if bytes.Compare(queryItem, lastItem) > 0 {
				continue
			}
			nextIdx := -1
			idx = 0
			if bytes.Compare(queryItem, firstItem) > 0 {
				nextIdx = sort.Search(len(headers), func(i int) bool {
					return bytes.Compare(headers[i].FirstItem, queryItem) > 0
				})
				idx = nextIdx - 1
			}
			if nextIdx >= 0 && nextIdx < len(headers) {
				plan.AdvanceBlockByKey[key] = nextIdx
			}
		}
		if idx >= 0 {
			plan.CandidateBlocks[idx] = struct{}{}
			plan.CandidateBlockByKey[key] = idx
		}
	}
	plan.DecodePath = &DecodePathSummary{
		Mode:                 mergesetSearchMode(options),
		QueryKeys:            append([]string(nil), options.QueryKeys...),
		KeyFilterApplied:     true,
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	return plan
}

func newMergesetScanPlan(options Options) *mergesetScanPlan {
	plan := &mergesetScanPlan{}
	if len(options.QueryKeys) > 0 {
		return plan
	}
	plan.SampleLimit = options.BlockSampleLimit
	plan.DecodePath = &DecodePathSummary{
		Mode:                 mergesetScanMode(options),
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	return plan
}

func (p *mergesetScanPlan) ObserveHeader(index int, header mergesetBlockHeader) {
	if p == nil || p.DecodePath == nil {
		return
	}
	summary := p.DecodePath
	blockBytes := int64(header.ItemsBlockSize) + int64(header.LensBlockSize)
	summary.BaselineDecodeBlocks++
	summary.BaselineDecodeValues += uint64ToInt(uint64(header.ItemsCount))
	summary.BaselineDecodeBytes += blockBytes
	summary.OptimizedDecodeBlocks++
	summary.FilteredDecodeBlocks++
	summary.LocationBlocks++
	summary.OptimizedDecodeValues += uint64ToInt(uint64(header.ItemsCount))
	summary.OptimizedDecodeBytes += blockBytes
	summary.LocationBlocksByType["mergeset-block"]++
	summary.DecodeBlocksByType["mergeset-block"]++
	if p.SampleLimit <= 0 || len(summary.Samples) >= p.SampleLimit {
		return
	}
	summary.Samples = append(summary.Samples, DecodePathBlockDecision{
		Key:               hex.EncodeToString(header.FirstItem),
		Type:              "mergeset-block",
		SizeBytes:         addUint32Saturating(header.ItemsBlockSize, header.LensBlockSize),
		ValueCount:        uint64ToInt(uint64(header.ItemsCount)),
		LocationCandidate: true,
		Decoded:           true,
		Reason:            "table_scan_block",
	})
}

func (p *mergesetScanPlan) ObserveDecodedBlock(index int, header mergesetBlockHeader, decoded mergesetDecodedBlockItems) {
	if p == nil || p.DecodePath == nil {
		return
	}
	summary := p.DecodePath
	summary.OptimizedOutputValues += uint64ToInt(decoded.Count)
	summary.mergesetScanItems = append(summary.mergesetScanItems, decoded.Items...)
	if p.SampleLimit > 0 && len(summary.CursorWindows) < p.SampleLimit {
		window := DecodePathCursorWindow{
			Key:             hex.EncodeToString(header.FirstItem),
			LocationBlocks:  1,
			DecodedBlocks:   1,
			Reason:          "table_scan_block",
			FirstBlockIndex: index,
		}
		if p.DecodePath.Mode == "mergeset-table-scan-descending" {
			summary.CursorWindows = append([]DecodePathCursorWindow{window}, summary.CursorWindows...)
		} else {
			summary.CursorWindows = append(summary.CursorWindows, window)
		}
	}
	if p.SampleLimit <= 0 {
		return
	}
	for _, item := range decoded.Samples {
		if len(summary.CursorOutputSamples) >= p.SampleLimit {
			break
		}
		output := DecodePathCursorOutput{
			Key:            string(item),
			Type:           "mergeset-table-scan-item",
			OptimizedValue: string(item),
			Matches:        true,
		}
		if p.DecodePath.Mode == "mergeset-table-scan-descending" {
			summary.CursorOutputSamples = append([]DecodePathCursorOutput{output}, summary.CursorOutputSamples...)
		} else {
			summary.CursorOutputSamples = append(summary.CursorOutputSamples, output)
		}
	}
}

func (p *mergesetScanPlan) Finish(options Options) {
	if p == nil || p.DecodePath == nil {
		return
	}
	summary := p.DecodePath
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	summary.BaselineOutputValues = summary.BaselineDecodeValues
	summary.CursorWindowCount = summary.LocationBlocks
	if summary.OptimizedDecodeBlocks > 0 {
		summary.Amplification = float64(summary.BaselineDecodeBlocks) / float64(summary.OptimizedDecodeBlocks)
	}
	summary.Recommendations = mergesetScanRecommendations(summary, options)
}

func (p *mergesetSearchPlan) ObserveHeader(index int, header mergesetBlockHeader) {
	if p == nil || p.DecodePath == nil {
		return
	}
	summary := p.DecodePath
	blockBytes := int64(header.ItemsBlockSize) + int64(header.LensBlockSize)
	summary.BaselineDecodeBlocks++
	summary.BaselineDecodeValues += uint64ToInt(uint64(header.ItemsCount))
	summary.BaselineDecodeBytes += blockBytes

	candidate, reason := p.searchBlockCandidate(index)
	if candidate {
		summary.OptimizedDecodeBlocks++
		summary.FilteredDecodeBlocks++
		summary.LocationBlocks++
		summary.OptimizedDecodeValues += uint64ToInt(uint64(header.ItemsCount))
		summary.OptimizedDecodeBytes += blockBytes
		summary.LocationBlocksByType["mergeset-block"]++
		summary.DecodeBlocksByType["mergeset-block"]++
	} else {
		summary.SkippedByKeyBlocks++
	}
	if len(summary.Samples) < p.SampleLimit {
		summary.Samples = append(summary.Samples, DecodePathBlockDecision{
			Key:               hex.EncodeToString(header.FirstItem),
			Type:              "mergeset-block",
			SizeBytes:         addUint32Saturating(header.ItemsBlockSize, header.LensBlockSize),
			ValueCount:        uint64ToInt(uint64(header.ItemsCount)),
			LocationCandidate: candidate,
			Decoded:           candidate,
			Reason:            reason,
		})
	}
	if candidate && p.SampleLimit > 0 && len(summary.CursorWindows) < p.SampleLimit {
		window := DecodePathCursorWindow{
			Key:             hex.EncodeToString(header.FirstItem),
			LocationBlocks:  1,
			DecodedBlocks:   1,
			Reason:          reason,
			FirstBlockIndex: index,
		}
		if p.DecodePath.Mode == "mergeset-item-search-descending" {
			summary.CursorWindows = append([]DecodePathCursorWindow{window}, summary.CursorWindows...)
		} else {
			summary.CursorWindows = append(summary.CursorWindows, window)
		}
	}
}

func (p *mergesetSearchPlan) searchBlockCandidate(index int) (bool, string) {
	if _, candidate := p.CandidateBlocks[index]; candidate {
		return true, "key_range_candidate"
	}
	if p.hasPendingAdvanceBlock(index) {
		return true, "cursor_advance_candidate"
	}
	return false, "key_not_in_block_range"
}

func (p *mergesetSearchPlan) hasPendingAdvanceBlock(index int) bool {
	for _, key := range p.QueryKeys {
		advanceBlock, ok := p.AdvanceBlockByKey[key]
		if !ok || advanceBlock != index {
			continue
		}
		if _, resolved := p.SeekResults[key]; !resolved {
			if _, decoded := p.CandidateDecodedByKey[key]; !decoded {
				continue
			}
			return true
		}
	}
	return false
}

func (p *mergesetSearchPlan) ObserveDecodedBlock(index int, decoded mergesetDecodedBlockItems) {
	if p == nil || p.DecodePath == nil {
		return
	}
	if candidate, _ := p.searchBlockCandidate(index); !candidate {
		return
	}
	for _, key := range p.QueryKeys {
		directCandidate := false
		if candidateBlock, ok := p.CandidateBlockByKey[key]; ok && candidateBlock == index {
			directCandidate = true
			p.CandidateDecodedByKey[key] = struct{}{}
		}
		advanceCandidate := false
		if advanceBlock, ok := p.AdvanceBlockByKey[key]; ok && advanceBlock == index {
			_, resolved := p.SeekResults[key]
			_, directDecoded := p.CandidateDecodedByKey[key]
			advanceCandidate = directDecoded && !resolved
		}
		if !directCandidate && !advanceCandidate {
			continue
		}
		result, ok := decoded.SeekResults[key]
		if !ok {
			continue
		}
		p.SeekResults[key] = result
		if advanceCandidate {
			p.DecodePath.TableSearchCursorAdvances++
		}
		if !result.Matches {
			if len(p.DecodePath.CursorOutputSamples) < p.SampleLimit {
				p.DecodePath.CursorOutputSamples = append(p.DecodePath.CursorOutputSamples, DecodePathCursorOutput{
					Key:            key,
					Type:           "mergeset-item",
					OptimizedValue: string(result.Item),
					Matches:        false,
				})
			}
			continue
		}
		p.MatchedKeys[key] = struct{}{}
		if len(p.DecodePath.CursorOutputSamples) < p.SampleLimit {
			p.DecodePath.CursorOutputSamples = append(p.DecodePath.CursorOutputSamples, DecodePathCursorOutput{
				Key:            key,
				Type:           "mergeset-item",
				OptimizedValue: string(result.Item),
				Matches:        true,
			})
		}
	}
}

func (p *mergesetSearchPlan) Finish(options Options) {
	if p == nil || p.DecodePath == nil {
		return
	}
	summary := p.DecodePath
	for _, key := range p.QueryKeys {
		if _, ok := p.MatchedKeys[key]; ok {
			summary.MatchedKeys = append(summary.MatchedKeys, key)
		} else {
			summary.MissingKeys = append(summary.MissingKeys, key)
		}
	}
	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	summary.BaselineOutputValues = len(p.QueryKeys)
	summary.OptimizedOutputValues = len(summary.MatchedKeys)
	summary.CursorWindowCount = summary.LocationBlocks
	summary.TableSearchSeekCalls = len(p.QueryKeys)
	summary.TableSearchHeapCandidates = len(p.SeekResults)
	summary.TableSearchOutputValues = len(p.SeekResults)
	summary.TableSearchExactMisses = len(summary.MissingKeys)
	summary.mergesetSeekResults = p.SeekResults
	populateMergesetSearchFinalOutputSamples(summary, p.SeekResults, p.QueryKeys, p.SampleLimit)
	if summary.OptimizedDecodeBlocks > 0 {
		summary.Amplification = float64(summary.BaselineDecodeBlocks) / float64(summary.OptimizedDecodeBlocks)
	}
	summary.Recommendations = mergesetSearchRecommendations(summary, options)
}

func populateMergesetSearchFinalOutputSamples(summary *DecodePathSummary, seekResults map[string]mergesetSeekResult, queryKeys []string, sampleLimit int) {
	if summary == nil || sampleLimit <= 0 {
		return
	}
	for _, key := range queryKeys {
		if len(summary.CursorFinalOutputSamples) >= sampleLimit {
			return
		}
		result, ok := seekResults[key]
		if !ok || !result.Matches {
			continue
		}
		summary.CursorFinalOutputSamples = append(summary.CursorFinalOutputSamples, DecodePathCursorOutput{
			Key:            key,
			Type:           "mergeset-item-search-final-output-item",
			OptimizedValue: string(result.Item),
			Matches:        true,
		})
	}
}

func mergesetSearchMode(options Options) string {
	if options.CursorDescending {
		return "mergeset-item-search-descending"
	}
	return "mergeset-item-search-ascending"
}

func mergesetScanMode(options Options) string {
	if options.CursorDescending {
		return "mergeset-table-scan-descending"
	}
	return "mergeset-table-scan-ascending"
}

func mergesetScanRecommendations(summary *DecodePathSummary, options Options) []string {
	recommendations := []string{}
	if summary.OptimizedOutputValues > 0 {
		recommendations = append(recommendations, fmt.Sprintf("scan %d decoded mergeset item(s) across %d block(s)", summary.OptimizedOutputValues, summary.OptimizedDecodeBlocks))
	}
	if len(summary.CursorOutputSamples) > 0 {
		recommendations = append(recommendations, "decoded item payloads are available for table scan output samples")
	}
	if len(recommendations) == 0 {
		recommendations = append(recommendations, "mergeset table scan has no decoded item payload candidates")
	}
	return recommendations
}

func mergesetSearchRecommendations(summary *DecodePathSummary, options Options) []string {
	recommendations := []string{}
	if len(summary.MissingKeys) > 0 {
		recommendations = append(recommendations, fmt.Sprintf("%d query item key(s) were not found in this mergeset part", len(summary.MissingKeys)))
	}
	if summary.SavedDecodeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf("sorted item lookup skips %d mergeset block(s) before payload inspection", summary.SavedDecodeBlocks))
	}
	if summary.TableSearchCursorAdvances > 0 {
		recommendations = append(recommendations, fmt.Sprintf("advanced %d local mergeset cursor step(s) to reach the next item candidate", summary.TableSearchCursorAdvances))
	}
	if len(summary.CursorFinalOutputSamples) > 0 {
		recommendations = append(recommendations, "final item-search output samples show exact local mergeset seek results")
	}
	if len(recommendations) == 0 && len(options.QueryKeys) > 0 {
		recommendations = append(recommendations, "all query item keys mapped to decoded mergeset block candidates")
	}
	return recommendations
}

func addMergesetItemPayloadSummary(report *FileReport, summary mergesetItemPayloadSummary, metadataFirstItem, metadataLastItem []byte, metadataItemsCount uint64, options Options) {
	report.Extra["item_payload_block_count"] = fmt.Sprint(summary.Blocks)
	report.Extra["item_payload_blocks_decoded"] = fmt.Sprint(summary.DecodedBlocks)
	report.Extra["item_payload_items_decoded"] = fmt.Sprint(summary.ItemsDecoded)
	if summary.DecodePath != nil {
		report.DecodePath = summary.DecodePath
		if len(summary.DecodePath.MatchedKeys) > 0 {
			report.QueryOverlapsFile = true
			report.QueryOverlapBlocks = summary.DecodePath.OptimizedDecodeBlocks
		}
	}
	if summary.ItemsDecoded == 0 {
		return
	}
	report.Extra["item_payload_first_item_hex"] = hex.EncodeToString(summary.FirstItem)
	report.Extra["item_payload_last_item_hex"] = hex.EncodeToString(summary.LastItem)
	if len(summary.Samples) > 0 {
		samples := make([]string, 0, len(summary.Samples))
		for _, sample := range summary.Samples {
			samples = append(samples, hex.EncodeToString(sample))
		}
		report.Extra["item_payload_samples_hex"] = strings.Join(samples, ",")
	}
	addMergesetCLVTextIndexSummary(report, summary.CLVText)
	addMergesetTSIIndexSummary(report, summary.TSIIndex, options)
	addMergesetFieldIndexSummary(report, summary.FieldIndex)
	if summary.ItemsDecoded != metadataItemsCount {
		report.Notices = append(report.Notices, fmt.Sprintf("mergeset decoded item payload count=%d differs from metadata items_count=%d", summary.ItemsDecoded, metadataItemsCount))
	}
	if !bytes.Equal(summary.FirstItem, metadataFirstItem) {
		report.Notices = append(report.Notices, fmt.Sprintf("mergeset decoded first item=%s differs from metadata first_item=%s", hex.EncodeToString(summary.FirstItem), hex.EncodeToString(metadataFirstItem)))
	}
	if !bytes.Equal(summary.LastItem, metadataLastItem) {
		report.Notices = append(report.Notices, fmt.Sprintf("mergeset decoded last item=%s differs from metadata last_item=%s", hex.EncodeToString(summary.LastItem), hex.EncodeToString(metadataLastItem)))
	}
}

const (
	opengeminiTSINSPrefixKeyToTSID          byte = 0
	opengeminiTSINSPrefixTSIDToKey          byte = 1
	opengeminiTSINSPrefixTagToTSIDs         byte = 2
	opengeminiTSINSPrefixTSIDToField        byte = 4
	opengeminiTSINSFieldToPID               byte = 5
	opengeminiTSINSMstToFieldKey            byte = 6
	opengeminiTSINSPrefixTagKeysToTagValues byte = 7
	opengeminiTSIEscape                     byte = 0
	opengeminiTSITagSeparator               byte = 1
	opengeminiTSINSSeparator                byte = 2
	opengeminiTSICompositeTagKeyPrefix      byte = 0xfe
	opengeminiTSIDToFieldItemSize                = 10

	opengeminiCLVPrefixPos        byte = 0
	opengeminiCLVPrefixTerm       byte = 1
	opengeminiCLVPrefixDictionary byte = 2
	opengeminiCLVPrefixVersion    byte = 3
	opengeminiCLVPrefixSID        byte = 4
	opengeminiCLVPrefixID         byte = 5
	opengeminiCLVPrefixMeta       byte = 6
	opengeminiCLVSuffix           byte = 9
	opengeminiCLVPosFlag          byte = 1
	opengeminiCLVIDFlag           byte = 2
)

type mergesetTSIIndexItem struct {
	Type        string
	SeriesKey   string
	Measurement string
	TagKey      string
	TagValue    string
	Tags        []TagFilter
	TSID        uint64
	TSIDs       []uint64
}

type openGeminiTSIParsedSeriesKey struct {
	Measurement string
	Tags        []TagFilter
	Display     string
}

func observeMergesetTSIIndexItems(summary *mergesetTSIIndexSummary, items [][]byte, sampleLimit int, clvTextDetected bool, trackQueryState bool) {
	for _, item := range items {
		parsed, err := parseMergesetTSIIndexItem(item)
		if err != nil {
			if isMergesetTSIIndexCandidate(item) {
				if clv, clvErr := parseMergesetCLVTextIndexItem(item); clvErr == nil && (isMergesetCLVTextIndexEvidence(clv) || clv.Type == "term" || clvTextDetected && isMergesetCLVTextIndexContextual(clv)) {
					continue
				}
				if clvTextDetected && isMergesetCLVTextIndexPrefix(item) {
					continue
				}
				if !clvTextDetected && isMergesetCLVTextIndexPrefix(item) {
					summary.DeferredCLVInvalids++
					continue
				}
				summary.Detected = true
				summary.InvalidItems++
			}
			continue
		}
		summary.Detected = true
		switch parsed.Type {
		case "key-tsid":
			summary.KeyToTSIDCount++
			observeMergesetTSISeriesKey(summary, parsed, trackQueryState)
			if sampleLimit > 0 && len(summary.KeyToTSIDSamples) < sampleLimit {
				summary.KeyToTSIDSamples = append(summary.KeyToTSIDSamples, fmt.Sprintf("%s->%d", parsed.SeriesKey, parsed.TSID))
			}
		case "tsid-key":
			summary.TSIDToKeyCount++
			observeMergesetTSISeriesKey(summary, parsed, trackQueryState)
			if sampleLimit > 0 && len(summary.TSIDToKeySamples) < sampleLimit {
				summary.TSIDToKeySamples = append(summary.TSIDToKeySamples, fmt.Sprintf("%d:%s", parsed.TSID, parsed.SeriesKey))
			}
		case "tag-tsid":
			summary.TagToTSIDCount++
			summary.TagToTSIDValueCount += len(parsed.TSIDs)
			observeMergesetTSITagTSIDs(summary, parsed, trackQueryState)
			if sampleLimit > 0 && len(summary.TagToTSIDSamples) < sampleLimit {
				summary.TagToTSIDSamples = append(summary.TagToTSIDSamples, formatMergesetTSITagToTSIDSample(parsed))
			}
		case "tag-value":
			summary.TagValueCount++
			observeMergesetTSITagValue(summary, parsed)
			if sampleLimit > 0 && len(summary.TagValueSamples) < sampleLimit {
				summary.TagValueSamples = append(summary.TagValueSamples, fmt.Sprintf("%s:%s=%s", parsed.Measurement, parsed.TagKey, parsed.TagValue))
			}
		}
	}
}

func observeMergesetTSISeriesKey(summary *mergesetTSIIndexSummary, item mergesetTSIIndexItem, trackQueryState bool) {
	measurement := summary.ensureMeasurement(item.Measurement)
	measurement.addSeriesID(item.TSID)
	for _, tag := range item.Tags {
		measurement.addTagValueSeriesIDs(tag.Key, tag.Value, []uint64{item.TSID}, trackQueryState)
	}
}

func observeMergesetTSITagTSIDs(summary *mergesetTSIIndexSummary, item mergesetTSIIndexItem, trackQueryState bool) {
	measurement := summary.ensureMeasurement(item.Measurement)
	measurement.addTagValueSeriesIDs(item.TagKey, item.TagValue, item.TSIDs, trackQueryState)
}

func observeMergesetTSITagValue(summary *mergesetTSIIndexSummary, item mergesetTSIIndexItem) {
	measurement := summary.ensureMeasurement(item.Measurement)
	measurement.addTagValue(item.TagKey, item.TagValue)
}

func (s *mergesetTSIIndexSummary) ensureMeasurement(name string) *mergesetTSIMeasurement {
	if s.measurements == nil {
		s.measurements = map[string]*mergesetTSIMeasurement{}
	}
	measurement := s.measurements[name]
	if measurement == nil {
		measurement = &mergesetTSIMeasurement{
			Name:      name,
			SeriesIDs: map[uint64]struct{}{},
			TagValues: map[string]map[string]struct{}{},
		}
		s.measurements[name] = measurement
	}
	return measurement
}

func (m *mergesetTSIMeasurement) addSeriesID(tsid uint64) {
	m.SeriesIDs[tsid] = struct{}{}
}

func (m *mergesetTSIMeasurement) addTagValue(tagKey, tagValue string) {
	values := m.TagValues[tagKey]
	if values == nil {
		values = map[string]struct{}{}
		m.TagValues[tagKey] = values
	}
	values[tagValue] = struct{}{}
}

func (m *mergesetTSIMeasurement) addTagValueSeriesIDs(tagKey, tagValue string, tsids []uint64, trackQueryState bool) {
	m.addTagValue(tagKey, tagValue)
	for _, tsid := range tsids {
		m.addSeriesID(tsid)
	}
	if !trackQueryState {
		return
	}
	if m.TagValueSeriesIDs == nil {
		m.TagValueSeriesIDs = map[string]map[string]map[uint64]struct{}{}
	}
	values := m.TagValueSeriesIDs[tagKey]
	if values == nil {
		values = map[string]map[uint64]struct{}{}
		m.TagValueSeriesIDs[tagKey] = values
	}
	seriesIDs := values[tagValue]
	if seriesIDs == nil {
		seriesIDs = map[uint64]struct{}{}
		values[tagValue] = seriesIDs
	}
	for _, tsid := range tsids {
		seriesIDs[tsid] = struct{}{}
	}
}

func parseMergesetTSIIndexItem(item []byte) (mergesetTSIIndexItem, error) {
	var parsed mergesetTSIIndexItem
	if len(item) == 0 {
		return parsed, fmt.Errorf("empty TSI index item")
	}
	switch item[0] {
	case opengeminiTSINSPrefixKeyToTSID:
		if len(item) < 18 {
			return parsed, fmt.Errorf("short key-to-tsid item")
		}
		sep := len(item) - 9
		if sep < 1 || item[sep] != opengeminiTSINSSeparator {
			return parsed, fmt.Errorf("key-to-tsid item missing separator")
		}
		seriesKey, err := parseOpenGeminiTSIIndexKeyStruct(item[1:sep])
		if err != nil {
			return parsed, err
		}
		parsed.Type = "key-tsid"
		parsed.SeriesKey = seriesKey.Display
		parsed.Measurement = seriesKey.Measurement
		parsed.Tags = seriesKey.Tags
		parsed.TSID = binary.BigEndian.Uint64(item[len(item)-8:])
		return parsed, nil
	case opengeminiTSINSPrefixTSIDToKey:
		if len(item) < 17 {
			return parsed, fmt.Errorf("short tsid-to-key item")
		}
		seriesKey, err := parseOpenGeminiTSIIndexKeyStruct(item[9:])
		if err != nil {
			return parsed, err
		}
		parsed.Type = "tsid-key"
		parsed.TSID = binary.BigEndian.Uint64(item[1:9])
		parsed.SeriesKey = seriesKey.Display
		parsed.Measurement = seriesKey.Measurement
		parsed.Tags = seriesKey.Tags
		return parsed, nil
	case opengeminiTSINSPrefixTagToTSIDs:
		if len(item) < 13 {
			return parsed, fmt.Errorf("short tag-to-tsid item")
		}
		parsed.Type = "tag-tsid"
		measurement, tagKey, tagValue, tsidBytes, err := parseOpenGeminiTSITagTupleWithTail(item[1:])
		if err != nil {
			return parsed, err
		}
		if len(tsidBytes) == 0 || len(tsidBytes)%8 != 0 {
			return parsed, fmt.Errorf("tag-to-tsid item has %d trailing TSID byte(s)", len(tsidBytes))
		}
		parsed.Measurement = measurement
		parsed.TagKey = tagKey
		parsed.TagValue = tagValue
		for len(tsidBytes) > 0 {
			parsed.TSIDs = append(parsed.TSIDs, binary.BigEndian.Uint64(tsidBytes[:8]))
			tsidBytes = tsidBytes[8:]
		}
		parsed.TSID = parsed.TSIDs[0]
		return parsed, nil
	case opengeminiTSINSPrefixTagKeysToTagValues:
		if len(item) < 5 {
			return parsed, fmt.Errorf("short tag-key-to-value item")
		}
		parsed.Type = "tag-value"
		measurement, tagKey, tagValue, err := parseOpenGeminiTSITagTuple(item[1:])
		if err != nil {
			return parsed, err
		}
		parsed.Measurement = measurement
		parsed.TagKey = tagKey
		parsed.TagValue = tagValue
		return parsed, nil
	default:
		return parsed, fmt.Errorf("not an openGemini TSI index item")
	}
}

func parseOpenGeminiTSITagTuple(data []byte) (string, string, string, error) {
	measurement, tagKey, tagValue, tail, err := parseOpenGeminiTSITagTupleWithTail(data)
	if err != nil {
		return "", "", "", err
	}
	if len(tail) != 0 {
		return "", "", "", fmt.Errorf("tag tuple has %d trailing byte(s)", len(tail))
	}
	return measurement, tagKey, tagValue, nil
}

func parseOpenGeminiTSITagTupleWithTail(data []byte) (string, string, string, []byte, error) {
	tail, compositeKey, err := parseOpenGeminiTSITagValue(data)
	if err != nil {
		return "", "", "", nil, err
	}
	tail, tagValue, err := parseOpenGeminiTSITagValue(tail)
	if err != nil {
		return "", "", "", nil, err
	}
	measurement, tagKey, err := parseOpenGeminiTSICompositeTagKey(compositeKey)
	if err != nil {
		return "", "", "", nil, err
	}
	return measurement, tagKey, string(tagValue), tail, nil
}

func parseOpenGeminiTSIIndexKeyStruct(data []byte) (openGeminiTSIParsedSeriesKey, error) {
	var parsed openGeminiTSIParsedSeriesKey
	if len(data) < 8 {
		return parsed, fmt.Errorf("short TSI index key")
	}
	keyLen := int(binary.BigEndian.Uint32(data[:4]))
	if keyLen != len(data) {
		return parsed, fmt.Errorf("TSI index key length=%d differs from payload size=%d", keyLen, len(data))
	}
	tail := data[4:]
	if len(tail) < 2 {
		return parsed, fmt.Errorf("short TSI measurement length")
	}
	measurementLen := int(binary.BigEndian.Uint16(tail[:2]))
	tail = tail[2:]
	if len(tail) < measurementLen+2 {
		return parsed, fmt.Errorf("short TSI measurement")
	}
	measurement := string(tail[:measurementLen])
	tail = tail[measurementLen:]
	tagCount := int(binary.BigEndian.Uint16(tail[:2]))
	tail = tail[2:]

	var display strings.Builder
	display.WriteString(measurement)
	parsed.Measurement = measurement
	parsed.Tags = make([]TagFilter, 0, tagCount)
	for i := 0; i < tagCount; i++ {
		if len(tail) < 2 {
			return parsed, fmt.Errorf("short TSI tag key length")
		}
		tagKeyLen := int(binary.BigEndian.Uint16(tail[:2]))
		tail = tail[2:]
		if len(tail) < tagKeyLen+2 {
			return parsed, fmt.Errorf("short TSI tag key")
		}
		tagKey := string(tail[:tagKeyLen])
		tail = tail[tagKeyLen:]
		tagValueLen := int(binary.BigEndian.Uint16(tail[:2]))
		tail = tail[2:]
		if len(tail) < tagValueLen {
			return parsed, fmt.Errorf("short TSI tag value")
		}
		tagValue := string(tail[:tagValueLen])
		tail = tail[tagValueLen:]
		parsed.Tags = append(parsed.Tags, TagFilter{Key: tagKey, Value: tagValue})
		display.WriteByte(',')
		display.WriteString(tagKey)
		display.WriteByte('=')
		display.WriteString(tagValue)
	}
	if len(tail) != 0 {
		return parsed, fmt.Errorf("TSI index key has %d trailing byte(s)", len(tail))
	}
	parsed.Display = display.String()
	return parsed, nil
}

func parseOpenGeminiTSITagValue(data []byte) ([]byte, []byte, error) {
	value := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case opengeminiTSITagSeparator:
			return data[i+1:], value, nil
		case opengeminiTSIEscape:
			i++
			if i >= len(data) {
				return nil, nil, fmt.Errorf("truncated TSI tag escape")
			}
			switch data[i] {
			case '0':
				value = append(value, opengeminiTSIEscape)
			case '1':
				value = append(value, opengeminiTSITagSeparator)
			case '2':
				value = append(value, opengeminiTSINSSeparator)
			default:
				return nil, nil, fmt.Errorf("invalid TSI tag escape %q", data[i])
			}
		default:
			value = append(value, data[i])
		}
	}
	return nil, nil, fmt.Errorf("missing TSI tag separator")
}

func parseOpenGeminiTSICompositeTagKey(data []byte) (string, string, error) {
	if len(data) == 0 || data[0] != opengeminiTSICompositeTagKeyPrefix {
		return "", "", fmt.Errorf("invalid composite tag key prefix")
	}
	nameLen, n := binary.Uvarint(data[1:])
	if n <= 0 {
		return "", "", fmt.Errorf("invalid composite tag measurement length")
	}
	tail := data[1+n:]
	if uint64(len(tail)) < nameLen {
		return "", "", fmt.Errorf("short composite tag measurement")
	}
	measurement := string(tail[:nameLen])
	tagKey := string(tail[nameLen:])
	return measurement, tagKey, nil
}

func isMergesetTSIIndexCandidate(item []byte) bool {
	if len(item) == 0 {
		return false
	}
	switch item[0] {
	case opengeminiTSINSPrefixKeyToTSID:
		return len(item) >= 18
	case opengeminiTSINSPrefixTSIDToKey:
		return len(item) >= 17
	case opengeminiTSINSPrefixTagToTSIDs:
		return len(item) >= 13
	case opengeminiTSINSPrefixTagKeysToTagValues:
		return len(item) >= 5
	default:
		return false
	}
}

func formatMergesetTSITagToTSIDSample(item mergesetTSIIndexItem) string {
	prefix := item.Measurement
	if item.TagKey != "" {
		prefix = fmt.Sprintf("%s:%s=%s", item.Measurement, item.TagKey, item.TagValue)
	}
	return prefix + "->" + formatMergesetTSIListSample(item.TSIDs)
}

func formatMergesetTSIListSample(values []uint64) string {
	if len(values) == 0 {
		return "[]"
	}
	if len(values) == 1 {
		return fmt.Sprint(values[0])
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprint(value))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func addMergesetTSIIndexSummary(report *FileReport, summary mergesetTSIIndexSummary, options Options) {
	if !summary.Detected {
		return
	}
	if index := buildMergesetTSIIndexReport(summary, options); index != nil {
		report.Index = index
	}
	report.Extra["opengemini_tsi_index_detected"] = "true"
	report.Extra["opengemini_tsi_index_key_tsid_mappings"] = fmt.Sprint(summary.KeyToTSIDCount)
	report.Extra["opengemini_tsi_index_tsid_key_mappings"] = fmt.Sprint(summary.TSIDToKeyCount)
	report.Extra["opengemini_tsi_index_tag_tsid_mappings"] = fmt.Sprint(summary.TagToTSIDCount)
	report.Extra["opengemini_tsi_index_tag_tsid_values"] = fmt.Sprint(summary.TagToTSIDValueCount)
	report.Extra["opengemini_tsi_index_tag_value_mappings"] = fmt.Sprint(summary.TagValueCount)
	report.Extra["opengemini_tsi_index_invalid_items"] = fmt.Sprint(summary.InvalidItems)
	if len(summary.KeyToTSIDSamples) > 0 {
		report.Extra["opengemini_tsi_index_key_tsid_samples"] = strings.Join(summary.KeyToTSIDSamples, ",")
	}
	if len(summary.TSIDToKeySamples) > 0 {
		report.Extra["opengemini_tsi_index_tsid_key_samples"] = strings.Join(summary.TSIDToKeySamples, ",")
	}
	if len(summary.TagToTSIDSamples) > 0 {
		report.Extra["opengemini_tsi_index_tag_tsid_samples"] = strings.Join(summary.TagToTSIDSamples, ",")
	}
	if len(summary.TagValueSamples) > 0 {
		report.Extra["opengemini_tsi_index_tag_value_samples"] = strings.Join(summary.TagValueSamples, ",")
	}
	report.BlocksByType["opengemini-tsi-index-key-tsid"] = summary.KeyToTSIDCount
	report.BlocksByType["opengemini-tsi-index-tsid-key"] = summary.TSIDToKeyCount
	report.BlocksByType["opengemini-tsi-index-tag-tsid"] = summary.TagToTSIDCount
	report.BlocksByType["opengemini-tsi-index-tag-tsid-value"] = summary.TagToTSIDValueCount
	report.BlocksByType["opengemini-tsi-index-tag-value"] = summary.TagValueCount
	if summary.InvalidItems > 0 {
		report.BlocksByType["opengemini-tsi-index-invalid-item"] = summary.InvalidItems
		report.Notices = append(report.Notices, fmt.Sprintf("openGemini TSI index has %d invalid namespaced item(s)", summary.InvalidItems))
	}
}

func buildMergesetTSIIndexReport(summary mergesetTSIIndexSummary, options Options) *IndexSummary {
	if len(summary.measurements) == 0 {
		return nil
	}
	names := sortedMergesetTSIMeasurementNames(summary.measurements)
	index := &IndexSummary{
		Type:             "opengemini-tsi-mergeset",
		MeasurementCount: len(names),
	}
	for _, name := range names {
		measurement := summary.measurements[name]
		tagKeyCount, tagValueCount := measurement.tagCounts()
		index.SeriesRefs += int64(len(measurement.SeriesIDs))
		index.TagKeyCount += tagKeyCount
		index.TagValueCount += tagValueCount
		if len(index.MeasurementSamples) < options.KeySampleLimit {
			index.MeasurementSamples = append(index.MeasurementSamples, IndexMeasurementReport{
				Name:          name,
				SeriesCount:   uint64(len(measurement.SeriesIDs)),
				TagKeyCount:   tagKeyCount,
				TagValueCount: tagValueCount,
			})
		}
	}
	index.Query = buildMergesetTSIIndexQuerySummary(summary.measurements, names, options)
	return index
}

func buildMergesetTSIIndexQuerySummary(measurements map[string]*mergesetTSIMeasurement, names []string, options Options) *IndexQuerySummary {
	if len(options.QueryMeasurements) == 0 && len(options.QueryTags) == 0 {
		return nil
	}
	query := &IndexQuerySummary{
		MeasurementFilterApplied: len(options.QueryMeasurements) > 0,
		TagFilterApplied:         len(options.QueryTags) > 0,
		QueryMeasurements:        append([]string(nil), options.QueryMeasurements...),
		QueryTags:                append([]TagFilter(nil), options.QueryTags...),
	}
	measurementSet := queryKeySet(options.QueryMeasurements)
	for _, measurement := range query.QueryMeasurements {
		if _, ok := measurements[measurement]; ok {
			query.MatchedMeasurements = append(query.MatchedMeasurements, measurement)
		} else {
			query.MissingMeasurements = append(query.MissingMeasurements, measurement)
		}
	}

	matchedTags := map[string]TagFilter{}
	for _, name := range names {
		measurement := measurements[name]
		if len(measurementSet) > 0 {
			if _, ok := measurementSet[name]; !ok {
				continue
			}
		}
		for _, filter := range query.QueryTags {
			if measurement.hasTagSeries(filter) {
				matchedTags[tagFilterID(filter.Key, filter.Value)] = filter
			}
		}
		matchingSeriesIDs := measurement.matchingSeriesIDs(query.QueryTags)
		if len(matchingSeriesIDs) == 0 {
			continue
		}
		tagKeyCount, tagValueCount := measurement.tagCounts()
		query.CandidateMeasurements++
		query.SeriesRefs += int64(len(matchingSeriesIDs))
		query.TagKeyCount += tagKeyCount
		query.TagValueCount += tagValueCount
		if len(query.MeasurementSamples) < options.KeySampleLimit {
			query.MeasurementSamples = append(query.MeasurementSamples, IndexQueryMeasurementReport{
				Name:        name,
				SeriesCount: uint64(len(matchingSeriesIDs)),
				Tags:        measurement.queryTagReports(query.QueryTags, options.BlockSampleLimit),
			})
		}
	}
	for _, filter := range query.QueryTags {
		id := tagFilterID(filter.Key, filter.Value)
		if matched, ok := matchedTags[id]; ok {
			query.MatchedTags = append(query.MatchedTags, matched)
		} else {
			query.MissingTags = append(query.MissingTags, filter)
		}
	}
	return query
}

func sortedMergesetTSIMeasurementNames(measurements map[string]*mergesetTSIMeasurement) []string {
	names := make([]string, 0, len(measurements))
	for name := range measurements {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (m *mergesetTSIMeasurement) tagCounts() (tagKeyCount, tagValueCount int) {
	tagKeyCount = len(m.TagValues)
	for _, values := range m.TagValues {
		tagValueCount += len(values)
	}
	return tagKeyCount, tagValueCount
}

func (m *mergesetTSIMeasurement) hasTagSeries(filter TagFilter) bool {
	if m == nil {
		return false
	}
	values := m.TagValueSeriesIDs[filter.Key]
	if len(values) == 0 {
		return false
	}
	return len(values[filter.Value]) > 0
}

func (m *mergesetTSIMeasurement) matchingSeriesIDs(filters []TagFilter) map[uint64]struct{} {
	if m == nil {
		return nil
	}
	if len(filters) == 0 {
		return cloneMergesetTSISeriesIDSet(m.SeriesIDs)
	}
	var matches map[uint64]struct{}
	for _, filter := range filters {
		values := m.TagValueSeriesIDs[filter.Key]
		if len(values) == 0 {
			return nil
		}
		seriesIDs := values[filter.Value]
		if len(seriesIDs) == 0 {
			return nil
		}
		if matches == nil {
			matches = cloneMergesetTSISeriesIDSet(seriesIDs)
		} else {
			matches = intersectTSISeriesIDSets(matches, seriesIDs)
		}
		if len(matches) == 0 {
			return nil
		}
	}
	return matches
}

func cloneMergesetTSISeriesIDSet(src map[uint64]struct{}) map[uint64]struct{} {
	dst := make(map[uint64]struct{}, len(src))
	for id := range src {
		dst[id] = struct{}{}
	}
	return dst
}

func (m *mergesetTSIMeasurement) queryTagReports(filters []TagFilter, sampleLimit int) []IndexQueryTagReport {
	if m == nil || sampleLimit <= 0 {
		return nil
	}
	if len(filters) > 0 {
		reports := make([]IndexQueryTagReport, 0, minInt(len(filters), sampleLimit))
		for _, filter := range filters {
			if len(reports) >= sampleLimit {
				break
			}
			seriesIDs := m.TagValueSeriesIDs[filter.Key][filter.Value]
			if len(seriesIDs) == 0 {
				continue
			}
			reports = append(reports, IndexQueryTagReport{
				Key: filter.Key,
				Values: []IndexQueryTagValueReport{{
					Value:       filter.Value,
					SeriesCount: uint64(len(seriesIDs)),
				}},
			})
		}
		return reports
	}

	tagKeys := make([]string, 0, len(m.TagValues))
	for key := range m.TagValues {
		tagKeys = append(tagKeys, key)
	}
	sort.Strings(tagKeys)
	reports := make([]IndexQueryTagReport, 0, minInt(len(tagKeys), sampleLimit))
	for _, key := range tagKeys {
		if len(reports) >= sampleLimit {
			break
		}
		values := sortedMergesetTSITagValues(m.TagValues[key])
		report := IndexQueryTagReport{Key: key}
		for _, value := range values {
			if len(report.Values) >= sampleLimit {
				break
			}
			seriesIDs := m.TagValueSeriesIDs[key][value]
			if len(seriesIDs) == 0 {
				continue
			}
			report.Values = append(report.Values, IndexQueryTagValueReport{
				Value:       value,
				SeriesCount: uint64(len(seriesIDs)),
			})
		}
		if len(report.Values) == 0 {
			continue
		}
		reports = append(reports, report)
	}
	return reports
}

func sortedMergesetTSITagValues(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

type mergesetCLVTextIndexItem struct {
	Type          string
	Token         string
	Version       uint32
	SIDGroups     int
	PositionCount int
	IDCount       int
}

func observeMergesetCLVTextIndexItems(summary *mergesetCLVTextIndexSummary, items [][]byte, sampleLimit int) {
	type observation struct {
		item   []byte
		parsed mergesetCLVTextIndexItem
		err    error
	}
	observations := make([]observation, 0, len(items))
	detected := summary.Detected
	for _, item := range items {
		parsed, err := parseMergesetCLVTextIndexItem(item)
		observations = append(observations, observation{
			item:   item,
			parsed: parsed,
			err:    err,
		})
		if err != nil {
			if _, tsiErr := parseMergesetTSIIndexItem(item); tsiErr != nil && isMergesetCLVTextIndexInvalidCandidate(item, false) {
				detected = true
			}
			continue
		}
		if isMergesetCLVTextIndexEvidence(parsed) {
			detected = true
		}
	}
	summary.Detected = detected

	for _, observation := range observations {
		parsed := observation.parsed
		if observation.err != nil {
			if _, tsiErr := parseMergesetTSIIndexItem(observation.item); tsiErr == nil {
				continue
			}
			if isMergesetCLVTextIndexInvalidCandidate(observation.item, summary.Detected) {
				summary.InvalidItems++
			} else if isMergesetCLVTextIndexPrefix(observation.item) {
				summary.DeferredInvalidItems++
			}
			continue
		}
		switch parsed.Type {
		case "document":
			summary.DocumentRows++
			summary.PositionEntries += parsed.PositionCount
			summary.PositionSIDGroups += parsed.SIDGroups
			summary.DocumentIDs += parsed.IDCount
			if sampleLimit > 0 && len(summary.PositionSamples) < sampleLimit {
				summary.PositionSamples = append(summary.PositionSamples, fmt.Sprintf("%s sid_groups=%d positions=%d ids=%d", formatMergesetCLVTextTokenSample(parsed.Token), parsed.SIDGroups, parsed.PositionCount, parsed.IDCount))
			}
		case "term":
			summary.TermRows++
			if sampleLimit > 0 && len(summary.TermSamples) < sampleLimit {
				summary.TermSamples = append(summary.TermSamples, parsed.Token)
			}
		case "dictionary":
			summary.DictionaryRows++
			if sampleLimit > 0 && len(summary.DictionarySamples) < sampleLimit {
				summary.DictionarySamples = append(summary.DictionarySamples, fmt.Sprintf("v%d:%s", parsed.Version, formatMergesetCLVTextTokenSample(parsed.Token)))
			}
		case "dictionary-version":
			summary.DictionaryVersionRows++
			if sampleLimit > 0 && len(summary.VersionSamples) < sampleLimit {
				summary.VersionSamples = append(summary.VersionSamples, fmt.Sprint(parsed.Version))
			}
		}
	}
}

func parseMergesetCLVTextIndexItem(item []byte) (mergesetCLVTextIndexItem, error) {
	var parsed mergesetCLVTextIndexItem
	if len(item) == 0 {
		return parsed, fmt.Errorf("empty CLV text index item")
	}
	switch item[0] {
	case opengeminiCLVPrefixPos:
		return parseMergesetCLVPositionItem(item)
	case opengeminiCLVPrefixTerm:
		if len(item) < 2 {
			return parsed, fmt.Errorf("short CLV term item")
		}
		token, err := parseMergesetCLVTermToken(item[1:])
		if err != nil {
			return parsed, err
		}
		parsed.Type = "term"
		parsed.Token = token
		return parsed, nil
	case opengeminiCLVPrefixDictionary:
		if len(item) < 7 {
			return parsed, fmt.Errorf("short CLV dictionary item")
		}
		if item[5] != opengeminiCLVSuffix {
			return parsed, fmt.Errorf("CLV dictionary item missing suffix")
		}
		parsed.Type = "dictionary"
		parsed.Version = binary.BigEndian.Uint32(item[1:5])
		parsed.Token = string(item[6:])
		return parsed, nil
	case opengeminiCLVPrefixVersion:
		if len(item) != 6 {
			return parsed, fmt.Errorf("CLV dictionary version item size=%d; want 6", len(item))
		}
		if item[1] != opengeminiCLVSuffix {
			return parsed, fmt.Errorf("CLV dictionary version item missing suffix")
		}
		parsed.Type = "dictionary-version"
		parsed.Version = binary.BigEndian.Uint32(item[2:6])
		return parsed, nil
	default:
		return parsed, fmt.Errorf("not an openGemini CLV text index item")
	}
}

func parseMergesetCLVPositionItem(item []byte) (mergesetCLVTextIndexItem, error) {
	var parsed mergesetCLVTextIndexItem
	if len(item) < 5 {
		return parsed, fmt.Errorf("short CLV document item")
	}
	suffixOffset := bytes.IndexByte(item[1:], opengeminiCLVSuffix)
	if suffixOffset < 0 {
		return parsed, fmt.Errorf("CLV document item missing suffix")
	}
	suffix := suffixOffset + 1
	if suffix <= 1 {
		return parsed, fmt.Errorf("CLV document item has empty token")
	}
	if len(item) < suffix+4 {
		return parsed, fmt.Errorf("short CLV document item metadata")
	}
	metaOffset := int(binary.BigEndian.Uint16(item[len(item)-2:]))
	flag := item[len(item)-3]
	if metaOffset <= suffix || metaOffset > len(item)-3 {
		return parsed, fmt.Errorf("CLV document meta offset=%d outside payload", metaOffset)
	}
	if item[metaOffset] != opengeminiCLVPrefixMeta {
		return parsed, fmt.Errorf("CLV document item missing meta prefix")
	}
	if flag == 0 {
		return parsed, fmt.Errorf("CLV document item has empty meta flag")
	}
	if flag&^(opengeminiCLVPosFlag|opengeminiCLVIDFlag) != 0 {
		return parsed, fmt.Errorf("CLV document item has unknown meta flag %d", flag)
	}

	meta := item[metaOffset+1 : len(item)-3]
	var sidLens []uint16
	var idCount int
	if flag&opengeminiCLVPosFlag != 0 {
		if len(meta) < 2 {
			return parsed, fmt.Errorf("short CLV document sid group count")
		}
		groups := int(binary.BigEndian.Uint16(meta[:2]))
		meta = meta[2:]
		if groups == 0 {
			return parsed, fmt.Errorf("CLV document position flag has zero sid groups")
		}
		if len(meta) < groups*2 {
			return parsed, fmt.Errorf("short CLV document sid lengths")
		}
		sidLens = make([]uint16, 0, groups)
		for i := 0; i < groups; i++ {
			sidLens = append(sidLens, binary.BigEndian.Uint16(meta[:2]))
			meta = meta[2:]
		}
	}
	if flag&opengeminiCLVIDFlag != 0 {
		if len(meta) < 2 {
			return parsed, fmt.Errorf("short CLV document id count")
		}
		idCount = int(binary.BigEndian.Uint16(meta[:2]))
		meta = meta[2:]
		if idCount == 0 {
			return parsed, fmt.Errorf("CLV document id flag has zero ids")
		}
	}
	if len(meta) != 0 {
		return parsed, fmt.Errorf("CLV document meta has %d trailing byte(s)", len(meta))
	}

	sidGroups, positions, ids, err := parseMergesetCLVPositionPayload(item[suffix+1:metaOffset], sidLens, idCount)
	if err != nil {
		return parsed, err
	}
	parsed.Type = "document"
	parsed.Token = string(item[1:suffix])
	parsed.SIDGroups = sidGroups
	parsed.PositionCount = positions
	parsed.IDCount = ids
	return parsed, nil
}

func parseMergesetCLVTermToken(data []byte) (string, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("CLV term token is empty")
	}
	if !utf8.Valid(data) {
		return "", fmt.Errorf("CLV term token is not valid UTF-8")
	}
	for _, ch := range string(data) {
		if ch < ' ' {
			return "", fmt.Errorf("CLV term token contains control character")
		}
	}
	return string(data), nil
}

func formatMergesetCLVTextTokenSample(token string) string {
	if utf8.ValidString(token) {
		printable := true
		for _, ch := range token {
			if ch < ' ' {
				printable = false
				break
			}
		}
		if printable {
			return token
		}
	}
	return "0x" + hex.EncodeToString([]byte(token))
}

func finalizeMergesetItemNamespaceSummaries(clv *mergesetCLVTextIndexSummary, tsi *mergesetTSIIndexSummary) {
	if clv.Detected {
		clv.InvalidItems += clv.DeferredInvalidItems
		clv.DeferredInvalidItems = 0
		tsi.DeferredCLVInvalids = 0
		return
	}
	if tsi.DeferredCLVInvalids > 0 {
		tsi.Detected = true
		tsi.InvalidItems += tsi.DeferredCLVInvalids
	}
	clv.DeferredInvalidItems = 0
	tsi.DeferredCLVInvalids = 0
}

func parseMergesetCLVPositionPayload(data []byte, sidLens []uint16, idCount int) (int, int, int, error) {
	positionCount := 0
	for group, sidLen := range sidLens {
		if len(data) < 9 {
			return 0, 0, 0, fmt.Errorf("short CLV sid group #%d", group+1)
		}
		if data[0] != opengeminiCLVPrefixSID {
			return 0, 0, 0, fmt.Errorf("CLV sid group #%d missing sid prefix", group+1)
		}
		data = data[9:]
		positionBytes := int(sidLen) * 10
		if len(data) < positionBytes {
			return 0, 0, 0, fmt.Errorf("short CLV positions for sid group #%d", group+1)
		}
		positionCount += int(sidLen)
		data = data[positionBytes:]
	}
	if idCount > 0 {
		if len(data) < 1 {
			return 0, 0, 0, fmt.Errorf("short CLV id list")
		}
		if data[0] != opengeminiCLVPrefixID {
			return 0, 0, 0, fmt.Errorf("CLV id list missing prefix")
		}
		data = data[1:]
		idBytes := idCount * 4
		if len(data) < idBytes {
			return 0, 0, 0, fmt.Errorf("short CLV id list payload")
		}
		data = data[idBytes:]
	}
	if len(data) != 0 {
		return 0, 0, 0, fmt.Errorf("CLV position payload has %d trailing byte(s)", len(data))
	}
	return len(sidLens), positionCount, idCount, nil
}

func isMergesetCLVTextIndexCandidate(item []byte) bool {
	if len(item) == 0 {
		return false
	}
	switch item[0] {
	case opengeminiCLVPrefixPos:
		return len(item) >= 5 && bytes.IndexByte(item[1:], opengeminiCLVSuffix) > 0
	case opengeminiCLVPrefixDictionary:
		return len(item) >= 6
	case opengeminiCLVPrefixVersion:
		return len(item) >= 2
	default:
		return false
	}
}

func isMergesetCLVTextIndexInvalidCandidate(item []byte, detected bool) bool {
	if detected {
		return isMergesetCLVTextIndexPrefix(item)
	}
	return isMergesetCLVTextIndexCandidate(item) && isMergesetCLVTextIndexStrongCandidate(item)
}

func isMergesetCLVTextIndexPrefix(item []byte) bool {
	if len(item) == 0 {
		return false
	}
	switch item[0] {
	case opengeminiCLVPrefixPos, opengeminiCLVPrefixTerm, opengeminiCLVPrefixDictionary, opengeminiCLVPrefixVersion:
		return true
	default:
		return false
	}
}

func isMergesetCLVTextIndexStrongCandidate(item []byte) bool {
	if len(item) == 0 {
		return false
	}
	switch item[0] {
	case opengeminiCLVPrefixPos:
		return isMergesetCLVDocumentStructureCandidate(item)
	case opengeminiCLVPrefixVersion:
		return true
	default:
		return false
	}
}

func isMergesetCLVDocumentStructureCandidate(item []byte) bool {
	if len(item) < 5 {
		return false
	}
	suffixOffset := bytes.IndexByte(item[1:], opengeminiCLVSuffix)
	if suffixOffset <= 0 {
		return false
	}
	suffix := suffixOffset + 1
	metaOffset := int(binary.BigEndian.Uint16(item[len(item)-2:]))
	return metaOffset > suffix && metaOffset <= len(item)-3 && item[metaOffset] == opengeminiCLVPrefixMeta
}

func isMergesetCLVTextIndexEvidence(item mergesetCLVTextIndexItem) bool {
	switch item.Type {
	case "document", "dictionary-version":
		return true
	default:
		return false
	}
}

func isMergesetCLVTextIndexContextual(item mergesetCLVTextIndexItem) bool {
	switch item.Type {
	case "dictionary":
		return true
	default:
		return false
	}
}

func addMergesetCLVTextIndexSummary(report *FileReport, summary mergesetCLVTextIndexSummary) {
	if !summary.Detected {
		return
	}
	report.Extra["opengemini_clv_text_index_detected"] = "true"
	report.Extra["opengemini_clv_text_index_document_rows"] = fmt.Sprint(summary.DocumentRows)
	report.Extra["opengemini_clv_text_index_position_entries"] = fmt.Sprint(summary.PositionEntries)
	report.Extra["opengemini_clv_text_index_sid_groups"] = fmt.Sprint(summary.PositionSIDGroups)
	report.Extra["opengemini_clv_text_index_document_ids"] = fmt.Sprint(summary.DocumentIDs)
	report.Extra["opengemini_clv_text_index_terms"] = fmt.Sprint(summary.TermRows)
	report.Extra["opengemini_clv_text_index_dictionary_rows"] = fmt.Sprint(summary.DictionaryRows)
	report.Extra["opengemini_clv_text_index_dictionary_versions"] = fmt.Sprint(summary.DictionaryVersionRows)
	report.Extra["opengemini_clv_text_index_invalid_items"] = fmt.Sprint(summary.InvalidItems)
	if len(summary.PositionSamples) > 0 {
		report.Extra["opengemini_clv_text_index_position_samples"] = strings.Join(summary.PositionSamples, ",")
	}
	if len(summary.TermSamples) > 0 {
		report.Extra["opengemini_clv_text_index_term_samples"] = strings.Join(summary.TermSamples, ",")
	}
	if len(summary.DictionarySamples) > 0 {
		report.Extra["opengemini_clv_text_index_dictionary_samples"] = strings.Join(summary.DictionarySamples, ",")
	}
	if len(summary.VersionSamples) > 0 {
		report.Extra["opengemini_clv_text_index_version_samples"] = strings.Join(summary.VersionSamples, ",")
	}
	report.BlocksByType["opengemini-clv-text-document"] = summary.DocumentRows
	report.BlocksByType["opengemini-clv-text-position"] = summary.PositionEntries
	report.BlocksByType["opengemini-clv-text-term"] = summary.TermRows
	report.BlocksByType["opengemini-clv-text-dictionary"] = summary.DictionaryRows
	report.BlocksByType["opengemini-clv-text-dictionary-version"] = summary.DictionaryVersionRows
	if summary.InvalidItems > 0 {
		report.BlocksByType["opengemini-clv-text-invalid-item"] = summary.InvalidItems
		report.Notices = append(report.Notices, fmt.Sprintf("openGemini CLV text index has %d invalid namespaced item(s)", summary.InvalidItems))
	}
}

type mergesetFieldIndexItem struct {
	Type        string
	Measurement string
	Field       string
	TSID        uint64
	FieldValue  string
	PID         uint64
}

func observeMergesetFieldIndexItems(summary *mergesetFieldIndexSummary, items [][]byte, sampleLimit int) {
	if summary.MeasurementFieldKeys == nil {
		summary.MeasurementFieldKeys = map[string]string{}
	}
	for _, item := range items {
		parsed, err := parseMergesetFieldIndexItem(item)
		if err != nil {
			if isMergesetFieldIndexPrefix(item) {
				summary.Detected = true
				summary.InvalidItems++
			}
			continue
		}
		summary.Detected = true
		switch parsed.Type {
		case "measurement-field-key":
			if existing, ok := summary.MeasurementFieldKeys[parsed.Measurement]; ok && existing != parsed.Field {
				summary.DuplicateMeasurementKey++
			}
			summary.MeasurementFieldKeys[parsed.Measurement] = parsed.Field
			if sampleLimit > 0 && len(summary.MeasurementSamples) < sampleLimit {
				summary.MeasurementSamples = append(summary.MeasurementSamples, parsed.Measurement+":"+parsed.Field)
			}
		case "tsid-field-value":
			summary.TSIDToFieldValueCount++
			if sampleLimit > 0 && len(summary.FieldValueSamples) < sampleLimit {
				summary.FieldValueSamples = append(summary.FieldValueSamples, fmt.Sprintf("%d:%s", parsed.TSID, parsed.FieldValue))
			}
		case "field-pid":
			summary.FieldToPIDCount++
			if sampleLimit > 0 && len(summary.FieldToPIDSamples) < sampleLimit {
				summary.FieldToPIDSamples = append(summary.FieldToPIDSamples, fmt.Sprintf("%d:%s->%d", parsed.TSID, parsed.FieldValue, parsed.PID))
			}
		}
	}
}

func parseMergesetFieldIndexItem(item []byte) (mergesetFieldIndexItem, error) {
	var parsed mergesetFieldIndexItem
	if len(item) == 0 {
		return parsed, fmt.Errorf("empty field index item")
	}
	switch item[0] {
	case opengeminiTSINSMstToFieldKey:
		if len(item) < 3 {
			return parsed, fmt.Errorf("short measurement field-key item")
		}
		tail := item[1:]
		measurementLen := int(binary.BigEndian.Uint16(tail[:2]))
		tail = tail[2:]
		if len(tail) < measurementLen+2 {
			return parsed, fmt.Errorf("short measurement field-key payload")
		}
		parsed.Measurement = string(tail[:measurementLen])
		tail = tail[measurementLen:]
		fieldLen := int(binary.BigEndian.Uint16(tail[:2]))
		tail = tail[2:]
		if len(tail) != fieldLen {
			return parsed, fmt.Errorf("field-key length=%d leaves %d bytes", fieldLen, len(tail))
		}
		parsed.Type = "measurement-field-key"
		parsed.Field = string(tail)
		return parsed, nil
	case opengeminiTSINSFieldToPID:
		if len(item) < 18 {
			return parsed, fmt.Errorf("short field-to-pid item")
		}
		parsed.Type = "field-pid"
		parsed.TSID = binary.BigEndian.Uint64(item[1:9])
		tail := item[9:]
		sep := len(tail) - 9
		if sep < 0 || tail[sep] != opengeminiTSINSSeparator {
			return parsed, fmt.Errorf("field-to-pid item missing separator")
		}
		pidBytes := tail[sep+1:]
		parsed.FieldValue = string(tail[:sep])
		parsed.PID = binary.BigEndian.Uint64(pidBytes)
		return parsed, nil
	case opengeminiTSINSPrefixTSIDToField:
		if len(item) < opengeminiTSIDToFieldItemSize {
			return parsed, fmt.Errorf("short tsid-to-field item")
		}
		parsed.Type = "tsid-field-value"
		parsed.TSID = binary.BigEndian.Uint64(item[1:9])
		tail := item[9:]
		fieldLen := int(tail[0])
		tail = tail[1:]
		if len(tail) != fieldLen {
			return parsed, fmt.Errorf("field value length=%d leaves %d bytes", fieldLen, len(tail))
		}
		parsed.FieldValue = string(tail)
		return parsed, nil
	default:
		return parsed, fmt.Errorf("not an openGemini field index item")
	}
}

func isMergesetFieldIndexPrefix(item []byte) bool {
	if len(item) == 0 {
		return false
	}
	switch item[0] {
	case opengeminiTSINSPrefixTSIDToField, opengeminiTSINSFieldToPID, opengeminiTSINSMstToFieldKey:
		return true
	default:
		return false
	}
}

func addMergesetFieldIndexSummary(report *FileReport, summary mergesetFieldIndexSummary) {
	if !summary.Detected {
		return
	}
	report.Extra["opengemini_field_index_detected"] = "true"
	report.Extra["opengemini_field_index_measurements"] = fmt.Sprint(len(summary.MeasurementFieldKeys))
	report.Extra["opengemini_field_index_tsid_field_values"] = fmt.Sprint(summary.TSIDToFieldValueCount)
	report.Extra["opengemini_field_index_field_pid_mappings"] = fmt.Sprint(summary.FieldToPIDCount)
	report.Extra["opengemini_field_index_invalid_items"] = fmt.Sprint(summary.InvalidItems)
	if len(summary.MeasurementSamples) > 0 {
		report.Extra["opengemini_field_index_measurement_samples"] = strings.Join(summary.MeasurementSamples, ",")
	}
	if len(summary.FieldValueSamples) > 0 {
		report.Extra["opengemini_field_index_value_samples"] = strings.Join(summary.FieldValueSamples, ",")
	}
	if len(summary.FieldToPIDSamples) > 0 {
		report.Extra["opengemini_field_index_pid_samples"] = strings.Join(summary.FieldToPIDSamples, ",")
	}
	report.BlocksByType["opengemini-field-index-measurement"] = len(summary.MeasurementFieldKeys)
	report.BlocksByType["opengemini-field-index-tsid-value"] = summary.TSIDToFieldValueCount
	report.BlocksByType["opengemini-field-index-field-pid"] = summary.FieldToPIDCount
	if summary.InvalidItems > 0 {
		report.BlocksByType["opengemini-field-index-invalid-item"] = summary.InvalidItems
		report.Notices = append(report.Notices, fmt.Sprintf("openGemini field index has %d invalid namespaced item(s)", summary.InvalidItems))
	}
	if summary.DuplicateMeasurementKey > 0 {
		report.BlocksByType["opengemini-field-index-duplicate-measurement-key"] = summary.DuplicateMeasurementKey
		report.Notices = append(report.Notices, fmt.Sprintf("openGemini field index has %d duplicate measurement field-key mapping(s)", summary.DuplicateMeasurementKey))
	}
}

func metaindexBlockHeaders(rows []mergesetMetaindexRow) uint64 {
	var total uint64
	for _, row := range rows {
		total += uint64(row.BlockHeadersCount)
	}
	return total
}

func uint64ToInt(value uint64) int {
	maxInt := int(^uint(0) >> 1)
	if value > uint64(maxInt) {
		return maxInt
	}
	return int(value)
}

func uint64ToNonNegativeInt64(value uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}

func addUint32Saturating(a, b uint32) uint32 {
	if uint32(^uint32(0))-a < b {
		return ^uint32(0)
	}
	return a + b
}
