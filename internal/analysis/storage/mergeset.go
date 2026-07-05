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
	DecodePath    *DecodePathSummary
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
		addMergesetItemPayloadSummary(&report, payloadSummary, firstItem, lastItem, metadata.ItemsCount)
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
	summary.DecodePath = search.DecodePath
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
	notices := []string{}
	for i, header := range headers {
		search.ObserveHeader(i, header)
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
		decoded, err := decodeMergesetBlockItems(header, itemsData, lensData, decoder, options.KeySampleLimit-len(summary.Samples), search.QuerySet)
		if err != nil {
			notices = append(notices, fmt.Sprintf("mergeset item payload decode unavailable at block=%d first_item=%s: %v", i+1, hex.EncodeToString(header.FirstItem), err))
			continue
		}
		search.ObserveDecodedBlock(i, decoded)
		summary.DecodedBlocks++
		summary.ItemsDecoded += decoded.Count
		if len(summary.FirstItem) == 0 {
			summary.FirstItem = append(summary.FirstItem[:0], decoded.FirstItem...)
		}
		summary.LastItem = append(summary.LastItem[:0], decoded.LastItem...)
		summary.Samples = append(summary.Samples, decoded.Samples...)
	}
	search.Finish(options)
	return summary, notices
}

type mergesetDecodedBlockItems struct {
	Count       uint64
	FirstItem   []byte
	LastItem    []byte
	Samples     [][]byte
	QuerySet    map[string]struct{}
	MatchedKeys map[string]struct{}
}

func decodeMergesetBlockItems(header mergesetBlockHeader, itemsData, lensData []byte, decoder *zstd.Decoder, sampleLimit int, querySet map[string]struct{}) (mergesetDecodedBlockItems, error) {
	if !bytes.HasPrefix(header.FirstItem, header.CommonPrefix) {
		return mergesetDecodedBlockItems{}, fmt.Errorf("firstItem does not start with commonPrefix")
	}
	switch header.MarshalType {
	case mergesetMarshalTypePlain:
		return decodeMergesetPlainBlockItems(header, itemsData, lensData, sampleLimit, querySet)
	case mergesetMarshalTypeZSTD:
		return decodeMergesetZSTDBlockItems(header, itemsData, lensData, decoder, sampleLimit, querySet)
	default:
		return mergesetDecodedBlockItems{}, fmt.Errorf("unknown marshalType=%d", header.MarshalType)
	}
}

func decodeMergesetPlainBlockItems(header mergesetBlockHeader, itemsData, lensData []byte, sampleLimit int, querySet map[string]struct{}) (mergesetDecodedBlockItems, error) {
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

	decoded := newMergesetDecodedBlockItems(header.FirstItem, sampleLimit, querySet)
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

func decodeMergesetZSTDBlockItems(header mergesetBlockHeader, itemsData, lensData []byte, decoder *zstd.Decoder, sampleLimit int, querySet map[string]struct{}) (mergesetDecodedBlockItems, error) {
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

	decoded := newMergesetDecodedBlockItems(header.FirstItem, sampleLimit, querySet)
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

func newMergesetDecodedBlockItems(firstItem []byte, sampleLimit int, querySet map[string]struct{}) mergesetDecodedBlockItems {
	decoded := mergesetDecodedBlockItems{
		Count:       1,
		FirstItem:   append([]byte(nil), firstItem...),
		LastItem:    append([]byte(nil), firstItem...),
		QuerySet:    querySet,
		MatchedKeys: map[string]struct{}{},
	}
	decoded.observeQueryMatch(firstItem, querySet)
	if sampleLimit > 0 {
		decoded.Samples = append(decoded.Samples, append([]byte(nil), firstItem...))
	}
	return decoded
}

func (decoded *mergesetDecodedBlockItems) appendItem(item []byte, sampleLimit int) error {
	if bytes.Compare(decoded.LastItem, item) > 0 {
		return fmt.Errorf("decoded data block contains unsorted items")
	}
	decoded.Count++
	decoded.LastItem = append(decoded.LastItem[:0], item...)
	decoded.observeQueryMatch(item, decoded.QuerySet)
	if len(decoded.Samples) < sampleLimit {
		decoded.Samples = append(decoded.Samples, append([]byte(nil), item...))
	}
	return nil
}

func (decoded *mergesetDecodedBlockItems) observeQueryMatch(item []byte, querySet map[string]struct{}) {
	if len(querySet) == 0 {
		return
	}
	key := string(item)
	if _, ok := querySet[key]; ok {
		decoded.MatchedKeys[key] = struct{}{}
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
	QueryKeys       []string
	QuerySet        map[string]struct{}
	CandidateBlocks map[int]struct{}
	MatchedKeys     map[string]struct{}
	SampleLimit     int
	DecodePath      *DecodePathSummary
}

func newMergesetSearchPlan(headers []mergesetBlockHeader, options Options, firstItem, lastItem []byte) *mergesetSearchPlan {
	plan := &mergesetSearchPlan{}
	if len(options.QueryKeys) == 0 {
		return plan
	}
	plan.QueryKeys = append([]string(nil), options.QueryKeys...)
	plan.QuerySet = queryKeySet(options.QueryKeys)
	plan.CandidateBlocks = map[int]struct{}{}
	plan.MatchedKeys = map[string]struct{}{}
	plan.SampleLimit = options.BlockSampleLimit
	for _, key := range options.QueryKeys {
		queryItem := []byte(key)
		if bytes.Compare(queryItem, firstItem) < 0 || bytes.Compare(queryItem, lastItem) > 0 {
			continue
		}
		idx := sort.Search(len(headers), func(i int) bool {
			return bytes.Compare(headers[i].FirstItem, queryItem) > 0
		}) - 1
		if idx >= 0 {
			plan.CandidateBlocks[idx] = struct{}{}
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

func (p *mergesetSearchPlan) ObserveHeader(index int, header mergesetBlockHeader) {
	if p == nil || p.DecodePath == nil {
		return
	}
	summary := p.DecodePath
	blockBytes := int64(header.ItemsBlockSize) + int64(header.LensBlockSize)
	summary.BaselineDecodeBlocks++
	summary.BaselineDecodeValues += uint64ToInt(uint64(header.ItemsCount))
	summary.BaselineDecodeBytes += blockBytes

	_, candidate := p.CandidateBlocks[index]
	reason := "key_not_in_block_range"
	if candidate {
		reason = "key_range_candidate"
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
}

func (p *mergesetSearchPlan) ObserveDecodedBlock(index int, decoded mergesetDecodedBlockItems) {
	if p == nil || p.DecodePath == nil {
		return
	}
	if _, candidate := p.CandidateBlocks[index]; !candidate {
		return
	}
	for _, key := range p.QueryKeys {
		if _, ok := decoded.MatchedKeys[key]; !ok {
			continue
		}
		p.MatchedKeys[key] = struct{}{}
		if len(p.DecodePath.CursorOutputSamples) < p.SampleLimit {
			p.DecodePath.CursorOutputSamples = append(p.DecodePath.CursorOutputSamples, DecodePathCursorOutput{
				Key:     key,
				Type:    "mergeset-item",
				Matches: true,
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
	if summary.OptimizedDecodeBlocks > 0 {
		summary.Amplification = float64(summary.BaselineDecodeBlocks) / float64(summary.OptimizedDecodeBlocks)
	}
	summary.Recommendations = mergesetSearchRecommendations(summary, options)
}

func mergesetSearchMode(options Options) string {
	if options.CursorDescending {
		return "mergeset-item-search-descending"
	}
	return "mergeset-item-search-ascending"
}

func mergesetSearchRecommendations(summary *DecodePathSummary, options Options) []string {
	recommendations := []string{}
	if len(summary.MissingKeys) > 0 {
		recommendations = append(recommendations, fmt.Sprintf("%d query item key(s) were not found in this mergeset part", len(summary.MissingKeys)))
	}
	if summary.SavedDecodeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf("sorted item lookup skips %d mergeset block(s) before payload inspection", summary.SavedDecodeBlocks))
	}
	if len(recommendations) == 0 && len(options.QueryKeys) > 0 {
		recommendations = append(recommendations, "all query item keys mapped to decoded mergeset block candidates")
	}
	return recommendations
}

func addMergesetItemPayloadSummary(report *FileReport, summary mergesetItemPayloadSummary, metadataFirstItem, metadataLastItem []byte, metadataItemsCount uint64) {
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
