package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
