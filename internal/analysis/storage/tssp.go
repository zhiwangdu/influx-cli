package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

const (
	tsspMagic        = "53ac2021"
	tsspHeaderSize   = 16
	tsspFooterSize   = 8
	tsspMetaIndexLen = 40

	tsspChunkMetaCompressNone   = 0
	tsspChunkMetaCompressSnappy = 1
	tsspChunkMetaCompressLZ4    = 2
	tsspChunkMetaCompressSelf   = 3
	tsspChunkMetaFixedLen       = 8 + 8 + 4 + 4 + 4
	tsspSegmentLen              = 8 + 4
	tsspSegmentRangeLen         = 8 + 8
)

type tsspTrailer struct {
	DataOffset         int64
	DataSize           int64
	IndexSize          int64
	MetaIndexSize      int64
	BloomSize          int64
	IDTimeSize         int64
	IDCount            int64
	MinID              uint64
	MaxID              uint64
	MinTime            int64
	MaxTime            int64
	MetaIndexItemCount int64
	BloomM             uint64
	BloomK             uint64
	TimeStoreFlag      uint8
	ChunkMetaCompress  uint8
	ChunkMetaHeader    []string
	MeasurementName    string
}

type tsspMetaIndex struct {
	ID      uint64
	MinTime int64
	MaxTime int64
	Offset  int64
	Count   uint32
	Size    uint32
}

// tsspChunkMeta mirrors openGemini immutable.ChunkMeta for uncompressed
// attached TSSP index blocks.
type tsspChunkMeta struct {
	SID          uint64
	Offset       int64
	Size         uint32
	ColumnCount  uint32
	SegmentCount uint32
	TimeRanges   []tsspTimeRange
	Columns      []tsspColumnMeta
}

type tsspTimeRange struct {
	Min int64
	Max int64
}

type tsspColumnMeta struct {
	Name        string
	Type        byte
	PreAggBytes int
	Segments    []tsspSegment
}

type tsspSegment struct {
	Offset int64
	Size   uint32
}

func analyzeTSSP(path string, info os.FileInfo, options Options) (FileReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return FileReport{}, err
	}
	defer f.Close()

	if info.Size() < tsspHeaderSize+tsspFooterSize {
		return FileReport{}, fmt.Errorf("file too small for TSSP header/footer")
	}

	header := make([]byte, tsspHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return FileReport{}, err
	}
	if string(header[:len(tsspMagic)]) != tsspMagic {
		return FileReport{}, fmt.Errorf("invalid TSSP magic")
	}
	version := binary.BigEndian.Uint64(header[len(tsspMagic):])

	footer := make([]byte, tsspFooterSize)
	if _, err := f.ReadAt(footer, info.Size()-tsspFooterSize); err != nil {
		return FileReport{}, err
	}
	trailerOffset := decodeGeminiInt64(footer)
	if trailerOffset < tsspHeaderSize || trailerOffset >= info.Size()-tsspFooterSize {
		return FileReport{}, fmt.Errorf("invalid TSSP trailer offset %d", trailerOffset)
	}
	trailerBytes := make([]byte, info.Size()-tsspFooterSize-trailerOffset)
	if _, err := f.ReadAt(trailerBytes, trailerOffset); err != nil {
		return FileReport{}, err
	}
	trailer, err := parseTSSPTrailer(trailerBytes)
	if err != nil {
		return FileReport{}, err
	}

	metaIndexOffset := trailer.DataOffset + trailer.DataSize + trailer.IndexSize
	if metaIndexOffset < tsspHeaderSize || metaIndexOffset+trailer.MetaIndexSize > trailerOffset {
		return FileReport{}, fmt.Errorf("invalid TSSP meta-index range offset=%d size=%d", metaIndexOffset, trailer.MetaIndexSize)
	}
	metaIndexes, err := readTSSPMetaIndexes(f, metaIndexOffset, trailer.MetaIndexSize)
	if err != nil {
		return FileReport{}, err
	}

	report := FileReport{
		Path:       path,
		Format:     FormatTSSP,
		SizeBytes:  info.Size(),
		ModTime:    info.ModTime(),
		MinTime:    trailer.MinTime,
		MaxTime:    trailer.MaxTime,
		KeyCount:   int(trailer.IDCount),
		BlockCount: sumTSSPChunkMetaCount(metaIndexes),
		BlocksByType: map[string]int{
			"chunk-meta": sumTSSPChunkMetaCount(metaIndexes),
			"meta-index": len(metaIndexes),
		},
		SeriesID: SeriesIDSummary{
			Min:   trailer.MinID,
			Max:   trailer.MaxID,
			Count: trailer.IDCount,
		},
		QueryOverlapsFile: options.QueryRange.Overlaps(trailer.MinTime, trailer.MaxTime),
		Extra: map[string]string{
			"version":             fmt.Sprint(version),
			"measurement":         trailer.MeasurementName,
			"trailer_offset":      fmt.Sprint(trailerOffset),
			"data_size":           fmt.Sprint(trailer.DataSize),
			"index_size":          fmt.Sprint(trailer.IndexSize),
			"meta_index_size":     fmt.Sprint(trailer.MetaIndexSize),
			"bloom_size":          fmt.Sprint(trailer.BloomSize),
			"id_time_size":        fmt.Sprint(trailer.IDTimeSize),
			"chunk_meta_compress": fmt.Sprint(trailer.ChunkMetaCompress),
			"chunk_meta_header":   fmt.Sprint(len(trailer.ChunkMetaHeader)),
			"chunk_meta_expanded": "false",
			"time_store":          fmt.Sprint(trailer.TimeStoreFlag),
		},
	}
	if trailer.MeasurementName != "" {
		report.KeySamples = append(report.KeySamples, "measurement:"+trailer.MeasurementName)
	}
	for _, meta := range metaIndexes {
		if len(report.KeySamples) >= options.KeySampleLimit {
			break
		}
		report.KeySamples = append(report.KeySamples, fmt.Sprintf("sid:%d", meta.ID))
	}

	chunkMetas, expanded := []tsspChunkMeta(nil), false
	switch trailer.ChunkMetaCompress {
	case tsspChunkMetaCompressNone, tsspChunkMetaCompressSnappy, tsspChunkMetaCompressLZ4, tsspChunkMetaCompressSelf:
		chunks, err := readTSSPChunkMetas(f, metaIndexes, trailerOffset, trailer)
		if err != nil {
			report.Notices = append(report.Notices, fmt.Sprintf("chunk metadata expansion unavailable: %v", err))
		} else {
			chunkMetas = chunks
			expanded = true
			report.BlockCount = len(chunkMetas)
			report.BlocksByType["chunk-meta"] = len(chunkMetas)
			report.Extra["chunk_meta_expanded"] = "true"
			report.Extra["chunk_meta_compress_supported"] = "true"
			if options.QueryRange.Set {
				report.Extra["query_overlap_precision"] = "chunk-meta"
			}
		}
	default:
		report.Notices = append(report.Notices, fmt.Sprintf("unsupported chunk metadata compression mode %d; falling back to meta-index granularity", trailer.ChunkMetaCompress))
	}

	if expanded {
		populateTSSPChunkReports(&report, chunkMetas, options)
	} else {
		populateTSSPMetaIndexReports(&report, metaIndexes, options)
	}
	var dataProbe *tsspAttachedDataProbe
	if expanded {
		var dataProbed bool
		dataProbe, dataProbed, err = probeTSSPAttachedDataBlocks(f, info.Size(), trailer, chunkMetas, options)
		report.Extra["data_block_probe_checked"] = fmt.Sprint(dataProbed)
		if err != nil {
			report.Notices = append(report.Notices, fmt.Sprintf("TSSP data block probe unavailable: %v", err))
			dataProbe = nil
		} else if dataProbe != nil {
			report.Extra["data_block_probe_blocks"] = fmt.Sprint(dataProbe.BlocksChecked)
			report.Extra["data_block_probe_bytes"] = fmt.Sprint(dataProbe.BytesRead)
			report.Extra["data_block_probe_valid_blocks"] = fmt.Sprint(dataProbe.ValidBlocks)
			report.Extra["data_block_probe_failures"] = fmt.Sprint(dataProbe.Failures())
			report.Extra["data_block_probe_row_count_blocks"] = fmt.Sprint(dataProbe.RowCountBlocks)
			report.Extra["data_block_probe_row_count_unknowns"] = fmt.Sprint(dataProbe.RowCountUnknowns)
			report.Extra["data_block_probe_row_count_mismatches"] = fmt.Sprint(dataProbe.RowCountMismatches)
			report.Extra["data_block_probe_output_points"] = fmt.Sprint(dataProbe.OutputPoints)
			report.Extra["data_block_probe_value_blocks"] = fmt.Sprint(dataProbe.ValueBlocks)
			report.Extra["data_block_probe_value_unknowns"] = fmt.Sprint(dataProbe.ValueUnknowns)
			report.Extra["data_block_probe_null_values"] = fmt.Sprint(dataProbe.NullValues)
			report.Extra["data_block_probe_record_samples"] = fmt.Sprint(dataProbe.RecordSamples)
			report.Extra["data_block_probe_filter_rows"] = fmt.Sprint(dataProbe.FilterRows)
			report.Extra["data_block_probe_filter_matches"] = fmt.Sprint(dataProbe.FilterMatches)
			report.Extra["data_block_probe_filter_rejects"] = fmt.Sprint(dataProbe.FilterRejects)
			report.Extra["data_block_probe_filter_evaluations"] = fmt.Sprint(dataProbe.FilterEvaluations)
			report.Extra["data_block_probe_required_filter_evaluations"] = fmt.Sprint(dataProbe.FilterRequiredEvals)
			report.Extra["data_block_probe_any_filter_evaluations"] = fmt.Sprint(dataProbe.FilterAnyEvals)
			report.Extra["data_block_probe_none_filter_evaluations"] = fmt.Sprint(dataProbe.FilterNoneEvals)
			report.Extra["data_block_probe_filter_evaluation_matches"] = fmt.Sprint(dataProbe.FilterEvalMatches)
			report.Extra["data_block_probe_filter_evaluation_misses"] = fmt.Sprint(dataProbe.FilterEvalMisses)
			if len(dataProbe.FilterOperators) > 0 {
				report.Extra["data_block_probe_filter_operator_evaluations"] = tsspDetachedDataProbeTypeSummary(dataProbe.FilterOperators)
			}
			if len(dataProbe.BlockTypes) > 0 {
				report.Extra["data_block_probe_types"] = tsspDetachedDataProbeTypeSummary(dataProbe.BlockTypes)
			}
			if len(dataProbe.ValueUnknownReasons) > 0 {
				reasonSummary := tsspDetachedDataProbeTypeSummary(dataProbe.ValueUnknownReasons)
				report.Extra["data_block_probe_value_unknown_reasons"] = reasonSummary
				report.Notices = append(report.Notices, fmt.Sprintf("TSSP data block probe found %d block(s) with unavailable value samples: %s", dataProbe.ValueUnknowns, reasonSummary))
			}
			if dataProbe.Failures() > 0 {
				report.Notices = append(report.Notices, fmt.Sprintf("TSSP data block probe found %d invalid block(s)", dataProbe.Failures()))
			}
		}
	}
	report.DecodePath = buildTSSPDecodePathSummary(metaIndexes, chunkMetas, options, dataProbe)
	if !expanded && options.QueryRange.Set {
		report.Extra["query_overlap_precision"] = "meta-index"
		report.Notices = append(report.Notices, "TSSP query overlap is estimated at meta-index granularity; individual chunk overlap requires chunk metadata expansion")
	}
	return report, nil
}

func parseTSSPTrailer(b []byte) (tsspTrailer, error) {
	var trailer tsspTrailer
	const fixed = 6*8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8
	if len(b) < fixed+2 {
		return trailer, fmt.Errorf("short TSSP trailer")
	}
	offset := 0
	nextInt64 := func() int64 {
		value := decodeGeminiInt64(b[offset : offset+8])
		offset += 8
		return value
	}
	nextUint64 := func() uint64 {
		value := binary.BigEndian.Uint64(b[offset : offset+8])
		offset += 8
		return value
	}

	trailer.DataOffset = nextInt64()
	trailer.DataSize = nextInt64()
	trailer.IndexSize = nextInt64()
	trailer.MetaIndexSize = nextInt64()
	trailer.BloomSize = nextInt64()
	trailer.IDTimeSize = nextInt64()
	trailer.IDCount = nextInt64()
	trailer.MinID = nextUint64()
	trailer.MaxID = nextUint64()
	trailer.MinTime = nextInt64()
	trailer.MaxTime = nextInt64()
	trailer.MetaIndexItemCount = nextInt64()
	trailer.BloomM = nextUint64()
	trailer.BloomK = nextUint64()

	if len(b)-offset < 2 {
		return trailer, fmt.Errorf("short TSSP extra data length")
	}
	extraLen := int(binary.BigEndian.Uint16(b[offset : offset+2]))
	offset += 2
	if len(b)-offset < extraLen {
		return trailer, fmt.Errorf("short TSSP extra data")
	}
	actualExtraLen := extraLen
	if extraLen >= 8 {
		extra := b[offset : offset+extraLen]
		flags := binary.LittleEndian.Uint64(extra[:8])
		trailer.TimeStoreFlag = uint8(flags & 0xff)
		trailer.ChunkMetaCompress = uint8((flags >> 8) & 0xff)
		// openGemini writes an 8-byte declared length for ExtraData, then stores
		// the actual ExtraData byte length in the upper 32 bits of the flags word.
		if size := int(flags >> 32); size > 0 {
			actualExtraLen = size
		}
	} else if extraLen == 1 {
		trailer.TimeStoreFlag = b[offset]
	} else if extraLen == 2 {
		trailer.TimeStoreFlag = b[offset]
		trailer.ChunkMetaCompress = b[offset+1]
	}
	if len(b)-offset < actualExtraLen {
		return trailer, fmt.Errorf("short TSSP expanded extra data")
	}
	if actualExtraLen >= 8 {
		header, err := parseTSSPChunkMetaHeader(b[offset : offset+actualExtraLen])
		if err != nil {
			return trailer, err
		}
		trailer.ChunkMetaHeader = header
	}
	offset += actualExtraLen

	if len(b)-offset < 2 {
		return trailer, fmt.Errorf("short TSSP measurement length")
	}
	nameLen := int(binary.BigEndian.Uint16(b[offset : offset+2]))
	offset += 2
	if len(b)-offset < nameLen {
		return trailer, fmt.Errorf("short TSSP measurement name")
	}
	trailer.MeasurementName = string(b[offset : offset+nameLen])
	return trailer, nil
}

func parseTSSPChunkMetaHeader(extra []byte) ([]string, error) {
	if len(extra) < 10 {
		return nil, nil
	}
	offset := 8
	// openGemini reserves this value for the number of header strings, but the
	// reader decodes strings until the extra-data payload ends.
	_ = binary.BigEndian.Uint16(extra[offset : offset+2])
	offset += 2

	values := []string(nil)
	for offset < len(extra) {
		if len(extra)-offset < 2 {
			return nil, fmt.Errorf("short TSSP chunk metadata header string length")
		}
		nameLen := int(binary.BigEndian.Uint16(extra[offset : offset+2]))
		offset += 2
		if len(extra)-offset < nameLen {
			return nil, fmt.Errorf("short TSSP chunk metadata header string")
		}
		values = append(values, string(extra[offset:offset+nameLen]))
		offset += nameLen
	}
	return values, nil
}

func readTSSPMetaIndexes(f *os.File, offset, size int64) ([]tsspMetaIndex, error) {
	if size == 0 {
		return nil, nil
	}
	if size%tsspMetaIndexLen != 0 {
		return nil, fmt.Errorf("meta-index size %d is not a multiple of %d", size, tsspMetaIndexLen)
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	items := make([]tsspMetaIndex, 0, int(size)/tsspMetaIndexLen)
	for len(buf) > 0 {
		items = append(items, tsspMetaIndex{
			ID:      binary.BigEndian.Uint64(buf[:8]),
			MinTime: decodeGeminiInt64(buf[8:16]),
			MaxTime: decodeGeminiInt64(buf[16:24]),
			Offset:  decodeGeminiInt64(buf[24:32]),
			Count:   binary.BigEndian.Uint32(buf[32:36]),
			Size:    binary.BigEndian.Uint32(buf[36:40]),
		})
		buf = buf[tsspMetaIndexLen:]
	}
	return items, nil
}

func readTSSPChunkMetas(f *os.File, items []tsspMetaIndex, trailerOffset int64, trailer tsspTrailer) ([]tsspChunkMeta, error) {
	chunks := make([]tsspChunkMeta, 0, sumTSSPChunkMetaCount(items))
	for idx, item := range items {
		metas, err := readTSSPChunkMetaBlock(f, item, trailerOffset, trailer)
		if err != nil {
			return nil, fmt.Errorf("meta-index %d sid %d: %w", idx, item.ID, err)
		}
		chunks = append(chunks, metas...)
	}
	return chunks, nil
}

func readTSSPChunkMetaBlock(f *os.File, item tsspMetaIndex, trailerOffset int64, trailer tsspTrailer) ([]tsspChunkMeta, error) {
	if item.Count == 0 {
		return nil, nil
	}
	if item.Size == 0 {
		return nil, fmt.Errorf("empty chunk metadata block")
	}
	size := int64(item.Size)
	if item.Offset < tsspHeaderSize || item.Offset > trailerOffset || size > trailerOffset-item.Offset {
		return nil, fmt.Errorf("invalid chunk metadata range offset=%d size=%d", item.Offset, item.Size)
	}
	buf := make([]byte, int(item.Size))
	if _, err := f.ReadAt(buf, item.Offset); err != nil {
		return nil, err
	}
	buf, err := decompressTSSPChunkMetaBlock(buf, trailer.ChunkMetaCompress)
	if err != nil {
		return nil, err
	}
	data, offsets, err := splitTSSPChunkMetaData(buf, item.Count)
	if err != nil {
		return nil, err
	}

	chunks := make([]tsspChunkMeta, 0, int(item.Count))
	for i, start := range offsets {
		end := uint32(len(data))
		if i < len(offsets)-1 {
			end = offsets[i+1]
		}
		if start >= end || int(end) > len(data) {
			return nil, fmt.Errorf("invalid chunk metadata offset window start=%d end=%d", start, end)
		}
		chunk, err := parseTSSPChunkMetaBlockForMode(data[start:end], trailer.ChunkMetaCompress, trailer.ChunkMetaHeader)
		if err != nil {
			return nil, fmt.Errorf("chunk %d: %w", i, err)
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func decompressTSSPChunkMetaBlock(src []byte, mode uint8) ([]byte, error) {
	switch mode {
	case tsspChunkMetaCompressNone, tsspChunkMetaCompressSelf:
		return src, nil
	case tsspChunkMetaCompressSnappy:
		return snappy.Decode(nil, src)
	case tsspChunkMetaCompressLZ4:
		if len(src) < 4 {
			return nil, fmt.Errorf("short LZ4 chunk metadata block")
		}
		// openGemini IndexCompressWriter prefixes LZ4 chunk-meta blocks with the uncompressed length.
		originalLen := binary.BigEndian.Uint32(src[:4])
		if originalLen == 0 {
			return nil, fmt.Errorf("invalid LZ4 chunk metadata original length 0")
		}
		dst := make([]byte, int(originalLen))
		n, err := lz4.UncompressBlock(src[4:], dst)
		if err != nil {
			return nil, err
		}
		if n != int(originalLen) {
			return nil, fmt.Errorf("LZ4 chunk metadata length mismatch got=%d want=%d", n, originalLen)
		}
		return dst, nil
	default:
		return nil, fmt.Errorf("unsupported chunk metadata compression mode %d", mode)
	}
}

func splitTSSPChunkMetaData(src []byte, itemCount uint32) ([]byte, []uint32, error) {
	if itemCount == 0 {
		return src, nil, nil
	}
	offsetBytes := int64(itemCount) * 4
	if int64(len(src)) <= offsetBytes {
		return nil, nil, fmt.Errorf("short chunk metadata block count=%d bytes=%d", itemCount, len(src))
	}
	dataLen := len(src) - int(offsetBytes)
	data := src[:dataLen]
	offsets := make([]uint32, int(itemCount))
	for i := range offsets {
		offsets[i] = binary.BigEndian.Uint32(src[dataLen+i*4 : dataLen+(i+1)*4])
		if int(offsets[i]) >= len(data) {
			return nil, nil, fmt.Errorf("chunk metadata offset %d out of range %d", offsets[i], len(data))
		}
		// openGemini writes absolute offsets into the chunk-meta data section;
		// the first chunk starts at byte zero.
		if i == 0 && offsets[i] != 0 {
			return nil, nil, fmt.Errorf("first chunk metadata offset = %d, want 0", offsets[i])
		}
		if i > 0 && offsets[i] <= offsets[i-1] {
			return nil, nil, fmt.Errorf("chunk metadata offsets are not increasing at index %d", i)
		}
	}
	return data, offsets, nil
}

func parseTSSPChunkMetaBlockForMode(block []byte, mode uint8, header []string) (tsspChunkMeta, error) {
	if mode == tsspChunkMetaCompressSelf {
		return parseTSSPSelfCompressedChunkMetaBlock(block, header)
	}
	return parseTSSPChunkMetaBlock(block)
}

func parseTSSPChunkMetaBlock(block []byte) (tsspChunkMeta, error) {
	chunk, _, err := parseTSSPChunkMetaBlockWithConsumed(block)
	return chunk, err
}

func parseTSSPChunkMetaBlockWithConsumed(block []byte) (tsspChunkMeta, int, error) {
	var chunk tsspChunkMeta
	if len(block) < tsspChunkMetaFixedLen {
		return chunk, 0, fmt.Errorf("short chunk metadata header")
	}
	offset := 0
	chunk.SID = binary.BigEndian.Uint64(block[offset : offset+8])
	offset += 8
	chunk.Offset = decodeGeminiInt64(block[offset : offset+8])
	offset += 8
	chunk.Size = binary.BigEndian.Uint32(block[offset : offset+4])
	offset += 4
	chunk.ColumnCount = binary.BigEndian.Uint32(block[offset : offset+4])
	offset += 4
	chunk.SegmentCount = binary.BigEndian.Uint32(block[offset : offset+4])
	offset += 4

	if chunk.SegmentCount == 0 {
		return chunk, 0, fmt.Errorf("chunk metadata has zero segments")
	}
	if chunk.ColumnCount == 0 {
		return chunk, 0, fmt.Errorf("chunk metadata has zero columns")
	}
	timeRangeBytes := int64(chunk.SegmentCount) * tsspSegmentRangeLen
	if int64(len(block)-offset) < timeRangeBytes {
		return chunk, 0, fmt.Errorf("short chunk time ranges")
	}
	chunk.TimeRanges = make([]tsspTimeRange, int(chunk.SegmentCount))
	for i := range chunk.TimeRanges {
		chunk.TimeRanges[i] = tsspTimeRange{
			Min: decodeGeminiInt64(block[offset : offset+8]),
			Max: decodeGeminiInt64(block[offset+8 : offset+16]),
		}
		offset += tsspSegmentRangeLen
	}

	minColumnBytes := int64(5) + int64(chunk.SegmentCount)*tsspSegmentLen
	// This sanity bound protects allocations. parseTSSPColumnMeta still owns
	// exact validation because names and pre-aggregation payloads are variable.
	if int64(chunk.ColumnCount) > int64(len(block)-offset)/minColumnBytes {
		return chunk, 0, fmt.Errorf("short chunk columns")
	}
	chunk.Columns = make([]tsspColumnMeta, int(chunk.ColumnCount))
	rest := block[offset:]
	for i := range chunk.Columns {
		column, remaining, err := parseTSSPColumnMeta(rest, chunk.SegmentCount)
		if err != nil {
			return chunk, 0, fmt.Errorf("column %d: %w", i, err)
		}
		chunk.Columns[i] = column
		rest = remaining
	}
	// openGemini ignores the unmarshal remainder; match that tolerance for
	// padding or future extension bytes after the known column metadata.
	return chunk, len(block) - len(rest), nil
}

func parseTSSPSelfCompressedChunkMetaBlock(block []byte, header []string) (tsspChunkMeta, error) {
	var chunk tsspChunkMeta
	if len(header) == 0 {
		return chunk, fmt.Errorf("self-compressed chunk metadata missing header dictionary")
	}
	if len(block) < 8 {
		return chunk, fmt.Errorf("short self-compressed chunk metadata header")
	}
	chunk.SID = binary.BigEndian.Uint64(block[:8])
	rest := block[8:]

	var ok bool
	var value uint64
	if value, rest, ok = readTSSPUvarint(rest); !ok {
		return chunk, fmt.Errorf("invalid self-compressed chunk data offset")
	}
	chunk.Offset = int64(value)
	if value, rest, ok = readTSSPUvarint(rest); !ok {
		return chunk, fmt.Errorf("invalid self-compressed chunk data size")
	}
	chunk.Size = uint32(value)
	if value, rest, ok = readTSSPUvarint(rest); !ok {
		return chunk, fmt.Errorf("invalid self-compressed chunk column count")
	}
	chunk.ColumnCount = uint32(value)
	if value, rest, ok = readTSSPUvarint(rest); !ok {
		return chunk, fmt.Errorf("invalid self-compressed chunk segment count")
	}
	chunk.SegmentCount = uint32(value)

	if chunk.SegmentCount == 0 {
		return chunk, fmt.Errorf("chunk metadata has zero segments")
	}
	if chunk.ColumnCount == 0 {
		return chunk, fmt.Errorf("chunk metadata has zero columns")
	}
	if int64(chunk.SegmentCount) > int64(len(rest)) {
		return chunk, fmt.Errorf("short self-compressed chunk time ranges")
	}
	chunk.TimeRanges = make([]tsspTimeRange, int(chunk.SegmentCount))
	timeTargets := make([]*int64, 0, int(chunk.SegmentCount)*2)
	for i := range chunk.TimeRanges {
		timeTargets = append(timeTargets, &chunk.TimeRanges[i].Min, &chunk.TimeRanges[i].Max)
	}
	rest, ok = decodeTSSPInt64sWithScale(rest, timeTargets...)
	if !ok {
		return chunk, fmt.Errorf("invalid self-compressed chunk time ranges")
	}

	minColumnBytes := int64(1+1+1+8) + int64(chunk.SegmentCount)*4
	if int64(chunk.ColumnCount) > int64(len(rest))/minColumnBytes {
		return chunk, fmt.Errorf("short self-compressed chunk columns")
	}
	chunk.Columns = make([]tsspColumnMeta, int(chunk.ColumnCount))
	for i := range chunk.Columns {
		column, remaining, err := parseTSSPSelfCompressedColumnMeta(rest, chunk.SegmentCount, header)
		if err != nil {
			return chunk, fmt.Errorf("column %d: %w", i, err)
		}
		chunk.Columns[i] = column
		rest = remaining
	}
	return chunk, nil
}

func parseTSSPSelfCompressedColumnMeta(src []byte, segmentCount uint32, header []string) (tsspColumnMeta, []byte, error) {
	var column tsspColumnMeta
	nameIndex, src, ok := readTSSPUvarint(src)
	if !ok {
		return column, src, fmt.Errorf("invalid column name index")
	}
	if nameIndex >= uint64(len(header)) {
		return column, src, fmt.Errorf("column name index %d out of range %d", nameIndex, len(header))
	}
	column.Name = header[nameIndex]

	minBytes := 1 + 1 + 8 + int(segmentCount)*4
	if len(src) < minBytes {
		return column, src, fmt.Errorf("short column metadata")
	}
	column.Type = src[0]
	preAggLen := int(src[1])
	src = src[2:]
	if len(src) < preAggLen+8+int(segmentCount)*4 {
		return column, src, fmt.Errorf("short column pre-aggregation data")
	}
	column.PreAggBytes = preAggLen
	src = src[preAggLen:]

	offset := int64(binary.BigEndian.Uint64(src[:8]))
	src = src[8:]
	column.Segments = make([]tsspSegment, int(segmentCount))
	for i := range column.Segments {
		size := binary.BigEndian.Uint32(src[:4])
		src = src[4:]
		column.Segments[i] = tsspSegment{
			Offset: offset,
			Size:   size,
		}
		offset += int64(size)
	}
	return column, src, nil
}

func readTSSPUvarint(src []byte) (uint64, []byte, bool) {
	value, n := binary.Uvarint(src)
	if n <= 0 {
		return 0, src, false
	}
	return value, src[n:], true
}

var tsspInt64Scales = [4]int64{1, 1e3, 1e6, 1e9}

func decodeTSSPInt64sWithScale(src []byte, dst ...*int64) ([]byte, bool) {
	if len(src) < 1 {
		return src, false
	}
	scaleIndex := int(src[0])
	if scaleIndex < 0 || scaleIndex >= len(tsspInt64Scales) {
		return src, false
	}
	scale := tsspInt64Scales[scaleIndex]
	offset := 1
	for i, target := range dst {
		value, n := binary.Uvarint(src[offset:])
		if n <= 0 {
			return src, false
		}
		offset += n
		*target = int64(value) * scale
		if i > 0 {
			*target += *dst[i-1]
		}
	}
	return src[offset:], true
}

func parseTSSPColumnMeta(src []byte, segmentCount uint32) (tsspColumnMeta, []byte, error) {
	var column tsspColumnMeta
	if len(src) < 2 {
		return column, src, fmt.Errorf("short column name length")
	}
	nameLen := int(binary.BigEndian.Uint16(src[:2]))
	src = src[2:]
	if len(src) < nameLen+1+2 {
		return column, src, fmt.Errorf("short column metadata for name length %d", nameLen)
	}
	column.Name = string(src[:nameLen])
	src = src[nameLen:]
	column.Type = src[0]
	src = src[1:]

	preAggLen := int(binary.BigEndian.Uint16(src[:2]))
	src = src[2:]
	if len(src) < preAggLen {
		return column, src, fmt.Errorf("short column pre-aggregation data")
	}
	column.PreAggBytes = preAggLen
	src = src[preAggLen:]

	segmentBytes := int64(segmentCount) * tsspSegmentLen
	if int64(len(src)) < segmentBytes {
		return column, src, fmt.Errorf("short column segment metadata")
	}
	column.Segments = make([]tsspSegment, int(segmentCount))
	for i := range column.Segments {
		column.Segments[i] = tsspSegment{
			Offset: decodeGeminiInt64(src[:8]),
			Size:   binary.BigEndian.Uint32(src[8:12]),
		}
		src = src[tsspSegmentLen:]
	}
	return column, src, nil
}

func populateTSSPChunkReports(report *FileReport, chunks []tsspChunkMeta, options Options) {
	seriesSet := querySeriesIDSet(options.QuerySeriesIDs)
	for i, chunk := range chunks {
		overlaps := tsspQuerySeriesSelected(chunk.SID, seriesSet) && chunk.queryOverlaps(options.QueryRange)
		if overlaps {
			report.QueryOverlapBlocks++
		}
		if i < options.BlockSampleLimit {
			minTime, maxTime := chunk.minMaxTime()
			report.Blocks = append(report.Blocks, BlockReport{
				SeriesID:      chunk.SID,
				MinTime:       minTime,
				MaxTime:       maxTime,
				Type:          "chunk-meta",
				Offset:        chunk.Offset,
				SizeBytes:     chunk.Size,
				ColumnCount:   int(chunk.ColumnCount),
				SegmentCount:  int(chunk.SegmentCount),
				QueryOverlaps: overlaps,
			})
		}
	}
}

func populateTSSPMetaIndexReports(report *FileReport, metaIndexes []tsspMetaIndex, options Options) {
	seriesSet := querySeriesIDSet(options.QuerySeriesIDs)
	for i, meta := range metaIndexes {
		overlaps := tsspQuerySeriesSelected(meta.ID, seriesSet) && options.QueryRange.Overlaps(meta.MinTime, meta.MaxTime)
		if overlaps {
			report.QueryOverlapBlocks += int(meta.Count)
		}
		if i < options.BlockSampleLimit {
			report.Blocks = append(report.Blocks, BlockReport{
				SeriesID:        meta.ID,
				MinTime:         meta.MinTime,
				MaxTime:         meta.MaxTime,
				Type:            "meta-index",
				Offset:          meta.Offset,
				SizeBytes:       meta.Size,
				QueryOverlaps:   overlaps,
				ContainedChunks: int(meta.Count),
			})
		}
	}
}

func (m tsspChunkMeta) minMaxTime() (int64, int64) {
	if len(m.TimeRanges) == 0 {
		return 0, 0
	}
	return m.TimeRanges[0].Min, m.TimeRanges[len(m.TimeRanges)-1].Max
}

func (m tsspChunkMeta) queryOverlaps(r TimeRange) bool {
	if !r.Set {
		return false
	}
	for _, timeRange := range m.TimeRanges {
		if r.Overlaps(timeRange.Min, timeRange.Max) {
			return true
		}
	}
	return false
}

func tsspQuerySeriesSelected(id uint64, seriesSet map[uint64]struct{}) bool {
	if len(seriesSet) == 0 {
		return true
	}
	_, ok := seriesSet[id]
	return ok
}

func sumTSSPChunkMetaCount(items []tsspMetaIndex) int {
	total := 0
	for _, item := range items {
		total += int(item.Count)
	}
	return total
}

// openGemini numberenc stores signed int64 values as big-endian zig-zag bits.
func decodeGeminiInt64(src []byte) int64 {
	u := binary.BigEndian.Uint64(src)
	return decodeGeminiZigZagUint64(u)
}

func decodeGeminiZigZagUint64(u uint64) int64 {
	return int64(u>>1) ^ (int64(u<<63) >> 63)
}
