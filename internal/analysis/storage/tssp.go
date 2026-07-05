package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	tsspMagic        = "53ac2021"
	tsspHeaderSize   = 16
	tsspFooterSize   = 8
	tsspMetaIndexLen = 40

	tsspChunkMetaCompressNone = 0
	tsspChunkMetaFixedLen     = 8 + 8 + 4 + 4 + 4
	tsspSegmentLen            = 8 + 4
	tsspSegmentRangeLen       = 8 + 8
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
	if trailer.ChunkMetaCompress == tsspChunkMetaCompressNone {
		chunks, err := readTSSPChunkMetas(f, metaIndexes, trailerOffset)
		if err != nil {
			report.Notices = append(report.Notices, fmt.Sprintf("chunk metadata expansion unavailable: %v", err))
		} else {
			chunkMetas = chunks
			expanded = true
			report.BlockCount = len(chunkMetas)
			report.BlocksByType["chunk-meta"] = len(chunkMetas)
			report.Extra["chunk_meta_expanded"] = "true"
			if options.QueryRange.Set {
				report.Extra["query_overlap_precision"] = "chunk-meta"
			}
		}
	} else {
		report.Notices = append(report.Notices, "chunk metadata is compressed; detailed chunk decode is not expanded in this report")
	}

	if expanded {
		populateTSSPChunkReports(&report, chunkMetas, options)
	} else {
		populateTSSPMetaIndexReports(&report, metaIndexes, options)
	}
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
		flags := binary.LittleEndian.Uint64(b[offset : offset+8])
		trailer.TimeStoreFlag = uint8(flags & 0xff)
		trailer.ChunkMetaCompress = uint8((flags >> 8) & 0xff)
		// openGemini writes an 8-byte declared length for ExtraData, then stores
		// the actual ExtraData byte length in the upper 32 bits of the flags word.
		if size := int(flags >> 32); size > actualExtraLen {
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

func readTSSPChunkMetas(f *os.File, items []tsspMetaIndex, trailerOffset int64) ([]tsspChunkMeta, error) {
	chunks := make([]tsspChunkMeta, 0, sumTSSPChunkMetaCount(items))
	for idx, item := range items {
		metas, err := readTSSPChunkMetaBlock(f, item, trailerOffset)
		if err != nil {
			return nil, fmt.Errorf("meta-index %d sid %d: %w", idx, item.ID, err)
		}
		chunks = append(chunks, metas...)
	}
	return chunks, nil
}

func readTSSPChunkMetaBlock(f *os.File, item tsspMetaIndex, trailerOffset int64) ([]tsspChunkMeta, error) {
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
		chunk, err := parseTSSPChunkMetaBlock(data[start:end])
		if err != nil {
			return nil, fmt.Errorf("chunk %d: %w", i, err)
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
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

func parseTSSPChunkMetaBlock(block []byte) (tsspChunkMeta, error) {
	var chunk tsspChunkMeta
	if len(block) < tsspChunkMetaFixedLen {
		return chunk, fmt.Errorf("short chunk metadata header")
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
		return chunk, fmt.Errorf("chunk metadata has zero segments")
	}
	if chunk.ColumnCount == 0 {
		return chunk, fmt.Errorf("chunk metadata has zero columns")
	}
	timeRangeBytes := int64(chunk.SegmentCount) * tsspSegmentRangeLen
	if int64(len(block)-offset) < timeRangeBytes {
		return chunk, fmt.Errorf("short chunk time ranges")
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
		return chunk, fmt.Errorf("short chunk columns")
	}
	chunk.Columns = make([]tsspColumnMeta, int(chunk.ColumnCount))
	rest := block[offset:]
	for i := range chunk.Columns {
		column, remaining, err := parseTSSPColumnMeta(rest, chunk.SegmentCount)
		if err != nil {
			return chunk, fmt.Errorf("column %d: %w", i, err)
		}
		chunk.Columns[i] = column
		rest = remaining
	}
	// openGemini ignores the unmarshal remainder; match that tolerance for
	// padding or future extension bytes after the known column metadata.
	return chunk, nil
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
	for i, chunk := range chunks {
		overlaps := chunk.queryOverlaps(options.QueryRange)
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
	for i, meta := range metaIndexes {
		overlaps := options.QueryRange.Overlaps(meta.MinTime, meta.MaxTime)
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
	return int64(u>>1) ^ (int64(u<<63) >> 63)
}
