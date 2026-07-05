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
	if trailer.ChunkMetaCompress != 0 {
		report.Notices = append(report.Notices, "chunk metadata is compressed; detailed chunk decode is not expanded in this report")
	}
	if options.QueryRange.Set {
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

func sumTSSPChunkMetaCount(items []tsspMetaIndex) int {
	total := 0
	for _, item := range items {
		total += int(item.Count)
	}
	return total
}

func decodeGeminiInt64(src []byte) int64 {
	u := binary.BigEndian.Uint64(src)
	return int64(u>>1) ^ (int64(u<<63) >> 63)
}
