package storage

import (
	"encoding/binary"
	"fmt"
	"os"
)

const (
	tsiMagic = "TSI1"

	tsiIndexFileVersion     = 1
	tsiIndexFileTrailerSize = 82

	tsiMeasurementBlockVersion     = 1
	tsiMeasurementBlockTrailerSize = 66
	tsiMeasurementFillSize         = 1

	tsiTagBlockVersion     = 1
	tsiTagBlockTrailerSize = 58

	tsiMeasurementTombstoneFlag   = 0x01
	tsiMeasurementSeriesIDSetFlag = 0x02
	tsiTagKeyTombstoneFlag        = 0x01
	tsiTagValueTombstoneFlag      = 0x01
	tsiTagValueSeriesIDSetFlag    = 0x02
)

type tsiRange struct {
	Offset int64
	Size   int64
}

type tsiIndexTrailer struct {
	Version               int
	MeasurementBlock      tsiRange
	SeriesIDSet           tsiRange
	TombstoneSeriesIDSet  tsiRange
	SeriesSketch          tsiRange
	TombstoneSeriesSketch tsiRange
}

type tsiMeasurementBlockTrailer struct {
	Version   int
	Data      tsiRange
	HashIndex tsiRange
	Sketch    tsiRange
	TSketch   tsiRange
}

type tsiTagBlockTrailer struct {
	Version   int
	ValueData tsiRange
	KeyData   tsiRange
	HashIndex tsiRange
	Size      int64
}

type tsiMeasurementElem struct {
	Flag             byte
	Name             string
	TagBlock         tsiRange
	SeriesCount      uint64
	SeriesDataSize   uint64
	SeriesIDSet      bool
	EncodedByteCount int
}

type tsiTagBlockSummary struct {
	TagKeyCount          int
	DeletedTagKeyCount   int
	TagValueCount        int
	DeletedTagValueCount int
}

type tsiTagKeyElem struct {
	Flag             byte
	Key              string
	Values           tsiRange
	ValueHashIndex   tsiRange
	EncodedByteCount int
}

type tsiTagValueElem struct {
	Flag             byte
	Value            string
	SeriesCount      uint64
	SeriesDataSize   uint64
	SeriesIDSet      bool
	EncodedByteCount int
}

func analyzeTSI(path string, info os.FileInfo, options Options) (FileReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return FileReport{}, err
	}
	if len(data) < len(tsiMagic)+tsiIndexFileTrailerSize {
		return FileReport{}, fmt.Errorf("file too small for TSI header/trailer")
	}
	if string(data[:len(tsiMagic)]) != tsiMagic {
		return FileReport{}, fmt.Errorf("invalid TSI magic")
	}

	trailer, err := parseTSIIndexTrailer(data)
	if err != nil {
		return FileReport{}, err
	}
	measurementBlock, err := sliceTSIRange(data, trailer.MeasurementBlock, "measurement block")
	if err != nil {
		return FileReport{}, err
	}

	index, keySamples, err := summarizeTSIMeasurements(data, measurementBlock, options)
	if err != nil {
		return FileReport{}, err
	}
	index.Type = "tsi1"
	index.SeriesIDSetBytes = trailer.SeriesIDSet.Size
	index.TombstoneSeriesSetBytes = trailer.TombstoneSeriesIDSet.Size
	index.SeriesSketchBytes = trailer.SeriesSketch.Size
	index.TombstoneSketchBytes = trailer.TombstoneSeriesSketch.Size

	report := FileReport{
		Path:       path,
		Format:     FormatTSI,
		SizeBytes:  info.Size(),
		ModTime:    info.ModTime(),
		KeyCount:   index.MeasurementCount,
		KeySamples: keySamples,
		BlockCount: index.MeasurementCount + index.TagKeyCount + index.TagValueCount,
		BlocksByType: map[string]int{
			"measurement": index.MeasurementCount,
			"tag-key":     index.TagKeyCount,
			"tag-value":   index.TagValueCount,
		},
		SeriesID: SeriesIDSummary{
			Count: index.SeriesRefs,
		},
		Index: &index,
		Extra: map[string]string{
			"version":                      fmt.Sprint(trailer.Version),
			"measurement_block_offset":     fmt.Sprint(trailer.MeasurementBlock.Offset),
			"measurement_block_size":       fmt.Sprint(trailer.MeasurementBlock.Size),
			"series_id_set_size":           fmt.Sprint(trailer.SeriesIDSet.Size),
			"tombstone_series_id_set_size": fmt.Sprint(trailer.TombstoneSeriesIDSet.Size),
			"series_sketch_size":           fmt.Sprint(trailer.SeriesSketch.Size),
			"tombstone_series_sketch_size": fmt.Sprint(trailer.TombstoneSeriesSketch.Size),
		},
	}
	return report, nil
}

func parseTSIIndexTrailer(data []byte) (tsiIndexTrailer, error) {
	var trailer tsiIndexTrailer
	if len(data) < tsiIndexFileTrailerSize {
		return trailer, fmt.Errorf("short TSI index trailer")
	}
	trailer.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if trailer.Version != tsiIndexFileVersion {
		return trailer, fmt.Errorf("unsupported TSI index version %d", trailer.Version)
	}

	buf := data[len(data)-tsiIndexFileTrailerSize:]
	trailer.MeasurementBlock, buf = readTSIRange(buf)
	trailer.SeriesIDSet, buf = readTSIRange(buf)
	trailer.TombstoneSeriesIDSet, buf = readTSIRange(buf)
	trailer.SeriesSketch, buf = readTSIRange(buf)
	trailer.TombstoneSeriesSketch, buf = readTSIRange(buf)
	if len(buf) != 2 {
		return trailer, fmt.Errorf("invalid TSI index trailer length")
	}
	if _, err := sliceTSIRange(data, trailer.MeasurementBlock, "measurement block"); err != nil {
		return trailer, err
	}
	for name, rng := range map[string]tsiRange{
		"series id set":           trailer.SeriesIDSet,
		"tombstone series id set": trailer.TombstoneSeriesIDSet,
		"series sketch":           trailer.SeriesSketch,
		"tombstone series sketch": trailer.TombstoneSeriesSketch,
	} {
		if _, err := sliceTSIRange(data, rng, name); err != nil {
			return trailer, err
		}
	}
	return trailer, nil
}

func summarizeTSIMeasurements(fileData, block []byte, options Options) (IndexSummary, []string, error) {
	var summary IndexSummary
	trailer, err := parseTSIMeasurementBlockTrailer(block)
	if err != nil {
		return summary, nil, err
	}
	data, err := sliceTSIRange(block, trailer.Data, "measurement data")
	if err != nil {
		return summary, nil, err
	}
	if len(data) < tsiMeasurementFillSize {
		return summary, nil, fmt.Errorf("short TSI measurement data")
	}
	data = data[tsiMeasurementFillSize:]

	keySamples := make([]string, 0, options.KeySampleLimit)
	for len(data) > 0 {
		elem, err := parseTSIMeasurementElem(data)
		if err != nil {
			return summary, keySamples, err
		}
		data = data[elem.EncodedByteCount:]

		summary.MeasurementCount++
		summary.SeriesRefs += int64(elem.SeriesCount)
		if elem.Flag&tsiMeasurementTombstoneFlag != 0 {
			summary.DeletedMeasurementCount++
		}
		if len(keySamples) < options.KeySampleLimit {
			keySamples = append(keySamples, elem.Name)
		}

		tagSummary, err := summarizeTSITagBlock(fileData, elem.TagBlock)
		if err != nil {
			return summary, keySamples, fmt.Errorf("measurement %q tag block: %w", elem.Name, err)
		}
		summary.TagKeyCount += tagSummary.TagKeyCount
		summary.DeletedTagKeyCount += tagSummary.DeletedTagKeyCount
		summary.TagValueCount += tagSummary.TagValueCount
		summary.DeletedTagValueCount += tagSummary.DeletedTagValueCount
		if len(summary.MeasurementSamples) < options.KeySampleLimit {
			summary.MeasurementSamples = append(summary.MeasurementSamples, IndexMeasurementReport{
				Name:                 elem.Name,
				Deleted:              elem.Flag&tsiMeasurementTombstoneFlag != 0,
				SeriesCount:          elem.SeriesCount,
				TagKeyCount:          tagSummary.TagKeyCount,
				DeletedTagKeyCount:   tagSummary.DeletedTagKeyCount,
				TagValueCount:        tagSummary.TagValueCount,
				DeletedTagValueCount: tagSummary.DeletedTagValueCount,
			})
		}
	}
	return summary, keySamples, nil
}

func parseTSIMeasurementBlockTrailer(data []byte) (tsiMeasurementBlockTrailer, error) {
	var trailer tsiMeasurementBlockTrailer
	if len(data) < tsiMeasurementBlockTrailerSize {
		return trailer, fmt.Errorf("short TSI measurement block trailer")
	}
	trailer.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if trailer.Version != tsiMeasurementBlockVersion {
		return trailer, fmt.Errorf("unsupported TSI measurement block version %d", trailer.Version)
	}
	buf := data[len(data)-tsiMeasurementBlockTrailerSize:]
	trailer.Data, buf = readTSIRange(buf)
	trailer.HashIndex, buf = readTSIRange(buf)
	trailer.Sketch, buf = readTSIRange(buf)
	trailer.TSketch, buf = readTSIRange(buf)
	if _, err := sliceTSIRange(data, trailer.Data, "measurement data"); err != nil {
		return trailer, err
	}
	for name, rng := range map[string]tsiRange{
		"measurement hash index": trailer.HashIndex,
		"measurement sketch":     trailer.Sketch,
		"measurement tsketch":    trailer.TSketch,
	} {
		if _, err := sliceTSIRange(data, rng, name); err != nil {
			return trailer, err
		}
	}
	return trailer, nil
}

func parseTSIMeasurementElem(data []byte) (tsiMeasurementElem, error) {
	var elem tsiMeasurementElem
	start := len(data)
	if len(data) < 1+8+8 {
		return elem, fmt.Errorf("short TSI measurement element")
	}
	elem.Flag, data = data[0], data[1:]
	elem.SeriesIDSet = elem.Flag&tsiMeasurementSeriesIDSetFlag != 0
	elem.TagBlock.Offset, data = int64(binary.BigEndian.Uint64(data[:8])), data[8:]
	elem.TagBlock.Size, data = int64(binary.BigEndian.Uint64(data[:8])), data[8:]

	nameLen, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("measurement name length: %w", err)
	}
	data = data[n:]
	if nameLen > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI measurement name")
	}
	elem.Name, data = string(data[:int(nameLen)]), data[int(nameLen):]

	seriesCount, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("measurement series count: %w", err)
	}
	elem.SeriesCount = seriesCount
	data = data[n:]

	seriesDataSize, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("measurement series data size: %w", err)
	}
	elem.SeriesDataSize = seriesDataSize
	data = data[n:]
	if seriesDataSize > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI measurement series data")
	}
	data = data[int(seriesDataSize):]
	elem.EncodedByteCount = start - len(data)
	return elem, nil
}

func summarizeTSITagBlock(fileData []byte, rng tsiRange) (tsiTagBlockSummary, error) {
	var summary tsiTagBlockSummary
	if rng.Size == 0 {
		return summary, nil
	}
	block, err := sliceTSIRange(fileData, rng, "tag block")
	if err != nil {
		return summary, err
	}
	trailer, err := parseTSITagBlockTrailer(block)
	if err != nil {
		return summary, err
	}
	if trailer.Size != int64(len(block)) {
		return summary, fmt.Errorf("tag block size mismatch: trailer=%d actual=%d", trailer.Size, len(block))
	}
	keyData, err := sliceTSIRange(block, trailer.KeyData, "tag key data")
	if err != nil {
		return summary, err
	}
	for len(keyData) > 0 {
		key, err := parseTSITagKeyElem(keyData, block)
		if err != nil {
			return summary, err
		}
		keyData = keyData[key.EncodedByteCount:]
		summary.TagKeyCount++
		if key.Flag&tsiTagKeyTombstoneFlag != 0 {
			summary.DeletedTagKeyCount++
		}
		values, err := sliceTSIRange(block, key.Values, "tag value data")
		if err != nil {
			return summary, err
		}
		for len(values) > 0 {
			value, err := parseTSITagValueElem(values)
			if err != nil {
				return summary, err
			}
			values = values[value.EncodedByteCount:]
			summary.TagValueCount++
			if value.Flag&tsiTagValueTombstoneFlag != 0 {
				summary.DeletedTagValueCount++
			}
		}
	}
	return summary, nil
}

func parseTSITagBlockTrailer(data []byte) (tsiTagBlockTrailer, error) {
	var trailer tsiTagBlockTrailer
	if len(data) < tsiTagBlockTrailerSize {
		return trailer, fmt.Errorf("short TSI tag block trailer")
	}
	trailer.Version = int(binary.BigEndian.Uint16(data[len(data)-2:]))
	if trailer.Version != tsiTagBlockVersion {
		return trailer, fmt.Errorf("unsupported TSI tag block version %d", trailer.Version)
	}
	buf := data[len(data)-tsiTagBlockTrailerSize:]
	trailer.ValueData, buf = readTSIRange(buf)
	trailer.KeyData, buf = readTSIRange(buf)
	trailer.HashIndex, buf = readTSIRange(buf)
	trailer.Size = int64(binary.BigEndian.Uint64(buf[:8]))
	for name, rng := range map[string]tsiRange{
		"tag value data": trailer.ValueData,
		"tag key data":   trailer.KeyData,
		"tag hash index": trailer.HashIndex,
	} {
		if _, err := sliceTSIRange(data, rng, name); err != nil {
			return trailer, err
		}
	}
	return trailer, nil
}

func parseTSITagKeyElem(data, block []byte) (tsiTagKeyElem, error) {
	var elem tsiTagKeyElem
	start := len(data)
	if len(data) < 1+8+8+8+8 {
		return elem, fmt.Errorf("short TSI tag key element")
	}
	elem.Flag, data = data[0], data[1:]
	elem.Values, data = readTSIRange(data)
	elem.ValueHashIndex, data = readTSIRange(data)
	if _, err := sliceTSIRange(block, elem.Values, "tag key value data"); err != nil {
		return elem, err
	}
	if _, err := sliceTSIRange(block, elem.ValueHashIndex, "tag key value hash index"); err != nil {
		return elem, err
	}
	keyLen, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("tag key length: %w", err)
	}
	data = data[n:]
	if keyLen > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI tag key")
	}
	elem.Key, data = string(data[:int(keyLen)]), data[int(keyLen):]
	elem.EncodedByteCount = start - len(data)
	return elem, nil
}

func parseTSITagValueElem(data []byte) (tsiTagValueElem, error) {
	var elem tsiTagValueElem
	start := len(data)
	if len(data) < 1 {
		return elem, fmt.Errorf("short TSI tag value element")
	}
	elem.Flag, data = data[0], data[1:]
	elem.SeriesIDSet = elem.Flag&tsiTagValueSeriesIDSetFlag != 0
	valueLen, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("tag value length: %w", err)
	}
	data = data[n:]
	if valueLen > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI tag value")
	}
	elem.Value, data = string(data[:int(valueLen)]), data[int(valueLen):]

	seriesCount, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("tag value series count: %w", err)
	}
	elem.SeriesCount = seriesCount
	data = data[n:]

	seriesDataSize, n, err := readTSIUvarint(data)
	if err != nil {
		return elem, fmt.Errorf("tag value series data size: %w", err)
	}
	elem.SeriesDataSize = seriesDataSize
	data = data[n:]
	if seriesDataSize > uint64(len(data)) {
		return elem, fmt.Errorf("short TSI tag value series data")
	}
	data = data[int(seriesDataSize):]
	elem.EncodedByteCount = start - len(data)
	return elem, nil
}

func readTSIRange(data []byte) (tsiRange, []byte) {
	return tsiRange{
		Offset: int64(binary.BigEndian.Uint64(data[:8])),
		Size:   int64(binary.BigEndian.Uint64(data[8:16])),
	}, data[16:]
}

func sliceTSIRange(data []byte, rng tsiRange, name string) ([]byte, error) {
	if rng.Offset < 0 || rng.Size < 0 || rng.Offset > int64(len(data)) || rng.Size > int64(len(data))-rng.Offset {
		return nil, fmt.Errorf("invalid TSI %s range offset=%d size=%d", name, rng.Offset, rng.Size)
	}
	return data[rng.Offset : rng.Offset+rng.Size], nil
}

func readTSIUvarint(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("short buffer")
	}
	value, n := binary.Uvarint(data)
	if n == 0 || n > len(data) {
		return 0, 0, fmt.Errorf("short buffer")
	}
	if n < 0 {
		return 0, 0, fmt.Errorf("invalid uvarint")
	}
	return value, n, nil
}
