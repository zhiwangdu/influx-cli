package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strconv"

	"github.com/golang/snappy"
)

const (
	tsmMagicNumber uint32 = 0x16D116D1
	tsmVersion     byte   = 1

	tsmHeaderSize     = 5
	tsmFooterSize     = 8
	tsmIndexEntrySize = 28
	tsmBlockCRCSize   = 4

	tsmBlockFloat    byte = 0
	tsmBlockInteger  byte = 1
	tsmBlockBoolean  byte = 2
	tsmBlockString   byte = 3
	tsmBlockUnsigned byte = 4

	tsmFloatCompressedGorilla byte   = 1
	tsmFloatNaNSentinel       uint64 = 0x7FF8000000000001
)

type tsmIndexEntry struct {
	Key                 string
	Type                byte
	MinTime             int64
	MaxTime             int64
	Offset              int64
	Size                uint32
	ValueCount          int
	ValueCountAvailable bool
	Timestamps          []int64
	Points              []tsmPoint
	PointsAvailable     bool
}

type tsmPoint struct {
	Timestamp int64
	Type      byte
	Value     string
	File      string
}

func analyzeTSM(path string, info os.FileInfo, options Options) (FileReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return FileReport{}, err
	}
	defer f.Close()

	if info.Size() < tsmHeaderSize+tsmFooterSize {
		return FileReport{}, fmt.Errorf("file too small for TSM header/footer")
	}

	header := make([]byte, tsmHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return FileReport{}, err
	}
	if binary.BigEndian.Uint32(header[:4]) != tsmMagicNumber {
		return FileReport{}, fmt.Errorf("invalid TSM magic")
	}
	if header[4] != tsmVersion {
		return FileReport{}, fmt.Errorf("unsupported TSM version %d", header[4])
	}

	indexOffset, indexSize, keys, entries, err := readTSMIndex(f, info.Size())
	if err != nil {
		return FileReport{}, err
	}
	valueCountErrors := populateTSMBlockValueCounts(f, entries)
	if len(entries) == 0 {
		return FileReport{}, fmt.Errorf("TSM index has no entries")
	}
	keySet := queryKeySet(options.QueryKeys)

	report := FileReport{
		Path:         path,
		Format:       FormatTSM,
		SizeBytes:    info.Size(),
		ModTime:      info.ModTime(),
		KeyCount:     len(keys),
		KeySamples:   sampleStrings(keys, options.KeySampleLimit),
		BlockCount:   len(entries),
		BlocksByType: map[string]int{},
		Extra: map[string]string{
			"index_offset": fmt.Sprint(indexOffset),
			"index_size":   fmt.Sprint(indexSize),
		},
	}
	report.MinKey = keys[0]
	report.MaxKey = keys[len(keys)-1]

	minTime, maxTime := entries[0].MinTime, entries[0].MaxTime
	for i, entry := range entries {
		if entry.MinTime < minTime {
			minTime = entry.MinTime
		}
		if entry.MaxTime > maxTime {
			maxTime = entry.MaxTime
		}
		typeName := tsmBlockTypeName(entry.Type)
		report.BlocksByType[typeName]++
		timeOverlaps := options.QueryRange.Overlaps(entry.MinTime, entry.MaxTime)
		queryOverlaps := timeOverlaps && tsmQueryKeySelected(entry.Key, keySet)
		if queryOverlaps {
			report.QueryOverlapBlocks++
		}
		if i < options.BlockSampleLimit {
			block := BlockReport{
				Key:           entry.Key,
				MinTime:       entry.MinTime,
				MaxTime:       entry.MaxTime,
				Type:          typeName,
				Offset:        entry.Offset,
				SizeBytes:     entry.Size,
				QueryOverlaps: queryOverlaps,
			}
			if entry.ValueCountAvailable {
				block.ValueCount = entry.ValueCount
			} else if valueCountErrors[i] != nil {
				report.Notices = append(report.Notices, fmt.Sprintf("block %d count unavailable: %v", i, valueCountErrors[i]))
			}
			report.Blocks = append(report.Blocks, block)
		}
	}
	report.MinTime = minTime
	report.MaxTime = maxTime
	report.QueryOverlapsFile = options.QueryRange.Overlaps(minTime, maxTime)
	if options.QueryRange.Set && len(keySet) > 0 {
		report.QueryOverlapsFile = report.QueryOverlapBlocks > 0
	}
	tombstones, tombstoneEntries, err := readTSMTombstoneSummary(path, entries, options)
	if err != nil {
		report.Notices = append(report.Notices, fmt.Sprintf("tombstone detail unavailable: %v", err))
	}
	report.Tombstones = tombstones
	report.DecodePath = buildTSMDecodePathSummary(entries, tombstoneEntries, options)
	return report, nil
}

func populateTSMBlockValueCounts(f *os.File, entries []tsmIndexEntry) []error {
	errs := make([]error, len(entries))
	for i := range entries {
		timestamps, points, pointsAvailable, err := readTSMBlockTimestampsAndPoints(f, entries[i])
		if err != nil {
			errs[i] = err
			continue
		}
		entries[i].ValueCount = len(timestamps)
		entries[i].ValueCountAvailable = true
		entries[i].Timestamps = timestamps
		entries[i].Points = points
		entries[i].PointsAvailable = pointsAvailable
	}
	return errs
}

func readTSMIndex(f *os.File, size int64) (int64, int64, []string, []tsmIndexEntry, error) {
	footer := make([]byte, tsmFooterSize)
	if _, err := f.ReadAt(footer, size-tsmFooterSize); err != nil {
		return 0, 0, nil, nil, err
	}
	indexOffset := int64(binary.BigEndian.Uint64(footer))
	if indexOffset < tsmHeaderSize || indexOffset >= size-tsmFooterSize {
		return 0, 0, nil, nil, fmt.Errorf("invalid TSM index offset %d", indexOffset)
	}

	indexSize := size - tsmFooterSize - indexOffset
	indexBytes := make([]byte, indexSize)
	if _, err := f.ReadAt(indexBytes, indexOffset); err != nil {
		return 0, 0, nil, nil, err
	}

	keys, entries, err := parseTSMIndex(indexBytes)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	return indexOffset, indexSize, keys, entries, nil
}

func parseTSMIndex(b []byte) ([]string, []tsmIndexEntry, error) {
	keys := make([]string, 0)
	entries := make([]tsmIndexEntry, 0)
	for offset := 0; offset < len(b); {
		if len(b)-offset < 2 {
			return nil, nil, fmt.Errorf("short key length at index offset %d", offset)
		}
		keyLen := int(binary.BigEndian.Uint16(b[offset : offset+2]))
		offset += 2
		if keyLen == 0 || len(b)-offset < keyLen+1+2 {
			return nil, nil, fmt.Errorf("short key entry at index offset %d", offset)
		}
		key := string(b[offset : offset+keyLen])
		offset += keyLen
		blockType := b[offset]
		offset++
		count := int(binary.BigEndian.Uint16(b[offset : offset+2]))
		offset += 2
		if count == 0 {
			return nil, nil, fmt.Errorf("key %q has zero index entries", key)
		}
		if len(b)-offset < count*tsmIndexEntrySize {
			return nil, nil, fmt.Errorf("short index entries for key %q", key)
		}
		keys = append(keys, key)
		for i := 0; i < count; i++ {
			entryBytes := b[offset : offset+tsmIndexEntrySize]
			offset += tsmIndexEntrySize
			entries = append(entries, tsmIndexEntry{
				Key:     key,
				Type:    blockType,
				MinTime: int64(binary.BigEndian.Uint64(entryBytes[:8])),
				MaxTime: int64(binary.BigEndian.Uint64(entryBytes[8:16])),
				Offset:  int64(binary.BigEndian.Uint64(entryBytes[16:24])),
				Size:    binary.BigEndian.Uint32(entryBytes[24:28]),
			})
		}
	}
	sort.Strings(keys)
	return keys, entries, nil
}

func readTSMBlockValueCount(f *os.File, entry tsmIndexEntry) (int, error) {
	timestamps, err := readTSMBlockTimestamps(f, entry)
	if err != nil {
		return 0, err
	}
	return len(timestamps), nil
}

func readTSMBlockTimestamps(f *os.File, entry tsmIndexEntry) ([]int64, error) {
	timestamps, _, _, err := readTSMBlockTimestampsAndPoints(f, entry)
	return timestamps, err
}

func readTSMBlockTimestampsAndPoints(f *os.File, entry tsmIndexEntry) ([]int64, []tsmPoint, bool, error) {
	if entry.Size <= tsmBlockCRCSize {
		return nil, nil, false, fmt.Errorf("short block size %d", entry.Size)
	}
	blockSize := int(entry.Size) - tsmBlockCRCSize
	raw := make([]byte, int(entry.Size))
	if _, err := f.ReadAt(raw, entry.Offset); err != nil {
		return nil, nil, false, err
	}
	checksum := binary.BigEndian.Uint32(raw[:tsmBlockCRCSize])
	block := raw[tsmBlockCRCSize:]
	if crc32.ChecksumIEEE(block) != checksum {
		return nil, nil, false, fmt.Errorf("crc mismatch")
	}
	if len(block) != blockSize {
		return nil, nil, false, fmt.Errorf("short block read")
	}
	return tsmBlockTimestampsAndPoints(block)
}

func tsmBlockCount(block []byte) (int, error) {
	timestamps, err := tsmBlockTimestamps(block)
	if err != nil {
		return 0, err
	}
	return len(timestamps), nil
}

func tsmBlockTimestamps(block []byte) ([]int64, error) {
	timestamps, _, _, err := tsmBlockTimestampsAndPoints(block)
	return timestamps, err
}

func tsmBlockTimestampsAndPoints(block []byte) ([]int64, []tsmPoint, bool, error) {
	if len(block) <= 1 {
		return nil, nil, false, fmt.Errorf("short encoded block")
	}
	timestampBlock, valueBlock, err := unpackTSMBlockParts(block[1:])
	if err != nil {
		return nil, nil, false, err
	}
	timestamps, err := decodeTSMTimestamps(timestampBlock)
	if err != nil {
		return nil, nil, false, err
	}
	points, err := decodeTSMBlockPoints(block[0], timestamps, valueBlock)
	if err != nil {
		return timestamps, nil, false, nil
	}
	return timestamps, points, true, nil
}

func unpackTSMBlockTimestamps(block []byte) ([]byte, error) {
	timestamps, _, err := unpackTSMBlockParts(block)
	return timestamps, err
}

func unpackTSMBlockParts(block []byte) ([]byte, []byte, error) {
	tsLen, n := binary.Uvarint(block)
	if n <= 0 {
		return nil, nil, fmt.Errorf("unable to read timestamp block length")
	}
	end := n + int(tsLen)
	if end > len(block) {
		return nil, nil, fmt.Errorf("timestamp block exceeds encoded block length")
	}
	return block[n:end], block[end:], nil
}

func countTSMTimestamps(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	encoding := b[0] >> 4
	switch encoding {
	case 0:
		return len(b[1:]) / 8, nil
	case 1:
		count, err := countSimple8bWords(b[9:])
		if err != nil {
			return 0, err
		}
		return count + 1, nil
	case 2:
		if len(b) < 9 {
			return 0, fmt.Errorf("short RLE timestamp block")
		}
		_, n := binary.Uvarint(b[9:])
		if n <= 0 {
			return 0, fmt.Errorf("invalid RLE delta")
		}
		count, n := binary.Uvarint(b[9+n:])
		if n <= 0 {
			return 0, fmt.Errorf("invalid RLE count")
		}
		return int(count), nil
	default:
		return 0, fmt.Errorf("unknown timestamp encoding %d", encoding)
	}
}

func decodeTSMTimestamps(b []byte) ([]int64, error) {
	if len(b) == 0 {
		return nil, nil
	}
	encoding := b[0] >> 4
	switch encoding {
	case 0:
		return decodeTSMRawTimestamps(b[1:])
	case 1:
		return decodeTSMSimple8bTimestamps(b)
	case 2:
		return decodeTSMRLETimestamps(b)
	default:
		return nil, fmt.Errorf("unknown timestamp encoding %d", encoding)
	}
}

func decodeTSMRawTimestamps(b []byte) ([]int64, error) {
	if len(b)%8 != 0 {
		return nil, fmt.Errorf("invalid raw timestamp byte length %d", len(b))
	}
	timestamps := make([]int64, len(b)/8)
	var previous uint64
	for i := range timestamps {
		value := binary.BigEndian.Uint64(b[i*8 : i*8+8])
		if i == 0 {
			previous = value
		} else {
			previous += value
		}
		timestamps[i] = int64(previous)
	}
	return timestamps, nil
}

func decodeTSMRLETimestamps(b []byte) ([]int64, error) {
	if len(b) < 9 {
		return nil, fmt.Errorf("short RLE timestamp block")
	}
	divisor, err := tsmTimestampDivisor(b[0] & 0x0f)
	if err != nil {
		return nil, err
	}
	offset := 1
	first := int64(binary.BigEndian.Uint64(b[offset : offset+8]))
	offset += 8
	delta, n := binary.Uvarint(b[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid RLE delta")
	}
	offset += n
	count, n := binary.Uvarint(b[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid RLE count")
	}
	timestamps := make([]int64, int(count))
	value := first
	step := int64(delta) * divisor
	for i := range timestamps {
		timestamps[i] = value
		value += step
	}
	return timestamps, nil
}

func decodeTSMSimple8bTimestamps(b []byte) ([]int64, error) {
	if len(b) < 9 {
		return nil, fmt.Errorf("short packed timestamp block")
	}
	divisor, err := tsmTimestampDivisor(b[0] & 0x0f)
	if err != nil {
		return nil, err
	}
	deltas, err := decodeSimple8bValues(b[9:])
	if err != nil {
		return nil, err
	}
	timestamps := make([]int64, 0, len(deltas)+1)
	previous := binary.BigEndian.Uint64(b[1:9])
	timestamps = append(timestamps, int64(previous))
	for _, delta := range deltas {
		previous += delta * uint64(divisor)
		timestamps = append(timestamps, int64(previous))
	}
	return timestamps, nil
}

func tsmTimestampDivisor(exp byte) (int64, error) {
	divisors := [...]int64{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000, 1000000000000}
	if int(exp) >= len(divisors) {
		return 0, fmt.Errorf("invalid timestamp divisor exponent %d", exp)
	}
	return divisors[exp], nil
}

func decodeSimple8bValues(b []byte) ([]uint64, error) {
	if len(b)%8 != 0 {
		return nil, fmt.Errorf("invalid simple8b byte length %d", len(b))
	}
	counts := [...]int{240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1}
	widths := [...]uint{0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60}
	values := make([]uint64, 0)
	for len(b) > 0 {
		word := binary.BigEndian.Uint64(b[:8])
		b = b[8:]
		selector := word >> 60
		if selector >= uint64(len(counts)) {
			return nil, fmt.Errorf("invalid simple8b selector %d", selector)
		}
		n := counts[selector]
		width := widths[selector]
		if width == 0 {
			for i := 0; i < n; i++ {
				values = append(values, 1)
			}
			continue
		}
		mask := (uint64(1) << width) - 1
		for i := 0; i < n; i++ {
			values = append(values, word&mask)
			word >>= width
		}
	}
	return values, nil
}

func decodeTSMBlockPoints(blockType byte, timestamps []int64, valueBlock []byte) ([]tsmPoint, error) {
	switch blockType {
	case tsmBlockFloat:
		values, err := decodeTSMFloatValues(valueBlock)
		if err != nil {
			return nil, err
		}
		if len(values) != len(timestamps) {
			return nil, fmt.Errorf("float value count %d does not match timestamp count %d", len(values), len(timestamps))
		}
		points := make([]tsmPoint, len(values))
		for i, value := range values {
			points[i] = tsmPoint{Timestamp: timestamps[i], Type: blockType, Value: strconv.FormatFloat(value, 'g', -1, 64)}
		}
		return points, nil
	case tsmBlockInteger:
		values, err := decodeTSMIntegerValues(valueBlock)
		if err != nil {
			return nil, err
		}
		if len(values) != len(timestamps) {
			return nil, fmt.Errorf("integer value count %d does not match timestamp count %d", len(values), len(timestamps))
		}
		points := make([]tsmPoint, len(values))
		for i, value := range values {
			points[i] = tsmPoint{Timestamp: timestamps[i], Type: blockType, Value: strconv.FormatInt(value, 10)}
		}
		return points, nil
	case tsmBlockUnsigned:
		values, err := decodeTSMUnsignedValues(valueBlock)
		if err != nil {
			return nil, err
		}
		if len(values) != len(timestamps) {
			return nil, fmt.Errorf("unsigned value count %d does not match timestamp count %d", len(values), len(timestamps))
		}
		points := make([]tsmPoint, len(values))
		for i, value := range values {
			points[i] = tsmPoint{Timestamp: timestamps[i], Type: blockType, Value: strconv.FormatUint(value, 10)}
		}
		return points, nil
	case tsmBlockBoolean:
		values, err := decodeTSMBooleanValues(valueBlock)
		if err != nil {
			return nil, err
		}
		if len(values) != len(timestamps) {
			return nil, fmt.Errorf("boolean value count %d does not match timestamp count %d", len(values), len(timestamps))
		}
		points := make([]tsmPoint, len(values))
		for i, value := range values {
			points[i] = tsmPoint{Timestamp: timestamps[i], Type: blockType, Value: strconv.FormatBool(value)}
		}
		return points, nil
	case tsmBlockString:
		values, err := decodeTSMStringValues(valueBlock)
		if err != nil {
			return nil, err
		}
		if len(values) != len(timestamps) {
			return nil, fmt.Errorf("string value count %d does not match timestamp count %d", len(values), len(timestamps))
		}
		points := make([]tsmPoint, len(values))
		for i, value := range values {
			points[i] = tsmPoint{Timestamp: timestamps[i], Type: blockType, Value: value}
		}
		return points, nil
	default:
		return nil, fmt.Errorf("value decode unsupported for TSM block type %d", blockType)
	}
}

func decodeTSMIntegerValues(b []byte) ([]int64, error) {
	if len(b) == 0 {
		return []int64{}, nil
	}
	encoding := b[0] >> 4
	switch encoding {
	case 0:
		return decodeTSMIntegerRawValues(b[1:])
	case 1:
		return decodeTSMIntegerSimple8bValues(b[1:])
	case 2:
		return decodeTSMIntegerRLEValues(b[1:])
	default:
		return nil, fmt.Errorf("unknown integer encoding %d", encoding)
	}
}

func decodeTSMUnsignedValues(b []byte) ([]uint64, error) {
	values, err := decodeTSMIntegerValues(b)
	if err != nil {
		return nil, err
	}
	unsigned := make([]uint64, len(values))
	for i, value := range values {
		unsigned[i] = uint64(value)
	}
	return unsigned, nil
}

func decodeTSMFloatValues(b []byte) ([]float64, error) {
	if len(b) == 0 {
		return []float64{}, nil
	}
	if encoding := b[0] >> 4; encoding != tsmFloatCompressedGorilla {
		return nil, fmt.Errorf("unknown float encoding %d", encoding)
	}
	if len(b) < 9 {
		return []float64{}, nil
	}
	reader := tsmBitReader{data: b[1:]}
	current, err := reader.readBits(64)
	if err != nil {
		return nil, err
	}
	if current == tsmFloatNaNSentinel {
		return []float64{}, nil
	}
	values := []float64{math.Float64frombits(current)}
	var leading, trailing uint64
	for {
		changed, err := reader.readBit()
		if err != nil {
			return nil, err
		}
		if changed {
			reuse, err := reader.readBit()
			if err != nil {
				return nil, err
			}
			if reuse {
				leading, err = reader.readBits(5)
				if err != nil {
					return nil, err
				}
				meaningful, err := reader.readBits(6)
				if err != nil {
					return nil, err
				}
				if meaningful == 0 {
					meaningful = 64
				}
				trailing = 64 - leading - meaningful
			}
			meaningful := uint(64 - leading - trailing)
			delta, err := reader.readBits(meaningful)
			if err != nil {
				return nil, err
			}
			current ^= delta << trailing
			if current == tsmFloatNaNSentinel {
				break
			}
		}
		values = append(values, math.Float64frombits(current))
	}
	return values, nil
}

type tsmBitReader struct {
	data []byte
	bit  int
}

func (r *tsmBitReader) readBit() (bool, error) {
	if r.bit >= len(r.data)*8 {
		return false, io.EOF
	}
	value := r.data[r.bit/8]&(128>>uint(r.bit&7)) != 0
	r.bit++
	return value, nil
}

func (r *tsmBitReader) readBits(n uint) (uint64, error) {
	if n == 0 || n > 64 {
		return 0, fmt.Errorf("invalid bit count %d", n)
	}
	var value uint64
	for i := uint(0); i < n; i++ {
		bit, err := r.readBit()
		if err != nil {
			return 0, err
		}
		value <<= 1
		if bit {
			value |= 1
		}
	}
	return value, nil
}

func decodeTSMBooleanValues(b []byte) ([]bool, error) {
	if len(b) == 0 {
		return []bool{}, nil
	}
	if encoding := b[0] >> 4; encoding != 1 {
		return nil, fmt.Errorf("unknown boolean encoding %d", encoding)
	}
	count, n := binary.Uvarint(b[1:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid boolean count")
	}
	data := b[1+n:]
	if maxCount := uint64(len(data) * 8); count > maxCount {
		count = maxCount
	}
	values := make([]bool, int(count))
	j := 0
	for _, value := range data {
		for mask := byte(128); mask > 0 && j < len(values); mask >>= 1 {
			values[j] = value&mask != 0
			j++
		}
	}
	return values, nil
}

func decodeTSMStringValues(b []byte) ([]string, error) {
	if len(b) == 0 {
		return []string{}, nil
	}
	if encoding := b[0] >> 4; encoding != 1 {
		return nil, fmt.Errorf("unknown string encoding %d", encoding)
	}
	decoded, err := snappy.Decode(nil, b[1:])
	if err != nil {
		return nil, fmt.Errorf("decode string block: %w", err)
	}
	values := make([]string, 0)
	for offset := 0; offset < len(decoded); {
		length, n := binary.Uvarint(decoded[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid string length")
		}
		start := offset + n
		if length > uint64(len(decoded)-start) {
			return nil, fmt.Errorf("short string value")
		}
		end := start + int(length)
		values = append(values, string(decoded[start:end]))
		offset = end
	}
	return values, nil
}

func decodeTSMIntegerRawValues(b []byte) ([]int64, error) {
	if len(b)%8 != 0 {
		return nil, fmt.Errorf("invalid raw integer byte length %d", len(b))
	}
	values := make([]int64, len(b)/8)
	var previous int64
	for i := range values {
		previous += tsmZigZagDecode(binary.BigEndian.Uint64(b[i*8 : i*8+8]))
		values[i] = previous
	}
	return values, nil
}

func decodeTSMIntegerSimple8bValues(b []byte) ([]int64, error) {
	if len(b) < 8 {
		return nil, fmt.Errorf("short packed integer block")
	}
	deltas, err := decodeSimple8bValues(b[8:])
	if err != nil {
		return nil, err
	}
	values := make([]int64, 0, len(deltas)+1)
	previous := tsmZigZagDecode(binary.BigEndian.Uint64(b[:8]))
	values = append(values, previous)
	for _, delta := range deltas {
		previous += tsmZigZagDecode(delta)
		values = append(values, previous)
	}
	return values, nil
}

func decodeTSMIntegerRLEValues(b []byte) ([]int64, error) {
	if len(b) < 8 {
		return nil, fmt.Errorf("short RLE integer block")
	}
	offset := 0
	first := tsmZigZagDecode(binary.BigEndian.Uint64(b[offset : offset+8]))
	offset += 8
	deltaValue, n := binary.Uvarint(b[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid RLE integer delta")
	}
	offset += n
	count, n := binary.Uvarint(b[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid RLE integer count")
	}
	count++
	values := make([]int64, int(count))
	value := first
	delta := tsmZigZagDecode(deltaValue)
	for i := range values {
		values[i] = value
		value += delta
	}
	return values, nil
}

func tsmZigZagEncode(v int64) uint64 {
	return uint64(uint64(v<<1) ^ uint64(v>>63))
}

func tsmZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}

func countSimple8bWords(b []byte) (int, error) {
	if len(b)%8 != 0 {
		return 0, fmt.Errorf("invalid simple8b byte length %d", len(b))
	}
	counts := [...]int{240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1}
	total := 0
	for len(b) > 0 {
		word := binary.BigEndian.Uint64(b[:8])
		selector := word >> 60
		if selector >= uint64(len(counts)) {
			return 0, fmt.Errorf("invalid simple8b selector %d", selector)
		}
		total += counts[selector]
		b = b[8:]
	}
	return total, nil
}

func tsmBlockTypeName(typ byte) string {
	switch typ {
	case tsmBlockFloat:
		return "float"
	case tsmBlockInteger:
		return "integer"
	case tsmBlockBoolean:
		return "boolean"
	case tsmBlockString:
		return "string"
	case tsmBlockUnsigned:
		return "unsigned"
	default:
		return fmt.Sprintf("unknown(%d)", typ)
	}
}

func sampleStrings(values []string, limit int) []string {
	if limit <= 0 || len(values) == 0 {
		return nil
	}
	if len(values) < limit {
		limit = len(values)
	}
	return append([]string(nil), values[:limit]...)
}
