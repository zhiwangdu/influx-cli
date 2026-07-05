package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
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
)

type tsmIndexEntry struct {
	Key     string
	Type    byte
	MinTime int64
	MaxTime int64
	Offset  int64
	Size    uint32
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

	footer := make([]byte, tsmFooterSize)
	if _, err := f.ReadAt(footer, info.Size()-tsmFooterSize); err != nil {
		return FileReport{}, err
	}
	indexOffset := int64(binary.BigEndian.Uint64(footer))
	if indexOffset < tsmHeaderSize || indexOffset >= info.Size()-tsmFooterSize {
		return FileReport{}, fmt.Errorf("invalid TSM index offset %d", indexOffset)
	}

	indexSize := info.Size() - tsmFooterSize - indexOffset
	indexBytes := make([]byte, indexSize)
	if _, err := f.ReadAt(indexBytes, indexOffset); err != nil {
		return FileReport{}, err
	}

	keys, entries, err := parseTSMIndex(indexBytes)
	if err != nil {
		return FileReport{}, err
	}
	if len(entries) == 0 {
		return FileReport{}, fmt.Errorf("TSM index has no entries")
	}

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
		overlaps := options.QueryRange.Overlaps(entry.MinTime, entry.MaxTime)
		if overlaps {
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
				QueryOverlaps: overlaps,
			}
			count, err := readTSMBlockValueCount(f, entry)
			if err == nil {
				block.ValueCount = count
			} else {
				report.Notices = append(report.Notices, fmt.Sprintf("block %d count unavailable: %v", i, err))
			}
			report.Blocks = append(report.Blocks, block)
		}
	}
	report.MinTime = minTime
	report.MaxTime = maxTime
	report.QueryOverlapsFile = options.QueryRange.Overlaps(minTime, maxTime)
	report.Tombstones = readTSMTombstoneSummary(path)
	return report, nil
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
	if entry.Size <= tsmBlockCRCSize {
		return 0, fmt.Errorf("short block size %d", entry.Size)
	}
	blockSize := int(entry.Size) - tsmBlockCRCSize
	raw := make([]byte, int(entry.Size))
	if _, err := f.ReadAt(raw, entry.Offset); err != nil {
		return 0, err
	}
	checksum := binary.BigEndian.Uint32(raw[:tsmBlockCRCSize])
	block := raw[tsmBlockCRCSize:]
	if crc32.ChecksumIEEE(block) != checksum {
		return 0, fmt.Errorf("crc mismatch")
	}
	if len(block) != blockSize {
		return 0, fmt.Errorf("short block read")
	}
	return tsmBlockCount(block)
}

func tsmBlockCount(block []byte) (int, error) {
	if len(block) <= 1 {
		return 0, fmt.Errorf("short encoded block")
	}
	timestamps, err := unpackTSMBlockTimestamps(block[1:])
	if err != nil {
		return 0, err
	}
	return countTSMTimestamps(timestamps)
}

func unpackTSMBlockTimestamps(block []byte) ([]byte, error) {
	tsLen, n := binary.Uvarint(block)
	if n <= 0 {
		return nil, fmt.Errorf("unable to read timestamp block length")
	}
	end := n + int(tsLen)
	if end > len(block) {
		return nil, fmt.Errorf("timestamp block exceeds encoded block length")
	}
	return block[n:end], nil
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

func readTSMTombstoneSummary(path string) TombstoneSummary {
	tombstonePath := path
	if filepath.Ext(path) != ".tombstone" {
		base := filepath.Base(path)
		ext := filepath.Ext(base)
		if ext != "" {
			base = base[:len(base)-len(ext)]
		}
		tombstonePath = filepath.Join(filepath.Dir(path), base+".tombstone")
	}
	info, err := os.Stat(tombstonePath)
	if err != nil {
		return TombstoneSummary{}
	}
	return TombstoneSummary{
		Exists:    true,
		Path:      tombstonePath,
		SizeBytes: info.Size(),
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
