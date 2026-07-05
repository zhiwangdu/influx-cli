package storage

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	tsmTombstoneHeaderSize = 4
	tsmTombstoneV2Header   = 0x1502
	tsmTombstoneV3Header   = 0x1503
	tsmTombstoneV4Header   = 0x1504

	maxTSMTombstoneKeyLen = 64 << 20
)

type tsmTombstoneEntry struct {
	Key string
	Min int64
	Max int64
}

func readTSMTombstoneSummary(path string, blocks []tsmIndexEntry, options Options) (TombstoneSummary, error) {
	tombstonePath := tsmTombstonePath(path)
	info, err := os.Stat(tombstonePath)
	if err != nil {
		return TombstoneSummary{}, nil
	}

	summary := TombstoneSummary{
		Exists:    true,
		Path:      tombstonePath,
		SizeBytes: info.Size(),
	}
	tombstones, version, err := readTSMTombstones(tombstonePath)
	summary.Version = version
	if err != nil {
		return summary, err
	}
	summarizeTSMTombstoneEntries(&summary, tombstones, blocks, options)
	return summary, nil
}

func tsmTombstonePath(path string) string {
	if strings.HasSuffix(path, ".tombstone") {
		return path
	}
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	if ext != "" {
		base = base[:len(base)-len(ext)]
	}
	return filepath.Join(filepath.Dir(path), base+".tombstone")
}

func readTSMTombstones(path string) ([]tsmTombstoneEntry, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()

	var header [tsmTombstoneHeaderSize]byte
	n, err := io.ReadFull(f, header[:])
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return nil, "", err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, "", err
	}
	if n < tsmTombstoneHeaderSize {
		tombstones, err := readTSMTombstoneV1(f)
		return tombstones, "v1", err
	}

	switch binary.BigEndian.Uint32(header[:]) {
	case tsmTombstoneV2Header:
		if _, err := f.Seek(tsmTombstoneHeaderSize, io.SeekStart); err != nil {
			return nil, "v2", err
		}
		tombstones, err := readTSMTombstoneBinaryEntries(f)
		return tombstones, "v2", err
	case tsmTombstoneV3Header:
		if _, err := f.Seek(tsmTombstoneHeaderSize, io.SeekStart); err != nil {
			return nil, "v3", err
		}
		tombstones, err := readTSMTombstoneGzipEntries(f)
		return tombstones, "v3", err
	case tsmTombstoneV4Header:
		if _, err := f.Seek(tsmTombstoneHeaderSize, io.SeekStart); err != nil {
			return nil, "v4", err
		}
		tombstones, err := readTSMTombstoneGzipEntries(f)
		return tombstones, "v4", err
	default:
		tombstones, err := readTSMTombstoneV1(f)
		return tombstones, "v1", err
	}
}

func readTSMTombstoneV1(r io.Reader) ([]tsmTombstoneEntry, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxTSMTombstoneKeyLen)
	var tombstones []tsmTombstoneEntry
	for scanner.Scan() {
		key := scanner.Text()
		if key == "" {
			continue
		}
		tombstones = append(tombstones, tsmTombstoneEntry{
			Key: key,
			Min: math.MinInt64,
			Max: math.MaxInt64,
		})
	}
	return tombstones, scanner.Err()
}

func readTSMTombstoneGzipEntries(r io.Reader) ([]tsmTombstoneEntry, error) {
	br := bufio.NewReader(r)
	var tombstones []tsmTombstoneEntry
	for {
		gr, err := gzip.NewReader(br)
		if err == io.EOF {
			return tombstones, nil
		}
		if err != nil {
			return nil, err
		}
		gr.Multistream(false)
		entries, readErr := readTSMTombstoneBinaryEntries(gr)
		closeErr := gr.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		tombstones = append(tombstones, entries...)
	}
}

func readTSMTombstoneBinaryEntries(r io.Reader) ([]tsmTombstoneEntry, error) {
	var tombstones []tsmTombstoneEntry
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(r, lenBuf[:]); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return nil, err
		}
		keyLen := binary.BigEndian.Uint32(lenBuf[:])
		if keyLen > maxTSMTombstoneKeyLen {
			return nil, fmt.Errorf("tombstone key length %d exceeds limit %d", keyLen, maxTSMTombstoneKeyLen)
		}
		key := make([]byte, int(keyLen))
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, err
		}

		var timeBuf [16]byte
		if _, err := io.ReadFull(r, timeBuf[:]); err != nil {
			return nil, err
		}
		tombstones = append(tombstones, tsmTombstoneEntry{
			Key: string(key),
			Min: int64(binary.BigEndian.Uint64(timeBuf[:8])),
			Max: int64(binary.BigEndian.Uint64(timeBuf[8:])),
		})
	}
	return tombstones, nil
}

func summarizeTSMTombstoneEntries(summary *TombstoneSummary, tombstones []tsmTombstoneEntry, blocks []tsmIndexEntry, options Options) {
	if len(tombstones) == 0 {
		return
	}
	summary.RangeCount = len(tombstones)
	summary.MinTime = tombstones[0].Min
	summary.MaxTime = tombstones[0].Max

	keys := make(map[string]struct{})
	affectedBlocks := make(map[int]struct{})
	for i, tombstone := range tombstones {
		keys[tombstone.Key] = struct{}{}
		if tombstone.Min < summary.MinTime {
			summary.MinTime = tombstone.Min
		}
		if tombstone.Max > summary.MaxTime {
			summary.MaxTime = tombstone.Max
		}

		queryOverlaps := options.QueryRange.Overlaps(tombstone.Min, tombstone.Max)
		if queryOverlaps {
			summary.QueryOverlapRanges++
		}
		rangeAffectedBlocks := 0
		for blockIndex, block := range blocks {
			if block.Key != tombstone.Key || !rangesOverlap(tombstone.Min, tombstone.Max, block.MinTime, block.MaxTime) {
				continue
			}
			rangeAffectedBlocks++
			affectedBlocks[blockIndex] = struct{}{}
		}
		if i < options.KeySampleLimit {
			summary.RangeSamples = append(summary.RangeSamples, TombstoneRangeReport{
				Key:            tombstone.Key,
				MinTime:        tombstone.Min,
				MaxTime:        tombstone.Max,
				QueryOverlaps:  queryOverlaps,
				AffectedBlocks: rangeAffectedBlocks,
			})
		}
	}
	summary.AffectedBlocks = len(affectedBlocks)
	summary.KeyCount = len(keys)
	summary.KeySamples = sampleMapKeys(keys, options.KeySampleLimit)
}

func rangesOverlap(aMin, aMax, bMin, bMax int64) bool {
	return aMin <= bMax && aMax >= bMin
}

func sampleMapKeys(values map[string]struct{}, limit int) []string {
	if limit <= 0 || len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return sampleStrings(keys, limit)
}
