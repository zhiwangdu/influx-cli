package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	tsspDetachedMetaIndexFileName   = "segment.idx"
	tsspDetachedMetaIndexHeaderSize = tsspHeaderSize
	// openGemini MetaIndex.marshalDetached stores id, minTime, maxTime,
	// offset, and size; Count is only present in attached meta-index records.
	tsspDetachedMetaIndexItemSize   = 8 + 8 + 8 + 8 + 4
	tsspDetachedMetaIndexRecordSize = 4 + tsspDetachedMetaIndexItemSize
	tsspDetachedChunkMetaReadNum    = 16 // openGemini immutable.ChunkMetaReadNum
)

func analyzeTSSPDetachedMetaIndex(path string, info os.FileInfo, options Options) (FileReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return FileReport{}, err
	}
	defer f.Close()

	if info.Size() < tsspDetachedMetaIndexHeaderSize {
		return FileReport{}, fmt.Errorf("file too small for detached TSSP meta-index header")
	}
	header := make([]byte, tsspDetachedMetaIndexHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return FileReport{}, err
	}
	if string(header[:len(tsspMagic)]) != tsspMagic {
		return FileReport{}, fmt.Errorf("invalid detached TSSP meta-index magic")
	}
	version := binary.BigEndian.Uint64(header[len(tsspMagic):])

	metaIndexes, err := readTSSPDetachedMetaIndexes(f, info.Size())
	if err != nil {
		return FileReport{}, err
	}
	minID, maxID, minTime, maxTime := summarizeTSSPDetachedMetaIndexes(metaIndexes)

	report := FileReport{
		Path:       path,
		Format:     FormatTSSPDetachedIndex,
		SizeBytes:  info.Size(),
		ModTime:    info.ModTime(),
		MinTime:    minTime,
		MaxTime:    maxTime,
		KeyCount:   len(metaIndexes),
		BlockCount: len(metaIndexes),
		BlocksByType: map[string]int{
			"detached-meta-index": len(metaIndexes),
		},
		MetaIndexID: SeriesIDSummary{
			Min:   minID,
			Max:   maxID,
			Count: int64(len(metaIndexes)),
		},
		QueryOverlapsFile: len(metaIndexes) > 0 && options.QueryRange.Overlaps(minTime, maxTime),
		Extra: map[string]string{
			"version":       fmt.Sprint(version),
			"layout":        "detached",
			"sidecar":       tsspDetachedMetaIndexFileName,
			"header_size":   fmt.Sprint(tsspDetachedMetaIndexHeaderSize),
			"record_size":   fmt.Sprint(tsspDetachedMetaIndexRecordSize),
			"item_size":     fmt.Sprint(tsspDetachedMetaIndexItemSize),
			"count_stored":  "false",
			"crc_algorithm": "ieee",
		},
	}
	for _, meta := range metaIndexes {
		if len(report.KeySamples) >= options.KeySampleLimit {
			break
		}
		report.KeySamples = append(report.KeySamples, fmt.Sprintf("meta-index-id:%d", meta.ID))
	}
	populateTSSPDetachedMetaIndexReports(&report, metaIndexes, options)
	report.DecodePath = buildTSSPDetachedMetaIndexDecodePathSummary(metaIndexes, options)
	return report, nil
}

func readTSSPDetachedMetaIndexes(f *os.File, size int64) ([]tsspMetaIndex, error) {
	payloadSize := size - tsspDetachedMetaIndexHeaderSize
	if payloadSize == 0 {
		return nil, nil
	}
	if payloadSize%tsspDetachedMetaIndexRecordSize != 0 {
		return nil, fmt.Errorf("detached meta-index payload size %d is not a multiple of %d", payloadSize, tsspDetachedMetaIndexRecordSize)
	}
	buf := make([]byte, payloadSize)
	if _, err := f.ReadAt(buf, tsspDetachedMetaIndexHeaderSize); err != nil {
		return nil, fmt.Errorf("read detached meta-index payload at offset %d: %w", tsspDetachedMetaIndexHeaderSize, err)
	}

	items := make([]tsspMetaIndex, 0, int(payloadSize)/tsspDetachedMetaIndexRecordSize)
	for i := 0; len(buf) > 0; i++ {
		record := buf[:tsspDetachedMetaIndexRecordSize]
		wantCRC := binary.BigEndian.Uint32(record[:4])
		payload := record[4:]
		if gotCRC := crc32.ChecksumIEEE(payload); gotCRC != wantCRC {
			return nil, fmt.Errorf("detached meta-index record %d crc mismatch", i)
		}
		items = append(items, tsspMetaIndex{
			ID:      binary.BigEndian.Uint64(payload[:8]),
			MinTime: decodeGeminiInt64(payload[8:16]),
			MaxTime: decodeGeminiInt64(payload[16:24]),
			Offset:  decodeGeminiInt64(payload[24:32]),
			Size:    binary.BigEndian.Uint32(payload[32:36]),
		})
		buf = buf[tsspDetachedMetaIndexRecordSize:]
	}
	return items, nil
}

func summarizeTSSPDetachedMetaIndexes(metaIndexes []tsspMetaIndex) (uint64, uint64, int64, int64) {
	if len(metaIndexes) == 0 {
		return 0, 0, 0, 0
	}
	minID, maxID := metaIndexes[0].ID, metaIndexes[0].ID
	minTime, maxTime := metaIndexes[0].MinTime, metaIndexes[0].MaxTime
	for _, meta := range metaIndexes[1:] {
		if meta.ID < minID {
			minID = meta.ID
		}
		if meta.ID > maxID {
			maxID = meta.ID
		}
		if meta.MinTime < minTime {
			minTime = meta.MinTime
		}
		if meta.MaxTime > maxTime {
			maxTime = meta.MaxTime
		}
	}
	return minID, maxID, minTime, maxTime
}

func populateTSSPDetachedMetaIndexReports(report *FileReport, metaIndexes []tsspMetaIndex, options Options) {
	idSet := queryMetaIndexIDSet(options.QueryMetaIndexIDs)
	for i, meta := range metaIndexes {
		overlaps := tsspQueryMetaIndexSelected(meta.ID, idSet) && options.QueryRange.Overlaps(meta.MinTime, meta.MaxTime)
		if overlaps {
			report.QueryOverlapBlocks++
		}
		if i < options.BlockSampleLimit {
			report.Blocks = append(report.Blocks, BlockReport{
				MetaIndexID:   meta.ID,
				MinTime:       meta.MinTime,
				MaxTime:       meta.MaxTime,
				Type:          "detached-meta-index",
				Offset:        meta.Offset,
				SizeBytes:     meta.Size,
				QueryOverlaps: overlaps,
			})
		}
	}
}

func buildTSSPDetachedMetaIndexDecodePathSummary(metaIndexes []tsspMetaIndex, options Options) *DecodePathSummary {
	if !options.QueryRange.Set {
		return nil
	}

	summary := &DecodePathSummary{
		Mode:                 tsspCursorMode("tssp-detached-meta-index", options),
		QueryRange:           options.QueryRange,
		CursorSeekTime:       tsspCursorSeekTime(options),
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	idSet := queryMetaIndexIDSet(options.QueryMetaIndexIDs)
	if len(idSet) > 0 {
		summary.QueryMetaIndexIDs = append([]uint64(nil), options.QueryMetaIndexIDs...)
		summary.KeyFilterApplied = true
	}
	populateTSSPDecodeMetaIndexMatches(summary, metaIndexes)

	selectedMetas := []tsspMetaIndex{}
	overlapMetas := []tsspMetaIndex{}
	for _, meta := range tsspMetaIndexesForCursor(metaIndexes, options.CursorDescending) {
		if !tsspQueryMetaIndexSelected(meta.ID, idSet) {
			summary.SkippedByKeyBlocks++
			continue
		}
		selectedMetas = append(selectedMetas, meta)

		summary.LocationBlocks++
		summary.LocationBlocksByType["detached-meta-index"]++
		summary.BaselineDecodeBlocks++
		summary.BaselineDecodeBytes += int64(meta.Size)
		summary.BaselineReadAtCalls++

		overlaps := options.QueryRange.Overlaps(meta.MinTime, meta.MaxTime)
		if overlaps {
			overlapMetas = append(overlapMetas, meta)
			if summary.IteratorCostFiles == 0 {
				summary.IteratorCostFiles = 1
			}
			summary.IteratorCostBlocks++
			summary.IteratorCostBytes += int64(meta.Size)
			summary.OptimizedDecodeBlocks++
			summary.FilteredDecodeBlocks++
			summary.OptimizedDecodeBytes += int64(meta.Size)
			summary.OptimizedReadAtCalls++
			summary.DecodeBlocksByType["detached-meta-index"]++
		} else if meta.MaxTime < options.QueryRange.Min {
			summary.SkippedBeforeSeekBlocks++
		} else {
			summary.SkippedAfterRangeBlocks++
		}
		appendTSSPDetachedMetaIndexDecodeSample(summary, meta, overlaps, options.BlockSampleLimit)
	}

	summary.SavedDecodeBlocks = summary.BaselineDecodeBlocks - summary.OptimizedDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedReadAtCalls = summary.BaselineReadAtCalls - summary.OptimizedReadAtCalls
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	populateTSSPDetachedChunkMetaBatches(summary, selectedMetas, overlapMetas, options)
	summary.Recommendations = tsspDetachedMetaIndexRecommendations(summary)
	return summary
}

func populateTSSPDetachedChunkMetaBatches(summary *DecodePathSummary, selected, overlapping []tsspMetaIndex, options Options) {
	summary.BaselineCursorReadCalls = tsspDetachedChunkMetaBatchCount(len(selected))
	summary.OptimizedCursorReadCalls = tsspDetachedChunkMetaBatchCount(len(overlapping))
	summary.CursorWindowCount = summary.BaselineCursorReadCalls
	if options.BlockSampleLimit <= 0 {
		return
	}
	overlapIDs := map[uint64]struct{}{}
	for _, meta := range overlapping {
		overlapIDs[meta.ID] = struct{}{}
	}
	for start := 0; start < len(selected) && len(summary.CursorWindows) < options.BlockSampleLimit; start += tsspDetachedChunkMetaReadNum {
		end := start + tsspDetachedChunkMetaReadNum
		if end > len(selected) {
			end = len(selected)
		}
		batch := selected[start:end]
		decoded := 0
		minTime, maxTime := batch[0].MinTime, batch[0].MaxTime
		for _, meta := range batch {
			if meta.MinTime < minTime {
				minTime = meta.MinTime
			}
			if meta.MaxTime > maxTime {
				maxTime = meta.MaxTime
			}
			if _, ok := overlapIDs[meta.ID]; ok {
				decoded++
			}
		}
		reason := "detached_chunk_meta_batch_overlap"
		if decoded == 0 {
			reason = "outside_query_range"
		} else if decoded < len(batch) {
			reason = "detached_chunk_meta_batch_filtered"
		}
		summary.CursorWindows = append(summary.CursorWindows, DecodePathCursorWindow{
			MinTime:         minTime,
			MaxTime:         maxTime,
			LocationBlocks:  len(batch),
			DecodedBlocks:   decoded,
			SavedBlocks:     len(batch) - decoded,
			Reason:          reason,
			FirstBlockIndex: start,
		})
	}
}

func tsspDetachedChunkMetaBatchCount(records int) int {
	if records <= 0 {
		return 0
	}
	return (records + tsspDetachedChunkMetaReadNum - 1) / tsspDetachedChunkMetaReadNum
}

func appendTSSPDetachedMetaIndexDecodeSample(summary *DecodePathSummary, meta tsspMetaIndex, overlaps bool, sampleLimit int) {
	if len(summary.Samples) >= sampleLimit {
		return
	}
	reason := "outside_query_range"
	optimizedReadAtCalls := 0
	readAtRanges := []DecodePathReadAtRange(nil)
	if overlaps {
		reason = "overlaps_query_range"
		optimizedReadAtCalls = 1
		readAtRanges = append(readAtRanges, DecodePathReadAtRange{
			MinTime:   meta.MinTime,
			MaxTime:   meta.MaxTime,
			Offset:    meta.Offset,
			SizeBytes: meta.Size,
		})
	}
	summary.Samples = append(summary.Samples, DecodePathBlockDecision{
		MetaIndexID:           meta.ID,
		MinTime:               meta.MinTime,
		MaxTime:               meta.MaxTime,
		Type:                  "detached-meta-index",
		SizeBytes:             meta.Size,
		BaselineReadAtCalls:   1,
		OptimizedReadAtCalls:  optimizedReadAtCalls,
		LocationCandidate:     true,
		Decoded:               overlaps,
		Reason:                reason,
		OptimizedReadAtRanges: readAtRanges,
	})
}

func populateTSSPDecodeMetaIndexMatches(summary *DecodePathSummary, metaIndexes []tsspMetaIndex) {
	if len(summary.QueryMetaIndexIDs) == 0 {
		return
	}
	seen := map[uint64]struct{}{}
	for _, meta := range metaIndexes {
		seen[meta.ID] = struct{}{}
	}
	for _, id := range summary.QueryMetaIndexIDs {
		if _, ok := seen[id]; ok {
			summary.MatchedMetaIndexIDs = append(summary.MatchedMetaIndexIDs, id)
		} else {
			summary.MissingMetaIndexIDs = append(summary.MissingMetaIndexIDs, id)
		}
	}
}

func queryMetaIndexIDSet(ids []uint64) map[uint64]struct{} {
	return querySeriesIDSet(ids)
}

func tsspQueryMetaIndexSelected(id uint64, idSet map[uint64]struct{}) bool {
	if len(idSet) == 0 {
		return true
	}
	_, ok := idSet[id]
	return ok
}

func tsspDetachedMetaIndexRecommendations(summary *DecodePathSummary) []string {
	if summary == nil {
		return nil
	}
	recommendations := make([]string, 0, 4)
	if summary.SkippedByKeyBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"skip %d detached TSSP meta-index record(s) outside the requested ID set",
			summary.SkippedByKeyBlocks,
		))
	}
	if summary.SavedDecodeBlocks > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"filter %d detached TSSP meta-index record(s) outside the query range before loading detached chunk metadata",
			summary.SavedDecodeBlocks,
		))
	}
	if summary.SavedReadAtCalls > 0 {
		recommendations = append(recommendations, fmt.Sprintf(
			"avoid %d detached chunk metadata ReadAt call(s) after meta-index filtering",
			summary.SavedReadAtCalls,
		))
	}
	if summary.BaselineCursorReadCalls > summary.OptimizedCursorReadCalls {
		recommendations = append(recommendations, fmt.Sprintf(
			"reduce detached chunk metadata batch read(s) from %d to %d after query filtering",
			summary.BaselineCursorReadCalls,
			summary.OptimizedCursorReadCalls,
		))
	}
	return recommendations
}

func isTSSPDetachedMetaIndexPath(path string) bool {
	return strings.EqualFold(filepath.Base(path), tsspDetachedMetaIndexFileName)
}
