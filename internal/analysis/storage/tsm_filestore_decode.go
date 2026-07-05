package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

type tsmFileStoreData struct {
	path       string
	maxTime    int64
	entries    []tsmIndexEntry
	tombstones []tsmTombstoneEntry
}

type tsmFileStoreLocation struct {
	path    string
	entry   tsmIndexEntry
	decoded bool
	index   int
}

func buildTSMFileStoreDecodePathSummary(files []FileReport, options Options) (*DecodePathSummary, error) {
	if !options.QueryRange.Set {
		return nil, nil
	}

	tsmFiles := make([]FileReport, 0)
	for _, file := range files {
		if file.Format == FormatTSM {
			tsmFiles = append(tsmFiles, file)
		}
	}
	if len(tsmFiles) == 0 {
		return nil, nil
	}
	sort.Slice(tsmFiles, func(i, j int) bool {
		return tsmFiles[i].Path < tsmFiles[j].Path
	})

	fileData := make([]tsmFileStoreData, 0, len(tsmFiles))
	allKeys := map[string]struct{}{}
	for _, file := range tsmFiles {
		data, err := readTSMFileStoreData(file.Path)
		if err != nil {
			return nil, err
		}
		fileData = append(fileData, data)
		for _, entry := range data.entries {
			allKeys[entry.Key] = struct{}{}
		}
	}

	keySet := queryKeySet(options.QueryKeys)
	keys := make([]string, 0)
	if len(keySet) > 0 {
		keys = append(keys, options.QueryKeys...)
	} else {
		for key := range allKeys {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	summary := &DecodePathSummary{
		Mode:                 "tsm-filestore-key-cursor-ascending",
		QueryRange:           options.QueryRange,
		CursorSeekTime:       options.QueryRange.Min,
		LocationBlocksByType: map[string]int{},
		DecodeBlocksByType:   map[string]int{},
	}
	if len(keySet) > 0 {
		summary.QueryKeys = append([]string(nil), options.QueryKeys...)
		summary.KeyFilterApplied = true
		for _, data := range fileData {
			for _, entry := range data.entries {
				if _, ok := keySet[entry.Key]; !ok {
					summary.SkippedByKeyBlocks++
				}
			}
		}
	}

	matchedKeys := map[string]struct{}{}
	locationsByKey := map[string][]tsmFileStoreLocation{}
	for _, key := range keys {
		if _, ok := allKeys[key]; ok {
			matchedKeys[key] = struct{}{}
		}
		locations := tsmFileStoreLocationsForKey(fileData, key, options, summary)
		if len(locations) == 0 {
			continue
		}
		locationsByKey[key] = locations
		for _, location := range locations {
			typeName := tsmBlockTypeName(location.entry.Type)
			summary.LocationBlocks++
			summary.BaselineDecodeBytes += int64(location.entry.Size)
			if location.entry.ValueCountAvailable {
				summary.BaselineDecodeValues += location.entry.ValueCount
			}
			summary.LocationBlocksByType[typeName]++
			if location.decoded {
				summary.FilteredDecodeBlocks++
				summary.OptimizedDecodeBytes += int64(location.entry.Size)
				if location.entry.ValueCountAvailable {
					summary.OptimizedDecodeValues += location.entry.ValueCount
				}
				summary.DecodeBlocksByType[typeName]++
			} else {
				summary.SkippedAfterRangeBlocks++
			}
			if len(summary.Samples) < options.BlockSampleLimit {
				reason := "query_overlap"
				if !location.decoded {
					reason = "outside_query_range"
				}
				summary.Samples = append(summary.Samples, DecodePathBlockDecision{
					Path:              location.path,
					Key:               location.entry.Key,
					MinTime:           location.entry.MinTime,
					MaxTime:           location.entry.MaxTime,
					Type:              typeName,
					SizeBytes:         location.entry.Size,
					ValueCount:        location.entry.ValueCount,
					LocationCandidate: true,
					Decoded:           location.decoded,
					Reason:            reason,
				})
			}
		}
	}

	if len(keySet) > 0 {
		for _, key := range options.QueryKeys {
			if _, ok := matchedKeys[key]; ok {
				summary.MatchedKeys = append(summary.MatchedKeys, key)
			} else {
				summary.MissingKeys = append(summary.MissingKeys, key)
			}
		}
	}
	summary.BaselineDecodeBlocks = summary.LocationBlocks
	summary.OptimizedDecodeBlocks = summary.FilteredDecodeBlocks
	summary.SavedDecodeBlocks = summary.LocationBlocks - summary.FilteredDecodeBlocks
	summary.SavedDecodeBytes = summary.BaselineDecodeBytes - summary.OptimizedDecodeBytes
	summary.SavedDecodeValues = summary.BaselineDecodeValues - summary.OptimizedDecodeValues
	if summary.FilteredDecodeBlocks > 0 {
		summary.Amplification = float64(summary.LocationBlocks) / float64(summary.FilteredDecodeBlocks)
	}
	populateTSMFileStoreCursorWindows(summary, locationsByKey, options.BlockSampleLimit)
	summary.Recommendations = tsmDecodeRecommendations(summary)
	return summary, nil
}

func readTSMFileStoreData(path string) (tsmFileStoreData, error) {
	info, err := os.Stat(path)
	if err != nil {
		return tsmFileStoreData{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return tsmFileStoreData{}, err
	}
	defer f.Close()

	if info.Size() < tsmHeaderSize+tsmFooterSize {
		return tsmFileStoreData{}, fmt.Errorf("%s: file too small for TSM header/footer", path)
	}
	header := make([]byte, tsmHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return tsmFileStoreData{}, err
	}
	if binary.BigEndian.Uint32(header[:4]) != tsmMagicNumber {
		return tsmFileStoreData{}, fmt.Errorf("%s: invalid TSM magic", path)
	}
	if header[4] != tsmVersion {
		return tsmFileStoreData{}, fmt.Errorf("%s: unsupported TSM version %d", path, header[4])
	}
	_, _, _, entries, err := readTSMIndex(f, info.Size())
	if err != nil {
		return tsmFileStoreData{}, fmt.Errorf("%s: %w", path, err)
	}
	populateTSMBlockValueCounts(f, entries)
	if len(entries) == 0 {
		return tsmFileStoreData{}, fmt.Errorf("%s: TSM index has no entries", path)
	}

	maxTime := entries[0].MaxTime
	for _, entry := range entries[1:] {
		if entry.MaxTime > maxTime {
			maxTime = entry.MaxTime
		}
	}

	tombstones, _, err := readTSMTombstones(tsmTombstonePath(path))
	if os.IsNotExist(err) {
		err = nil
	}
	if err != nil {
		return tsmFileStoreData{}, fmt.Errorf("%s: tombstones: %w", path, err)
	}
	return tsmFileStoreData{
		path:       path,
		maxTime:    maxTime,
		entries:    entries,
		tombstones: tombstones,
	}, nil
}

func tsmFileStoreLocationsForKey(files []tsmFileStoreData, key string, options Options, summary *DecodePathSummary) []tsmFileStoreLocation {
	locations := make([]tsmFileStoreLocation, 0)
	for _, file := range files {
		if file.maxTime < options.QueryRange.Min {
			continue
		}
		for _, entry := range file.entries {
			if entry.Key != key {
				continue
			}
			if tsmEntryFullyTombstoned(entry, file.tombstones) {
				summary.FullyTombstonedBlocks++
				continue
			}
			if entry.MaxTime < options.QueryRange.Min {
				summary.SkippedBeforeSeekBlocks++
				continue
			}
			locations = append(locations, tsmFileStoreLocation{
				path:    file.path,
				entry:   entry,
				decoded: options.QueryRange.Overlaps(entry.MinTime, entry.MaxTime),
			})
		}
	}
	sort.SliceStable(locations, func(i, j int) bool {
		a, b := locations[i], locations[j]
		if rangesOverlap(a.entry.MinTime, a.entry.MaxTime, b.entry.MinTime, b.entry.MaxTime) {
			return a.path < b.path
		}
		return a.entry.MinTime < b.entry.MinTime
	})
	for i := range locations {
		locations[i].index = i
	}
	return locations
}

func populateTSMFileStoreCursorWindows(summary *DecodePathSummary, locationsByKey map[string][]tsmFileStoreLocation, sampleLimit int) {
	if len(locationsByKey) == 0 {
		return
	}
	mergeKeys := map[string]struct{}{}
	keys := make([]string, 0, len(locationsByKey))
	for key := range locationsByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		locations := locationsByKey[key]
		var window tsmFileStoreCursorWindowBuilder
		for _, location := range locations {
			if !window.started {
				window.start(location)
				continue
			}
			if location.entry.MinTime <= window.maxTime {
				window.add(location)
				continue
			}
			finishTSMFileStoreCursorWindow(summary, &window, mergeKeys, sampleLimit)
			window.start(location)
		}
		if window.started {
			finishTSMFileStoreCursorWindow(summary, &window, mergeKeys, sampleLimit)
		}
	}
	summary.MergeWindowKeys = len(mergeKeys)
}

type tsmFileStoreCursorWindowBuilder struct {
	started         bool
	key             string
	minTime         int64
	maxTime         int64
	locationBlocks  int
	decodedBlocks   int
	firstBlockIndex int
	files           map[string]struct{}
}

func (w *tsmFileStoreCursorWindowBuilder) start(location tsmFileStoreLocation) {
	*w = tsmFileStoreCursorWindowBuilder{
		started:         true,
		key:             location.entry.Key,
		minTime:         location.entry.MinTime,
		maxTime:         location.entry.MaxTime,
		locationBlocks:  1,
		firstBlockIndex: location.index,
		files: map[string]struct{}{
			location.path: {},
		},
	}
	if location.decoded {
		w.decodedBlocks = 1
	}
}

func (w *tsmFileStoreCursorWindowBuilder) add(location tsmFileStoreLocation) {
	if location.entry.MinTime < w.minTime {
		w.minTime = location.entry.MinTime
	}
	if location.entry.MaxTime > w.maxTime {
		w.maxTime = location.entry.MaxTime
	}
	w.locationBlocks++
	w.files[location.path] = struct{}{}
	if location.decoded {
		w.decodedBlocks++
	}
}

func finishTSMFileStoreCursorWindow(summary *DecodePathSummary, window *tsmFileStoreCursorWindowBuilder, mergeKeys map[string]struct{}, sampleLimit int) {
	summary.CursorWindowCount++
	requiresMerge := window.decodedBlocks > 1
	if requiresMerge {
		summary.MergeWindowCount++
		summary.MergeWindowBlocks += window.decodedBlocks
		mergeKeys[window.key] = struct{}{}
	}
	if sampleLimit <= 0 || len(summary.CursorWindows) >= sampleLimit {
		return
	}
	reason := "single_decode"
	switch {
	case window.decodedBlocks == 0:
		reason = "outside_query_range"
	case requiresMerge:
		reason = "merge_overlap"
	}
	summary.CursorWindows = append(summary.CursorWindows, DecodePathCursorWindow{
		Key:             window.key,
		Files:           sampleMapKeys(window.files, sampleLimit),
		MinTime:         window.minTime,
		MaxTime:         window.maxTime,
		LocationBlocks:  window.locationBlocks,
		DecodedBlocks:   window.decodedBlocks,
		SavedBlocks:     window.locationBlocks - window.decodedBlocks,
		RequiresMerge:   requiresMerge,
		Reason:          reason,
		FirstBlockIndex: window.firstBlockIndex,
	})
}
