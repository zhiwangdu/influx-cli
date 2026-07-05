package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func Analyze(ctx context.Context, paths []string, options Options) (Report, error) {
	if len(paths) == 0 {
		return Report{}, fmt.Errorf("storage analyze requires at least one file or directory")
	}
	if options.Format == "" {
		options.Format = FormatAuto
	}
	if options.KeySampleLimit < 0 {
		return Report{}, fmt.Errorf("key sample limit cannot be negative")
	}
	if options.BlockSampleLimit < 0 {
		return Report{}, fmt.Errorf("block sample limit cannot be negative")
	}
	options.QueryKeys = normalizeQueryKeys(options.QueryKeys)
	options.QuerySeriesIDs = normalizeQuerySeriesIDs(options.QuerySeriesIDs)
	options.QueryMetaIndexIDs = normalizeQuerySeriesIDs(options.QueryMetaIndexIDs)
	options.QueryMeasurements = normalizeQueryKeys(options.QueryMeasurements)
	options.QueryTags = normalizeTagFilters(options.QueryTags)
	if len(options.QueryKeys) > 0 && !options.QueryRange.Set {
		return Report{}, fmt.Errorf("query key filter requires query range")
	}
	if len(options.QuerySeriesIDs) > 0 && !options.QueryRange.Set {
		return Report{}, fmt.Errorf("query series id filter requires query range")
	}
	if len(options.QueryMetaIndexIDs) > 0 && !options.QueryRange.Set {
		return Report{}, fmt.Errorf("query meta-index id filter requires query range")
	}

	files, err := expandPaths(ctx, paths, options)
	if err != nil {
		return Report{}, err
	}

	report := Report{}
	for _, path := range files {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		fileReport, err := analyzeFile(path, options)
		if err != nil {
			report.Notices = append(report.Notices, fmt.Sprintf("%s: %v", path, err))
			continue
		}
		report.Files = append(report.Files, fileReport)
		for _, notice := range fileReport.Notices {
			report.Notices = append(report.Notices, fmt.Sprintf("%s: %s", path, notice))
		}
		accumulateSummary(&report.Summary, fileReport, options.QueryRange)
	}
	if options.QueryRange.Set {
		decodePath, err := buildTSMFileStoreDecodePathSummary(report.Files, options)
		if err != nil {
			report.Notices = append(report.Notices, fmt.Sprintf("tsm filestore decode path unavailable: %v", err))
		} else if decodePath != nil {
			report.DecodePath = decodePath
		}
		if report.DecodePath == nil {
			report.DecodePath = buildTSSPFileSetDecodePathSummary(report.Files, options)
		}
	}
	report.Summary.FileCount = len(report.Files)
	return report, nil
}

func normalizeQueryKeys(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	keys := make([]string, 0, len(values))
	for _, value := range values {
		key := strings.TrimSpace(value)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func queryKeySet(keys []string) map[string]struct{} {
	if len(keys) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		set[key] = struct{}{}
	}
	return set
}

func normalizeQuerySeriesIDs(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	seen := map[uint64]struct{}{}
	ids := make([]uint64, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		ids = append(ids, value)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	return ids
}

func querySeriesIDSet(ids []uint64) map[uint64]struct{} {
	if len(ids) == 0 {
		return nil
	}
	set := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func normalizeTagFilters(values []TagFilter) []TagFilter {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	filters := make([]TagFilter, 0, len(values))
	for _, value := range values {
		filter := TagFilter{
			Key:   strings.TrimSpace(value.Key),
			Value: strings.TrimSpace(value.Value),
		}
		if filter.Key == "" {
			continue
		}
		id := filter.Key + "\x00" + filter.Value
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		filters = append(filters, filter)
	}
	sort.Slice(filters, func(i, j int) bool {
		if filters[i].Key == filters[j].Key {
			return filters[i].Value < filters[j].Value
		}
		return filters[i].Key < filters[j].Key
	})
	return filters
}

func expandPaths(ctx context.Context, paths []string, options Options) ([]string, error) {
	var files []string
	for _, input := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		path := strings.TrimSpace(input)
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			files = append(files, path)
			continue
		}

		if options.Recursive {
			err = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if d.IsDir() {
					return nil
				}
				if isStorageCandidate(p, options.Format) {
					files = append(files, p)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			continue
		}

		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			p := filepath.Join(path, entry.Name())
			if isStorageCandidate(p, options.Format) {
				files = append(files, p)
			}
		}
	}
	sort.Strings(files)
	return files, nil
}

func analyzeFile(path string, options Options) (FileReport, error) {
	info, err := os.Stat(path)
	if err != nil {
		return FileReport{}, err
	}
	format := options.Format
	if format == FormatAuto {
		format, err = detectFormat(path)
		if err != nil {
			return FileReport{}, err
		}
	}

	switch format {
	case FormatTSM:
		return analyzeTSM(path, info, options)
	case FormatWAL:
		return analyzeWAL(path, info, options)
	case FormatTSSP:
		return analyzeTSSP(path, info, options)
	case FormatTSSPDetachedIndex:
		return analyzeTSSPDetachedMetaIndex(path, info, options)
	case FormatTSI:
		return analyzeTSI(path, info, options)
	case FormatTSILog:
		return analyzeTSILog(path, info, options)
	default:
		return FileReport{}, fmt.Errorf("unsupported storage format %q", format)
	}
}

func detectFormat(path string) (Format, error) {
	if isWALPath(path) {
		return FormatWAL, nil
	}
	if isTSILogPath(path) {
		return FormatTSILog, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var header [16]byte
	n, err := io.ReadFull(f, header[:])
	if err != nil && err != io.ErrUnexpectedEOF {
		return "", err
	}
	if n >= 5 && binary.BigEndian.Uint32(header[:4]) == tsmMagicNumber && header[4] == tsmVersion {
		return FormatTSM, nil
	}
	if n >= len(tsspMagic) && string(header[:len(tsspMagic)]) == tsspMagic {
		if isTSSPDetachedMetaIndexPath(path) {
			return FormatTSSPDetachedIndex, nil
		}
		return FormatTSSP, nil
	}
	if n >= len(tsiMagic) && string(header[:len(tsiMagic)]) == tsiMagic {
		return FormatTSI, nil
	}
	if n < 5 {
		return "", fmt.Errorf("file too small to detect storage format")
	}
	return "", fmt.Errorf("unknown storage file format")
}

func isStorageCandidate(path string, format Format) bool {
	lower := strings.ToLower(filepath.Base(path))
	switch format {
	case FormatTSM:
		return strings.HasSuffix(lower, ".tsm")
	case FormatWAL:
		return isWALPath(path)
	case FormatTSSP:
		return strings.Contains(lower, ".tssp")
	case FormatTSSPDetachedIndex:
		return isTSSPDetachedMetaIndexPath(path)
	case FormatTSI:
		return strings.HasSuffix(lower, ".tsi")
	case FormatTSILog:
		return isTSILogPath(path)
	default:
		return strings.HasSuffix(lower, ".tsm") || isWALPath(path) || strings.Contains(lower, ".tssp") || isTSSPDetachedMetaIndexPath(path) || strings.HasSuffix(lower, ".tsi") || isTSILogPath(path)
	}
}

func isTSILogPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(filepath.Base(path)), ".tsl")
}

func accumulateSummary(summary *Summary, file FileReport, queryRange TimeRange) {
	summary.TotalSizeBytes += file.SizeBytes
	summary.KeyCount += file.KeyCount
	summary.BlockCount += file.BlockCount
	if file.Tombstones.Exists {
		summary.TombstoneFiles++
	}
	if queryRange.Set {
		if file.QueryOverlapsFile {
			summary.QueryOverlapFiles++
		}
		summary.QueryOverlapBlocks += file.QueryOverlapBlocks
	}
}
