package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
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
	options.QueryColumns = normalizeQueryKeys(options.QueryColumns)
	options.QueryFields = normalizeFieldFilters(options.QueryFields)
	if err := validateFieldFilters(options.QueryFields); err != nil {
		return Report{}, err
	}
	options.QueryAnyFields = normalizeFieldFilters(options.QueryAnyFields)
	if err := validateFieldFilters(options.QueryAnyFields); err != nil {
		return Report{}, err
	}
	options.QueryNoneFields = normalizeFieldFilters(options.QueryNoneFields)
	if err := validateFieldFilters(options.QueryNoneFields); err != nil {
		return Report{}, err
	}
	options.QueryMeasurements = normalizeQueryKeys(options.QueryMeasurements)
	options.QueryTags = normalizeTagFilters(options.QueryTags)
	if len(options.QueryKeys) > 0 && !options.QueryRange.Set && options.Format != FormatMergeset {
		return Report{}, fmt.Errorf("query key filter requires query range")
	}
	if len(options.QuerySeriesIDs) > 0 && !options.QueryRange.Set && options.Format != FormatSeriesFile {
		return Report{}, fmt.Errorf("query series id filter requires query range")
	}
	if len(options.QueryMetaIndexIDs) > 0 && !options.QueryRange.Set {
		return Report{}, fmt.Errorf("query meta-index id filter requires query range")
	}
	if len(options.QueryColumns) > 0 && !options.QueryRange.Set {
		return Report{}, fmt.Errorf("query column filter requires query range")
	}
	if (len(options.QueryFields) > 0 || len(options.QueryAnyFields) > 0 || len(options.QueryNoneFields) > 0) && !options.QueryRange.Set {
		return Report{}, fmt.Errorf("query field filter requires query range")
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
		accumulateSummary(&report.Summary, fileReport, options)
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
		if report.DecodePath == nil {
			report.DecodePath = buildTSSPDetachedFileSetDecodePathSummary(report.Files, options)
		}
	}
	if options.Format == FormatMergeset {
		if decodePath := buildMergesetFileSetDecodePathSummary(report.Files, options); decodePath != nil {
			report.DecodePath = decodePath
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

func normalizeFieldFilters(values []FieldFilter) []FieldFilter {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	filters := make([]FieldFilter, 0, len(values))
	for _, value := range values {
		filter := FieldFilter{
			Key:   strings.TrimSpace(value.Key),
			Op:    normalizeFieldFilterOperator(value.Op),
			Value: strings.TrimSpace(value.Value),
		}
		if filter.Key == "" {
			continue
		}
		id := filter.Key + "\x00" + fieldFilterOperator(filter) + "\x00" + filter.Value
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		filters = append(filters, filter)
	}
	sort.Slice(filters, func(i, j int) bool {
		if filters[i].Key == filters[j].Key {
			if fieldFilterOperator(filters[i]) != fieldFilterOperator(filters[j]) {
				return fieldFilterOperator(filters[i]) < fieldFilterOperator(filters[j])
			}
			return filters[i].Value < filters[j].Value
		}
		return filters[i].Key < filters[j].Key
	})
	return filters
}

func normalizeFieldFilterOperator(op string) string {
	op = strings.ToLower(strings.TrimSpace(op))
	op = strings.ReplaceAll(op, "_", "-")
	switch op {
	case "=", "==", "is", "":
		return ""
	case "<>":
		return "!="
	case "!>", "not >", "not->":
		return "<="
	case "!>=", "not >=", "not->=":
		return "<"
	case "!<", "not <", "not-<":
		return ">="
	case "!<=", "not <=", "not-<=":
		return ">"
	case "!between", "not between", "not-between":
		return "not-between"
	case "!in", "not in", "not-in":
		return "not-in"
	case "!contains", "not contains", "not-contains":
		return "not-contains"
	case "!icontains", "not icontains", "not-icontains":
		return "not-icontains"
	case "!like", "not like", "not-like":
		return "not-like"
	case "!ilike", "not ilike", "not-ilike":
		return "not-ilike"
	case "matches", "match", "regex", "regexp":
		return "=~"
	case "!matches", "!match", "!regex", "!regexp", "not matches", "not match", "not regex", "not regexp", "not-matches", "not-match", "not-regex", "not-regexp":
		return "!~"
	case "!exists", "not exists", "not-exists":
		return "not-exists"
	case "starts with", "starts-with":
		return "starts-with"
	case "!starts-with", "not starts with", "not-starts-with":
		return "not-starts-with"
	case "istarts with", "istarts-with":
		return "istarts-with"
	case "!istarts-with", "not istarts with", "not-istarts-with":
		return "not-istarts-with"
	case "ends with", "ends-with":
		return "ends-with"
	case "!ends-with", "not ends with", "not-ends-with":
		return "not-ends-with"
	case "iends with", "iends-with":
		return "iends-with"
	case "!iends-with", "not iends with", "not-iends-with":
		return "not-iends-with"
	case "is not", "is-not":
		return "!="
	default:
		return op
	}
}

func fieldFilterOperator(filter FieldFilter) string {
	op := normalizeFieldFilterOperator(filter.Op)
	if op == "" {
		return "="
	}
	return op
}

func validFieldFilterOperator(op string) bool {
	switch normalizeFieldFilterOperator(op) {
	case "", "!=", ">", ">=", "<", "<=", "in", "not-in", "between", "not-between", "contains", "not-contains", "icontains", "not-icontains", "like", "not-like", "ilike", "not-ilike", "starts-with", "not-starts-with", "istarts-with", "not-istarts-with", "ends-with", "not-ends-with", "iends-with", "not-iends-with", "=~", "!~", "exists", "not-exists":
		return true
	default:
		return false
	}
}

func validateFieldFilters(filters []FieldFilter) error {
	for _, filter := range filters {
		if !validFieldFilterOperator(filter.Op) {
			return fmt.Errorf("query field filter %q has unsupported operator %q", filter.Key, filter.Op)
		}
		op := fieldFilterOperator(filter)
		if op == "exists" || op == "not-exists" {
			if strings.TrimSpace(filter.Value) != "" {
				return fmt.Errorf("query field filter %q does not take a value for operator %q", filter.Key, op)
			}
			continue
		}
		if op == "in" || op == "not-in" {
			if len(fieldFilterSetValues(filter.Value)) == 0 {
				return fmt.Errorf("query field filter %q requires at least one value for operator %q", filter.Key, op)
			}
			continue
		}
		if op == "between" || op == "not-between" {
			values := fieldFilterSetValues(filter.Value)
			if len(values) != 2 {
				return fmt.Errorf("query field filter %q requires exactly two values for operator %q", filter.Key, op)
			}
			for _, value := range values {
				if value == "null" {
					return fmt.Errorf("query field filter %q does not support null bounds for operator %q", filter.Key, op)
				}
			}
			continue
		}
		if filter.Value == "" && op != "=" && op != "!=" {
			return fmt.Errorf("query field filter %q requires a value for operator %q", filter.Key, op)
		}
		if op == "=~" || op == "!~" {
			if _, err := regexp.Compile(fieldFilterScalarValue(filter.Value)); err != nil {
				return fmt.Errorf("query field filter %q has invalid regex for operator %q: %w", filter.Key, op, err)
			}
		}
	}
	return nil
}

func fieldFilterSetValues(value string) []string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '(' && value[len(value)-1] == ')' {
		value = strings.TrimSpace(value[1 : len(value)-1])
	}
	if value == "" {
		return nil
	}
	parts := splitFieldFilterSetLiterals(value)
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		literal, quoted := fieldFilterLiteralValue(part)
		if literal == "" && !quoted {
			continue
		}
		values = append(values, literal)
	}
	return values
}

func splitFieldFilterSetLiterals(value string) []string {
	var parts []string
	var part strings.Builder
	var quote byte
	escaped := false
	for i := 0; i < len(value); i++ {
		c := value[i]
		if escaped {
			part.WriteByte(c)
			escaped = false
			continue
		}
		if quote != 0 {
			if c == '\\' {
				part.WriteByte(c)
				escaped = true
				continue
			}
			if c == quote {
				quote = 0
			}
			part.WriteByte(c)
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
			part.WriteByte(c)
		case ',':
			parts = append(parts, part.String())
			part.Reset()
		default:
			part.WriteByte(c)
		}
	}
	parts = append(parts, part.String())
	return parts
}

func fieldFilterScalarValue(value string) string {
	literal, _ := fieldFilterLiteralValue(strings.TrimSpace(value))
	return literal
}

func fieldFilterLiteralValue(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) < 2 {
		return value, false
	}
	quote := value[0]
	if (quote != '\'' && quote != '"') || value[len(value)-1] != quote {
		return value, false
	}
	var literal strings.Builder
	escaped := false
	for i := 1; i < len(value)-1; i++ {
		c := value[i]
		if escaped {
			switch c {
			case 'n':
				literal.WriteByte('\n')
			case 'r':
				literal.WriteByte('\r')
			case 't':
				literal.WriteByte('\t')
			default:
				literal.WriteByte(c)
			}
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			continue
		}
		literal.WriteByte(c)
	}
	if escaped {
		literal.WriteByte('\\')
	}
	return literal.String(), true
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
		if isStorageCandidate(path, options.Format) {
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
					if p != path && isStorageCandidate(p, options.Format) {
						files = append(files, p)
						return filepath.SkipDir
					}
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
	case FormatSeriesFile:
		return analyzeSeriesFile(path, info, options)
	case FormatFieldsIndex:
		return analyzeFieldsIndex(path, info, options)
	case FormatMergeset:
		return analyzeMergesetPart(path, info, options)
	case FormatOpenGeminiMeta:
		return analyzeOpenGeminiMeta(path, info, options)
	case FormatOpenGeminiPKMeta:
		return analyzeOpenGeminiPKMeta(path, info, options)
	case FormatOpenGeminiPKIndex:
		return analyzeOpenGeminiPKIndex(path, info, options)
	case FormatOpenGeminiBloom:
		return analyzeOpenGeminiBloomFilter(path, info, options)
	case FormatOpenGeminiText:
		return analyzeOpenGeminiTextIndex(path, info, options)
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
	if isMergesetPartPath(path) {
		return FormatMergeset, nil
	}
	if isOpenGeminiMetaPath(path) {
		return FormatOpenGeminiMeta, nil
	}
	if isOpenGeminiPKMetaPath(path) {
		return FormatOpenGeminiPKMeta, nil
	}
	if isOpenGeminiBloomFilterPath(path) {
		return FormatOpenGeminiBloom, nil
	}
	if isOpenGeminiTextIndexPath(path) {
		return FormatOpenGeminiText, nil
	}
	if isSeriesFilePath(path) {
		return FormatSeriesFile, nil
	}
	if isFieldsIndexPath(path) {
		return FormatFieldsIndex, nil
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
	if n >= seriesSegmentHeaderSize && string(header[:len(seriesSegmentMagic)]) == seriesSegmentMagic && header[len(seriesSegmentMagic)] == seriesSegmentVersion {
		return FormatSeriesFile, nil
	}
	if n >= len(fieldsIndexMagicNumber) && string(header[:len(fieldsIndexMagicNumber)]) == string(fieldsIndexMagicNumber) {
		return FormatFieldsIndex, nil
	}
	if n >= len(opengeminiPKMagic) && string(header[:len(opengeminiPKMagic)]) == opengeminiPKMagic {
		return FormatOpenGeminiPKIndex, nil
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
		return isTSSPFileCandidatePath(path)
	case FormatTSSPDetachedIndex:
		return isTSSPDetachedMetaIndexPath(path)
	case FormatTSI:
		return strings.HasSuffix(lower, ".tsi")
	case FormatTSILog:
		return isTSILogPath(path)
	case FormatSeriesFile:
		return isSeriesFilePath(path)
	case FormatFieldsIndex:
		return isFieldsIndexPath(path)
	case FormatMergeset:
		return isMergesetPartPath(path)
	case FormatOpenGeminiMeta:
		return isOpenGeminiMetaPath(path)
	case FormatOpenGeminiPKMeta:
		return isOpenGeminiPKMetaPath(path)
	case FormatOpenGeminiPKIndex:
		return isOpenGeminiPKIndexCandidatePath(path)
	case FormatOpenGeminiBloom:
		return isOpenGeminiBloomFilterPath(path)
	case FormatOpenGeminiText:
		return false
	default:
		return strings.HasSuffix(lower, ".tsm") || isWALPath(path) || isTSSPFileCandidatePath(path) || isTSSPDetachedMetaIndexPath(path) || strings.HasSuffix(lower, ".tsi") || isTSILogPath(path) || isSeriesFilePath(path) || isFieldsIndexPath(path) || isMergesetPartPath(path) || isOpenGeminiMetaPath(path) || isOpenGeminiPKMetaPath(path) || isOpenGeminiBloomFilterPath(path) || isOpenGeminiPKIndexCandidatePath(path)
	}
}

func isTSSPFileCandidatePath(path string) bool {
	if isOpenGeminiTextIndexPath(path) {
		return false
	}
	return strings.Contains(strings.ToLower(filepath.Base(path)), ".tssp")
}

func isOpenGeminiMetaPath(path string) bool {
	lower := strings.ToLower(filepath.Base(path))
	switch lower {
	case "meta.pb", "opengemini-meta.pb", "opengemini-meta.json":
		return true
	default:
		return strings.HasSuffix(lower, ".ogmeta")
	}
}

func isOpenGeminiPKMetaPath(path string) bool {
	return strings.EqualFold(filepath.Base(path), opengeminiPKMetaFileName)
}

func isOpenGeminiPKIndexCandidatePath(path string) bool {
	lower := strings.ToLower(filepath.Base(path))
	switch lower {
	case opengeminiPKMetaFileName, opengeminiPKDataFileName, tsspDetachedMetaIndexFileName, fieldsIndexFileName:
		return false
	default:
		return strings.HasSuffix(lower, ".idx")
	}
}

func isTSILogPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(filepath.Base(path)), ".tsl")
}

func accumulateSummary(summary *Summary, file FileReport, options Options) {
	summary.TotalSizeBytes += file.SizeBytes
	summary.KeyCount += file.KeyCount
	summary.BlockCount += file.BlockCount
	if len(file.BlocksByType) > 0 {
		if summary.BlocksByType == nil {
			summary.BlocksByType = map[string]int{}
		}
		for blockType, count := range file.BlocksByType {
			if count > 0 {
				summary.BlocksByType[blockType] += count
			}
		}
	}
	if file.Tombstones.Exists {
		summary.TombstoneFiles++
		summary.TombstoneSizeBytes += file.Tombstones.SizeBytes
		summary.TombstoneRanges += file.Tombstones.RangeCount
		summary.TombstoneQueryOverlapRanges += file.Tombstones.QueryOverlapRanges
		summary.TombstoneAffectedBlocks += file.Tombstones.AffectedBlocks
	}
	if optionsHasQueryTarget(options) {
		if file.QueryOverlapsFile {
			summary.QueryOverlapFiles++
		}
		summary.QueryOverlapBlocks += file.QueryOverlapBlocks
	}
}

func optionsHasQueryTarget(options Options) bool {
	return options.QueryRange.Set ||
		len(options.QueryKeys) > 0 ||
		len(options.QuerySeriesIDs) > 0 ||
		len(options.QueryMetaIndexIDs) > 0 ||
		len(options.QueryColumns) > 0 ||
		len(options.QueryFields) > 0 ||
		len(options.QueryAnyFields) > 0 ||
		len(options.QueryNoneFields) > 0 ||
		len(options.QueryMeasurements) > 0 ||
		len(options.QueryTags) > 0
}
