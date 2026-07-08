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
	opengeminiBloomFilterLayoutLine     = "attached-line"
	opengeminiBloomFilterLayoutVertical = "detached-vertical"
	opengeminiBloomFilterType           = "opengemini-bloom-filter"
	opengeminiBloomFilterPrefix         = "bloomfilter_"
	opengeminiBloomFilterDetachedSuffix = ".idx"
	opengeminiBloomFilterAttachedSuffix = ".bf"
	opengeminiBloomFilterFullTextField  = "fullText"
	opengeminiBloomFilterVersion        = 5

	opengeminiBloomFilterPayloadSize       int64 = 256*1024 + 64
	opengeminiBloomFilterCRCSize           int64 = 4
	opengeminiBloomFilterBlockSize         int64 = opengeminiBloomFilterPayloadSize + opengeminiBloomFilterCRCSize
	opengeminiBloomFilterVerticalGroupSize int64 = 128
	opengeminiBloomFilterVerticalPieceMem  int64 = 8 * opengeminiBloomFilterVerticalGroupSize
	opengeminiBloomFilterVerticalPieceSize int64 = opengeminiBloomFilterVerticalPieceMem + opengeminiBloomFilterCRCSize
	opengeminiBloomFilterVerticalPieces    int64 = opengeminiBloomFilterPayloadSize / 8
	opengeminiBloomFilterVerticalDiskSize  int64 = opengeminiBloomFilterVerticalPieceSize * opengeminiBloomFilterVerticalPieces
)

var opengeminiBloomFilterCRCTable = crc32.MakeTable(crc32.Castagnoli)

type opengeminiBloomFilterPathInfo struct {
	Field    string
	Layout   string
	FullText bool
}

type opengeminiBloomFilterAnalysis struct {
	PathInfo      opengeminiBloomFilterPathInfo
	ValidBytes    int64
	TrailingBytes int64
	BlockCount    int64
	GroupCount    int64
	PieceCount    int64
	CRCMismatches int
	BlocksByType  map[string]int
	Notices       []string
}

func analyzeOpenGeminiBloomFilter(path string, info os.FileInfo, options Options) (FileReport, error) {
	if info.IsDir() {
		return FileReport{}, fmt.Errorf("opengemini-bloom-filter format requires a bloom filter index file")
	}
	if !isOpenGeminiBloomFilterPath(path) {
		return FileReport{}, fmt.Errorf("%s does not match opengemini-bloom-filter sidecar naming; expected .bf other than mergeset.bf/*_mergeset.bf or bloomfilter_*.idx", filepath.Base(path))
	}

	pathInfo := openGeminiBloomFilterPathInfo(path)
	analysis, err := parseOpenGeminiBloomFilterFile(path, info.Size(), pathInfo)
	if err != nil {
		return FileReport{}, err
	}

	keySamples := openGeminiBloomFilterKeySamples(pathInfo, options.KeySampleLimit)
	blocks := openGeminiBloomFilterBlockSamples(analysis, options.BlockSampleLimit)
	extra := map[string]string{
		"layout":                          pathInfo.Layout,
		"field":                           pathInfo.Field,
		"full_text":                       fmt.Sprint(pathInfo.FullText),
		"version":                         fmt.Sprint(opengeminiBloomFilterVersion),
		"filter_data_mem_size":            fmt.Sprint(opengeminiBloomFilterPayloadSize),
		"filter_data_disk_size":           fmt.Sprint(opengeminiBloomFilterBlockSize),
		"filter_count_per_vertical_group": fmt.Sprint(opengeminiBloomFilterVerticalGroupSize),
		"vertical_piece_mem_size":         fmt.Sprint(opengeminiBloomFilterVerticalPieceMem),
		"vertical_piece_disk_size":        fmt.Sprint(opengeminiBloomFilterVerticalPieceSize),
		"vertical_piece_count_per_filter": fmt.Sprint(opengeminiBloomFilterVerticalPieces),
		"vertical_group_disk_size":        fmt.Sprint(opengeminiBloomFilterVerticalDiskSize),
		"valid_bytes":                     fmt.Sprint(analysis.ValidBytes),
		"trailing_bytes":                  fmt.Sprint(analysis.TrailingBytes),
		"crc_mismatches":                  fmt.Sprint(analysis.CRCMismatches),
		"local_only":                      "true",
	}

	report := FileReport{
		Path:         path,
		Format:       FormatOpenGeminiBloom,
		SizeBytes:    info.Size(),
		ModTime:      info.ModTime(),
		KeyCount:     openGeminiBloomFilterKeyCount(pathInfo),
		KeySamples:   keySamples,
		BlockCount:   int(analysis.BlockCount),
		BlocksByType: analysis.BlocksByType,
		Blocks:       blocks,
		SecondaryIndex: &SecondaryIndexSummary{
			Type:             opengeminiBloomFilterType,
			Layout:           pathInfo.Layout,
			Field:            pathInfo.Field,
			Version:          opengeminiBloomFilterVersion,
			BlockCount:       analysis.BlockCount,
			GroupCount:       analysis.GroupCount,
			PieceCount:       analysis.PieceCount,
			PayloadSizeBytes: opengeminiBloomFilterPayloadSize,
			BlockSizeBytes:   opengeminiBloomFilterBlockSize,
			PieceSizeBytes:   opengeminiBloomFilterVerticalPieceSize,
			GroupSizeBytes:   opengeminiBloomFilterVerticalDiskSize,
			ValidBytes:       analysis.ValidBytes,
			TrailingBytes:    analysis.TrailingBytes,
			CRCMismatches:    analysis.CRCMismatches,
		},
		Extra:   extra,
		Notices: analysis.Notices,
	}
	if pathInfo.Field != "" {
		report.MinKey = pathInfo.Field
		report.MaxKey = pathInfo.Field
	}
	return report, nil
}

func parseOpenGeminiBloomFilterFile(path string, size int64, pathInfo opengeminiBloomFilterPathInfo) (opengeminiBloomFilterAnalysis, error) {
	analysis := opengeminiBloomFilterAnalysis{
		PathInfo:     pathInfo,
		BlocksByType: map[string]int{},
	}
	if size < 0 {
		return analysis, fmt.Errorf("invalid bloom filter file size %d", size)
	}
	if size == 0 {
		analysis.Notices = append(analysis.Notices, "openGemini bloom filter file is empty")
		return analysis, nil
	}

	switch pathInfo.Layout {
	case opengeminiBloomFilterLayoutVertical:
		analysis.ValidBytes = size - size%opengeminiBloomFilterVerticalDiskSize
		analysis.TrailingBytes = size - analysis.ValidBytes
		if analysis.ValidBytes > 0 {
			analysis.GroupCount = analysis.ValidBytes / opengeminiBloomFilterVerticalDiskSize
			analysis.BlockCount = analysis.GroupCount * opengeminiBloomFilterVerticalGroupSize
			analysis.PieceCount = analysis.GroupCount * opengeminiBloomFilterVerticalPieces
			analysis.BlocksByType["bloom-filter-vertical-group"] = int(analysis.GroupCount)
			analysis.BlocksByType["bloom-filter-vertical-piece"] = int(analysis.PieceCount)
		}
	case opengeminiBloomFilterLayoutLine:
		analysis.ValidBytes = size - size%opengeminiBloomFilterBlockSize
		analysis.TrailingBytes = size - analysis.ValidBytes
		if analysis.ValidBytes > 0 {
			analysis.BlockCount = analysis.ValidBytes / opengeminiBloomFilterBlockSize
			analysis.BlocksByType["bloom-filter-line-block"] = int(analysis.BlockCount)
		}
	default:
		return analysis, fmt.Errorf("unknown openGemini bloom filter layout %q", pathInfo.Layout)
	}
	if analysis.TrailingBytes > 0 {
		analysis.BlocksByType["bloom-filter-trailing-bytes"] = 1
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("openGemini bloom filter has %d trailing byte(s) after complete %s block(s)", analysis.TrailingBytes, pathInfo.Layout))
	}
	if analysis.ValidBytes == 0 {
		if analysis.TrailingBytes > 0 {
			analysis.Notices = append(analysis.Notices, "openGemini bloom filter has no complete current-version block; the file may use an older logstore filter size")
		}
		return analysis, nil
	}

	mismatches, err := verifyOpenGeminiBloomFilterCRC(path, pathInfo.Layout, analysis.ValidBytes)
	if err != nil {
		return analysis, err
	}
	analysis.CRCMismatches = mismatches
	if mismatches > 0 {
		analysis.BlocksByType["bloom-filter-crc-mismatch"] = mismatches
		analysis.Notices = append(analysis.Notices, fmt.Sprintf("openGemini bloom filter has %d CRC mismatch(es)", mismatches))
	}
	return analysis, nil
}

func verifyOpenGeminiBloomFilterCRC(path, layout string, validBytes int64) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	switch layout {
	case opengeminiBloomFilterLayoutVertical:
		return verifyOpenGeminiBloomFilterCRCBlocks(f, validBytes, opengeminiBloomFilterVerticalPieceSize, opengeminiBloomFilterVerticalPieceMem)
	default:
		return verifyOpenGeminiBloomFilterCRCBlocks(f, validBytes, opengeminiBloomFilterBlockSize, opengeminiBloomFilterPayloadSize)
	}
}

func verifyOpenGeminiBloomFilterCRCBlocks(r io.Reader, validBytes, blockSize, payloadSize int64) (int, error) {
	if blockSize <= 0 || payloadSize <= 0 || payloadSize+opengeminiBloomFilterCRCSize != blockSize {
		return 0, fmt.Errorf("invalid bloom filter CRC block size: block=%d payload=%d", blockSize, payloadSize)
	}
	buf := make([]byte, blockSize)
	mismatches := 0
	for offset := int64(0); offset < validBytes; offset += blockSize {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		payload := buf[:payloadSize]
		written := binary.LittleEndian.Uint32(buf[payloadSize:blockSize])
		computed := crc32.Checksum(payload, opengeminiBloomFilterCRCTable)
		if written != computed {
			mismatches++
		}
	}
	return mismatches, nil
}

func openGeminiBloomFilterBlockSamples(analysis opengeminiBloomFilterAnalysis, limit int) []BlockReport {
	if limit <= 0 || analysis.BlockCount == 0 {
		return nil
	}
	count := openGeminiBloomFilterSampleCount(analysis, limit)
	reports := make([]BlockReport, 0, count)
	for i := int64(0); i < count; i++ {
		switch analysis.PathInfo.Layout {
		case opengeminiBloomFilterLayoutVertical:
			reports = append(reports, BlockReport{
				Key:          analysis.PathInfo.Field,
				Type:         "bloom-filter-vertical-group",
				Offset:       i * opengeminiBloomFilterVerticalDiskSize,
				SizeBytes:    clampUint32(opengeminiBloomFilterVerticalDiskSize),
				SegmentCount: int(opengeminiBloomFilterVerticalGroupSize),
			})
		default:
			reports = append(reports, BlockReport{
				Key:       analysis.PathInfo.Field,
				Type:      "bloom-filter-line-block",
				Offset:    i * opengeminiBloomFilterBlockSize,
				SizeBytes: clampUint32(opengeminiBloomFilterBlockSize),
			})
		}
	}
	return reports
}

func openGeminiBloomFilterSampleCount(analysis opengeminiBloomFilterAnalysis, limit int) int64 {
	count := int64(limit)
	available := analysis.BlockCount
	if analysis.PathInfo.Layout == opengeminiBloomFilterLayoutVertical {
		available = analysis.GroupCount
	}
	if count > available {
		return available
	}
	return count
}

func openGeminiBloomFilterKeySamples(pathInfo opengeminiBloomFilterPathInfo, limit int) []string {
	if limit <= 0 || pathInfo.Field == "" {
		return nil
	}
	if pathInfo.FullText {
		return []string{"field:" + pathInfo.Field + " (full-text)"}
	}
	return []string{"field:" + pathInfo.Field}
}

func openGeminiBloomFilterKeyCount(pathInfo opengeminiBloomFilterPathInfo) int {
	if pathInfo.Field == "" {
		return 0
	}
	return 1
}

func openGeminiBloomFilterPathInfo(path string) opengeminiBloomFilterPathInfo {
	base := filepath.Base(path)
	lower := strings.ToLower(base)
	info := opengeminiBloomFilterPathInfo{Layout: opengeminiBloomFilterLayoutLine}
	if strings.HasPrefix(lower, opengeminiBloomFilterPrefix) && strings.HasSuffix(lower, opengeminiBloomFilterDetachedSuffix) {
		info.Layout = opengeminiBloomFilterLayoutVertical
		info.Field = base[len(opengeminiBloomFilterPrefix) : len(base)-len(opengeminiBloomFilterDetachedSuffix)]
	} else if strings.HasSuffix(lower, opengeminiBloomFilterAttachedSuffix) {
		stem := base[:len(base)-len(opengeminiBloomFilterAttachedSuffix)]
		info.Field = stem
		if idx := strings.LastIndex(stem, "."); idx >= 0 && idx < len(stem)-1 {
			info.Field = stem[idx+1:]
		}
		if strings.HasPrefix(strings.ToLower(info.Field), opengeminiBloomFilterPrefix) {
			info.Field = info.Field[len(opengeminiBloomFilterPrefix):]
		}
	}
	if strings.EqualFold(info.Field, opengeminiBloomFilterFullTextField) {
		info.FullText = true
	}
	return info
}

func isOpenGeminiBloomFilterPath(path string) bool {
	base := filepath.Base(path)
	lower := strings.ToLower(base)
	if strings.HasPrefix(lower, opengeminiBloomFilterPrefix) && strings.HasSuffix(lower, opengeminiBloomFilterDetachedSuffix) {
		return true
	}
	if strings.HasSuffix(lower, opengeminiBloomFilterAttachedSuffix) {
		return lower != "mergeset.bf" && !strings.HasSuffix(lower, "_mergeset.bf")
	}
	return false
}
