package storage

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	mergesetMetadataFile  = "metadata.json"
	mergesetMetaindexFile = "metaindex.bin"
	mergesetIndexFile     = "index.bin"
	mergesetItemsFile     = "items.bin"
	mergesetLensFile      = "lens.bin"
)

type mergesetPartName struct {
	ItemsCount  uint64
	BlocksCount uint64
	Suffix      string
}

type mergesetPartMetadata struct {
	ItemsCount  uint64 `json:"ItemsCount"`
	BlocksCount uint64 `json:"BlocksCount"`
	FirstItem   string `json:"FirstItem"`
	LastItem    string `json:"LastItem"`
}

func analyzeMergesetPart(path string, info os.FileInfo, options Options) (FileReport, error) {
	if !info.IsDir() {
		return FileReport{}, fmt.Errorf("mergeset part must be a directory")
	}
	name, err := parseMergesetPartName(filepath.Base(path))
	if err != nil {
		return FileReport{}, err
	}
	metadata, err := readMergesetPartMetadata(path)
	if err != nil {
		return FileReport{}, err
	}
	if metadata.ItemsCount != name.ItemsCount {
		return FileReport{}, fmt.Errorf("invalid mergeset ItemsCount in metadata: got %d, want %d", metadata.ItemsCount, name.ItemsCount)
	}
	notices := []string{}
	if metadata.BlocksCount != name.BlocksCount {
		notices = append(notices, fmt.Sprintf("mergeset part name blocks_count=%d differs from metadata blocks_count=%d", name.BlocksCount, metadata.BlocksCount))
	}

	componentSizes, totalSize, err := mergesetComponentSizes(path)
	if err != nil {
		return FileReport{}, err
	}
	firstItem, err := decodeMergesetHexItem(metadata.FirstItem, "FirstItem")
	if err != nil {
		return FileReport{}, err
	}
	lastItem, err := decodeMergesetHexItem(metadata.LastItem, "LastItem")
	if err != nil {
		return FileReport{}, err
	}

	keySamples := make([]string, 0, 2)
	if options.KeySampleLimit > 0 {
		keySamples = append(keySamples, "first:"+metadata.FirstItem)
		if options.KeySampleLimit > 1 && metadata.LastItem != metadata.FirstItem {
			keySamples = append(keySamples, "last:"+metadata.LastItem)
		}
	}

	report := FileReport{
		Path:       path,
		Format:     FormatMergeset,
		SizeBytes:  totalSize,
		ModTime:    info.ModTime(),
		KeyCount:   uint64ToInt(metadata.ItemsCount),
		KeySamples: keySamples,
		BlockCount: uint64ToInt(metadata.BlocksCount),
		BlocksByType: map[string]int{
			"mergeset-block": uint64ToInt(metadata.BlocksCount),
		},
		Extra: map[string]string{
			"layout":             "part",
			"items_count":        fmt.Sprint(metadata.ItemsCount),
			"blocks_count":       fmt.Sprint(metadata.BlocksCount),
			"part_name_items":    fmt.Sprint(name.ItemsCount),
			"part_name_blocks":   fmt.Sprint(name.BlocksCount),
			"part_suffix":        name.Suffix,
			"first_item_hex":     metadata.FirstItem,
			"last_item_hex":      metadata.LastItem,
			"first_item_bytes":   fmt.Sprint(len(firstItem)),
			"last_item_bytes":    fmt.Sprint(len(lastItem)),
			"metadata_json_size": fmt.Sprint(componentSizes[mergesetMetadataFile]),
			"metaindex_size":     fmt.Sprint(componentSizes[mergesetMetaindexFile]),
			"index_size":         fmt.Sprint(componentSizes[mergesetIndexFile]),
			"items_size":         fmt.Sprint(componentSizes[mergesetItemsFile]),
			"lens_size":          fmt.Sprint(componentSizes[mergesetLensFile]),
		},
		Notices: notices,
	}
	return report, nil
}

func isMergesetPartPath(path string) bool {
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return false
	}
	if _, err := parseMergesetPartName(filepath.Base(path)); err != nil {
		return false
	}
	if _, err := os.Stat(filepath.Join(path, mergesetMetadataFile)); err != nil {
		return false
	}
	return true
}

func parseMergesetPartName(name string) (mergesetPartName, error) {
	var part mergesetPartName
	fields := strings.Split(name, "_")
	if len(fields) != 3 {
		return part, fmt.Errorf("invalid mergeset part name %q: expected items_blocks_suffix", name)
	}
	itemsCount, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return part, fmt.Errorf("invalid mergeset items count in part name %q: %w", name, err)
	}
	if itemsCount == 0 {
		return part, fmt.Errorf("mergeset part %q cannot contain zero items", name)
	}
	blocksCount, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return part, fmt.Errorf("invalid mergeset blocks count in part name %q: %w", name, err)
	}
	if blocksCount == 0 {
		return part, fmt.Errorf("mergeset part %q cannot contain zero blocks", name)
	}
	if blocksCount > itemsCount {
		return part, fmt.Errorf("mergeset part %q has blocks_count=%d greater than items_count=%d", name, blocksCount, itemsCount)
	}
	part.ItemsCount = itemsCount
	part.BlocksCount = blocksCount
	part.Suffix = fields[2]
	return part, nil
}

func readMergesetPartMetadata(path string) (mergesetPartMetadata, error) {
	var metadata mergesetPartMetadata
	data, err := os.ReadFile(filepath.Join(path, mergesetMetadataFile))
	if err != nil {
		return metadata, err
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return metadata, fmt.Errorf("parse mergeset metadata: %w", err)
	}
	return metadata, nil
}

func mergesetComponentSizes(path string) (map[string]int64, int64, error) {
	names := []string{
		mergesetMetadataFile,
		mergesetMetaindexFile,
		mergesetIndexFile,
		mergesetItemsFile,
		mergesetLensFile,
	}
	sizes := make(map[string]int64, len(names))
	var total int64
	for _, name := range names {
		info, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			return nil, 0, fmt.Errorf("stat mergeset component %s: %w", name, err)
		}
		if info.IsDir() {
			return nil, 0, fmt.Errorf("mergeset component %s is a directory", name)
		}
		sizes[name] = info.Size()
		total += info.Size()
	}
	return sizes, total, nil
}

func decodeMergesetHexItem(value, field string) ([]byte, error) {
	item, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("invalid mergeset %s hex item: %w", field, err)
	}
	return item, nil
}

func uint64ToInt(value uint64) int {
	maxInt := int(^uint(0) >> 1)
	if value > uint64(maxInt) {
		return maxInt
	}
	return int(value)
}
