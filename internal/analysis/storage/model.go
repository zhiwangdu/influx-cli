package storage

import (
	"fmt"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

type Format string

const (
	FormatAuto Format = "auto"
	FormatTSM  Format = "tsm"
	FormatTSSP Format = "tssp"
)

type Options struct {
	Format           Format
	Recursive        bool
	KeySampleLimit   int
	BlockSampleLimit int
	QueryRange       TimeRange
}

type TimeRange struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
	Set bool  `json:"set"`
}

func NewTimeRange(min, max int64) (TimeRange, error) {
	if min > max {
		return TimeRange{}, fmt.Errorf("invalid time range: min %d is greater than max %d", min, max)
	}
	return TimeRange{Min: min, Max: max, Set: true}, nil
}

func (r TimeRange) Overlaps(min, max int64) bool {
	if !r.Set {
		return false
	}
	return min <= r.Max && max >= r.Min
}

type Report struct {
	Files   []FileReport `json:"files"`
	Summary Summary      `json:"summary"`
	Notices []string     `json:"notices,omitempty"`
}

type Summary struct {
	FileCount          int   `json:"file_count"`
	TotalSizeBytes     int64 `json:"total_size_bytes"`
	KeyCount           int   `json:"key_count"`
	BlockCount         int   `json:"block_count"`
	QueryOverlapFiles  int   `json:"query_overlap_files,omitempty"`
	QueryOverlapBlocks int   `json:"query_overlap_blocks,omitempty"`
	TombstoneFiles     int   `json:"tombstone_files,omitempty"`
}

type FileReport struct {
	Path               string            `json:"path"`
	Format             Format            `json:"format"`
	SizeBytes          int64             `json:"size_bytes"`
	ModTime            time.Time         `json:"mod_time"`
	MinTime            int64             `json:"min_time"`
	MaxTime            int64             `json:"max_time"`
	MinKey             string            `json:"min_key,omitempty"`
	MaxKey             string            `json:"max_key,omitempty"`
	KeyCount           int               `json:"key_count"`
	KeySamples         []string          `json:"key_samples,omitempty"`
	BlockCount         int               `json:"block_count"`
	BlocksByType       map[string]int    `json:"blocks_by_type,omitempty"`
	Blocks             []BlockReport     `json:"blocks,omitempty"`
	QueryOverlapsFile  bool              `json:"query_overlaps_file,omitempty"`
	QueryOverlapBlocks int               `json:"query_overlap_blocks,omitempty"`
	Tombstones         TombstoneSummary  `json:"tombstones,omitempty"`
	SeriesID           SeriesIDSummary   `json:"series_id,omitempty"`
	Extra              map[string]string `json:"extra,omitempty"`
	Notices            []string          `json:"notices,omitempty"`
}

type SeriesIDSummary struct {
	Min   uint64 `json:"min,omitempty"`
	Max   uint64 `json:"max,omitempty"`
	Count int64  `json:"count,omitempty"`
}

type TombstoneSummary struct {
	Exists    bool   `json:"exists"`
	Path      string `json:"path,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type BlockReport struct {
	Key             string `json:"key,omitempty"`
	SeriesID        uint64 `json:"series_id,omitempty"`
	MinTime         int64  `json:"min_time"`
	MaxTime         int64  `json:"max_time"`
	Type            string `json:"type"`
	Offset          int64  `json:"offset,omitempty"`
	SizeBytes       uint32 `json:"size_bytes,omitempty"`
	ValueCount      int    `json:"value_count,omitempty"`
	QueryOverlaps   bool   `json:"query_overlaps,omitempty"`
	ContainedChunks int    `json:"contained_chunks,omitempty"`
}

func (r Report) Result() result.Result {
	table := result.NewTable([]string{
		"file",
		"format",
		"size",
		"time_min",
		"time_max",
		"keys/series",
		"blocks",
		"query_blocks",
		"tombstone",
		"samples",
	})
	for _, file := range r.Files {
		tombstone := ""
		if file.Tombstones.Exists {
			tombstone = fmt.Sprintf("yes (%d bytes)", file.Tombstones.SizeBytes)
		}
		table.AddRow(
			file.Path,
			string(file.Format),
			file.SizeBytes,
			FormatUnixNano(file.MinTime),
			FormatUnixNano(file.MaxTime),
			file.KeyCount,
			file.BlockCount,
			file.QueryOverlapBlocks,
			tombstone,
			joinSamples(file.KeySamples),
		)
	}
	return result.Result{
		Kind:  result.KindTable,
		Table: table,
		Metadata: result.Metadata{
			RowCount: len(r.Files),
			Notices:  append([]string(nil), r.Notices...),
			Source:   "storage-analyzer",
		},
	}
}

func FormatUnixNano(v int64) string {
	return time.Unix(0, v).UTC().Format(time.RFC3339Nano)
}

func joinSamples(values []string) string {
	const maxLen = 96
	out := ""
	for i, value := range values {
		if i > 0 {
			out += ", "
		}
		out += value
		if len(out) > maxLen {
			return out[:maxLen-3] + "..."
		}
	}
	return out
}
