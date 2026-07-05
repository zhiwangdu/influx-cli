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
	FormatTSI  Format = "tsi"
)

type Options struct {
	Format            Format
	Recursive         bool
	KeySampleLimit    int
	BlockSampleLimit  int
	QueryRange        TimeRange
	QueryKeys         []string
	QueryMeasurements []string
	QueryTags         []TagFilter
}

type TimeRange struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
	Set bool  `json:"set"`
}

type TagFilter struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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
	Files      []FileReport       `json:"files"`
	Summary    Summary            `json:"summary"`
	DecodePath *DecodePathSummary `json:"decode_path,omitempty"`
	Notices    []string           `json:"notices,omitempty"`
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
	Path               string             `json:"path"`
	Format             Format             `json:"format"`
	SizeBytes          int64              `json:"size_bytes"`
	ModTime            time.Time          `json:"mod_time"`
	MinTime            int64              `json:"min_time"`
	MaxTime            int64              `json:"max_time"`
	MinKey             string             `json:"min_key,omitempty"`
	MaxKey             string             `json:"max_key,omitempty"`
	KeyCount           int                `json:"key_count"`
	KeySamples         []string           `json:"key_samples,omitempty"`
	BlockCount         int                `json:"block_count"`
	BlocksByType       map[string]int     `json:"blocks_by_type,omitempty"`
	Blocks             []BlockReport      `json:"blocks,omitempty"`
	QueryOverlapsFile  bool               `json:"query_overlaps_file,omitempty"`
	QueryOverlapBlocks int                `json:"query_overlap_blocks,omitempty"`
	DecodePath         *DecodePathSummary `json:"decode_path,omitempty"`
	Tombstones         TombstoneSummary   `json:"tombstones,omitempty"`
	SeriesID           SeriesIDSummary    `json:"series_id,omitempty"`
	Index              *IndexSummary      `json:"index,omitempty"`
	Extra              map[string]string  `json:"extra,omitempty"`
	Notices            []string           `json:"notices,omitempty"`
}

type SeriesIDSummary struct {
	Min   uint64 `json:"min,omitempty"`
	Max   uint64 `json:"max,omitempty"`
	Count int64  `json:"count,omitempty"`
}

type TombstoneSummary struct {
	Exists             bool                   `json:"exists"`
	Path               string                 `json:"path,omitempty"`
	SizeBytes          int64                  `json:"size_bytes,omitempty"`
	Version            string                 `json:"version,omitempty"`
	RangeCount         int                    `json:"range_count,omitempty"`
	KeyCount           int                    `json:"key_count,omitempty"`
	KeySamples         []string               `json:"key_samples,omitempty"`
	MinTime            int64                  `json:"min_time,omitempty"`
	MaxTime            int64                  `json:"max_time,omitempty"`
	QueryOverlapRanges int                    `json:"query_overlap_ranges,omitempty"`
	AffectedBlocks     int                    `json:"affected_blocks,omitempty"`
	RangeSamples       []TombstoneRangeReport `json:"range_samples,omitempty"`
}

type TombstoneRangeReport struct {
	Key            string `json:"key"`
	MinTime        int64  `json:"min_time"`
	MaxTime        int64  `json:"max_time"`
	QueryOverlaps  bool   `json:"query_overlaps,omitempty"`
	AffectedBlocks int    `json:"affected_blocks,omitempty"`
}

type IndexSummary struct {
	Type                    string                   `json:"type,omitempty"`
	MeasurementCount        int                      `json:"measurement_count,omitempty"`
	DeletedMeasurementCount int                      `json:"deleted_measurement_count,omitempty"`
	SeriesRefs              int64                    `json:"series_refs,omitempty"`
	TagKeyCount             int                      `json:"tag_key_count,omitempty"`
	DeletedTagKeyCount      int                      `json:"deleted_tag_key_count,omitempty"`
	TagValueCount           int                      `json:"tag_value_count,omitempty"`
	DeletedTagValueCount    int                      `json:"deleted_tag_value_count,omitempty"`
	SeriesIDSetBytes        int64                    `json:"series_id_set_bytes,omitempty"`
	TombstoneSeriesSetBytes int64                    `json:"tombstone_series_set_bytes,omitempty"`
	SeriesSketchBytes       int64                    `json:"series_sketch_bytes,omitempty"`
	TombstoneSketchBytes    int64                    `json:"tombstone_sketch_bytes,omitempty"`
	MeasurementSamples      []IndexMeasurementReport `json:"measurement_samples,omitempty"`
	Query                   *IndexQuerySummary       `json:"query,omitempty"`
}

type IndexMeasurementReport struct {
	Name                 string `json:"name"`
	Deleted              bool   `json:"deleted,omitempty"`
	SeriesCount          uint64 `json:"series_count,omitempty"`
	TagKeyCount          int    `json:"tag_key_count,omitempty"`
	DeletedTagKeyCount   int    `json:"deleted_tag_key_count,omitempty"`
	TagValueCount        int    `json:"tag_value_count,omitempty"`
	DeletedTagValueCount int    `json:"deleted_tag_value_count,omitempty"`
}

type IndexQuerySummary struct {
	MeasurementFilterApplied bool                          `json:"measurement_filter_applied,omitempty"`
	TagFilterApplied         bool                          `json:"tag_filter_applied,omitempty"`
	QueryMeasurements        []string                      `json:"query_measurements,omitempty"`
	QueryTags                []TagFilter                   `json:"query_tags,omitempty"`
	MatchedMeasurements      []string                      `json:"matched_measurements,omitempty"`
	MissingMeasurements      []string                      `json:"missing_measurements,omitempty"`
	MatchedTags              []TagFilter                   `json:"matched_tags,omitempty"`
	MissingTags              []TagFilter                   `json:"missing_tags,omitempty"`
	CandidateMeasurements    int                           `json:"candidate_measurements,omitempty"`
	SeriesRefs               int64                         `json:"series_refs,omitempty"`
	TagKeyCount              int                           `json:"tag_key_count,omitempty"`
	TagValueCount            int                           `json:"tag_value_count,omitempty"`
	MeasurementSamples       []IndexQueryMeasurementReport `json:"measurement_samples,omitempty"`
}

type IndexQueryMeasurementReport struct {
	Name        string                `json:"name"`
	Deleted     bool                  `json:"deleted,omitempty"`
	SeriesCount uint64                `json:"series_count,omitempty"`
	Tags        []IndexQueryTagReport `json:"tags,omitempty"`
}

type IndexQueryTagReport struct {
	Key     string                     `json:"key"`
	Deleted bool                       `json:"deleted,omitempty"`
	Values  []IndexQueryTagValueReport `json:"values,omitempty"`
}

type IndexQueryTagValueReport struct {
	Value       string `json:"value"`
	Deleted     bool   `json:"deleted,omitempty"`
	SeriesCount uint64 `json:"series_count,omitempty"`
}

type DecodePathSummary struct {
	Mode                    string                    `json:"mode,omitempty"`
	QueryRange              TimeRange                 `json:"query_range,omitempty"`
	QueryKeys               []string                  `json:"query_keys,omitempty"`
	MatchedKeys             []string                  `json:"matched_keys,omitempty"`
	MissingKeys             []string                  `json:"missing_keys,omitempty"`
	KeyFilterApplied        bool                      `json:"key_filter_applied,omitempty"`
	CursorSeekTime          int64                     `json:"cursor_seek_time,omitempty"`
	BaselineDecodeBlocks    int                       `json:"baseline_decode_blocks,omitempty"`
	OptimizedDecodeBlocks   int                       `json:"optimized_decode_blocks,omitempty"`
	LocationBlocks          int                       `json:"location_blocks,omitempty"`
	FilteredDecodeBlocks    int                       `json:"filtered_decode_blocks,omitempty"`
	SavedDecodeBlocks       int                       `json:"saved_decode_blocks,omitempty"`
	SkippedByKeyBlocks      int                       `json:"skipped_by_key_blocks,omitempty"`
	SkippedBeforeSeekBlocks int                       `json:"skipped_before_seek_blocks,omitempty"`
	SkippedAfterRangeBlocks int                       `json:"skipped_after_range_blocks,omitempty"`
	FullyTombstonedBlocks   int                       `json:"fully_tombstoned_blocks,omitempty"`
	CursorWindowCount       int                       `json:"cursor_window_count,omitempty"`
	MergeWindowCount        int                       `json:"merge_window_count,omitempty"`
	MergeWindowBlocks       int                       `json:"merge_window_blocks,omitempty"`
	MergeWindowKeys         int                       `json:"merge_window_keys,omitempty"`
	Amplification           float64                   `json:"amplification,omitempty"`
	LocationBlocksByType    map[string]int            `json:"location_blocks_by_type,omitempty"`
	DecodeBlocksByType      map[string]int            `json:"decode_blocks_by_type,omitempty"`
	Samples                 []DecodePathBlockDecision `json:"samples,omitempty"`
	CursorWindows           []DecodePathCursorWindow  `json:"cursor_windows,omitempty"`
	Recommendations         []string                  `json:"recommendations,omitempty"`
}

type DecodePathBlockDecision struct {
	Path              string `json:"path,omitempty"`
	Key               string `json:"key,omitempty"`
	MinTime           int64  `json:"min_time"`
	MaxTime           int64  `json:"max_time"`
	Type              string `json:"type"`
	LocationCandidate bool   `json:"location_candidate,omitempty"`
	Decoded           bool   `json:"decoded,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

type DecodePathCursorWindow struct {
	Key             string   `json:"key"`
	Files           []string `json:"files,omitempty"`
	MinTime         int64    `json:"min_time"`
	MaxTime         int64    `json:"max_time"`
	LocationBlocks  int      `json:"location_blocks"`
	DecodedBlocks   int      `json:"decoded_blocks"`
	SavedBlocks     int      `json:"saved_blocks,omitempty"`
	RequiresMerge   bool     `json:"requires_merge,omitempty"`
	Reason          string   `json:"reason,omitempty"`
	FirstBlockIndex int      `json:"first_block_index,omitempty"`
}

type BlockReport struct {
	Key             string `json:"key,omitempty"`
	SeriesID        uint64 `json:"series_id,omitempty"`
	MinTime         int64  `json:"min_time"`
	MaxTime         int64  `json:"max_time"`
	Type            string `json:"type"`
	Offset          int64  `json:"offset,omitempty"`
	SizeBytes       uint32 `json:"size_bytes,omitempty"`
	ColumnCount     int    `json:"column_count,omitempty"`
	SegmentCount    int    `json:"segment_count,omitempty"`
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
			if file.Tombstones.RangeCount > 0 {
				tombstone = fmt.Sprintf("yes (%d bytes, %d ranges", file.Tombstones.SizeBytes, file.Tombstones.RangeCount)
				if file.Tombstones.AffectedBlocks > 0 {
					tombstone += fmt.Sprintf(", %d blocks", file.Tombstones.AffectedBlocks)
				}
				tombstone += ")"
			}
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
