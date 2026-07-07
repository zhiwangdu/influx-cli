package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

type Format string

const (
	FormatAuto              Format = "auto"
	FormatTSM               Format = "tsm"
	FormatWAL               Format = "wal"
	FormatTSSP              Format = "tssp"
	FormatTSSPDetachedIndex Format = "tssp-metaindex"
	FormatTSI               Format = "tsi"
	FormatTSILog            Format = "tsi-log"
	FormatSeriesFile        Format = "series-file"
	FormatFieldsIndex       Format = "fields-index"
	FormatMergeset          Format = "mergeset"
	FormatOpenGeminiMeta    Format = "opengemini-meta"
	FormatOpenGeminiPKMeta  Format = "opengemini-pk-meta"
	FormatOpenGeminiPKIndex Format = "opengemini-pk-index"
	FormatOpenGeminiBloom   Format = "opengemini-bloom-filter"
	FormatOpenGeminiText    Format = "opengemini-text-index"
)

type Options struct {
	Format            Format
	Recursive         bool
	KeySampleLimit    int
	BlockSampleLimit  int
	QueryRange        TimeRange
	QueryKeys         []string
	QuerySeriesIDs    []uint64
	QueryMetaIndexIDs []uint64
	QueryColumns      []string
	QueryFields       []FieldFilter
	QueryAnyFields    []FieldFilter
	QueryNoneFields   []FieldFilter
	QueryMeasurements []string
	QueryTags         []TagFilter
	CursorDescending  bool
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

type FieldFilter struct {
	Key   string `json:"key"`
	Op    string `json:"op,omitempty"`
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
	Path               string                 `json:"path"`
	Format             Format                 `json:"format"`
	SizeBytes          int64                  `json:"size_bytes"`
	ModTime            time.Time              `json:"mod_time"`
	MinTime            int64                  `json:"min_time"`
	MaxTime            int64                  `json:"max_time"`
	MinKey             string                 `json:"min_key,omitempty"`
	MaxKey             string                 `json:"max_key,omitempty"`
	KeyCount           int                    `json:"key_count"`
	KeySamples         []string               `json:"key_samples,omitempty"`
	BlockCount         int                    `json:"block_count"`
	BlocksByType       map[string]int         `json:"blocks_by_type,omitempty"`
	Blocks             []BlockReport          `json:"blocks,omitempty"`
	QueryOverlapsFile  bool                   `json:"query_overlaps_file,omitempty"`
	QueryOverlapBlocks int                    `json:"query_overlap_blocks,omitempty"`
	DecodePath         *DecodePathSummary     `json:"decode_path,omitempty"`
	Tombstones         TombstoneSummary       `json:"tombstones,omitempty"`
	SeriesID           SeriesIDSummary        `json:"series_id,omitempty"`
	MetaIndexID        SeriesIDSummary        `json:"meta_index_id,omitempty"`
	Index              *IndexSummary          `json:"index,omitempty"`
	Fields             *FieldIndexSummary     `json:"fields,omitempty"`
	PrimaryKey         *PrimaryKeySummary     `json:"primary_key,omitempty"`
	SecondaryIndex     *SecondaryIndexSummary `json:"secondary_index,omitempty"`
	Extra              map[string]string      `json:"extra,omitempty"`
	Notices            []string               `json:"notices,omitempty"`
}

type PrimaryKeySummary struct {
	Type                    string                   `json:"type"`
	Version                 uint32                   `json:"version"`
	Schema                  []PrimaryKeyColumnReport `json:"schema,omitempty"`
	ColumnCount             int                      `json:"column_count"`
	TimeClusterLocation     int                      `json:"time_cluster_location"`
	MetaBlockCount          int                      `json:"meta_block_count"`
	RowCount                uint64                   `json:"row_count"`
	DataSizeBytes           int64                    `json:"data_size_bytes"`
	ValidDataBytes          int64                    `json:"valid_data_bytes"`
	DataInline              bool                     `json:"data_inline"`
	DataFilePresent         bool                     `json:"data_file_present"`
	DataFileSizeBytes       int64                    `json:"data_file_size_bytes"`
	CRCMismatches           int                      `json:"crc_mismatches"`
	DataOutOfBoundsBlocks   int                      `json:"data_out_of_bounds_blocks"`
	ColumnOutOfBoundsBlocks int                      `json:"column_out_of_bounds_blocks"`
	ColumnUnorderedBlocks   int                      `json:"column_unordered_blocks"`
	BlockIDRangeSet         bool                     `json:"block_id_range_set"`
	MinBlockID              uint64                   `json:"min_block_id"`
	MaxBlockID              uint64                   `json:"max_block_id"`
	PublicInfoSizeBytes     int64                    `json:"public_info_size_bytes"`
	ValidMetaBytes          int64                    `json:"valid_meta_bytes"`
	TrailingMetaBytes       int64                    `json:"trailing_meta_bytes"`
	MetaRecordSizeBytes     int                      `json:"meta_record_size_bytes"`
	ColumnOffsetCount       int                      `json:"column_offset_count"`
}

type PrimaryKeyColumnReport struct {
	Name            string `json:"name"`
	Type            string `json:"type"`
	DataOffset      int64  `json:"data_offset,omitempty"`
	DataSizeBytes   int64  `json:"data_size_bytes,omitempty"`
	DataOutOfBounds bool   `json:"data_out_of_bounds,omitempty"`
}

type SecondaryIndexSummary struct {
	Type                   string `json:"type,omitempty"`
	Layout                 string `json:"layout,omitempty"`
	Field                  string `json:"field,omitempty"`
	Version                uint32 `json:"version,omitempty"`
	BlockCount             int64  `json:"block_count,omitempty"`
	GroupCount             int64  `json:"group_count,omitempty"`
	PieceCount             int64  `json:"piece_count,omitempty"`
	PartCount              int64  `json:"part_count,omitempty"`
	ItemCount              int64  `json:"item_count,omitempty"`
	DocumentCount          int64  `json:"document_count,omitempty"`
	TermCount              int64  `json:"term_count,omitempty"`
	DictionaryCount        int64  `json:"dictionary_count,omitempty"`
	DictionaryVersionCount int64  `json:"dictionary_version_count,omitempty"`
	PositionCount          int64  `json:"position_count,omitempty"`
	SIDGroupCount          int64  `json:"sid_group_count,omitempty"`
	DocumentIDCount        int64  `json:"document_id_count,omitempty"`
	PayloadSizeBytes       int64  `json:"payload_size_bytes,omitempty"`
	BlockSizeBytes         int64  `json:"block_size_bytes,omitempty"`
	PieceSizeBytes         int64  `json:"piece_size_bytes,omitempty"`
	GroupSizeBytes         int64  `json:"group_size_bytes,omitempty"`
	DataSizeBytes          int64  `json:"data_size_bytes,omitempty"`
	HeaderSizeBytes        int64  `json:"header_size_bytes,omitempty"`
	PartHeaderSizeBytes    int64  `json:"part_header_size_bytes,omitempty"`
	ValidBytes             int64  `json:"valid_bytes,omitempty"`
	TrailingBytes          int64  `json:"trailing_bytes,omitempty"`
	HeaderOutOfBoundsParts int    `json:"header_out_of_bounds_parts,omitempty"`
	DataOutOfBoundsBlocks  int    `json:"data_out_of_bounds_blocks,omitempty"`
	InvalidOffsetBlocks    int    `json:"invalid_offset_blocks,omitempty"`
	InvalidSizeBlocks      int    `json:"invalid_size_blocks,omitempty"`
	SegmentRangeOverflows  int    `json:"segment_range_overflows,omitempty"`
	CRCMismatches          int    `json:"crc_mismatches,omitempty"`
}

type FieldIndexSummary struct {
	Type               string                        `json:"type,omitempty"`
	MeasurementCount   int                           `json:"measurement_count,omitempty"`
	FieldCount         int                           `json:"field_count,omitempty"`
	FieldsByType       map[string]int                `json:"fields_by_type,omitempty"`
	ChangeSetCount     int                           `json:"change_set_count,omitempty"`
	ChangeCount        int                           `json:"change_count,omitempty"`
	AddFieldChanges    int                           `json:"add_field_changes,omitempty"`
	DeleteMeasurements int                           `json:"delete_measurements,omitempty"`
	MeasurementSamples []FieldIndexMeasurementReport `json:"measurement_samples,omitempty"`
}

type FieldIndexMeasurementReport struct {
	Name       string                  `json:"name"`
	FieldCount int                     `json:"field_count"`
	Fields     []FieldIndexFieldReport `json:"fields,omitempty"`
}

type FieldIndexFieldReport struct {
	Name string `json:"name"`
	Type string `json:"type"`
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
	Type                            string                   `json:"type,omitempty"`
	MeasurementCount                int                      `json:"measurement_count,omitempty"`
	DeletedMeasurementCount         int                      `json:"deleted_measurement_count,omitempty"`
	SeriesRefs                      int64                    `json:"series_refs,omitempty"`
	TagKeyCount                     int                      `json:"tag_key_count,omitempty"`
	DeletedTagKeyCount              int                      `json:"deleted_tag_key_count,omitempty"`
	TagValueCount                   int                      `json:"tag_value_count,omitempty"`
	DeletedTagValueCount            int                      `json:"deleted_tag_value_count,omitempty"`
	SeriesIDSetBytes                int64                    `json:"series_id_set_bytes,omitempty"`
	SeriesIDSetCardinality          int64                    `json:"series_id_set_cardinality,omitempty"`
	TombstoneSeriesSetBytes         int64                    `json:"tombstone_series_set_bytes,omitempty"`
	TombstoneSeriesIDSetCardinality int64                    `json:"tombstone_series_id_set_cardinality,omitempty"`
	SeriesSketchBytes               int64                    `json:"series_sketch_bytes,omitempty"`
	TombstoneSketchBytes            int64                    `json:"tombstone_sketch_bytes,omitempty"`
	MeasurementSamples              []IndexMeasurementReport `json:"measurement_samples,omitempty"`
	Query                           *IndexQuerySummary       `json:"query,omitempty"`
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
	Mode                         string                    `json:"mode,omitempty"`
	QueryRange                   TimeRange                 `json:"query_range,omitempty"`
	QueryKeys                    []string                  `json:"query_keys,omitempty"`
	QuerySeriesIDs               []uint64                  `json:"query_series_ids,omitempty"`
	QueryColumns                 []string                  `json:"query_columns,omitempty"`
	MatchedColumns               []string                  `json:"matched_columns,omitempty"`
	MissingColumns               []string                  `json:"missing_columns,omitempty"`
	QueryFields                  []FieldFilter             `json:"query_fields,omitempty"`
	MatchedFields                []FieldFilter             `json:"matched_fields,omitempty"`
	MissingFields                []FieldFilter             `json:"missing_fields,omitempty"`
	QueryAnyFields               []FieldFilter             `json:"query_any_fields,omitempty"`
	MatchedAnyFields             []FieldFilter             `json:"matched_any_fields,omitempty"`
	MissingAnyFields             []FieldFilter             `json:"missing_any_fields,omitempty"`
	QueryNoneFields              []FieldFilter             `json:"query_none_fields,omitempty"`
	MatchedNoneFields            []FieldFilter             `json:"matched_none_fields,omitempty"`
	MissingNoneFields            []FieldFilter             `json:"missing_none_fields,omitempty"`
	MatchedKeys                  []string                  `json:"matched_keys,omitempty"`
	MissingKeys                  []string                  `json:"missing_keys,omitempty"`
	MatchedSeriesIDs             []uint64                  `json:"matched_series_ids,omitempty"`
	MissingSeriesIDs             []uint64                  `json:"missing_series_ids,omitempty"`
	QueryMetaIndexIDs            []uint64                  `json:"query_meta_index_ids,omitempty"`
	MatchedMetaIndexIDs          []uint64                  `json:"matched_meta_index_ids,omitempty"`
	MissingMetaIndexIDs          []uint64                  `json:"missing_meta_index_ids,omitempty"`
	KeyFilterApplied             bool                      `json:"key_filter_applied,omitempty"`
	CursorSeekTime               int64                     `json:"cursor_seek_time,omitempty"`
	BaselineDecodeBlocks         int                       `json:"baseline_decode_blocks,omitempty"`
	OptimizedDecodeBlocks        int                       `json:"optimized_decode_blocks,omitempty"`
	BaselineDecodeBytes          int64                     `json:"baseline_decode_bytes,omitempty"`
	OptimizedDecodeBytes         int64                     `json:"optimized_decode_bytes,omitempty"`
	SavedDecodeBytes             int64                     `json:"saved_decode_bytes,omitempty"`
	BaselineDecodeValues         int                       `json:"baseline_decode_values,omitempty"`
	OptimizedDecodeValues        int                       `json:"optimized_decode_values,omitempty"`
	SavedDecodeValues            int                       `json:"saved_decode_values,omitempty"`
	BaselineReadSegments         int                       `json:"baseline_read_segments,omitempty"`
	OptimizedReadSegments        int                       `json:"optimized_read_segments,omitempty"`
	SavedReadSegments            int                       `json:"saved_read_segments,omitempty"`
	BaselineOutputValues         int                       `json:"baseline_output_values,omitempty"`
	OptimizedOutputValues        int                       `json:"optimized_output_values,omitempty"`
	DeduplicatedOutputValues     int                       `json:"deduplicated_output_values,omitempty"`
	DuplicateOutputValues        int                       `json:"duplicate_output_values,omitempty"`
	BaselineValueOutputPoints    int                       `json:"baseline_value_output_points,omitempty"`
	OptimizedValueOutputPoints   int                       `json:"optimized_value_output_points,omitempty"`
	ComparedValueOutputPoints    int                       `json:"compared_value_output_points,omitempty"`
	ValueOutputMismatches        int                       `json:"value_output_mismatches,omitempty"`
	ValueOutputUnavailableBlocks int                       `json:"value_output_unavailable_blocks,omitempty"`
	DataBlockProbeBlocks         int                       `json:"data_block_probe_blocks,omitempty"`
	DataBlockProbeBytes          int64                     `json:"data_block_probe_bytes,omitempty"`
	DataBlockProbeFailures       int                       `json:"data_block_probe_failures,omitempty"`
	DataBlockProbeCRCMismatches  int                       `json:"data_block_probe_crc_mismatches,omitempty"`
	DataBlockProbeValueBlocks    int                       `json:"data_block_probe_value_blocks,omitempty"`
	DataBlockProbeValueUnknowns  int                       `json:"data_block_probe_value_unknowns,omitempty"`
	DataBlockProbeNullValues     int                       `json:"data_block_probe_null_values,omitempty"`
	DataBlockProbeRecordSamples  int                       `json:"data_block_probe_record_samples,omitempty"`
	DataBlockProbeFilterRows     int                       `json:"data_block_probe_filter_rows,omitempty"`
	DataBlockProbeFilterMatches  int                       `json:"data_block_probe_filter_matches,omitempty"`
	DataBlockProbeFilterRejects  int                       `json:"data_block_probe_filter_rejects,omitempty"`
	DataBlockProbeFilterEvals    int                       `json:"data_block_probe_filter_evaluations,omitempty"`
	DataBlockProbeRequiredEvals  int                       `json:"data_block_probe_required_filter_evaluations,omitempty"`
	DataBlockProbeAnyEvals       int                       `json:"data_block_probe_any_filter_evaluations,omitempty"`
	DataBlockProbeNoneEvals      int                       `json:"data_block_probe_none_filter_evaluations,omitempty"`
	DataBlockProbeFilterEvalHits int                       `json:"data_block_probe_filter_evaluation_matches,omitempty"`
	DataBlockProbeFilterEvalMiss int                       `json:"data_block_probe_filter_evaluation_misses,omitempty"`
	DataBlockProbeFilterSkips    int                       `json:"data_block_probe_filter_short_circuit_skips,omitempty"`
	DataBlockProbeRequiredHits   int                       `json:"data_block_probe_required_filter_evaluation_matches,omitempty"`
	DataBlockProbeRequiredMiss   int                       `json:"data_block_probe_required_filter_evaluation_misses,omitempty"`
	DataBlockProbeRequiredSkips  int                       `json:"data_block_probe_required_filter_short_circuit_skips,omitempty"`
	DataBlockProbeAnyHits        int                       `json:"data_block_probe_any_filter_evaluation_matches,omitempty"`
	DataBlockProbeAnyMiss        int                       `json:"data_block_probe_any_filter_evaluation_misses,omitempty"`
	DataBlockProbeAnySkips       int                       `json:"data_block_probe_any_filter_short_circuit_skips,omitempty"`
	DataBlockProbeNoneHits       int                       `json:"data_block_probe_none_filter_evaluation_matches,omitempty"`
	DataBlockProbeNoneMiss       int                       `json:"data_block_probe_none_filter_evaluation_misses,omitempty"`
	DataBlockProbeNoneSkips      int                       `json:"data_block_probe_none_filter_short_circuit_skips,omitempty"`
	DataBlockProbeFilterOps      map[string]int            `json:"data_block_probe_filter_operator_evaluations,omitempty"`
	BaselineCursorOutputPoints   int                       `json:"baseline_cursor_output_points,omitempty"`
	OptimizedCursorOutputPoints  int                       `json:"optimized_cursor_output_points,omitempty"`
	BaselineCursorReadCalls      int                       `json:"baseline_cursor_read_calls,omitempty"`
	OptimizedCursorReadCalls     int                       `json:"optimized_cursor_read_calls,omitempty"`
	TableSearchSeekCalls         int                       `json:"table_search_seek_calls,omitempty"`
	TableSearchHeapCandidates    int                       `json:"table_search_heap_candidates,omitempty"`
	TableSearchHeapInserts       int                       `json:"table_search_heap_inserts,omitempty"`
	TableSearchHeapPops          int                       `json:"table_search_heap_pops,omitempty"`
	TableSearchCursorAdvances    int                       `json:"table_search_cursor_advances,omitempty"`
	TableSearchCursorExhaustions int                       `json:"table_search_cursor_exhaustions,omitempty"`
	TableSearchOutputValues      int                       `json:"table_search_output_values,omitempty"`
	TableSearchExactMisses       int                       `json:"table_search_exact_misses,omitempty"`
	BaselineReadAtCalls          int                       `json:"baseline_read_at_calls,omitempty"`
	OptimizedReadAtCalls         int                       `json:"optimized_read_at_calls,omitempty"`
	SavedReadAtCalls             int                       `json:"saved_read_at_calls,omitempty"`
	IteratorCostFiles            int                       `json:"iterator_cost_files,omitempty"`
	IteratorCostBlocks           int                       `json:"iterator_cost_blocks,omitempty"`
	IteratorCostBytes            int64                     `json:"iterator_cost_bytes,omitempty"`
	LocationBlocks               int                       `json:"location_blocks,omitempty"`
	FilteredDecodeBlocks         int                       `json:"filtered_decode_blocks,omitempty"`
	SavedDecodeBlocks            int                       `json:"saved_decode_blocks,omitempty"`
	SkippedByKeyBlocks           int                       `json:"skipped_by_key_blocks,omitempty"`
	SkippedByProjectionBlocks    int                       `json:"skipped_by_projection_blocks,omitempty"`
	SkippedBeforeSeekBlocks      int                       `json:"skipped_before_seek_blocks,omitempty"`
	SkippedAfterRangeBlocks      int                       `json:"skipped_after_range_blocks,omitempty"`
	FullyTombstonedBlocks        int                       `json:"fully_tombstoned_blocks,omitempty"`
	CursorWindowCount            int                       `json:"cursor_window_count,omitempty"`
	MergeWindowCount             int                       `json:"merge_window_count,omitempty"`
	MergeWindowBlocks            int                       `json:"merge_window_blocks,omitempty"`
	MergeWindowKeys              int                       `json:"merge_window_keys,omitempty"`
	Amplification                float64                   `json:"amplification,omitempty"`
	LocationBlocksByType         map[string]int            `json:"location_blocks_by_type,omitempty"`
	DecodeBlocksByType           map[string]int            `json:"decode_blocks_by_type,omitempty"`
	Samples                      []DecodePathBlockDecision `json:"samples,omitempty"`
	CursorWindows                []DecodePathCursorWindow  `json:"cursor_windows,omitempty"`
	CursorOutputSamples          []DecodePathCursorOutput  `json:"cursor_output_samples,omitempty"`
	CursorFinalOutputSamples     []DecodePathCursorOutput  `json:"cursor_final_output_samples,omitempty"`
	CursorExecutionSamples       []DecodePathCursorStep    `json:"cursor_execution_samples,omitempty"`
	Recommendations              []string                  `json:"recommendations,omitempty"`
	mergesetSeekResults          map[string]mergesetSeekResult
	mergesetScanItems            [][]byte
	mergesetEvictedCursorWindows int
}

type DecodePathBlockDecision struct {
	Path                  string                  `json:"path,omitempty"`
	Key                   string                  `json:"key,omitempty"`
	SeriesID              uint64                  `json:"series_id,omitempty"`
	MetaIndexID           uint64                  `json:"meta_index_id,omitempty"`
	MinTime               int64                   `json:"min_time"`
	MaxTime               int64                   `json:"max_time"`
	Type                  string                  `json:"type"`
	SizeBytes             uint32                  `json:"size_bytes,omitempty"`
	ValueCount            int                     `json:"value_count,omitempty"`
	SegmentCount          int                     `json:"segment_count,omitempty"`
	OutputValues          int                     `json:"output_values,omitempty"`
	OutputSegments        int                     `json:"output_segments,omitempty"`
	ValueOutputPoints     int                     `json:"value_output_points,omitempty"`
	ValueOutputAvailable  bool                    `json:"value_output_available,omitempty"`
	BaselineReadAtCalls   int                     `json:"baseline_read_at_calls,omitempty"`
	OptimizedReadAtCalls  int                     `json:"optimized_read_at_calls,omitempty"`
	LocationCandidate     bool                    `json:"location_candidate,omitempty"`
	Decoded               bool                    `json:"decoded,omitempty"`
	Reason                string                  `json:"reason,omitempty"`
	OptimizedReadAtRanges []DecodePathReadAtRange `json:"optimized_read_at_ranges,omitempty"`
}

type DecodePathReadAtRange struct {
	Segment   int    `json:"segment"`
	Column    string `json:"column,omitempty"`
	MinTime   int64  `json:"min_time"`
	MaxTime   int64  `json:"max_time"`
	Offset    int64  `json:"offset"`
	SizeBytes uint32 `json:"size_bytes"`
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

type DecodePathCursorOutput struct {
	Key            string               `json:"key"`
	Time           int64                `json:"time"`
	Type           string               `json:"type"`
	File           string               `json:"file,omitempty"`
	MergeFiles     DecodePathStringList `json:"merge_files,omitempty"`
	BaselineValue  string               `json:"baseline_value,omitempty"`
	OptimizedValue string               `json:"optimized_value,omitempty"`
	Matches        bool                 `json:"matches"`
	RequiresDedup  bool                 `json:"requires_dedup,omitempty"`
	RequiresMerge  bool                 `json:"requires_merge,omitempty"`
}

type DecodePathCursorStep struct {
	Step                int    `json:"step"`
	Type                string `json:"type"`
	Action              string `json:"action"`
	Key                 string `json:"key"`
	CandidateValue      string `json:"candidate_value,omitempty"`
	File                string `json:"file,omitempty"`
	HeapSizeBefore      int    `json:"heap_size_before"`
	HeapSizeAfterPop    int    `json:"heap_size_after_pop"`
	HeapSizeAfterAction int    `json:"heap_size_after_action"`
	CursorIndexBefore   int    `json:"cursor_index_before"`
	CursorIndexAfter    int    `json:"cursor_index_after"`
	CursorAdvanced      bool   `json:"cursor_advanced"`
	CursorExhausted     bool   `json:"cursor_exhausted"`
}

type DecodePathStringList string

func newDecodePathStringList(values []string) DecodePathStringList {
	if len(values) == 0 {
		return ""
	}
	data, err := json.Marshal(values)
	if err != nil {
		return ""
	}
	return DecodePathStringList(data)
}

func (l DecodePathStringList) MarshalJSON() ([]byte, error) {
	if l == "" {
		return json.Marshal([]string(nil))
	}
	return []byte(l), nil
}

func (l *DecodePathStringList) UnmarshalJSON(data []byte) error {
	var values []string
	if err := json.Unmarshal(data, &values); err == nil {
		*l = newDecodePathStringList(values)
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*l = newDecodePathStringList([]string{value})
	return nil
}

type BlockReport struct {
	Key             string `json:"key,omitempty"`
	SeriesID        uint64 `json:"series_id,omitempty"`
	MetaIndexID     uint64 `json:"meta_index_id,omitempty"`
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
		"decode_path",
		"advice",
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
			decodePathText(file.DecodePath),
			joinSamples(decodePathRecommendations(file.DecodePath)),
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

func decodePathText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	parts := make([]string, 0, 10)
	if summary.Mode != "" {
		parts = append(parts, summary.Mode)
	}
	if summary.BaselineDecodeBlocks > 0 || summary.OptimizedDecodeBlocks > 0 || summary.SavedDecodeBlocks > 0 {
		parts = append(parts, fmt.Sprintf("blocks %d->%d", summary.BaselineDecodeBlocks, summary.OptimizedDecodeBlocks))
	}
	if summary.SavedDecodeBytes > 0 {
		parts = append(parts, fmt.Sprintf("saved_bytes %d", summary.SavedDecodeBytes))
	}
	if summary.BaselineReadSegments > 0 || summary.OptimizedReadSegments > 0 || summary.SavedReadSegments > 0 {
		parts = append(parts, fmt.Sprintf("segments %d->%d", summary.BaselineReadSegments, summary.OptimizedReadSegments))
	}
	if summary.BaselineCursorReadCalls > 0 || summary.OptimizedCursorReadCalls > 0 {
		parts = append(parts, fmt.Sprintf("cursor_reads %d->%d", summary.BaselineCursorReadCalls, summary.OptimizedCursorReadCalls))
	}
	if summary.BaselineReadAtCalls > 0 || summary.OptimizedReadAtCalls > 0 {
		parts = append(parts, fmt.Sprintf("read_at calls %d->%d", summary.BaselineReadAtCalls, summary.OptimizedReadAtCalls))
	}
	if summary.IteratorCostFiles > 0 || summary.IteratorCostBlocks > 0 || summary.IteratorCostBytes > 0 {
		parts = append(parts, fmt.Sprintf("iterator_cost files=%d blocks=%d bytes=%d", summary.IteratorCostFiles, summary.IteratorCostBlocks, summary.IteratorCostBytes))
	}
	if summary.TableSearchHeapInserts > 0 || summary.TableSearchHeapPops > 0 {
		parts = append(parts, fmt.Sprintf("table_search_heap inserts=%d pops=%d", summary.TableSearchHeapInserts, summary.TableSearchHeapPops))
	}
	if summary.TableSearchCursorAdvances > 0 || summary.TableSearchCursorExhaustions > 0 {
		parts = append(parts, fmt.Sprintf("table_search_cursor advances=%d exhaustions=%d", summary.TableSearchCursorAdvances, summary.TableSearchCursorExhaustions))
	}
	if summary.TableSearchSeekCalls > 0 || summary.TableSearchHeapCandidates > 0 || summary.TableSearchOutputValues > 0 || summary.TableSearchExactMisses > 0 {
		parts = append(parts, fmt.Sprintf("table_search seeks=%d candidates=%d outputs=%d exact_misses=%d", summary.TableSearchSeekCalls, summary.TableSearchHeapCandidates, summary.TableSearchOutputValues, summary.TableSearchExactMisses))
	}
	if summary.DeduplicatedOutputValues > 0 || summary.DuplicateOutputValues > 0 {
		parts = append(parts, fmt.Sprintf("dedup outputs=%d duplicates=%d", summary.DeduplicatedOutputValues, summary.DuplicateOutputValues))
	}
	if summary.DataBlockProbeFilterRows > 0 || summary.DataBlockProbeFilterMatches > 0 || summary.DataBlockProbeFilterRejects > 0 {
		parts = append(parts, fmt.Sprintf("field_filter rows=%d matches=%d rejects=%d evals=%d eval_matches=%d eval_misses=%d required=%d required_matches=%d required_misses=%d any=%d any_matches=%d any_misses=%d none=%d none_matches=%d none_misses=%d", summary.DataBlockProbeFilterRows, summary.DataBlockProbeFilterMatches, summary.DataBlockProbeFilterRejects, summary.DataBlockProbeFilterEvals, summary.DataBlockProbeFilterEvalHits, summary.DataBlockProbeFilterEvalMiss, summary.DataBlockProbeRequiredEvals, summary.DataBlockProbeRequiredHits, summary.DataBlockProbeRequiredMiss, summary.DataBlockProbeAnyEvals, summary.DataBlockProbeAnyHits, summary.DataBlockProbeAnyMiss, summary.DataBlockProbeNoneEvals, summary.DataBlockProbeNoneHits, summary.DataBlockProbeNoneMiss))
	}
	if summary.DataBlockProbeFilterSkips > 0 {
		parts = append(parts, fmt.Sprintf("field_filter_short_circuit skips=%d required=%d any=%d none=%d", summary.DataBlockProbeFilterSkips, summary.DataBlockProbeRequiredSkips, summary.DataBlockProbeAnySkips, summary.DataBlockProbeNoneSkips))
	}
	if summary.ValueOutputMismatches > 0 {
		parts = append(parts, fmt.Sprintf("mismatches %d", summary.ValueOutputMismatches))
	}
	return strings.Join(parts, ", ")
}

func decodePathRecommendations(summary *DecodePathSummary) []string {
	if summary == nil {
		return nil
	}
	return summary.Recommendations
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
