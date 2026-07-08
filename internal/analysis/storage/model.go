package storage

import (
	"encoding/json"
	"fmt"
	"sort"
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
	FileCount          int            `json:"file_count"`
	TotalSizeBytes     int64          `json:"total_size_bytes"`
	KeyCount           int            `json:"key_count"`
	BlockCount         int            `json:"block_count"`
	BlocksByType       map[string]int `json:"blocks_by_type,omitempty"`
	QueryOverlapFiles  int            `json:"query_overlap_files,omitempty"`
	QueryOverlapBlocks int            `json:"query_overlap_blocks,omitempty"`
	TombstoneFiles     int            `json:"tombstone_files,omitempty"`
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
	DataBlockProbeValidBlocks    int                       `json:"data_block_probe_valid_blocks,omitempty"`
	DataBlockProbeFailures       int                       `json:"data_block_probe_failures,omitempty"`
	DataBlockProbeCRCMismatches  int                       `json:"data_block_probe_crc_mismatches,omitempty"`
	DataBlockProbeShortBlocks    int                       `json:"data_block_probe_short_blocks,omitempty"`
	DataBlockProbeUnknownTypes   int                       `json:"data_block_probe_unknown_block_types,omitempty"`
	DataBlockProbeReadErrors     int                       `json:"data_block_probe_read_errors,omitempty"`
	DataBlockProbeFailureReasons map[string]int            `json:"data_block_probe_failure_reasons,omitempty"`
	DataBlockProbeRowCountBlocks int                       `json:"data_block_probe_row_count_blocks,omitempty"`
	DataBlockProbeRowUnknowns    int                       `json:"data_block_probe_row_count_unknowns,omitempty"`
	DataBlockProbeRowMismatches  int                       `json:"data_block_probe_row_count_mismatches,omitempty"`
	DataBlockProbeOutputPoints   int                       `json:"data_block_probe_output_points,omitempty"`
	DataBlockProbeTypes          map[string]int            `json:"data_block_probe_types,omitempty"`
	DataBlockProbeValueBlocks    int                       `json:"data_block_probe_value_blocks,omitempty"`
	DataBlockProbeValueUnknowns  int                       `json:"data_block_probe_value_unknowns,omitempty"`
	DataBlockProbeValueReasons   map[string]int            `json:"data_block_probe_value_unknown_reasons,omitempty"`
	DataBlockProbeNullValues     int                       `json:"data_block_probe_null_values,omitempty"`
	DataBlockProbeRecordSamples  int                       `json:"data_block_probe_record_samples,omitempty"`
	DataBlockProbeRangeRows      int                       `json:"data_block_probe_range_rows,omitempty"`
	DataBlockProbeRangeMatches   int                       `json:"data_block_probe_range_matches,omitempty"`
	DataBlockProbeRangeRejects   int                       `json:"data_block_probe_range_rejects,omitempty"`
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
	FilterExecutionSamples       []DecodePathCursorStep    `json:"filter_execution_samples,omitempty"`
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
		"details",
		"samples",
		"decode_path",
		"advice",
	})
	for _, file := range r.Files {
		table.AddRow(
			file.Path,
			string(file.Format),
			file.SizeBytes,
			FormatUnixNano(file.MinTime),
			FormatUnixNano(file.MaxTime),
			file.KeyCount,
			file.BlockCount,
			file.QueryOverlapBlocks,
			tombstoneText(file.Tombstones),
			fileDetailsText(file),
			joinSamples(file.KeySamples),
			decodePathText(file.DecodePath),
			joinSamples(decodePathRecommendations(file.DecodePath)),
		)
	}
	if r.DecodePath != nil {
		tombstone := ""
		if r.Summary.TombstoneFiles > 0 {
			tombstone = fmt.Sprintf("%d files", r.Summary.TombstoneFiles)
		}
		table.AddRow(
			"<file-set>",
			"file-set",
			r.Summary.TotalSizeBytes,
			"",
			"",
			r.Summary.KeyCount,
			r.Summary.BlockCount,
			r.Summary.QueryOverlapBlocks,
			tombstone,
			summaryDetailsText(r.Summary, len(r.Files), len(r.Notices)),
			"",
			decodePathText(r.DecodePath),
			joinSamples(decodePathRecommendations(r.DecodePath)),
		)
	}
	return result.Result{
		Kind:  result.KindTable,
		Table: table,
		Metadata: result.Metadata{
			RowCount: table.RowCount(),
			Notices:  append([]string(nil), r.Notices...),
			Source:   "storage-analyzer",
		},
	}
}

func tombstoneText(summary TombstoneSummary) string {
	if !summary.Exists {
		return ""
	}
	if summary.RangeCount == 0 {
		return fmt.Sprintf("yes (%d bytes)", summary.SizeBytes)
	}
	parts := []string{
		fmt.Sprintf("%d bytes", summary.SizeBytes),
		fmt.Sprintf("%d ranges", summary.RangeCount),
	}
	if summary.QueryOverlapRanges > 0 {
		parts = append(parts, fmt.Sprintf("query_ranges=%d", summary.QueryOverlapRanges))
	}
	if summary.AffectedBlocks > 0 {
		parts = append(parts, fmt.Sprintf("%d blocks", summary.AffectedBlocks))
	}
	return "yes (" + strings.Join(parts, ", ") + ")"
}

func fileDetailsText(file FileReport) string {
	parts := make([]string, 0, 5)
	if blocksByType := countMapText(file.BlocksByType); blocksByType != "" {
		parts = append(parts, "block_types "+blocksByType)
	}
	if series := seriesIDDetailsText("series_id", file.SeriesID); series != "" {
		parts = append(parts, series)
	}
	if metaIndex := seriesIDDetailsText("meta_index_id", file.MetaIndexID); metaIndex != "" {
		parts = append(parts, metaIndex)
	}
	if file.Index != nil {
		parts = append(parts, indexDetailsText(file.Index))
	}
	if file.Fields != nil {
		parts = append(parts, fieldIndexDetailsText(file.Fields))
	}
	if file.PrimaryKey != nil {
		parts = append(parts, primaryKeyDetailsText(file.PrimaryKey))
	}
	if file.SecondaryIndex != nil {
		parts = append(parts, secondaryIndexDetailsText(file.SecondaryIndex))
	}
	if len(file.Notices) > 0 {
		parts = append(parts, fmt.Sprintf("notices=%d", len(file.Notices)))
	}
	return strings.Join(nonEmptyStrings(parts), "; ")
}

func seriesIDDetailsText(label string, summary SeriesIDSummary) string {
	parts := make([]string, 0, 2)
	if summary.Count > 0 {
		parts = append(parts, fmt.Sprintf("count=%d", summary.Count))
	}
	if summary.Min > 0 || summary.Max > 0 {
		parts = append(parts, fmt.Sprintf("range=%d..%d", summary.Min, summary.Max))
	}
	if len(parts) == 0 {
		return ""
	}
	return label + " " + strings.Join(parts, " ")
}

func summaryDetailsText(summary Summary, fileCount, noticeCount int) string {
	parts := []string{fmt.Sprintf("files=%d", fileCount)}
	if summary.QueryOverlapFiles > 0 {
		parts = append(parts, fmt.Sprintf("query_files=%d", summary.QueryOverlapFiles))
	}
	if summary.TombstoneFiles > 0 {
		parts = append(parts, fmt.Sprintf("tombstone_files=%d", summary.TombstoneFiles))
	}
	if noticeCount > 0 {
		parts = append(parts, fmt.Sprintf("notices=%d", noticeCount))
	}
	if blocksByType := countMapText(summary.BlocksByType); blocksByType != "" {
		parts = append(parts, "block_types "+blocksByType)
	}
	return strings.Join(parts, "; ")
}

func indexDetailsText(summary *IndexSummary) string {
	if summary == nil {
		return ""
	}
	parts := []string{fmt.Sprintf("measurements=%d", summary.MeasurementCount)}
	if summary.SeriesRefs > 0 {
		parts = append(parts, fmt.Sprintf("series_refs=%d", summary.SeriesRefs))
	}
	if summary.SeriesIDSetCardinality > 0 {
		parts = append(parts, fmt.Sprintf("series_ids=%d", summary.SeriesIDSetCardinality))
	}
	if summary.TagKeyCount > 0 || summary.TagValueCount > 0 {
		parts = append(parts, fmt.Sprintf("tags=%d values=%d", summary.TagKeyCount, summary.TagValueCount))
	}
	if summary.DeletedMeasurementCount > 0 || summary.DeletedTagKeyCount > 0 || summary.DeletedTagValueCount > 0 || summary.TombstoneSeriesIDSetCardinality > 0 {
		parts = append(parts, fmt.Sprintf("deleted measurements=%d tag_keys=%d tag_values=%d series_ids=%d", summary.DeletedMeasurementCount, summary.DeletedTagKeyCount, summary.DeletedTagValueCount, summary.TombstoneSeriesIDSetCardinality))
	}
	if query := indexQueryDetailsText(summary.Query); query != "" {
		parts = append(parts, query)
	}
	return "index " + strings.Join(parts, " ")
}

func indexQueryDetailsText(summary *IndexQuerySummary) string {
	if summary == nil {
		return ""
	}
	parts := make([]string, 0, 4)
	if summary.MeasurementFilterApplied {
		parts = append(parts, "measurement_filter=true")
	}
	if summary.TagFilterApplied {
		parts = append(parts, "tag_filter=true")
	}
	if text := queryMatchCountText("measurements", len(summary.QueryMeasurements), len(summary.MatchedMeasurements), len(summary.MissingMeasurements)); text != "" {
		parts = append(parts, text)
	}
	if text := queryMatchCountText("tags", len(summary.QueryTags), len(summary.MatchedTags), len(summary.MissingTags)); text != "" {
		parts = append(parts, text)
	}
	if summary.CandidateMeasurements > 0 {
		parts = append(parts, fmt.Sprintf("candidates=%d", summary.CandidateMeasurements))
	}
	if summary.SeriesRefs > 0 {
		parts = append(parts, fmt.Sprintf("query_series_refs=%d", summary.SeriesRefs))
	}
	if len(parts) == 0 {
		return ""
	}
	return "query " + strings.Join(parts, " ")
}

func fieldIndexDetailsText(summary *FieldIndexSummary) string {
	if summary == nil {
		return ""
	}
	parts := []string{
		fmt.Sprintf("measurements=%d", summary.MeasurementCount),
		fmt.Sprintf("fields=%d", summary.FieldCount),
	}
	if byType := countMapText(summary.FieldsByType); byType != "" {
		parts = append(parts, "types="+byType)
	}
	if summary.ChangeCount > 0 || summary.AddFieldChanges > 0 || summary.DeleteMeasurements > 0 {
		parts = append(parts, fmt.Sprintf("changes=%d adds=%d deletes=%d", summary.ChangeCount, summary.AddFieldChanges, summary.DeleteMeasurements))
	}
	return "fields " + strings.Join(parts, " ")
}

func primaryKeyDetailsText(summary *PrimaryKeySummary) string {
	if summary == nil {
		return ""
	}
	parts := make([]string, 0, 8)
	if summary.Type != "" {
		parts = append(parts, "type="+summary.Type)
	}
	if summary.ColumnCount > 0 {
		parts = append(parts, fmt.Sprintf("columns=%d", summary.ColumnCount))
	}
	if summary.RowCount > 0 {
		parts = append(parts, fmt.Sprintf("rows=%d", summary.RowCount))
	}
	if summary.BlockIDRangeSet {
		parts = append(parts, fmt.Sprintf("block_ids=%d..%d", summary.MinBlockID, summary.MaxBlockID))
	}
	if summary.DataSizeBytes > 0 || summary.ValidDataBytes > 0 {
		parts = append(parts, fmt.Sprintf("data=%d valid=%d", summary.DataSizeBytes, summary.ValidDataBytes))
	}
	if summary.CRCMismatches > 0 || summary.DataOutOfBoundsBlocks > 0 || summary.ColumnOutOfBoundsBlocks > 0 || summary.ColumnUnorderedBlocks > 0 {
		parts = append(parts, fmt.Sprintf("crc=%d data_oob=%d column_oob=%d column_unordered=%d", summary.CRCMismatches, summary.DataOutOfBoundsBlocks, summary.ColumnOutOfBoundsBlocks, summary.ColumnUnorderedBlocks))
	}
	if len(parts) == 0 {
		return ""
	}
	return "primary_key " + strings.Join(parts, " ")
}

func secondaryIndexDetailsText(summary *SecondaryIndexSummary) string {
	if summary == nil {
		return ""
	}
	parts := make([]string, 0, 10)
	if summary.Type != "" {
		parts = append(parts, "type="+summary.Type)
	}
	if summary.Layout != "" {
		parts = append(parts, "layout="+summary.Layout)
	}
	if summary.Field != "" {
		parts = append(parts, "field="+summary.Field)
	}
	for _, part := range []struct {
		name  string
		value int64
	}{
		{name: "blocks", value: summary.BlockCount},
		{name: "groups", value: summary.GroupCount},
		{name: "pieces", value: summary.PieceCount},
		{name: "items", value: summary.ItemCount},
		{name: "documents", value: summary.DocumentCount},
		{name: "terms", value: summary.TermCount},
		{name: "dictionaries", value: summary.DictionaryCount},
		{name: "dictionary_versions", value: summary.DictionaryVersionCount},
		{name: "positions", value: summary.PositionCount},
		{name: "sid_groups", value: summary.SIDGroupCount},
		{name: "document_ids", value: summary.DocumentIDCount},
	} {
		if part.value > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", part.name, part.value))
		}
	}
	for _, part := range []struct {
		name  string
		value int64
	}{
		{name: "payload_bytes", value: summary.PayloadSizeBytes},
		{name: "block_bytes", value: summary.BlockSizeBytes},
		{name: "piece_bytes", value: summary.PieceSizeBytes},
		{name: "group_bytes", value: summary.GroupSizeBytes},
		{name: "valid_bytes", value: summary.ValidBytes},
	} {
		if part.value > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", part.name, part.value))
		}
	}
	if summary.CRCMismatches > 0 || summary.TrailingBytes > 0 || summary.DataOutOfBoundsBlocks > 0 {
		parts = append(parts, fmt.Sprintf("crc=%d trailing=%d data_oob=%d", summary.CRCMismatches, summary.TrailingBytes, summary.DataOutOfBoundsBlocks))
	}
	if len(parts) == 0 {
		return ""
	}
	return "secondary_index " + strings.Join(parts, " ")
}

func nonEmptyStrings(values []string) []string {
	out := values[:0]
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func decodePathText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	parts := make([]string, 0, 10)
	if summary.Mode != "" {
		parts = append(parts, summary.Mode)
	}
	if query := queryContextSummaryText(summary); query != "" {
		parts = append(parts, query)
	}
	if blocks := countDeltaSummaryText("blocks", summary.BaselineDecodeBlocks, summary.OptimizedDecodeBlocks, summary.SavedDecodeBlocks); blocks != "" {
		parts = append(parts, blocks)
	}
	if filter := blockFilterSummaryText(summary); filter != "" {
		parts = append(parts, filter)
	}
	if targets := queryTargetSummaryText(summary); targets != "" {
		parts = append(parts, targets)
	}
	if locationBlockTypes := countMapText(summary.LocationBlocksByType); locationBlockTypes != "" {
		parts = append(parts, "location_block_types "+locationBlockTypes)
	}
	if decodeBlockTypes := countMapText(summary.DecodeBlocksByType); decodeBlockTypes != "" {
		parts = append(parts, "decode_block_types "+decodeBlockTypes)
	}
	if bytes := decodeByteSummaryText(summary); bytes != "" {
		parts = append(parts, bytes)
	}
	if values := decodeValueSummaryText(summary); values != "" {
		parts = append(parts, values)
	}
	if segments := countDeltaSummaryText("segments", summary.BaselineReadSegments, summary.OptimizedReadSegments, summary.SavedReadSegments); segments != "" {
		parts = append(parts, segments)
	}
	if summary.BaselineCursorReadCalls > 0 || summary.OptimizedCursorReadCalls > 0 {
		parts = append(parts, fmt.Sprintf("cursor_reads %d->%d", summary.BaselineCursorReadCalls, summary.OptimizedCursorReadCalls))
	}
	if readAt := countDeltaSummaryText("read_at calls", summary.BaselineReadAtCalls, summary.OptimizedReadAtCalls, summary.SavedReadAtCalls); readAt != "" {
		parts = append(parts, readAt)
	}
	if ranges := readAtRangeSummaryText(summary); ranges != "" {
		parts = append(parts, ranges)
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
	if output := valueOutputSummaryText(summary); output != "" {
		parts = append(parts, output)
	}
	if output := cursorOutputSummaryText(summary); output != "" {
		parts = append(parts, output)
	}
	if execution := executionDiagnosticsSummaryText(summary); execution != "" {
		parts = append(parts, execution)
	}
	if summary.DataBlockProbeBlocks > 0 || summary.DataBlockProbeBytes > 0 || summary.DataBlockProbeValidBlocks > 0 || summary.DataBlockProbeFailures > 0 || summary.DataBlockProbeCRCMismatches > 0 || summary.DataBlockProbeShortBlocks > 0 || summary.DataBlockProbeUnknownTypes > 0 || summary.DataBlockProbeReadErrors > 0 || summary.DataBlockProbeRowCountBlocks > 0 || summary.DataBlockProbeRowUnknowns > 0 || summary.DataBlockProbeRowMismatches > 0 || summary.DataBlockProbeOutputPoints > 0 || summary.DataBlockProbeValueBlocks > 0 || summary.DataBlockProbeValueUnknowns > 0 || summary.DataBlockProbeNullValues > 0 || summary.DataBlockProbeRecordSamples > 0 {
		parts = append(parts, fmt.Sprintf("data_probe blocks=%d bytes=%d valid=%d failures=%d crc_mismatches=%d short=%d unknown_types=%d read_errors=%d row_blocks=%d row_unknowns=%d row_mismatches=%d output_points=%d value_blocks=%d value_unknowns=%d nulls=%d record_samples=%d", summary.DataBlockProbeBlocks, summary.DataBlockProbeBytes, summary.DataBlockProbeValidBlocks, summary.DataBlockProbeFailures, summary.DataBlockProbeCRCMismatches, summary.DataBlockProbeShortBlocks, summary.DataBlockProbeUnknownTypes, summary.DataBlockProbeReadErrors, summary.DataBlockProbeRowCountBlocks, summary.DataBlockProbeRowUnknowns, summary.DataBlockProbeRowMismatches, summary.DataBlockProbeOutputPoints, summary.DataBlockProbeValueBlocks, summary.DataBlockProbeValueUnknowns, summary.DataBlockProbeNullValues, summary.DataBlockProbeRecordSamples))
	}
	if failureReasons := countMapText(summary.DataBlockProbeFailureReasons); failureReasons != "" {
		parts = append(parts, "data_probe_failure_reasons "+failureReasons)
	}
	if probeTypes := countMapText(summary.DataBlockProbeTypes); probeTypes != "" {
		parts = append(parts, "data_probe_types "+probeTypes)
	}
	if unknownReasons := countMapText(summary.DataBlockProbeValueReasons); unknownReasons != "" {
		parts = append(parts, "data_probe_value_unknown_reasons "+unknownReasons)
	}
	if filters := fieldFilterSummaryText(summary); filters != "" {
		parts = append(parts, filters)
	}
	if summary.DataBlockProbeFilterRows > 0 || summary.DataBlockProbeFilterMatches > 0 || summary.DataBlockProbeFilterRejects > 0 {
		parts = append(parts, fmt.Sprintf("field_filter rows=%d matches=%d rejects=%d evals=%d eval_matches=%d eval_misses=%d required=%d required_matches=%d required_misses=%d any=%d any_matches=%d any_misses=%d none=%d none_matches=%d none_misses=%d", summary.DataBlockProbeFilterRows, summary.DataBlockProbeFilterMatches, summary.DataBlockProbeFilterRejects, summary.DataBlockProbeFilterEvals, summary.DataBlockProbeFilterEvalHits, summary.DataBlockProbeFilterEvalMiss, summary.DataBlockProbeRequiredEvals, summary.DataBlockProbeRequiredHits, summary.DataBlockProbeRequiredMiss, summary.DataBlockProbeAnyEvals, summary.DataBlockProbeAnyHits, summary.DataBlockProbeAnyMiss, summary.DataBlockProbeNoneEvals, summary.DataBlockProbeNoneHits, summary.DataBlockProbeNoneMiss))
	}
	if ops := countMapText(summary.DataBlockProbeFilterOps); ops != "" {
		parts = append(parts, "field_filter_ops "+ops)
	}
	if summary.DataBlockProbeRangeRows > 0 {
		parts = append(parts, fmt.Sprintf("row_range rows=%d matches=%d rejects=%d", summary.DataBlockProbeRangeRows, summary.DataBlockProbeRangeMatches, summary.DataBlockProbeRangeRejects))
	}
	if summary.DataBlockProbeFilterSkips > 0 {
		parts = append(parts, fmt.Sprintf("field_filter_short_circuit skips=%d required=%d any=%d none=%d", summary.DataBlockProbeFilterSkips, summary.DataBlockProbeRequiredSkips, summary.DataBlockProbeAnySkips, summary.DataBlockProbeNoneSkips))
	}
	return strings.Join(parts, ", ")
}

func countDeltaSummaryText(label string, baseline, optimized, saved int) string {
	if baseline == 0 && optimized == 0 && saved == 0 {
		return ""
	}
	parts := []string{fmt.Sprintf("%s %d->%d", label, baseline, optimized)}
	if saved > 0 {
		parts = append(parts, fmt.Sprintf("saved=%d", saved))
	}
	return strings.Join(parts, " ")
}

func queryContextSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if summary.QueryRange.Set {
		parts = append(parts, fmt.Sprintf("range=%s..%s", FormatUnixNano(summary.QueryRange.Min), FormatUnixNano(summary.QueryRange.Max)))
	}
	if summary.CursorSeekTime != 0 || (summary.QueryRange.Set && decodePathModeHasSeek(summary.Mode)) {
		seekTime := summary.CursorSeekTime
		if seekTime == 0 {
			seekTime = summary.QueryRange.Min
			if strings.HasSuffix(summary.Mode, "descending") {
				seekTime = summary.QueryRange.Max
			}
		}
		parts = append(parts, fmt.Sprintf("seek=%s", FormatUnixNano(seekTime)))
	}
	if summary.KeyFilterApplied {
		parts = append(parts, "target_filter=true")
	}
	if len(parts) == 0 {
		return ""
	}
	return "query " + strings.Join(parts, " ")
}

func decodePathModeHasSeek(mode string) bool {
	return strings.Contains(mode, "cursor") || strings.HasPrefix(mode, "tssp-detached-meta-index")
}

func blockFilterSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	parts := make([]string, 0, 7)
	if summary.LocationBlocks > 0 {
		parts = append(parts, fmt.Sprintf("locations=%d", summary.LocationBlocks))
	}
	if summary.FilteredDecodeBlocks > 0 {
		parts = append(parts, fmt.Sprintf("decoded=%d", summary.FilteredDecodeBlocks))
	}
	if summary.SkippedByKeyBlocks > 0 {
		parts = append(parts, fmt.Sprintf("skipped_key=%d", summary.SkippedByKeyBlocks))
	}
	if summary.SkippedByProjectionBlocks > 0 {
		parts = append(parts, fmt.Sprintf("skipped_projection=%d", summary.SkippedByProjectionBlocks))
	}
	if summary.SkippedBeforeSeekBlocks > 0 {
		parts = append(parts, fmt.Sprintf("skipped_before=%d", summary.SkippedBeforeSeekBlocks))
	}
	if summary.SkippedAfterRangeBlocks > 0 {
		parts = append(parts, fmt.Sprintf("skipped_after=%d", summary.SkippedAfterRangeBlocks))
	}
	if summary.FullyTombstonedBlocks > 0 {
		parts = append(parts, fmt.Sprintf("tombstoned=%d", summary.FullyTombstonedBlocks))
	}
	if len(parts) == 0 {
		return ""
	}
	return "block_filter " + strings.Join(parts, " ")
}

func decodeByteSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	if summary.BaselineDecodeBytes == 0 && summary.OptimizedDecodeBytes == 0 && summary.SavedDecodeBytes == 0 {
		return ""
	}
	parts := []string{fmt.Sprintf("%d->%d", summary.BaselineDecodeBytes, summary.OptimizedDecodeBytes)}
	if summary.SavedDecodeBytes > 0 {
		parts = append(parts, fmt.Sprintf("saved=%d", summary.SavedDecodeBytes))
	}
	return "decode_bytes " + strings.Join(parts, " ")
}

func decodeValueSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	if summary.BaselineDecodeValues == 0 && summary.OptimizedDecodeValues == 0 &&
		summary.SavedDecodeValues == 0 && summary.BaselineOutputValues == 0 &&
		summary.OptimizedOutputValues == 0 {
		return ""
	}
	parts := make([]string, 0, 3)
	if summary.BaselineDecodeValues > 0 || summary.OptimizedDecodeValues > 0 || summary.SavedDecodeValues > 0 {
		decodeParts := []string{fmt.Sprintf("decode=%d->%d", summary.BaselineDecodeValues, summary.OptimizedDecodeValues)}
		if summary.SavedDecodeValues > 0 {
			decodeParts = append(decodeParts, fmt.Sprintf("saved=%d", summary.SavedDecodeValues))
		}
		parts = append(parts, strings.Join(decodeParts, " "))
	}
	if summary.BaselineOutputValues > 0 || summary.OptimizedOutputValues > 0 {
		parts = append(parts, fmt.Sprintf("output=%d->%d", summary.BaselineOutputValues, summary.OptimizedOutputValues))
	}
	return "values " + strings.Join(parts, " ")
}

func readAtRangeSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	rangeCount := 0
	sampledBlocks := 0
	var bytes int64
	columns := map[string]struct{}{}
	for _, sample := range summary.Samples {
		if len(sample.OptimizedReadAtRanges) == 0 {
			continue
		}
		sampledBlocks++
		for _, readRange := range sample.OptimizedReadAtRanges {
			rangeCount++
			bytes += int64(readRange.SizeBytes)
			if readRange.Column != "" {
				columns[readRange.Column] = struct{}{}
			}
		}
	}
	if rangeCount == 0 {
		return ""
	}
	parts := []string{
		fmt.Sprintf("ranges=%d", rangeCount),
		fmt.Sprintf("sampled_blocks=%d", sampledBlocks),
	}
	if bytes > 0 {
		parts = append(parts, fmt.Sprintf("bytes=%d", bytes))
	}
	if len(columns) > 0 {
		parts = append(parts, fmt.Sprintf("columns=%d", len(columns)))
	}
	return "read_at_ranges " + strings.Join(parts, " ")
}

func valueOutputSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	if summary.BaselineValueOutputPoints == 0 && summary.OptimizedValueOutputPoints == 0 &&
		summary.ComparedValueOutputPoints == 0 && summary.ValueOutputUnavailableBlocks == 0 &&
		summary.ValueOutputMismatches == 0 {
		return ""
	}
	parts := []string{fmt.Sprintf("points=%d->%d", summary.BaselineValueOutputPoints, summary.OptimizedValueOutputPoints)}
	if summary.ComparedValueOutputPoints > 0 {
		parts = append(parts, fmt.Sprintf("compared=%d", summary.ComparedValueOutputPoints))
	}
	if summary.ValueOutputUnavailableBlocks > 0 {
		parts = append(parts, fmt.Sprintf("unavailable_blocks=%d", summary.ValueOutputUnavailableBlocks))
	}
	if summary.ValueOutputMismatches > 0 {
		parts = append(parts, fmt.Sprintf("mismatches=%d", summary.ValueOutputMismatches))
	}
	return "value_output " + strings.Join(parts, " ")
}

func cursorOutputSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	if summary.BaselineCursorOutputPoints == 0 && summary.OptimizedCursorOutputPoints == 0 &&
		len(summary.CursorOutputSamples) == 0 && len(summary.CursorFinalOutputSamples) == 0 {
		return ""
	}
	parts := make([]string, 0, 2)
	if summary.BaselineCursorOutputPoints > 0 || summary.OptimizedCursorOutputPoints > 0 {
		parts = append(parts, fmt.Sprintf("points=%d->%d", summary.BaselineCursorOutputPoints, summary.OptimizedCursorOutputPoints))
	}
	if len(summary.CursorOutputSamples) > 0 || len(summary.CursorFinalOutputSamples) > 0 {
		parts = append(parts, fmt.Sprintf("samples=%d final_samples=%d", len(summary.CursorOutputSamples), len(summary.CursorFinalOutputSamples)))
	}
	return "cursor_output " + strings.Join(parts, " ")
}

func executionDiagnosticsSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	windowParts := make([]string, 0, 5)
	if summary.CursorWindowCount > 0 {
		windowParts = append(windowParts, fmt.Sprintf("cursor=%d", summary.CursorWindowCount))
	}
	if len(summary.CursorWindows) > 0 {
		windowParts = append(windowParts, fmt.Sprintf("sampled=%d", len(summary.CursorWindows)))
	}
	if summary.MergeWindowCount > 0 {
		windowParts = append(windowParts, fmt.Sprintf("merge=%d", summary.MergeWindowCount))
	}
	if summary.MergeWindowBlocks > 0 {
		windowParts = append(windowParts, fmt.Sprintf("merge_blocks=%d", summary.MergeWindowBlocks))
	}
	if summary.MergeWindowKeys > 0 {
		windowParts = append(windowParts, fmt.Sprintf("merge_keys=%d", summary.MergeWindowKeys))
	}
	if len(windowParts) > 0 {
		parts = append(parts, "windows "+strings.Join(windowParts, " "))
	}
	if len(summary.Samples) > 0 || len(summary.CursorExecutionSamples) > 0 || len(summary.FilterExecutionSamples) > 0 {
		parts = append(parts, fmt.Sprintf("samples decisions=%d cursor_steps=%d filter_steps=%d", len(summary.Samples), len(summary.CursorExecutionSamples), len(summary.FilterExecutionSamples)))
	}
	if summary.Amplification > 0 {
		if formatted := fmt.Sprintf("%.2f", summary.Amplification); formatted != "0.00" {
			parts = append(parts, "amplification="+formatted+"x")
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "execution " + strings.Join(parts, " ")
}

func queryTargetSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	parts := make([]string, 0, 4)
	if text := queryMatchCountText("keys", len(summary.QueryKeys), len(summary.MatchedKeys), len(summary.MissingKeys)); text != "" {
		parts = append(parts, text)
	}
	if text := queryMatchCountText("series_ids", len(summary.QuerySeriesIDs), len(summary.MatchedSeriesIDs), len(summary.MissingSeriesIDs)); text != "" {
		parts = append(parts, text)
	}
	if text := queryMatchCountText("meta_index_ids", len(summary.QueryMetaIndexIDs), len(summary.MatchedMetaIndexIDs), len(summary.MissingMetaIndexIDs)); text != "" {
		parts = append(parts, text)
	}
	if text := queryMatchCountText("columns", len(summary.QueryColumns), len(summary.MatchedColumns), len(summary.MissingColumns)); text != "" {
		parts = append(parts, text)
	}
	return strings.Join(parts, " ")
}

func queryMatchCountText(name string, query, matched, missing int) string {
	if query == 0 && matched == 0 && missing == 0 {
		return ""
	}
	return fmt.Sprintf("%s=%d/%d/%d", name, query, matched, missing)
}

func fieldFilterSummaryText(summary *DecodePathSummary) string {
	if summary == nil {
		return ""
	}
	if len(summary.QueryFields) == 0 && len(summary.MatchedFields) == 0 && len(summary.MissingFields) == 0 &&
		len(summary.QueryAnyFields) == 0 && len(summary.MatchedAnyFields) == 0 && len(summary.MissingAnyFields) == 0 &&
		len(summary.QueryNoneFields) == 0 && len(summary.MatchedNoneFields) == 0 && len(summary.MissingNoneFields) == 0 {
		return ""
	}
	return fmt.Sprintf(
		"field_filters required=%d matched=%d missing=%d any=%d any_matched=%d any_missing=%d none=%d none_matched=%d none_missing=%d",
		len(summary.QueryFields),
		len(summary.MatchedFields),
		len(summary.MissingFields),
		len(summary.QueryAnyFields),
		len(summary.MatchedAnyFields),
		len(summary.MissingAnyFields),
		len(summary.QueryNoneFields),
		len(summary.MatchedNoneFields),
		len(summary.MissingNoneFields),
	)
}

func countMapText(counts map[string]int) string {
	if len(counts) == 0 {
		return ""
	}
	keys := make([]string, 0, len(counts))
	for key, count := range counts {
		if count <= 0 {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", key, counts[key]))
	}
	return strings.Join(parts, " ")
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
