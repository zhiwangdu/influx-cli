package storage

import "sort"

type tsspColumnProjection struct {
	applied  bool
	selected map[string]struct{}
}

func newTSSPColumnProjection(chunk tsspChunkMeta, queryColumns []string, queryFields []FieldFilter, queryAnyFields []FieldFilter) tsspColumnProjection {
	if len(queryColumns) == 0 {
		return tsspColumnProjection{}
	}
	want := queryKeySet(queryColumns)
	for _, filter := range queryFields {
		if filter.Key != "" {
			want[filter.Key] = struct{}{}
		}
	}
	for _, filter := range queryAnyFields {
		if filter.Key != "" {
			want[filter.Key] = struct{}{}
		}
	}
	selected := map[string]struct{}{}
	hasTimeColumn := false
	needsTimeColumn := false
	for _, column := range chunk.Columns {
		if column.Name == "time" {
			hasTimeColumn = true
		}
		if _, ok := want[column.Name]; !ok {
			continue
		}
		selected[column.Name] = struct{}{}
		if column.Name != "time" {
			needsTimeColumn = true
		}
	}
	if needsTimeColumn && hasTimeColumn {
		selected["time"] = struct{}{}
	}
	return tsspColumnProjection{applied: true, selected: selected}
}

func (p tsspColumnProjection) selectedColumn(name string) bool {
	if !p.applied {
		return true
	}
	_, ok := p.selected[name]
	return ok
}

func (p tsspColumnProjection) missingAllColumns() bool {
	return p.applied && len(p.selected) == 0
}

func populateTSSPColumnProjectionMatches(summary *DecodePathSummary, chunks []tsspChunkMeta, queryColumns []string) {
	if summary == nil || len(queryColumns) == 0 {
		return
	}
	queryColumns = normalizeQueryKeys(queryColumns)
	if len(queryColumns) == 0 {
		return
	}
	summary.QueryColumns = append([]string(nil), queryColumns...)
	want := queryKeySet(queryColumns)
	matchedSet := map[string]struct{}{}
	for _, chunk := range chunks {
		for _, column := range chunk.Columns {
			if _, ok := want[column.Name]; ok {
				matchedSet[column.Name] = struct{}{}
			}
		}
	}
	summary.MatchedColumns = make([]string, 0, len(matchedSet))
	for column := range matchedSet {
		summary.MatchedColumns = append(summary.MatchedColumns, column)
	}
	sort.Strings(summary.MatchedColumns)
	for _, column := range queryColumns {
		if _, ok := matchedSet[column]; !ok {
			summary.MissingColumns = append(summary.MissingColumns, column)
		}
	}
}

func populateTSSPFieldFilterMatches(summary *DecodePathSummary, chunks []tsspChunkMeta, queryFields []FieldFilter) {
	if summary == nil || len(queryFields) == 0 {
		return
	}
	queryFields = normalizeFieldFilters(queryFields)
	if len(queryFields) == 0 {
		return
	}
	summary.QueryFields = append([]FieldFilter(nil), queryFields...)
	want := map[string]struct{}{}
	for _, filter := range queryFields {
		want[filter.Key] = struct{}{}
	}
	matchedKeys := map[string]struct{}{}
	for _, chunk := range chunks {
		for _, column := range chunk.Columns {
			if _, ok := want[column.Name]; ok {
				matchedKeys[column.Name] = struct{}{}
			}
		}
	}
	for _, filter := range queryFields {
		if _, ok := matchedKeys[filter.Key]; ok {
			summary.MatchedFields = append(summary.MatchedFields, filter)
		} else {
			summary.MissingFields = append(summary.MissingFields, filter)
		}
	}
}

func populateTSSPAnyFieldFilterMatches(summary *DecodePathSummary, chunks []tsspChunkMeta, queryFields []FieldFilter) {
	if summary == nil || len(queryFields) == 0 {
		return
	}
	queryFields = normalizeFieldFilters(queryFields)
	if len(queryFields) == 0 {
		return
	}
	summary.QueryAnyFields = append([]FieldFilter(nil), queryFields...)
	want := map[string]struct{}{}
	for _, filter := range queryFields {
		want[filter.Key] = struct{}{}
	}
	matchedKeys := map[string]struct{}{}
	for _, chunk := range chunks {
		for _, column := range chunk.Columns {
			if _, ok := want[column.Name]; ok {
				matchedKeys[column.Name] = struct{}{}
			}
		}
	}
	for _, filter := range queryFields {
		if _, ok := matchedKeys[filter.Key]; ok {
			summary.MatchedAnyFields = append(summary.MatchedAnyFields, filter)
		} else {
			summary.MissingAnyFields = append(summary.MissingAnyFields, filter)
		}
	}
}

func populateTSSPFileSetColumnProjectionMatches(summary *DecodePathSummary, queryColumns []string, matchedSet map[string]struct{}) {
	if summary == nil || len(queryColumns) == 0 {
		return
	}
	queryColumns = normalizeQueryKeys(queryColumns)
	if len(queryColumns) == 0 {
		return
	}
	summary.QueryColumns = append([]string(nil), queryColumns...)
	for _, column := range queryColumns {
		if _, ok := matchedSet[column]; ok {
			summary.MatchedColumns = append(summary.MatchedColumns, column)
		} else {
			summary.MissingColumns = append(summary.MissingColumns, column)
		}
	}
}

func populateTSSPFileSetFieldFilterMatches(summary *DecodePathSummary, queryFields []FieldFilter, matchedSet map[string]struct{}) {
	if summary == nil || len(queryFields) == 0 {
		return
	}
	queryFields = normalizeFieldFilters(queryFields)
	if len(queryFields) == 0 {
		return
	}
	summary.QueryFields = append([]FieldFilter(nil), queryFields...)
	for _, filter := range queryFields {
		if _, ok := matchedSet[filter.Key]; ok {
			summary.MatchedFields = append(summary.MatchedFields, filter)
		} else {
			summary.MissingFields = append(summary.MissingFields, filter)
		}
	}
}

func populateTSSPFileSetAnyFieldFilterMatches(summary *DecodePathSummary, queryFields []FieldFilter, matchedSet map[string]struct{}) {
	if summary == nil || len(queryFields) == 0 {
		return
	}
	queryFields = normalizeFieldFilters(queryFields)
	if len(queryFields) == 0 {
		return
	}
	summary.QueryAnyFields = append([]FieldFilter(nil), queryFields...)
	for _, filter := range queryFields {
		if _, ok := matchedSet[filter.Key]; ok {
			summary.MatchedAnyFields = append(summary.MatchedAnyFields, filter)
		} else {
			summary.MissingAnyFields = append(summary.MissingAnyFields, filter)
		}
	}
}
