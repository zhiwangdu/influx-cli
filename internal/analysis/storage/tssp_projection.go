package storage

import "sort"

type tsspColumnProjection struct {
	applied  bool
	selected map[string]struct{}
}

func newTSSPColumnProjection(chunk tsspChunkMeta, queryColumns []string) tsspColumnProjection {
	if len(queryColumns) == 0 {
		return tsspColumnProjection{}
	}
	want := queryKeySet(queryColumns)
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
