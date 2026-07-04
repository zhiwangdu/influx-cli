package result

import (
	"time"

	"github.com/zhiwangdu/influx-cli/internal/schema"
)

type Kind string

const (
	KindEmpty  Kind = "empty"
	KindTable  Kind = "table"
	KindSeries Kind = "series"
	KindSchema Kind = "schema"
)

type Result struct {
	Kind     Kind
	Table    *Table
	Series   []Series
	Schema   *schema.Snapshot
	Metadata Metadata
}

type Metadata struct {
	StatementCount int
	RowCount       int
	PointCount     int
	SeriesCount    int
	Source         string
	Notices        []string
}

type Table struct {
	Columns []string
	Rows    [][]any
}

func NewTable(columns []string) *Table {
	return &Table{Columns: append([]string(nil), columns...)}
}

func (t *Table) AddRow(row ...any) {
	if t == nil {
		return
	}
	t.Rows = append(t.Rows, append([]any(nil), row...))
}

func (t *Table) RowCount() int {
	if t == nil {
		return 0
	}
	return len(t.Rows)
}

type Series struct {
	Name   string
	Tags   map[string]string
	Points []Point
}

type Point struct {
	Time  time.Time
	Value float64
}

func Empty() Result {
	return Result{Kind: KindEmpty, Table: NewTable(nil)}
}

func FromTable(table *Table) Result {
	kind := KindTable
	rowCount := 0
	if table != nil {
		rowCount = table.RowCount()
	}
	if table == nil || rowCount == 0 {
		kind = KindEmpty
	}
	return Result{
		Kind:  kind,
		Table: table,
		Metadata: Metadata{
			RowCount: rowCount,
		},
	}
}

func SchemaResult(snapshot schema.Snapshot) Result {
	table := NewTable([]string{"measurement", "kind", "name", "type"})
	for _, measurement := range snapshot.Measurements {
		for _, field := range measurement.Fields {
			table.AddRow(measurement.Name, "field", field.Name, field.Type)
		}
		for _, tag := range measurement.Tags {
			table.AddRow(measurement.Name, "tag", tag.Name, "")
		}
	}
	return Result{
		Kind:   KindSchema,
		Table:  table,
		Schema: &snapshot,
		Metadata: Metadata{
			RowCount: table.RowCount(),
		},
	}
}
