package render

import (
	"strings"
	"testing"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

func TestRenderTablePrintsHeaderRowsAndTruncation(t *testing.T) {
	table := result.NewTable([]string{"name", "value"})
	table.AddRow("cpu", 1)
	table.AddRow("mem", 2)

	output := RenderTable(result.FromTable(table), Options{MaxRows: 1, Width: 40})
	if !strings.Contains(output, "name") || !strings.Contains(output, "cpu") {
		t.Fatalf("missing table content: %q", output)
	}
	if !strings.Contains(output, "... 1 more rows") {
		t.Fatalf("missing truncation notice: %q", output)
	}
	if strings.Contains(output, "mem") {
		t.Fatalf("truncated row was printed: %q", output)
	}
}

func TestRenderTableHandlesEmptyTable(t *testing.T) {
	output := RenderTable(result.Empty(), Options{})
	if output != "no rows" {
		t.Fatalf("output = %q, want no rows", output)
	}
}

func TestRenderDefaultsToTable(t *testing.T) {
	table := result.NewTable([]string{"time", "value"})
	table.AddRow(time.Unix(0, 0).UTC(), 1.5)
	res := result.Result{
		Kind:  result.KindSeries,
		Table: table,
		Series: []result.Series{{
			Name: "cpu",
			Points: []result.Point{
				{Time: time.Unix(0, 0).UTC(), Value: 1.5},
			},
		}},
	}

	output, renderer, err := Render(res, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if renderer != FormatTable {
		t.Fatalf("renderer = %q, want table", renderer)
	}
	if !strings.Contains(output, "time") || !strings.Contains(output, "value") {
		t.Fatalf("expected table output, got %q", output)
	}
}

func TestNormalizeFormatRejectsUnknownFormat(t *testing.T) {
	if _, err := NormalizeFormat("wide"); err == nil {
		t.Fatal("expected unknown format error")
	}
}
