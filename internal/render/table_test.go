package render

import (
	"strings"
	"testing"

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
