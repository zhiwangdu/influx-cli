package render

import (
	"strings"
	"testing"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

func TestDrawSparklineRespectsWidth(t *testing.T) {
	line := drawSparkline([]float64{-5, -2, 0, 2, 4, 8}, 3)
	if got := len([]rune(line)); got != 3 {
		t.Fatalf("sparkline width = %d, want 3: %q", got, line)
	}
}

func TestDrawSparklineHandlesConstantSeries(t *testing.T) {
	line := drawSparkline([]float64{7, 7, 7}, 10)
	if got := len([]rune(line)); got != 3 {
		t.Fatalf("sparkline width = %d, want original point count 3", got)
	}
	if line == "" {
		t.Fatal("constant sparkline is empty")
	}
}

func TestRenderAutoSelectsSparklineForSeries(t *testing.T) {
	res := result.Result{
		Kind: result.KindSeries,
		Series: []result.Series{
			{
				Name: "cpu",
				Tags: map[string]string{"host": "a"},
				Points: []result.Point{
					{Time: time.Unix(0, 0), Value: -1},
					{Time: time.Unix(60, 0), Value: 2},
				},
			},
		},
	}

	output, renderer, err := Render(res, Options{Format: FormatAuto, Width: 8})
	if err != nil {
		t.Fatal(err)
	}
	if renderer != FormatSparkline {
		t.Fatalf("renderer = %q, want sparkline", renderer)
	}
	if !strings.Contains(output, "cpu{host=a}") || !strings.Contains(output, "points=2") {
		t.Fatalf("unexpected sparkline output: %q", output)
	}
}

func TestRenderSparklineFallsBackForTableOnlyResult(t *testing.T) {
	table := result.NewTable([]string{"name"})
	table.AddRow("metrics")
	output := RenderSparkline(result.FromTable(table), Options{Width: 10})
	if !strings.Contains(output, "metrics") {
		t.Fatalf("expected table fallback, got %q", output)
	}
}
