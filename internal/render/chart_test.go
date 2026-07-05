package render

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

func TestRenderChartPlotsSeries(t *testing.T) {
	res := result.Result{
		Kind: result.KindSeries,
		Series: []result.Series{
			{
				Name: "cpu",
				Tags: map[string]string{"host": "a"},
				Points: []result.Point{
					{Time: time.Unix(0, 0), Value: 1},
					{Time: time.Unix(1, 0), Value: 4},
					{Time: time.Unix(2, 0), Value: 2},
					{Time: time.Unix(3, 0), Value: 8},
				},
			},
		},
	}

	output := RenderChart(res, Options{Width: 32, MaxSeries: 1})
	for _, want := range []string{"cpu{host=a}", "points=4", "|", "+", "*"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected %q in chart output:\n%s", want, output)
		}
	}
}

func TestRenderChartFallsBackForTableOnlyResult(t *testing.T) {
	table := result.NewTable([]string{"name"})
	table.AddRow("metrics")
	output := RenderChart(result.FromTable(table), Options{Width: 20})
	if !strings.Contains(output, "metrics") {
		t.Fatalf("expected table fallback, got %q", output)
	}
}

func TestRenderChartDoesNotOnlyRenderTruncationNotice(t *testing.T) {
	res := result.Result{
		Kind: result.KindSeries,
		Series: []result.Series{
			{Name: "cpu", Points: []result.Point{{Value: math.NaN()}}},
			{Name: "mem", Points: []result.Point{{Value: 1}}},
		},
	}
	output := RenderChart(res, Options{Width: 24, MaxSeries: 1})
	if output != "no numeric time-series points" {
		t.Fatalf("chart output = %q, want no numeric time-series points", output)
	}
}

func TestRenderChartFormat(t *testing.T) {
	res := result.Result{
		Kind: result.KindSeries,
		Series: []result.Series{
			{Name: "cpu", Points: []result.Point{{Value: 1}, {Value: 2}}},
		},
	}
	output, renderer, err := Render(res, Options{Format: FormatChart, Width: 24})
	if err != nil {
		t.Fatal(err)
	}
	if renderer != FormatChart {
		t.Fatalf("renderer = %q, want chart", renderer)
	}
	if !strings.Contains(output, "cpu") || !strings.Contains(output, "+") {
		t.Fatalf("unexpected chart output: %q", output)
	}
}
