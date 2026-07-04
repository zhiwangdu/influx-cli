package render

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

var sparkBlocks = []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

func RenderSparkline(res result.Result, options Options) string {
	if len(res.Series) == 0 {
		if res.Table != nil && res.Table.RowCount() > 0 {
			return RenderTable(res, options)
		}
		return "no numeric time-series points"
	}

	maxSeries := options.MaxSeries
	if maxSeries <= 0 {
		maxSeries = 5
	}
	width := options.Width
	if width <= 0 {
		width = 60
	}
	if width > 120 {
		width = 120
	}
	if width < 8 {
		width = 8
	}

	var builder strings.Builder
	limit := len(res.Series)
	if limit > maxSeries {
		limit = maxSeries
	}
	for i := 0; i < limit; i++ {
		series := res.Series[i]
		values := valuesFromPoints(series.Points)
		if len(values) == 0 {
			continue
		}
		if builder.Len() > 0 {
			builder.WriteByte('\n')
		}
		minValue, maxValue, avgValue := stats(values)
		line := drawSparkline(values, width)
		builder.WriteString(fmt.Sprintf("%s %s min=%s max=%s avg=%s points=%d",
			seriesLabel(series),
			line,
			formatFloat(minValue),
			formatFloat(maxValue),
			formatFloat(avgValue),
			len(values),
		))
	}
	if len(res.Series) > limit {
		builder.WriteString(fmt.Sprintf("\n... %d more series", len(res.Series)-limit))
	}
	if builder.Len() == 0 {
		return "no numeric time-series points"
	}
	return builder.String()
}

func drawSparkline(values []float64, width int) string {
	if len(values) == 0 {
		return ""
	}
	reduced := reduce(values, width)
	minValue, maxValue, _ := stats(reduced)
	if maxValue == minValue {
		return strings.Repeat(string(sparkBlocks[len(sparkBlocks)/2]), len(reduced))
	}

	var builder strings.Builder
	for _, value := range reduced {
		ratio := (value - minValue) / (maxValue - minValue)
		index := int(math.Round(ratio * float64(len(sparkBlocks)-1)))
		if index < 0 {
			index = 0
		}
		if index >= len(sparkBlocks) {
			index = len(sparkBlocks) - 1
		}
		builder.WriteRune(sparkBlocks[index])
	}
	return builder.String()
}

func reduce(values []float64, width int) []float64 {
	if width <= 0 || len(values) <= width {
		return append([]float64(nil), values...)
	}
	out := make([]float64, width)
	for i := 0; i < width; i++ {
		start := i * len(values) / width
		end := (i + 1) * len(values) / width
		if end <= start {
			end = start + 1
		}
		if end > len(values) {
			end = len(values)
		}
		total := 0.0
		for _, value := range values[start:end] {
			total += value
		}
		out[i] = total / float64(end-start)
	}
	return out
}

func valuesFromPoints(points []result.Point) []float64 {
	values := make([]float64, 0, len(points))
	for _, point := range points {
		if math.IsNaN(point.Value) || math.IsInf(point.Value, 0) {
			continue
		}
		values = append(values, point.Value)
	}
	return values
}

func stats(values []float64) (float64, float64, float64) {
	minValue := values[0]
	maxValue := values[0]
	total := 0.0
	for _, value := range values {
		if value < minValue {
			minValue = value
		}
		if value > maxValue {
			maxValue = value
		}
		total += value
	}
	return minValue, maxValue, total / float64(len(values))
}

func seriesLabel(series result.Series) string {
	label := series.Name
	if label == "" {
		label = "series"
	}
	if len(series.Tags) == 0 {
		return label
	}
	keys := make([]string, 0, len(series.Tags))
	for key := range series.Tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		value := series.Tags[key]
		parts = append(parts, key+"="+value)
	}
	return label + "{" + strings.Join(parts, ",") + "}"
}

func formatFloat(value float64) string {
	return fmt.Sprintf("%.4g", value)
}
