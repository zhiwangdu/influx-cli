package render

import (
	"fmt"
	"math"
	"strings"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

const chartHeight = 8

func RenderChart(res result.Result, options Options) string {
	if len(res.Series) == 0 {
		if res.Table != nil && len(res.Table.Columns) > 0 {
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
		width = 80
	}
	if width > 140 {
		width = 140
	}
	if width < 24 {
		width = 24
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
			builder.WriteString("\n\n")
		}
		builder.WriteString(renderSeriesChart(series, values, width))
	}
	if builder.Len() > 0 && len(res.Series) > limit {
		builder.WriteString(fmt.Sprintf("\n... %d more series", len(res.Series)-limit))
	}
	if builder.Len() == 0 {
		return "no numeric time-series points"
	}
	return builder.String()
}

func renderSeriesChart(series result.Series, values []float64, width int) string {
	minValue, maxValue, avgValue := stats(values)
	minLabel := formatFloat(minValue)
	maxLabel := formatFloat(maxValue)
	labelWidth := textWidth(minLabel)
	if textWidth(maxLabel) > labelWidth {
		labelWidth = textWidth(maxLabel)
	}
	if labelWidth < 4 {
		labelWidth = 4
	}
	plotWidth := width - labelWidth - 3
	if plotWidth < 8 {
		plotWidth = 8
	}

	points := reduce(values, plotWidth)
	grid := newChartGrid(chartHeight, len(points))
	for col, value := range points {
		row := chartRow(value, minValue, maxValue, chartHeight)
		grid[row][col] = '*'
		if col == 0 {
			continue
		}
		prevRow := chartRow(points[col-1], minValue, maxValue, chartHeight)
		connectChartPoints(grid, prevRow, row, col-1, col)
	}

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%s min=%s max=%s avg=%s points=%d\n",
		seriesLabel(series),
		minLabel,
		maxLabel,
		formatFloat(avgValue),
		len(values),
	))
	for row := 0; row < chartHeight; row++ {
		label := ""
		switch row {
		case 0:
			label = maxLabel
		case chartHeight - 1:
			label = minLabel
		}
		builder.WriteString(fmt.Sprintf("%*s | %s\n", labelWidth, label, string(grid[row])))
	}
	builder.WriteString(strings.Repeat(" ", labelWidth))
	builder.WriteString(" + ")
	builder.WriteString(strings.Repeat("-", len(points)))
	return builder.String()
}

func newChartGrid(height, width int) [][]rune {
	grid := make([][]rune, height)
	for i := range grid {
		grid[i] = []rune(strings.Repeat(" ", width))
	}
	return grid
}

func chartRow(value, minValue, maxValue float64, height int) int {
	if maxValue == minValue {
		return height / 2
	}
	ratio := (value - minValue) / (maxValue - minValue)
	row := height - 1 - int(math.Round(ratio*float64(height-1)))
	if row < 0 {
		return 0
	}
	if row >= height {
		return height - 1
	}
	return row
}

func connectChartPoints(grid [][]rune, prevRow, row, prevCol, col int) {
	if prevRow == row {
		if grid[row][prevCol] == ' ' {
			grid[row][prevCol] = '-'
		}
		if grid[row][col] == ' ' {
			grid[row][col] = '-'
		}
		return
	}

	start, end := prevRow, row
	if start > end {
		start, end = end, start
	}
	for r := start + 1; r < end; r++ {
		if grid[r][prevCol] == ' ' {
			grid[r][prevCol] = '|'
		}
	}
}
