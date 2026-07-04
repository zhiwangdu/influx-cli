package render

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

func RenderTable(res result.Result, options Options) string {
	table := res.Table
	if table == nil || len(table.Columns) == 0 {
		return "no rows"
	}

	maxRows := options.MaxRows
	if maxRows <= 0 {
		maxRows = 200
	}

	rows := table.Rows
	truncated := false
	if len(rows) > maxRows {
		rows = rows[:maxRows]
		truncated = true
	}

	formattedRows := make([][]string, len(rows))
	widths := make([]int, len(table.Columns))
	for i, column := range table.Columns {
		widths[i] = textWidth(column)
	}

	for rowIndex, row := range rows {
		formattedRows[rowIndex] = make([]string, len(table.Columns))
		for columnIndex := range table.Columns {
			var value any
			if columnIndex < len(row) {
				value = row[columnIndex]
			}
			formatted := formatValue(value)
			formattedRows[rowIndex][columnIndex] = formatted
			if width := textWidth(formatted); width > widths[columnIndex] {
				widths[columnIndex] = width
			}
		}
	}

	limitColumnWidths(widths, options.Width)

	var builder strings.Builder
	writeRow(&builder, table.Columns, widths)
	builder.WriteByte('\n')
	separators := make([]string, len(table.Columns))
	for i, width := range widths {
		separators[i] = strings.Repeat("-", width)
	}
	writeRow(&builder, separators, widths)
	for _, row := range formattedRows {
		builder.WriteByte('\n')
		writeRow(&builder, row, widths)
	}
	if truncated {
		builder.WriteString(fmt.Sprintf("\n... %d more rows", len(table.Rows)-len(rows)))
	}
	return builder.String()
}

func writeRow(builder *strings.Builder, values []string, widths []int) {
	for i, width := range widths {
		if i > 0 {
			builder.WriteString("  ")
		}
		value := ""
		if i < len(values) {
			value = truncate(values[i], width)
		}
		builder.WriteString(value)
		builder.WriteString(strings.Repeat(" ", width-textWidth(value)))
	}
}

func formatValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return sanitizeCell(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return sanitizeCell(fmt.Sprint(typed))
	}
}

func sanitizeCell(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\t", " ")
	return strings.Join(strings.Fields(value), " ")
}

func limitColumnWidths(widths []int, totalWidth int) {
	if totalWidth <= 0 || len(widths) == 0 {
		for i, width := range widths {
			if width > 48 {
				widths[i] = 48
			}
		}
		return
	}
	separatorWidth := (len(widths) - 1) * 2
	available := totalWidth - separatorWidth
	if available < len(widths)*4 {
		available = len(widths) * 4
	}
	for sum(widths) > available {
		largest := 0
		for i := range widths {
			if widths[i] > widths[largest] {
				largest = i
			}
		}
		if widths[largest] <= 8 {
			break
		}
		widths[largest]--
	}
}

func sum(values []int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}

func truncate(value string, width int) string {
	if textWidth(value) <= width {
		return value
	}
	if width <= 3 {
		return strings.Repeat(".", width)
	}
	runes := []rune(value)
	if len(runes) <= width {
		return value
	}
	return string(runes[:width-3]) + "..."
}

func textWidth(value string) int {
	return len([]rune(value))
}
