package tui

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/zhiwangdu/influx-cli/internal/history"
	"github.com/zhiwangdu/influx-cli/internal/schema"
)

func replaceTrailingPrefix(value, prefix, candidate string) string {
	if prefix == "" {
		return value + candidate
	}
	if !strings.HasSuffix(value, prefix) {
		return value + candidate
	}
	return value[:len(value)-len(prefix)] + candidate
}

func inferMeasurement(query string) string {
	tokens := queryTokens(query)
	for i, token := range tokens {
		if strings.EqualFold(token, "from") && i+1 < len(tokens) {
			parts := make([]string, 0, 2)
			for _, candidate := range tokens[i+1:] {
				if isClauseToken(candidate) {
					break
				}
				parts = append(parts, candidate)
			}
			return cleanMeasurement(strings.Join(parts, "."))
		}
	}
	return ""
}

func queryTokens(input string) []string {
	var tokens []string
	runes := []rune(input)
	for i := 0; i < len(runes); {
		for i < len(runes) && isTokenSeparator(runes[i]) {
			i++
		}
		if i >= len(runes) {
			break
		}
		if runes[i] == '"' || runes[i] == '\'' {
			quote := runes[i]
			i++
			start := i
			for i < len(runes) && runes[i] != quote {
				i++
			}
			tokens = append(tokens, string(runes[start:i]))
			if i < len(runes) {
				i++
			}
			continue
		}
		start := i
		for i < len(runes) && !isTokenSeparator(runes[i]) {
			i++
		}
		tokens = append(tokens, string(runes[start:i]))
	}
	return tokens
}

func isTokenSeparator(r rune) bool {
	return unicode.IsSpace(r) || strings.ContainsRune(".,;()", r)
}

func isClauseToken(value string) bool {
	switch strings.ToLower(value) {
	case "where", "group", "order", "limit", "offset", "fill", "tz", "slimit", "soffset":
		return true
	default:
		return false
	}
}

func cleanMeasurement(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, `"'`)
	if value == "" {
		return ""
	}
	parts := strings.Split(value, ".")
	for i := len(parts) - 1; i >= 0; i-- {
		part := strings.Trim(strings.TrimSpace(parts[i]), `"'`)
		if part != "" {
			return part
		}
	}
	return value
}

func firstSchemaMeasurement(snapshot schema.Snapshot) string {
	if len(snapshot.Measurements) == 0 {
		return ""
	}
	return snapshot.Measurements[0].Name
}

func findMeasurement(snapshot schema.Snapshot, name string) schema.Measurement {
	for _, measurement := range snapshot.Measurements {
		if measurement.Name == name {
			return measurement
		}
	}
	if len(snapshot.Measurements) > 0 && name == "" {
		return snapshot.Measurements[0]
	}
	return schema.Measurement{}
}

func oneLine(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.Join(strings.Fields(value), " ")
	if len(value) > 160 {
		return value[:157] + "..."
	}
	return value
}

func printValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func truncateRunes(value string, width int) string {
	if width <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= width {
		return value
	}
	if width <= 3 {
		return strings.Repeat(".", width)
	}
	return string(runes[:width-3]) + "..."
}

func formatDuration(value time.Duration) string {
	if value <= 0 {
		return "-"
	}
	if value < time.Millisecond {
		return value.String()
	}
	return value.Truncate(time.Millisecond).String()
}

func clampIndex(index, length int) int {
	if length <= 0 {
		return 0
	}
	if index < 0 {
		return 0
	}
	if index >= length {
		return length - 1
	}
	return index
}

func historyDetail(entry history.Entry) string {
	parts := make([]string, 0, 3)
	if !entry.Time.IsZero() {
		parts = append(parts, entry.Time.Local().Format("2006-01-02 15:04:05"))
	}
	if entry.Database != "" || entry.RetentionPolicy != "" {
		parts = append(parts, fmt.Sprintf("%s/%s", printValue(entry.Database), printValue(entry.RetentionPolicy)))
	}
	if entry.Dialect != "" {
		parts = append(parts, entry.Dialect)
	}
	return strings.Join(parts, " | ")
}
