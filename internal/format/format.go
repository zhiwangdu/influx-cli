package format

import (
	"fmt"
	"strings"
)

const (
	Auto      = "auto"
	Table     = "table"
	Sparkline = "sparkline"
	Chart     = "chart"
	JSON      = "json"
)

func Normalize(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return Table, nil
	}
	switch normalized {
	case Auto, Table, Sparkline, Chart, JSON:
		return normalized, nil
	default:
		return "", fmt.Errorf("unknown render format %q", value)
	}
}
