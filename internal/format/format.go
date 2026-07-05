package format

import (
	"fmt"
	"strings"
)

const (
	Auto      = "auto"
	Table     = "table"
	Sparkline = "sparkline"
	JSON      = "json"
)

func Normalize(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return Table, nil
	}
	switch normalized {
	case Auto, Table, Sparkline, JSON:
		return normalized, nil
	default:
		return "", fmt.Errorf("unknown render format %q", value)
	}
}
