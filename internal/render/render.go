package render

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

const (
	FormatAuto      = "auto"
	FormatTable     = "table"
	FormatSparkline = "sparkline"
	FormatJSON      = "json"
)

type Options struct {
	Format    string
	Width     int
	MaxRows   int
	MaxSeries int
}

func Render(res result.Result, options Options) (string, string, error) {
	format := strings.ToLower(strings.TrimSpace(options.Format))
	if format == "" {
		format = FormatAuto
	}
	if format == FormatAuto {
		if len(res.Series) > 0 {
			format = FormatSparkline
		} else {
			format = FormatTable
		}
	}

	switch format {
	case FormatTable:
		return RenderTable(res, options), FormatTable, nil
	case FormatSparkline:
		return RenderSparkline(res, options), FormatSparkline, nil
	case FormatJSON:
		body, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			return "", "", fmt.Errorf("render json: %w", err)
		}
		return string(body), FormatJSON, nil
	default:
		return "", "", fmt.Errorf("unknown render format %q", options.Format)
	}
}
