package render

import (
	"encoding/json"
	"fmt"

	renderformat "github.com/zhiwangdu/influx-cli/internal/format"
	"github.com/zhiwangdu/influx-cli/internal/result"
)

const (
	FormatAuto      = renderformat.Auto
	FormatTable     = renderformat.Table
	FormatSparkline = renderformat.Sparkline
	FormatJSON      = renderformat.JSON
)

type Options struct {
	Format    string
	Width     int
	MaxRows   int
	MaxSeries int
	Color     bool
}

func Render(res result.Result, options Options) (string, string, error) {
	format, err := NormalizeFormat(options.Format)
	if err != nil {
		return "", "", err
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
	}
	return "", "", fmt.Errorf("unknown render format %q", options.Format)
}

func NormalizeFormat(format string) (string, error) {
	return renderformat.Normalize(format)
}
