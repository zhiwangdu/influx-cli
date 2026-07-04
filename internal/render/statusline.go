package render

import "strings"

const (
	ansiReset  = "\x1b[0m"
	ansiBold   = "\x1b[1m"
	ansiDim    = "\x1b[2m"
	ansiCyan   = "\x1b[36m"
	ansiBlue   = "\x1b[34m"
	ansiGreen  = "\x1b[32m"
	ansiRed    = "\x1b[31m"
	ansiYellow = "\x1b[33m"
)

func RenderStatusLine(status string, options Options) string {
	width := options.Width
	if width <= 0 {
		width = 80
	}
	if width < 20 {
		width = 20
	}
	if width > 160 {
		width = 160
	}

	return "\n" + strings.Repeat("-", width) + "\n" + ColorizeStatusLine(status, options.Color)
}

func ColorizeStatusLine(status string, color bool) string {
	if !color {
		return status
	}

	segments := strings.Split(status, " | ")
	for i, segment := range segments {
		segments[i] = colorizeStatusSegment(segment)
	}
	return strings.Join(segments, dim(" | "))
}

func colorizeStatusSegment(segment string) string {
	lower := strings.ToLower(segment)
	switch {
	case lower == "ok":
		return greenBold(segment)
	case strings.HasPrefix(lower, "error:"):
		return redBold(segment)
	case strings.HasPrefix(lower, "db:"):
		return colorizeKeyValue(segment, ansiCyan)
	case strings.HasPrefix(lower, "rp:"):
		return colorizeKeyValue(segment, ansiCyan)
	case strings.HasPrefix(lower, "mode:"):
		return colorizeKeyValue(segment, ansiBlue)
	case strings.HasPrefix(lower, "latency:"):
		return colorizeKeyValue(segment, ansiYellow)
	default:
		return segment
	}
}

func colorizeKeyValue(segment, valueColor string) string {
	key, value, ok := strings.Cut(segment, ":")
	if !ok {
		return segment
	}
	return dim(key+":") + " " + valueColor + strings.TrimSpace(value) + ansiReset
}

func dim(value string) string {
	return ansiDim + value + ansiReset
}

func greenBold(value string) string {
	return ansiBold + ansiGreen + value + ansiReset
}

func redBold(value string) string {
	return ansiBold + ansiRed + value + ansiReset
}
