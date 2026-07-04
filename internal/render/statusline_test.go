package render

import (
	"strings"
	"testing"
)

func TestRenderStatusLineAddsBlankLineAndDivider(t *testing.T) {
	output := RenderStatusLine("db: metrics | rp: autogen | mode: influxql | latency: 1ms | ok", Options{Width: 24})

	if !strings.HasPrefix(output, "\n------------------------\n") {
		t.Fatalf("status block missing blank line and divider: %q", output)
	}
	if !strings.Contains(output, "db: metrics") {
		t.Fatalf("status block missing status: %q", output)
	}
}

func TestColorizeStatusLineHighlightsFieldsAndState(t *testing.T) {
	output := ColorizeStatusLine("db: metrics | rp: - | mode: influxql | latency: 1ms | ok", true)

	for _, want := range []string{ansiCyan, ansiBlue, ansiYellow, ansiGreen} {
		if !strings.Contains(output, want) {
			t.Fatalf("colored status line missing %q: %q", want, output)
		}
	}
}

func TestColorizeStatusLineCanBeDisabled(t *testing.T) {
	status := "db: metrics | rp: - | mode: influxql | latency: 1ms | ok"
	output := ColorizeStatusLine(status, false)

	if output != status {
		t.Fatalf("output = %q, want %q", output, status)
	}
}
