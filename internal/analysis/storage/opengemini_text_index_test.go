package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeOpenGeminiTextIndexExplicitFileIsSkipped(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000001.tssp.content")
	partPath := base + opengeminiTextIndexPartSuffix
	if err := os.WriteFile(partPath, []byte("not a part header"), 0o600); err != nil {
		t.Fatalf("WriteFile(.ph) error = %v", err)
	}
	if err := os.Mkdir(base+opengeminiTextIndexHeadSuffix, 0o700); err != nil {
		t.Fatalf("Mkdir(.bh) error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format: FormatOpenGeminiText,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatOpenGeminiText; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got := file.BlockCount; got != 0 {
		t.Fatalf("block count = %d, want 0 for skipped analysis", got)
	}
	if len(file.Blocks) != 0 {
		t.Fatalf("blocks = %v, want none for skipped analysis", file.Blocks)
	}
	if file.SecondaryIndex != nil {
		t.Fatalf("secondary index summary = %+v, want nil for skipped analysis", file.SecondaryIndex)
	}
	for key, want := range map[string]string{
		"layout":          opengeminiTextIndexLayout,
		"field":           "content",
		"input_component": "part",
		"skipped":         "true",
		"skip_reason":     "text_index_analysis_disabled",
		"local_only":      "true",
	} {
		if got := file.Extra[key]; got != want {
			t.Fatalf("extra[%s] = %q, want %q", key, got, want)
		}
	}
	if !containsOpenGeminiTextNotice(file.Notices, "analysis is skipped") {
		t.Fatalf("notices = %v, want skipped notice", file.Notices)
	}
	if !containsOpenGeminiTextNotice(report.Notices, "analysis is skipped") {
		t.Fatalf("report notices = %v, want propagated skipped notice", report.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexAutoDirectFileIsSkipped(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "00000001-0001-00000002.tssp.message"+opengeminiTextIndexDataSuffix)
	if err := os.WriteFile(dataPath, []byte("not text index payload"), 0o600); err != nil {
		t.Fatalf("WriteFile(.pos) error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{dataPath}, Options{
		Format: FormatAuto,
	})
	if err != nil {
		t.Fatalf("Analyze() error = %v", err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatOpenGeminiText; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.Extra["input_component"], "data"; got != want {
		t.Fatalf("input component = %q, want %q", got, want)
	}
	if !containsOpenGeminiTextNotice(file.Notices, "analysis is skipped") {
		t.Fatalf("notices = %v, want skipped notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexDirectoryExpansionSkipsSidecars(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000003.tssp.content")
	for _, suffix := range []string{opengeminiTextIndexPartSuffix, opengeminiTextIndexHeadSuffix, opengeminiTextIndexDataSuffix} {
		if err := os.WriteFile(base+suffix, []byte("text index sidecar"), 0o600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", suffix, err)
		}
	}

	for _, format := range []Format{FormatAuto, FormatOpenGeminiText} {
		report, err := Analyze(context.Background(), []string{dir}, Options{
			Format: format,
		})
		if err != nil {
			t.Fatalf("Analyze(%s) error = %v", format, err)
		}
		if got := len(report.Files); got != 0 {
			t.Fatalf("Analyze(%s) file count = %d, want 0; files=%v notices=%v", format, got, report.Files, report.Notices)
		}
	}
}

func containsOpenGeminiTextNotice(notices []string, want string) bool {
	for _, notice := range notices {
		if strings.Contains(notice, want) {
			return true
		}
	}
	return false
}
