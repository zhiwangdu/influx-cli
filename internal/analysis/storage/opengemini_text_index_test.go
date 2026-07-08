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
	if _, ok := file.Extra["input_is_directory"]; ok {
		t.Fatalf("input_is_directory = %q, want omitted for direct file input", file.Extra["input_is_directory"])
	}
	if !containsOpenGeminiTextNotice(file.Notices, "analysis is skipped") {
		t.Fatalf("notices = %v, want skipped notice", file.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexDirectFileWithNonTextFormatIsSkipped(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "00000001-0001-00000003.tssp.message"+opengeminiTextIndexPartSuffix)
	if err := os.WriteFile(partPath, []byte("not a TSM file and should not be read as one"), 0o600); err != nil {
		t.Fatalf("WriteFile(.ph) error = %v", err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format: FormatTSM,
	})
	if err != nil {
		t.Fatalf("Analyze(%s) error = %v", FormatTSM, err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d; files=%v notices=%v", got, want, report.Files, report.Notices)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatOpenGeminiText; got != want {
		t.Fatalf("format = %q, want %q", got, want)
	}
	if got, want := file.Extra["input_component"], "part"; got != want {
		t.Fatalf("input component = %q, want %q", got, want)
	}
	if got := file.Extra["skipped"]; got != "true" {
		t.Fatalf("skipped = %q, want true", got)
	}
	if !containsOpenGeminiTextNotice(file.Notices, "analysis is skipped") {
		t.Fatalf("notices = %v, want skipped notice", file.Notices)
	}
	if len(report.Notices) != 1 || !containsOpenGeminiTextNotice(report.Notices, "analysis is skipped") {
		t.Fatalf("report notices = %v, want only skipped notice", report.Notices)
	}
}

func TestAnalyzeOpenGeminiTextIndexDirectSidecarDirectoryIsSkipped(t *testing.T) {
	dir := t.TempDir()
	headPath := filepath.Join(dir, "00000001-0001-00000004.tssp.message"+opengeminiTextIndexHeadSuffix)
	if err := os.Mkdir(headPath, 0o700); err != nil {
		t.Fatalf("Mkdir(.bh) error = %v", err)
	}
	if err := writeTestTSM(filepath.Join(headPath, "ignored.tsm")); err != nil {
		t.Fatalf("write ignored child TSM: %v", err)
	}

	for _, format := range []Format{FormatAuto, FormatOpenGeminiText, FormatTSM} {
		report, err := Analyze(context.Background(), []string{headPath}, Options{
			Format:    format,
			Recursive: true,
		})
		if err != nil {
			t.Fatalf("Analyze(%s) error = %v", format, err)
		}
		if got, want := len(report.Files), 1; got != want {
			t.Fatalf("Analyze(%s) file count = %d, want %d; files=%v notices=%v", format, got, want, report.Files, report.Notices)
		}
		file := report.Files[0]
		if got, want := file.Path, headPath; got != want {
			t.Fatalf("Analyze(%s) path = %q, want direct sidecar directory %q", format, got, want)
		}
		if got, want := file.Format, FormatOpenGeminiText; got != want {
			t.Fatalf("Analyze(%s) format = %q, want %q", format, got, want)
		}
		if got := file.SizeBytes; got != 0 {
			t.Fatalf("Analyze(%s) size bytes = %d, want 0 for skipped directory input", format, got)
		}
		if got := report.Summary.TotalSizeBytes; got != 0 {
			t.Fatalf("Analyze(%s) summary total size bytes = %d, want 0 for skipped directory input", format, got)
		}
		for key, want := range map[string]string{
			"input_component":    "head",
			"input_is_directory": "true",
			"skipped":            "true",
			"skip_reason":        "text_index_analysis_disabled",
			"local_only":         "true",
		} {
			if got := file.Extra[key]; got != want {
				t.Fatalf("Analyze(%s) extra[%s] = %q, want %q", format, key, got, want)
			}
		}
		if !containsOpenGeminiTextNotice(file.Notices, "analysis is skipped") {
			t.Fatalf("Analyze(%s) notices = %v, want skipped notice", format, file.Notices)
		}
	}
}

func TestAnalyzeOpenGeminiTextIndexDirectSidecarDirectoryVariantsAreSkipped(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		suffix    string
		component string
	}{
		{suffix: opengeminiTextIndexDataSuffix, component: "data"},
		{suffix: opengeminiTextIndexHeadSuffix, component: "head"},
		{suffix: opengeminiTextIndexPartSuffix, component: "part"},
	} {
		sidecarPath := filepath.Join(dir, "00000001-0001-00000007.tssp."+tc.component+tc.suffix)
		if err := os.Mkdir(sidecarPath, 0o700); err != nil {
			t.Fatalf("Mkdir(%s) error = %v", tc.suffix, err)
		}
		if err := writeTestTSM(filepath.Join(sidecarPath, "ignored.tsm")); err != nil {
			t.Fatalf("write ignored child TSM: %v", err)
		}

		report, err := Analyze(context.Background(), []string{sidecarPath + string(os.PathSeparator)}, Options{
			Format: FormatAuto,
		})
		if err != nil {
			t.Fatalf("Analyze(%s) error = %v", tc.suffix, err)
		}
		if got, want := len(report.Files), 1; got != want {
			t.Fatalf("Analyze(%s) file count = %d, want %d; files=%v notices=%v", tc.suffix, got, want, report.Files, report.Notices)
		}
		file := report.Files[0]
		if got, want := file.Path, sidecarPath; got != want {
			t.Fatalf("Analyze(%s) path = %q, want cleaned sidecar path %q", tc.suffix, got, want)
		}
		if got, want := file.Format, FormatOpenGeminiText; got != want {
			t.Fatalf("Analyze(%s) format = %q, want %q", tc.suffix, got, want)
		}
		if got := file.SizeBytes; got != 0 {
			t.Fatalf("Analyze(%s) size bytes = %d, want 0 for skipped directory input", tc.suffix, got)
		}
		if got := file.Extra["input_component"]; got != tc.component {
			t.Fatalf("Analyze(%s) input component = %q, want %q", tc.suffix, got, tc.component)
		}
		if got := file.Extra["input_is_directory"]; got != "true" {
			t.Fatalf("Analyze(%s) input_is_directory = %q, want true", tc.suffix, got)
		}
		if !containsOpenGeminiTextNotice(file.Notices, "analysis is skipped") {
			t.Fatalf("Analyze(%s) notices = %v, want skipped notice", tc.suffix, file.Notices)
		}
	}
}

func TestAnalyzeOpenGeminiTextIndexDirectoryExpansionSkipsSidecars(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "00000001-0001-00000008.tssp.content")
	for _, suffix := range []string{opengeminiTextIndexPartSuffix, opengeminiTextIndexHeadSuffix, opengeminiTextIndexDataSuffix} {
		if err := os.WriteFile(base+suffix, []byte("text index sidecar"), 0o600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", suffix, err)
		}
	}
	headDir := filepath.Join(dir, "00000001-0001-00000009.tssp.message"+opengeminiTextIndexHeadSuffix)
	if err := os.Mkdir(headDir, 0o700); err != nil {
		t.Fatalf("Mkdir(.bh) error = %v", err)
	}
	if err := writeTestTSM(filepath.Join(headDir, "ignored.tsm")); err != nil {
		t.Fatalf("write ignored child TSM: %v", err)
	}

	for _, tc := range []struct {
		format    Format
		recursive bool
	}{
		{format: FormatAuto},
		{format: FormatOpenGeminiText},
		{format: FormatAuto, recursive: true},
		{format: FormatOpenGeminiText, recursive: true},
		{format: FormatTSM, recursive: true},
	} {
		report, err := Analyze(context.Background(), []string{dir}, Options{
			Format:    tc.format,
			Recursive: tc.recursive,
		})
		if err != nil {
			t.Fatalf("Analyze(%s recursive=%t) error = %v", tc.format, tc.recursive, err)
		}
		if got := len(report.Files); got != 0 {
			t.Fatalf("Analyze(%s recursive=%t) file count = %d, want 0; files=%v notices=%v", tc.format, tc.recursive, got, report.Files, report.Notices)
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
