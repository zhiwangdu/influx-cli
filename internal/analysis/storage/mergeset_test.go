package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalyzeMergesetPartMetadata(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "41_2_1847A3A45055EEF0")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  41,
		BlocksCount: 2,
		FirstItem:   "6161",
		LastItem:    "7a7a",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format:         FormatAuto,
		KeySampleLimit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	file := report.Files[0]
	if got, want := file.Format, FormatMergeset; got != want {
		t.Fatalf("format = %s, want %s", got, want)
	}
	if got, want := file.KeyCount, 41; got != want {
		t.Fatalf("key count = %d, want %d", got, want)
	}
	if got, want := file.BlockCount, 2; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if got, want := file.BlocksByType["mergeset-block"], 2; got != want {
		t.Fatalf("mergeset block count = %d, want %d", got, want)
	}
	if got, want := file.KeySamples, []string{"first:6161", "last:7a7a"}; !equalStrings(got, want) {
		t.Fatalf("key samples = %v, want %v", got, want)
	}
	if got, want := file.Extra["part_suffix"], "1847A3A45055EEF0"; got != want {
		t.Fatalf("part suffix = %q, want %q", got, want)
	}
	if got, want := file.Extra["items_count"], "41"; got != want {
		t.Fatalf("items count extra = %q, want %q", got, want)
	}
	if got, want := file.Extra["first_item_bytes"], "2"; got != want {
		t.Fatalf("first item bytes = %q, want %q", got, want)
	}
	if file.SizeBytes == 0 {
		t.Fatal("expected non-zero component size")
	}
}

func TestAnalyzeMergesetDirectoryExpansion(t *testing.T) {
	dir := t.TempDir()
	partPath := filepath.Join(dir, "3_1_0000000000000001")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  3,
		BlocksCount: 1,
		FirstItem:   "01",
		LastItem:    "03",
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "tmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file.unsupported"), []byte("ignored"), 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{dir}, Options{
		Format:         FormatAuto,
		KeySampleLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 1; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if got, want := report.Files[0].Path, partPath; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
}

func TestAnalyzeMergesetBlocksMismatchNotice(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "10_2_0000000000000001")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  10,
		BlocksCount: 1,
		FirstItem:   "01",
		LastItem:    "02",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format: FormatMergeset,
	})
	if err != nil {
		t.Fatal(err)
	}
	file := report.Files[0]
	if got, want := file.BlockCount, 1; got != want {
		t.Fatalf("block count = %d, want %d", got, want)
	}
	if len(file.Notices) != 1 || !strings.Contains(file.Notices[0], "differs from metadata") {
		t.Fatalf("notices = %v, want block-count mismatch notice", file.Notices)
	}
	if len(report.Notices) != 1 || !strings.Contains(report.Notices[0], "differs from metadata") {
		t.Fatalf("report notices = %v, want propagated block-count mismatch notice", report.Notices)
	}
}

func TestAnalyzeMergesetItemsMismatchErrors(t *testing.T) {
	partPath := filepath.Join(t.TempDir(), "10_1_0000000000000001")
	if err := writeTestMergesetPart(partPath, mergesetPartMetadata{
		ItemsCount:  9,
		BlocksCount: 1,
		FirstItem:   "01",
		LastItem:    "02",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := Analyze(context.Background(), []string{partPath}, Options{
		Format: FormatMergeset,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Files), 0; got != want {
		t.Fatalf("file count = %d, want %d", got, want)
	}
	if len(report.Notices) != 1 || !strings.Contains(report.Notices[0], "invalid mergeset ItemsCount") {
		t.Fatalf("report notices = %v, want items-count mismatch notice", report.Notices)
	}
}

func writeTestMergesetPart(path string, metadata mergesetPartMetadata) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, mergesetMetadataFile), data, 0o600); err != nil {
		return err
	}
	for _, name := range []string{
		mergesetMetaindexFile,
		mergesetIndexFile,
		mergesetItemsFile,
		mergesetLensFile,
	} {
		if err := os.WriteFile(filepath.Join(path, name), []byte(name), 0o600); err != nil {
			return err
		}
	}
	return nil
}
