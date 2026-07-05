package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseStorageRange(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		got, err := parseStorageRange("", "")
		if err != nil {
			t.Fatal(err)
		}
		if got.Set {
			t.Fatalf("range set = true, want false")
		}
	})

	t.Run("rfc3339", func(t *testing.T) {
		got, err := parseStorageRange("2026-07-05T00:00:00Z", "2026-07-05T01:00:00Z")
		if err != nil {
			t.Fatal(err)
		}
		wantMin := time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC).UnixNano()
		wantMax := time.Date(2026, 7, 5, 1, 0, 0, 0, time.UTC).UnixNano()
		if got.Min != wantMin || got.Max != wantMax || !got.Set {
			t.Fatalf("range = %+v, want min=%d max=%d set=true", got, wantMin, wantMax)
		}
	})

	t.Run("unix nanoseconds", func(t *testing.T) {
		got, err := parseStorageRange("10", "20")
		if err != nil {
			t.Fatal(err)
		}
		if got.Min != 10 || got.Max != 20 || !got.Set {
			t.Fatalf("range = %+v, want min=10 max=20 set=true", got)
		}
	})

	t.Run("requires both endpoints", func(t *testing.T) {
		_, err := parseStorageRange("10", "")
		if err == nil || !strings.Contains(err.Error(), "both --from and --to") {
			t.Fatalf("error = %v, want both-endpoints error", err)
		}
	})

	t.Run("rejects inverted range", func(t *testing.T) {
		_, err := parseStorageRange("20", "10")
		if err == nil || !strings.Contains(err.Error(), "greater than") {
			t.Fatalf("error = %v, want inverted range error", err)
		}
	})
}

func TestParseStorageTimeRejectsUnknownFormat(t *testing.T) {
	_, err := parseStorageTime("now() - 1h")
	if err == nil || !strings.Contains(err.Error(), "RFC3339") {
		t.Fatalf("error = %v, want format guidance", err)
	}
}

func TestParseStorageTagFilters(t *testing.T) {
	got, err := parseStorageTagFilters([]string{" host = a ", "", "region=us-west"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("tag filters = %d, want 2", len(got))
	}
	if got[0].Key != "host" || got[0].Value != "a" {
		t.Fatalf("first tag filter = %+v, want host=a", got[0])
	}
	if got[1].Key != "region" || got[1].Value != "us-west" {
		t.Fatalf("second tag filter = %+v, want region=us-west", got[1])
	}
}

func TestParseStorageTagFiltersRejectsMalformedValues(t *testing.T) {
	if _, err := parseStorageTagFilters([]string{"host"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want key=value guidance", err)
	}
	if _, err := parseStorageTagFilters([]string{"=a"}); err == nil || !strings.Contains(err.Error(), "key cannot be empty") {
		t.Fatalf("error = %v, want empty key guidance", err)
	}
}

func TestParseStorageSeriesIDs(t *testing.T) {
	got, err := parseStorageSeriesIDs([]string{" 9 ", "", "42"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 9 || got[1] != 42 {
		t.Fatalf("series ids = %v, want [9 42]", got)
	}
}

func TestParseStorageSeriesIDsRejectsMalformedValues(t *testing.T) {
	if _, err := parseStorageSeriesIDs([]string{"abc"}); err == nil || !strings.Contains(err.Error(), "unsigned integer") {
		t.Fatalf("error = %v, want unsigned integer guidance", err)
	}
	if _, err := parseStorageSeriesIDs([]string{"-1"}); err == nil || !strings.Contains(err.Error(), "unsigned integer") {
		t.Fatalf("error = %v, want unsigned integer guidance", err)
	}
}

func TestParseStorageMetaIndexIDs(t *testing.T) {
	got, err := parseStorageMetaIndexIDs([]string{" 9 ", "", "42"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 9 || got[1] != 42 {
		t.Fatalf("meta-index ids = %v, want [9 42]", got)
	}
	if _, err := parseStorageMetaIndexIDs([]string{"abc"}); err == nil || !strings.Contains(err.Error(), "parse --meta-index-id") {
		t.Fatalf("error = %v, want meta-index flag guidance", err)
	}
}

func TestParseStorageCursorDescending(t *testing.T) {
	for _, value := range []string{"", "asc", "ascending"} {
		got, err := parseStorageCursorDescending(value)
		if err != nil {
			t.Fatalf("%q error = %v", value, err)
		}
		if got {
			t.Fatalf("%q descending = true, want false", value)
		}
	}
	for _, value := range []string{"desc", "descending"} {
		got, err := parseStorageCursorDescending(value)
		if err != nil {
			t.Fatalf("%q error = %v", value, err)
		}
		if !got {
			t.Fatalf("%q descending = false, want true", value)
		}
	}
	if _, err := parseStorageCursorDescending("sideways"); err == nil || !strings.Contains(err.Error(), "asc or desc") {
		t.Fatalf("error = %v, want cursor order guidance", err)
	}
}

func TestStorageAnalyzeKeyRequiresRange(t *testing.T) {
	cmd := newRootCommand()
	cmd.SetArgs([]string{"storage", "analyze", "--key", "cpu value", "missing.tsm"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--key requires --from and --to") {
		t.Fatalf("error = %v, want key range requirement", err)
	}
}

func TestStorageAnalyzeSeriesIDRequiresRange(t *testing.T) {
	cmd := newRootCommand()
	cmd.SetArgs([]string{"storage", "analyze", "--series-id", "9", "missing.tssp"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--series-id requires --from and --to") {
		t.Fatalf("error = %v, want series id range requirement", err)
	}
}

func TestStorageAnalyzeMetaIndexIDRequiresRange(t *testing.T) {
	cmd := newRootCommand()
	cmd.SetArgs([]string{"storage", "analyze", "--meta-index-id", "9", "missing.tssp"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--meta-index-id requires --from and --to") {
		t.Fatalf("error = %v, want meta-index id range requirement", err)
	}
}
