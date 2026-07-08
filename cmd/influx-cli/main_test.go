package main

import (
	"bytes"
	"os"
	"path/filepath"
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

func TestParseStorageFieldFilters(t *testing.T) {
	got, err := parseStorageFieldFilters([]string{
		" value = 99 ",
		"",
		"status!=true",
		"temperature>=1.5",
		"usage<10",
		"load<=2",
		"label!=a=b",
		"message=~^err",
		"message!~ok$",
		"status in (true,false)",
		"message in (\"error, transit\",\"ok)\")",
		"value in(1.25,2.5)",
		"value not in (1.25,2.5)",
		"region not-in(us,eu)",
		"device !in (cpu,mem)",
		"rack !in(us-east,us-west)",
		"value is null",
		"status is-not true",
		"region is not us",
		"notes in review is null",
		"message=error in transit",
		"notes in review>5",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		key   string
		op    string
		value string
	}{
		{"value", "", "99"},
		{"status", "!=", "true"},
		{"temperature", ">=", "1.5"},
		{"usage", "<", "10"},
		{"load", "<=", "2"},
		{"label", "!=", "a=b"},
		{"message", "=~", "^err"},
		{"message", "!~", "ok$"},
		{"status", "in", "(true,false)"},
		{"message", "in", "(\"error, transit\",\"ok)\")"},
		{"value", "in", "(1.25,2.5)"},
		{"value", "not-in", "(1.25,2.5)"},
		{"region", "not-in", "(us,eu)"},
		{"device", "not-in", "(cpu,mem)"},
		{"rack", "not-in", "(us-east,us-west)"},
		{"value", "", "null"},
		{"status", "!=", "true"},
		{"region", "!=", "us"},
		{"notes in review", "", "null"},
		{"message", "", "error in transit"},
		{"notes in review", ">", "5"},
	}
	if len(got) != len(want) {
		t.Fatalf("field filters = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Key != want[i].key || got[i].Op != want[i].op || got[i].Value != want[i].value {
			t.Fatalf("field filter %d = %+v, want key=%q op=%q value=%q", i, got[i], want[i].key, want[i].op, want[i].value)
		}
	}
}

func TestParseStorageFieldFiltersUsesLeftmostSymbolOperator(t *testing.T) {
	got, err := parseStorageFieldFilters([]string{
		`message=~[<>=!]`,
		`message!~[^!=]`,
		`message!~=foo`,
		`message=count>=5`,
		`message=a<>b`,
		`status==true`,
		`status<>false`,
		`temperature>=>`,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		key   string
		op    string
		value string
	}{
		{"message", "=~", "[<>=!]"},
		{"message", "!~", "[^!=]"},
		{"message", "!~", "=foo"},
		{"message", "", "count>=5"},
		{"message", "", "a<>b"},
		{"status", "", "true"},
		{"status", "!=", "false"},
		{"temperature", ">=", ">"},
	}
	if len(got) != len(want) {
		t.Fatalf("field filters = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Key != want[i].key || got[i].Op != want[i].op || got[i].Value != want[i].value {
			t.Fatalf("field filter %d = %+v, want key=%q op=%q value=%q", i, got[i], want[i].key, want[i].op, want[i].value)
		}
	}
}

func TestParseStorageFieldFiltersAllowsEmptyEqualityAndInequalityValues(t *testing.T) {
	got, err := parseStorageFieldFilters([]string{"empty=", "also==", "notempty<>", "notalso!="})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("field filters = %d, want 4", len(got))
	}
	if got[0].Key != "empty" || got[0].Op != "" || got[0].Value != "" {
		t.Fatalf("first field filter = %+v, want empty equality", got[0])
	}
	if got[1].Key != "also" || got[1].Op != "" || got[1].Value != "" {
		t.Fatalf("second field filter = %+v, want double-equals empty equality", got[1])
	}
	if got[2].Key != "notempty" || got[2].Op != "!=" || got[2].Value != "" {
		t.Fatalf("third field filter = %+v, want not-equals empty inequality", got[2])
	}
	if got[3].Key != "notalso" || got[3].Op != "!=" || got[3].Value != "" {
		t.Fatalf("fourth field filter = %+v, want bang-equals empty inequality", got[3])
	}
}

func TestParseStorageFieldFiltersParsesBetweenOperators(t *testing.T) {
	got, err := parseStorageFieldFilters([]string{
		"value between (1,3)",
		"plain between(1,3)",
		"value not-between(1,3)",
		"region not between (a,z)",
		"zone !between (a,z)",
		"name between (\"a=b\",\"z\")",
		"status in (\"a<>b\",\"c\")",
		"name is between (1,3)",
		"raw between 1,3",
		"raw not between 1,3",
		"disk !between 1,3",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		key   string
		op    string
		value string
	}{
		{"value", "between", "(1,3)"},
		{"plain", "between", "(1,3)"},
		{"value", "not-between", "(1,3)"},
		{"region", "not-between", "(a,z)"},
		{"zone", "not-between", "(a,z)"},
		{"name", "between", "(\"a=b\",\"z\")"},
		{"status", "in", "(\"a<>b\",\"c\")"},
		{"name", "", "between (1,3)"},
		{"raw", "between", "1,3"},
		{"raw", "not-between", "1,3"},
		{"disk", "not-between", "1,3"},
	}
	if len(got) != len(want) {
		t.Fatalf("field filters = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Key != want[i].key || got[i].Op != want[i].op || got[i].Value != want[i].value {
			t.Fatalf("field filter %d = %+v, want key=%q op=%q value=%q", i, got[i], want[i].key, want[i].op, want[i].value)
		}
	}
}

func TestParseStorageFieldFiltersRejectsMalformedValues(t *testing.T) {
	if _, err := parseStorageFieldFilters([]string{"value"}); err == nil || !strings.Contains(err.Error(), "key=value") || !strings.Contains(err.Error(), "key=~regex") {
		t.Fatalf("error = %v, want key=value guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"=99"}); err == nil || !strings.Contains(err.Error(), "key cannot be empty") {
		t.Fatalf("error = %v, want empty key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"value>"}); err == nil || !strings.Contains(err.Error(), "value cannot be empty") {
		t.Fatalf("error = %v, want empty comparison value guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not in (1,2)"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"!in (1,2)"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing bang-in key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not between (1,2)"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing between key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"!between (1,2)"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing bang-between key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not contains value"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing contains key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not like value"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing like key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not starts with value"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing starts-with key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not ends with value"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing ends-with key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not is value"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing is key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not is not value"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing is-not key guidance", err)
	}
}

func TestParseStorageFieldFiltersParsesStringOperators(t *testing.T) {
	got, err := parseStorageFieldFilters([]string{
		"message contains error",
		"notice contains warning",
		"message like bl%",
		"message not like r_d",
		"message not-like tmp%",
		"message !like _tmp%",
		"message not contains ok",
		"message !contains retry",
		"notify starts with edge",
		"region not-contains us",
		"host starts-with edge",
		"host starts with edge",
		"path not-starts-with tmp",
		"path not starts with tmp",
		"path !starts-with var",
		"region ends-with east",
		"region ends with west",
		"device not-ends-with old",
		"device not ends with old",
		"device !ends-with stale",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		key   string
		op    string
		value string
	}{
		{"message", "contains", "error"},
		{"notice", "contains", "warning"},
		{"message", "like", "bl%"},
		{"message", "not-like", "r_d"},
		{"message", "not-like", "tmp%"},
		{"message", "not-like", "_tmp%"},
		{"message", "not-contains", "ok"},
		{"message", "not-contains", "retry"},
		{"notify", "starts-with", "edge"},
		{"region", "not-contains", "us"},
		{"host", "starts-with", "edge"},
		{"host", "starts-with", "edge"},
		{"path", "not-starts-with", "tmp"},
		{"path", "not-starts-with", "tmp"},
		{"path", "not-starts-with", "var"},
		{"region", "ends-with", "east"},
		{"region", "ends-with", "west"},
		{"device", "not-ends-with", "old"},
		{"device", "not-ends-with", "old"},
		{"device", "not-ends-with", "stale"},
	}
	if len(got) != len(want) {
		t.Fatalf("field filters = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Key != want[i].key || got[i].Op != want[i].op || got[i].Value != want[i].value {
			t.Fatalf("field filter %d = %+v, want key=%q op=%q value=%q", i, got[i], want[i].key, want[i].op, want[i].value)
		}
	}
}

func TestParseStorageFieldFiltersIgnoresWordOperatorsInsideSets(t *testing.T) {
	got, err := parseStorageFieldFilters([]string{"status in (this is true,not in service)"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("field filters = %d, want 1", len(got))
	}
	if got[0].Key != "status" || got[0].Op != "in" || got[0].Value != "(this is true,not in service)" {
		t.Fatalf("field filter = %+v, want status in full set", got[0])
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

func TestStorageAnalyzeColumnFlagRegistered(t *testing.T) {
	cmd := newRootCommand()
	found, _, err := cmd.Find([]string{"storage", "analyze"})
	if err != nil {
		t.Fatal(err)
	}
	if flag := found.Flags().Lookup("column"); flag == nil {
		t.Fatal("storage analyze --column flag is not registered")
	}
	if flag := found.Flags().Lookup("field"); flag == nil {
		t.Fatal("storage analyze --field flag is not registered")
	}
	if flag := found.Flags().Lookup("field-any"); flag == nil {
		t.Fatal("storage analyze --field-any flag is not registered")
	}
	if flag := found.Flags().Lookup("field-none"); flag == nil {
		t.Fatal("storage analyze --field-none flag is not registered")
	}
	if flag := found.Flags().Lookup("report"); flag == nil {
		t.Fatal("storage analyze --report flag is not registered")
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

func TestStorageAnalyzeMergesetKeyDoesNotRequireRange(t *testing.T) {
	cmd := newRootCommand()
	cmd.SetArgs([]string{"storage", "analyze", "--storage-format", "mergeset", "--key", "aa", "missing"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected missing path error")
	}
	if strings.Contains(err.Error(), "--key requires --from and --to") {
		t.Fatalf("error = %v, want mergeset key search to allow key-only lookup", err)
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

func TestStorageAnalyzeColumnRequiresRange(t *testing.T) {
	cmd := newRootCommand()
	cmd.SetArgs([]string{"storage", "analyze", "--column", "value", "missing.tssp"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--column requires --from and --to") {
		t.Fatalf("error = %v, want column range requirement", err)
	}
}

func TestStorageAnalyzeFieldRequiresRange(t *testing.T) {
	cmd := newRootCommand()
	cmd.SetArgs([]string{"storage", "analyze", "--field", "value=99", "missing.tssp"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--field requires --from and --to") {
		t.Fatalf("error = %v, want field range requirement", err)
	}
}

func TestStorageAnalyzeAnyFieldRequiresRange(t *testing.T) {
	cmd := newRootCommand()
	cmd.SetArgs([]string{"storage", "analyze", "--field-any", "value=99", "missing.tssp"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--field-any requires --from and --to") {
		t.Fatalf("error = %v, want OR field range requirement", err)
	}
}

func TestStorageAnalyzeNoneFieldRequiresRange(t *testing.T) {
	cmd := newRootCommand()
	cmd.SetArgs([]string{"storage", "analyze", "--field-none", "value=99", "missing.tssp"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--field-none requires --from and --to") {
		t.Fatalf("error = %v, want NOT field range requirement", err)
	}
}

func TestStorageAnalyzeTableWarningsAreCountOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fields.idxl")
	if err := os.WriteFile(path, []byte{1}, 0o600); err != nil {
		t.Fatalf("write fields index log: %v", err)
	}

	cmd := newRootCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--width", "240", "storage", "analyze", "--storage-format", "fields-index", path})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if got := stdout.String(); !strings.Contains(got, "notices=1") {
		t.Fatalf("stdout = %q, want notice count in table details", got)
	}
	warning := stderr.String()
	if !strings.Contains(warning, "storage analyzer produced 1 notice(s)") {
		t.Fatalf("stderr = %q, want count-only warning", warning)
	}
	for _, notWant := range []string{path, "offset"} {
		if strings.Contains(warning, notWant) {
			t.Fatalf("stderr = %q, want no raw notice detail %q", warning, notWant)
		}
	}
}

func TestStorageAnalyzeReportOutputsMarkdownDiagnostic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fields.idxl")
	if err := os.WriteFile(path, []byte{1}, 0o600); err != nil {
		t.Fatalf("write fields index log: %v", err)
	}

	cmd := newRootCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"storage", "analyze", "--report", "--storage-format", "fields-index", path})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"# Storage Analyzer Report",
		"## Summary",
		"| files | 1 |",
		"| notices | 1 |",
		"## Files",
		"| file-1 | fields-index |",
		"notices=1",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout = %q, want %q", out, want)
		}
	}
	for _, notWant := range []string{path, "offset"} {
		if strings.Contains(out, notWant) {
			t.Fatalf("stdout = %q, want no raw report detail %q", out, notWant)
		}
	}
	if got := strings.TrimSpace(stderr.String()); got != "" {
		t.Fatalf("stderr = %q, want no table warning for markdown report", got)
	}
}
