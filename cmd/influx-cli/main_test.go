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
	got, err := parseStorageFieldFilters([]string{" value = 99 ", "", "status!=true", "temperature>=1.5", "usage<10", "load<=2", "label!=a=b", "message=~^err", "message!~ok$", "status in (true,false)", "message in (\"error, transit\",\"ok)\")", "value in(1.25,2.5)", "value not in (1.25,2.5)", "region not-in(us,eu)", "value is null", "status is-not true", "region is not us", "notes in review is null", "message=error in transit", "notes in review>5"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 19 {
		t.Fatalf("field filters = %d, want 19", len(got))
	}
	if got[0].Key != "value" || got[0].Op != "" || got[0].Value != "99" {
		t.Fatalf("first field filter = %+v, want value=99", got[0])
	}
	if got[1].Key != "status" || got[1].Op != "!=" || got[1].Value != "true" {
		t.Fatalf("second field filter = %+v, want status!=true", got[1])
	}
	if got[2].Key != "temperature" || got[2].Op != ">=" || got[2].Value != "1.5" {
		t.Fatalf("third field filter = %+v, want temperature>=1.5", got[2])
	}
	if got[3].Key != "usage" || got[3].Op != "<" || got[3].Value != "10" {
		t.Fatalf("fourth field filter = %+v, want usage<10", got[3])
	}
	if got[4].Key != "load" || got[4].Op != "<=" || got[4].Value != "2" {
		t.Fatalf("fifth field filter = %+v, want load<=2", got[4])
	}
	if got[5].Key != "label" || got[5].Op != "!=" || got[5].Value != "a=b" {
		t.Fatalf("sixth field filter = %+v, want label!=a=b", got[5])
	}
	if got[6].Key != "message" || got[6].Op != "=~" || got[6].Value != "^err" {
		t.Fatalf("seventh field filter = %+v, want message=~^err", got[6])
	}
	if got[7].Key != "message" || got[7].Op != "!~" || got[7].Value != "ok$" {
		t.Fatalf("eighth field filter = %+v, want message!~ok$", got[7])
	}
	if got[8].Key != "status" || got[8].Op != "in" || got[8].Value != "(true,false)" {
		t.Fatalf("ninth field filter = %+v, want status in (true,false)", got[8])
	}
	if got[9].Key != "message" || got[9].Op != "in" || got[9].Value != "(\"error, transit\",\"ok)\")" {
		t.Fatalf("tenth field filter = %+v, want quoted message set", got[9])
	}
	if got[10].Key != "value" || got[10].Op != "in" || got[10].Value != "(1.25,2.5)" {
		t.Fatalf("eleventh field filter = %+v, want value in(1.25,2.5)", got[10])
	}
	if got[11].Key != "value" || got[11].Op != "not-in" || got[11].Value != "(1.25,2.5)" {
		t.Fatalf("twelfth field filter = %+v, want value not-in (1.25,2.5)", got[11])
	}
	if got[12].Key != "region" || got[12].Op != "not-in" || got[12].Value != "(us,eu)" {
		t.Fatalf("thirteenth field filter = %+v, want region not-in(us,eu)", got[12])
	}
	if got[13].Key != "value" || got[13].Op != "" || got[13].Value != "null" {
		t.Fatalf("fourteenth field filter = %+v, want value is null", got[13])
	}
	if got[14].Key != "status" || got[14].Op != "!=" || got[14].Value != "true" {
		t.Fatalf("fifteenth field filter = %+v, want status is-not true", got[14])
	}
	if got[15].Key != "region" || got[15].Op != "!=" || got[15].Value != "us" {
		t.Fatalf("sixteenth field filter = %+v, want region is not us", got[15])
	}
	if got[16].Key != "notes in review" || got[16].Op != "" || got[16].Value != "null" {
		t.Fatalf("seventeenth field filter = %+v, want notes in review is null", got[16])
	}
	if got[17].Key != "message" || got[17].Op != "" || got[17].Value != "error in transit" {
		t.Fatalf("eighteenth field filter = %+v, want message=error in transit", got[17])
	}
	if got[18].Key != "notes in review" || got[18].Op != ">" || got[18].Value != "5" {
		t.Fatalf("nineteenth field filter = %+v, want notes in review>5", got[18])
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
		"name between (\"a=b\",\"z\")",
		"status in (\"a<>b\",\"c\")",
		"name is between (1,3)",
		"raw between 1,3",
		"raw not between 1,3",
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
		{"name", "between", "(\"a=b\",\"z\")"},
		{"status", "in", "(\"a<>b\",\"c\")"},
		{"name", "", "between (1,3)"},
		{"raw", "between", "1,3"},
		{"raw", "not-between", "1,3"},
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
	if _, err := parseStorageFieldFilters([]string{"value !in (1,2)"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want unsupported !in guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not in (1,2)"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing key guidance", err)
	}
	if _, err := parseStorageFieldFilters([]string{"not between (1,2)"}); err == nil || !strings.Contains(err.Error(), "key=value") {
		t.Fatalf("error = %v, want missing between key guidance", err)
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
