package history

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreAppendAndSearchesRecentEntries(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "history.jsonl"), Options{})
	base := time.Date(2026, 7, 5, 8, 0, 0, 0, time.UTC)

	mustAppend(t, store, Entry{Time: base, Database: "metrics", Query: "SELECT mean(value) FROM cpu"})
	mustAppend(t, store, Entry{Time: base.Add(time.Minute), Database: "telegraf", Query: "SHOW MEASUREMENTS"})
	mustAppend(t, store, Entry{Time: base.Add(2 * time.Minute), Database: "metrics", Query: "SELECT mean(usage) FROM mem"})

	recent, err := store.Search("", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(recent), 2; got != want {
		t.Fatalf("recent entries = %d, want %d", got, want)
	}
	if recent[0].ID != 3 || recent[0].Query != "SELECT mean(usage) FROM mem" {
		t.Fatalf("newest entry = %#v", recent[0])
	}
	if recent[1].ID != 2 || recent[1].Query != "SHOW MEASUREMENTS" {
		t.Fatalf("second entry = %#v", recent[1])
	}

	filtered, err := store.Search("cpu", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(filtered), 1; got != want {
		t.Fatalf("filtered entries = %d, want %d", got, want)
	}
	if filtered[0].ID != 1 || !strings.Contains(filtered[0].Query, "cpu") {
		t.Fatalf("filtered entry = %#v", filtered[0])
	}
}

func TestStoreCreatesPrivateHistoryFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "history.jsonl")
	store := NewStore(path, Options{})

	mustAppend(t, store, Entry{Query: "SHOW DATABASES"})

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := info.Mode().Perm(), os.FileMode(0o600); got != want {
		t.Fatalf("history file mode = %o, want %o", got, want)
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := dirInfo.Mode().Perm(), os.FileMode(0o700); got != want {
		t.Fatalf("history dir mode = %o, want %o", got, want)
	}
}

func TestStoreCompactsToMaxEntries(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "history.jsonl"), Options{MaxEntries: 2})

	mustAppend(t, store, Entry{Query: "SELECT 1"})
	mustAppend(t, store, Entry{Query: "SELECT 2"})
	mustAppend(t, store, Entry{Query: "SELECT 3"})

	entries, err := store.Search("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("entries = %d, want %d", got, want)
	}
	if entries[0].Query != "SELECT 3" || entries[1].Query != "SELECT 2" {
		t.Fatalf("entries were not compacted to newest queries: %#v", entries)
	}
}

func TestStoreSkipsMalformedHistoryLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	if err := os.WriteFile(path, []byte("{bad json\n{\"query\":\"SELECT ok\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewStore(path, Options{})

	entries, err := store.Search("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("entries = %d, want %d", got, want)
	}
	if entries[0].Query != "SELECT ok" {
		t.Fatalf("entry = %#v", entries[0])
	}
	if err := store.Append(Entry{Query: "SELECT after_bad_line"}); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultPathUsesXDGStateHome(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)

	got := DefaultPath()
	want := filepath.Join(stateHome, "influx-cli", "history.jsonl")
	if got != want {
		t.Fatalf("DefaultPath = %q, want %q", got, want)
	}
}

func TestDefaultPathIgnoresRelativeXDGStateHome(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "relative-state")
	got := DefaultPath()
	if strings.HasPrefix(got, "relative-state") {
		t.Fatalf("DefaultPath used relative XDG_STATE_HOME: %q", got)
	}
}

func mustAppend(t *testing.T, store *Store, entry Entry) {
	t.Helper()
	if err := store.Append(entry); err != nil {
		t.Fatal(err)
	}
}
