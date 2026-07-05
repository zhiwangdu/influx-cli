package repl

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/chzyer/readline"

	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/config"
	"github.com/zhiwangdu/influx-cli/internal/history"
)

func TestReadlineCompleterReturnsCandidateSuffixes(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	completer := readlineCompleter{ctx: context.Background(), executor: executor}

	candidates, length := completer.Do([]rune(":ca"), len([]rune(":ca")))
	if length != len([]rune(":ca")) {
		t.Fatalf("completion length = %d, want 3", length)
	}
	if len(candidates) != 1 || string(candidates[0]) != "ncel" {
		t.Fatalf("candidates = %#v, want cancel suffix", candidates)
	}
}

func TestTerminalLineReaderRecallsSavedReadlineHistory(t *testing.T) {
	line := readSavedHistory(t, "SELECT mean(value) FROM cpu")
	if line != "SELECT mean(value) FROM cpu" {
		t.Fatalf("line = %q, want saved history", line)
	}
}

func TestTerminalLineReaderRecallsSavedMultilineHistory(t *testing.T) {
	line := readSavedHistory(t, "SELECT mean(value)\nFROM cpu")
	if line != "SELECT mean(value)\nFROM cpu" {
		t.Fatalf("line = %q, want saved multiline history", line)
	}
}

func TestTerminalLineReaderRecallOrder(t *testing.T) {
	reader := newTestTerminalLineReader(t, "\x1b[A\x1b[A\x1b[B\n")
	if err := reader.SaveHistory("SELECT first FROM cpu"); err != nil {
		t.Fatal(err)
	}
	if err := reader.SaveHistory("SELECT second FROM mem"); err != nil {
		t.Fatal(err)
	}

	line, err := reader.ReadLine("> ")
	if err != nil {
		t.Fatal(err)
	}
	if line != "SELECT second FROM mem" {
		t.Fatalf("line = %q, want newest history after Up Up Down", line)
	}
}

func TestLoadLineHistoryMakesStoredQueriesRecallable(t *testing.T) {
	store := history.NewStore(filepath.Join(t.TempDir(), "history.jsonl"), history.Options{})
	if err := store.Append(history.Entry{Query: "SELECT first FROM cpu"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(history.Entry{Query: "SELECT second FROM mem"}); err != nil {
		t.Fatal(err)
	}
	reader := newTestTerminalLineReader(t, "\x1b[A\n")

	if err := loadLineHistory(reader, store); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadLine("> ")
	if err != nil {
		t.Fatal(err)
	}
	if line != "SELECT second FROM mem" {
		t.Fatalf("line = %q, want newest loaded history", line)
	}
}

func TestLoadLineHistoryAddsReplayTerminator(t *testing.T) {
	store := history.NewStore(filepath.Join(t.TempDir(), "history.jsonl"), history.Options{})
	if err := store.Append(history.Entry{Query: "SELECT now()"}); err != nil {
		t.Fatal(err)
	}
	reader := newTestTerminalLineReader(t, "\x1b[A\n")

	if err := loadLineHistory(reader, store); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadLine("> ")
	if err != nil {
		t.Fatal(err)
	}
	if line != "SELECT now();" {
		t.Fatalf("line = %q, want replayable loaded history", line)
	}
}

func readSavedHistory(t *testing.T, history string) string {
	t.Helper()

	reader := newTestTerminalLineReader(t, "\x1b[A\n")
	if err := reader.SaveHistory(history); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadLine("> ")
	if err != nil {
		t.Fatal(err)
	}
	return line
}

func newTestTerminalLineReader(t *testing.T, input string) *terminalLineReader {
	t.Helper()

	instance, err := readline.NewEx(&readline.Config{
		HistoryLimit:           lineHistoryLimit,
		DisableAutoSaveHistory: true,
		Stdin:                  io.NopCloser(bytes.NewBufferString(input)),
		Stdout:                 io.Discard,
		Stderr:                 io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = instance.Close()
	})
	return &terminalLineReader{instance: instance}
}
