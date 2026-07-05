package repl

import (
	"context"
	"testing"

	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/config"
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
