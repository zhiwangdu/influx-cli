package repl

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/config"
	"github.com/zhiwangdu/influx-cli/internal/history"
	"github.com/zhiwangdu/influx-cli/internal/query"
	"github.com/zhiwangdu/influx-cli/internal/render"
	"github.com/zhiwangdu/influx-cli/internal/result"
	"github.com/zhiwangdu/influx-cli/internal/schema"
)

func TestRunChangesRenderFormat(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	input := strings.NewReader(strings.Join([]string{
		":format sparkline",
		"SELECT mean(value) FROM cpu",
		":fmt table",
		"SELECT mean(value) FROM cpu",
		":q",
		"",
	}, "\n"))
	var out bytes.Buffer

	err := Run(context.Background(), executor, input, &out, Options{
		Render: render.Options{Format: render.FormatTable, Width: 80},
	})
	if err != nil {
		t.Fatal(err)
	}

	output := out.String()
	if !strings.Contains(output, "format: sparkline") {
		t.Fatalf("missing sparkline format confirmation:\n%s", output)
	}
	if !strings.Contains(output, "cpu ") || !strings.Contains(output, "points=2") {
		t.Fatalf("missing sparkline output:\n%s", output)
	}
	if !strings.Contains(output, "format: table") {
		t.Fatalf("missing table format confirmation:\n%s", output)
	}
	if !strings.Contains(output, "time") || !strings.Contains(output, "value") {
		t.Fatalf("missing table output:\n%s", output)
	}
}

func TestRunShowsCurrentDefaultFormat(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	input := strings.NewReader(":format\n:q\n")
	var out bytes.Buffer

	if err := Run(context.Background(), executor, input, &out, Options{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "format: table") {
		t.Fatalf("default format output = %q, want table", out.String())
	}
}

func TestRunRejectsInvalidRenderFormatCommands(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	input := strings.NewReader(strings.Join([]string{
		":format sparkline",
		":format wide",
		":format table extra",
		":fmt",
		":q",
		"",
	}, "\n"))
	var out bytes.Buffer

	if err := Run(context.Background(), executor, input, &out, Options{}); err != nil {
		t.Fatal(err)
	}

	output := out.String()
	if !strings.Contains(output, `error: unknown render format "wide"`) {
		t.Fatalf("missing unknown format error:\n%s", output)
	}
	if !strings.Contains(output, "error: usage: :format [auto|table|sparkline|json]") {
		t.Fatalf("missing usage error:\n%s", output)
	}
	if got := strings.Count(output, "format: sparkline"); got != 2 {
		t.Fatalf("format confirmations = %d, want 2:\n%s", got, output)
	}
}

func TestRunExecutesMultilineQueryAndPersistsOneHistoryEntry(t *testing.T) {
	store := history.NewStore(filepath.Join(t.TempDir(), "history.jsonl"), history.Options{})
	fake := &fakeAdapter{}
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Database: "metrics", RetentionPolicy: "autogen", Precision: "rfc3339"}),
		fake,
	)
	input := strings.NewReader(strings.Join([]string{
		"SELECT mean(value)",
		"FROM cpu;",
		":history",
		":q",
		"",
	}, "\n"))
	var out bytes.Buffer

	if err := Run(context.Background(), executor, input, &out, Options{
		Render:  render.Options{Format: render.FormatTable, Width: 120},
		History: store,
	}); err != nil {
		t.Fatal(err)
	}

	if got, want := len(fake.queries), 1; got != want {
		t.Fatalf("query executions = %d, want %d", got, want)
	}
	if got, want := fake.queries[0].Raw, "SELECT mean(value)\nFROM cpu"; got != want {
		t.Fatalf("executed query = %q, want %q", got, want)
	}
	entries, err := store.Search("", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("history entries = %d, want %d", got, want)
	}
	if entries[0].Query != "SELECT mean(value)\nFROM cpu" {
		t.Fatalf("history query = %q", entries[0].Query)
	}
	if entries[0].Database != "metrics" || entries[0].RetentionPolicy != "autogen" {
		t.Fatalf("history context = %q/%q", entries[0].Database, entries[0].RetentionPolicy)
	}
	if entries[0].Dialect != "influxql" {
		t.Fatalf("history dialect = %q, want influxql", entries[0].Dialect)
	}
	output := out.String()
	if !strings.Contains(output, "SELECT mean(value) FROM cpu") {
		t.Fatalf("history output missing query:\n%s", output)
	}
	if !strings.Contains(output, "influxql") {
		t.Fatalf("history output missing dialect:\n%s", output)
	}
}

func TestRunRejectsMetaCommandDuringPendingQueryAndCancelClearsBuffer(t *testing.T) {
	fake := &fakeAdapter{}
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		fake,
	)
	input := strings.NewReader(strings.Join([]string{
		"SELECT mean(value)",
		":status",
		":cancel",
		"SHOW DATABASES",
		":q",
		"",
	}, "\n"))
	var out bytes.Buffer

	if err := Run(context.Background(), executor, input, &out, Options{}); err != nil {
		t.Fatal(err)
	}
	if got, want := len(fake.queries), 1; got != want {
		t.Fatalf("query executions = %d, want %d", got, want)
	}
	if fake.queries[0].Raw != "SHOW DATABASES" {
		t.Fatalf("query = %q, want SHOW DATABASES", fake.queries[0].Raw)
	}
	output := out.String()
	if !strings.Contains(output, "finish query with ; or cancel it first") {
		t.Fatalf("missing pending meta error:\n%s", output)
	}
	if !strings.Contains(output, "query cleared") {
		t.Fatalf("missing clear confirmation:\n%s", output)
	}
}

func TestRunCancelWithoutPendingQueryIsHandledLocally(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	var out bytes.Buffer
	if err := Run(context.Background(), executor, strings.NewReader(":cancel\n:q\n"), &out, Options{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "no pending query") {
		t.Fatalf("missing no pending query output:\n%s", out.String())
	}
}

func TestRunDoesNotExecutePendingQueryAtEOF(t *testing.T) {
	fake := &fakeAdapter{}
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		fake,
	)
	if err := Run(context.Background(), executor, strings.NewReader("SELECT mean(value)\n"), &bytes.Buffer{}, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(fake.queries) != 0 {
		t.Fatalf("pending query was executed: %#v", fake.queries)
	}
}

func TestRunReturnsNilWhenContextIsCanceledBeforeRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)

	if err := Run(ctx, executor, strings.NewReader(":q\n"), &bytes.Buffer{}, Options{}); err != nil {
		t.Fatal(err)
	}
}

func TestRunReturnsNilWhenContextIsCanceledDuringQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeAdapter{
		queryHook: cancel,
	}
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		fake,
	)
	input := strings.NewReader("SELECT mean(value) FROM cpu\n")
	var out bytes.Buffer

	if err := Run(ctx, executor, input, &out, Options{}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "context canceled") {
		t.Fatalf("unexpected context canceled output:\n%s", out.String())
	}
}

func TestRunReturnsNilWhenContextDeadlineExpiresDuringQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	fake := &fakeAdapter{
		waitForContextDone: true,
	}
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		fake,
	)
	input := strings.NewReader("SELECT mean(value) FROM cpu\n")
	var out bytes.Buffer

	if err := Run(ctx, executor, input, &out, Options{}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "context deadline exceeded") {
		t.Fatalf("unexpected context deadline output:\n%s", out.String())
	}
}

func TestRunSearchesHistoryByFilter(t *testing.T) {
	store := history.NewStore(filepath.Join(t.TempDir(), "history.jsonl"), history.Options{})
	if err := store.Append(history.Entry{Query: "SELECT mean(value) FROM cpu"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(history.Entry{Query: "SELECT mean(value) FROM mem"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(history.Entry{Query: "SELECT mean(value)\nFROM disk"}); err != nil {
		t.Fatal(err)
	}
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	input := strings.NewReader(":history cpu\n:q\n")
	var out bytes.Buffer

	if err := Run(context.Background(), executor, input, &out, Options{
		Render:  render.Options{Format: render.FormatTable, Width: 120},
		History: store,
	}); err != nil {
		t.Fatal(err)
	}
	output := out.String()
	if !strings.Contains(output, "SELECT mean(value) FROM cpu") {
		t.Fatalf("history output missing cpu query:\n%s", output)
	}
	if strings.Contains(output, "SELECT mean(value) FROM mem") {
		t.Fatalf("history filter returned mem query:\n%s", output)
	}
}

func TestRunRejectsInvalidHistoryLimit(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	input := strings.NewReader(":history 0\n:q\n")
	var out bytes.Buffer

	if err := Run(context.Background(), executor, input, &out, Options{
		History: history.NewStore(filepath.Join(t.TempDir(), "history.jsonl"), history.Options{}),
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "error: usage: :history [limit] [filter]") {
		t.Fatalf("missing history usage error:\n%s", out.String())
	}
}

func TestRunLoadsStoredHistoryIntoLineReader(t *testing.T) {
	store := history.NewStore(filepath.Join(t.TempDir(), "history.jsonl"), history.Options{})
	if err := store.Append(history.Entry{Query: "SELECT mean(value) FROM cpu"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(history.Entry{Query: "SELECT mean(value) FROM mem"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(history.Entry{Query: "SELECT mean(value)\nFROM disk"}); err != nil {
		t.Fatal(err)
	}
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	reader := &recordingLineReader{lines: []string{":q"}}
	var out bytes.Buffer

	if err := runWithReader(context.Background(), executor, reader, &out, Options{History: store}); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"SELECT mean(value) FROM cpu",
		"SELECT mean(value) FROM mem",
		"SELECT mean(value)\nFROM disk",
	}
	if !equalStrings(reader.saved, want) {
		t.Fatalf("saved line history = %#v, want %#v", reader.saved, want)
	}
}

func TestRunSavesSuccessfulQueryToLineHistory(t *testing.T) {
	store := history.NewStore(filepath.Join(t.TempDir(), "history.jsonl"), history.Options{})
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	reader := &recordingLineReader{lines: []string{
		":format table",
		"SELECT mean(value) FROM cpu",
		":history",
		":q",
	}}
	var out bytes.Buffer

	if err := runWithReader(context.Background(), executor, reader, &out, Options{
		History: store,
	}); err != nil {
		t.Fatal(err)
	}

	want := []string{"SELECT mean(value) FROM cpu"}
	if !equalStrings(reader.saved, want) {
		t.Fatalf("saved line history = %#v, want %#v", reader.saved, want)
	}
}

func TestRunPreservesMultilineQueryInLineHistory(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	reader := &recordingLineReader{lines: []string{
		"SELECT mean(value)",
		"FROM cpu;",
		":q",
	}}
	var out bytes.Buffer

	if err := runWithReader(context.Background(), executor, reader, &out, Options{}); err != nil {
		t.Fatal(err)
	}

	want := []string{"SELECT mean(value)\nFROM cpu"}
	if !equalStrings(reader.saved, want) {
		t.Fatalf("saved line history = %#v, want %#v", reader.saved, want)
	}
}

func TestRunSavesReplayableLineHistoryForTerminatedHeuristicQuery(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	reader := &recordingLineReader{lines: []string{
		`SELECT now() \`,
		";",
		":q",
	}}
	var out bytes.Buffer

	if err := runWithReader(context.Background(), executor, reader, &out, Options{}); err != nil {
		t.Fatal(err)
	}

	want := []string{"SELECT now();"}
	if !equalStrings(reader.saved, want) {
		t.Fatalf("saved line history = %#v, want %#v", reader.saved, want)
	}
}

func TestRunDoesNotSaveFailedQueryToLineHistory(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{queryErr: errors.New("query failed")},
	)
	reader := &recordingLineReader{lines: []string{
		"SELECT mean(value) FROM cpu",
		":q",
	}}
	var out bytes.Buffer

	if err := runWithReader(context.Background(), executor, reader, &out, Options{}); err != nil {
		t.Fatal(err)
	}
	if len(reader.saved) != 0 {
		t.Fatalf("saved line history = %#v, want none", reader.saved)
	}
}

func TestRunWarnsWhenLineHistorySaveFails(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	reader := &recordingLineReader{
		lines:   []string{"SELECT mean(value) FROM cpu", ":q"},
		saveErr: errors.New("line history unavailable"),
	}
	var out bytes.Buffer

	if err := runWithReader(context.Background(), executor, reader, &out, Options{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "warning: save query history: line history unavailable") {
		t.Fatalf("missing line history warning:\n%s", out.String())
	}
}

func TestRunWarnsWhenLineHistoryLoadFails(t *testing.T) {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}),
		&fakeAdapter{},
	)
	store := history.NewStore(t.TempDir(), history.Options{})
	reader := &recordingLineReader{lines: []string{":q"}}
	var out bytes.Buffer

	if err := runWithReader(context.Background(), executor, reader, &out, Options{
		History: store,
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "warning: load query history:") {
		t.Fatalf("missing line history load warning:\n%s", out.String())
	}
}

type fakeAdapter struct {
	queries            []query.Query
	queryHook          func()
	waitForContextDone bool
	queryErr           error
}

func (*fakeAdapter) Name() string {
	return "fake"
}

func (*fakeAdapter) Ping(ctx context.Context) error {
	return nil
}

func (f *fakeAdapter) Query(ctx context.Context, q query.Query) (result.Result, error) {
	f.queries = append(f.queries, q)
	if f.queryHook != nil {
		f.queryHook()
	}
	if f.waitForContextDone {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return result.Result{}, err
	}
	if f.queryErr != nil {
		return result.Result{}, f.queryErr
	}
	base := time.Unix(0, 0).UTC()
	table := result.NewTable([]string{"time", "value"})
	table.AddRow(base, 1.0)
	table.AddRow(base.Add(time.Minute), 2.0)
	return result.Result{
		Kind:  result.KindSeries,
		Table: table,
		Series: []result.Series{{
			Name: "cpu",
			Points: []result.Point{
				{Time: base, Value: 1.0},
				{Time: base.Add(time.Minute), Value: 2.0},
			},
		}},
		Metadata: result.Metadata{
			RowCount:    2,
			PointCount:  2,
			SeriesCount: 1,
		},
	}, nil
}

func (*fakeAdapter) ShowDatabases(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (*fakeAdapter) ShowRetentionPolicies(ctx context.Context, db string) ([]adapter.RetentionPolicy, error) {
	return nil, nil
}

func (*fakeAdapter) ShowMeasurements(ctx context.Context, db, rp string) ([]string, error) {
	return []string{"cpu", "mem"}, nil
}

func (*fakeAdapter) GetSchema(ctx context.Context, scope schema.Scope) (schema.Snapshot, error) {
	return schema.Snapshot{}, nil
}

type recordingLineReader struct {
	lines   []string
	saved   []string
	saveErr error
}

func (r *recordingLineReader) ReadLine(prompt string) (string, error) {
	if len(r.lines) == 0 {
		return "", io.EOF
	}
	line := r.lines[0]
	r.lines = r.lines[1:]
	return line, nil
}

func (r *recordingLineReader) Close() error {
	return nil
}

func (r *recordingLineReader) SaveHistory(line string) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	r.saved = append(r.saved, line)
	return nil
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
