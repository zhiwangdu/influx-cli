package repl

import (
	"bytes"
	"context"
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

func TestRunSearchesHistoryByFilter(t *testing.T) {
	store := history.NewStore(filepath.Join(t.TempDir(), "history.jsonl"), history.Options{})
	if err := store.Append(history.Entry{Query: "SELECT mean(value) FROM cpu"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(history.Entry{Query: "SELECT mean(value) FROM mem"}); err != nil {
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

type fakeAdapter struct {
	queries []query.Query
}

func (*fakeAdapter) Name() string {
	return "fake"
}

func (*fakeAdapter) Ping(ctx context.Context) error {
	return nil
}

func (f *fakeAdapter) Query(ctx context.Context, q query.Query) (result.Result, error) {
	f.queries = append(f.queries, q)
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
