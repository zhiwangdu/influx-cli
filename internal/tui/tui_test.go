package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/config"
	"github.com/zhiwangdu/influx-cli/internal/history"
	"github.com/zhiwangdu/influx-cli/internal/query"
	"github.com/zhiwangdu/influx-cli/internal/render"
	"github.com/zhiwangdu/influx-cli/internal/result"
	"github.com/zhiwangdu/influx-cli/internal/schema"
)

func TestModelRunsQueryAndRendersTable(t *testing.T) {
	fake := &fakeAdapter{queryResult: tableResult(), schemaSnapshot: cpuSnapshot()}
	model := newTestModel(fake)
	model.editor.SetValue("select usage_idle from cpu")

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlJ})
	model = updated.(Model)
	if !model.loading {
		t.Fatal("expected model to enter loading state")
	}
	if cmd == nil {
		t.Fatal("expected query command")
	}

	msg := cmd().(queryResultMsg)
	updated, schemaCmd := model.handleQueryResult(msg)
	model = updated.(Model)
	if model.loading {
		t.Fatal("expected query to finish")
	}
	if !strings.Contains(model.rendered, "usage_idle") {
		t.Fatalf("expected rendered table, got:\n%s", model.rendered)
	}
	if model.schemaMeasurement != "cpu" {
		t.Fatalf("schema measurement = %q, want cpu", model.schemaMeasurement)
	}
	if schemaCmd == nil {
		t.Fatal("expected schema load command")
	}
	schemaMsg := model.loadSchemaCmd("cpu")().(schemaLoadedMsg)
	model = model.handleSchema(schemaMsg)
	if model.schemaSnapshot == nil {
		t.Fatal("expected schema snapshot")
	}
	if !strings.Contains(strings.Join(model.schemaLines(), "\n"), "usage_idle:float") {
		t.Fatalf("expected schema lines to include usage_idle, got %v", model.schemaLines())
	}
}

func TestModelSwitchesRenderModes(t *testing.T) {
	model := newTestModel(&fakeAdapter{queryResult: seriesResult(), schemaSnapshot: cpuSnapshot()})
	model.lastResult = seriesResult()

	model.setRenderMode(render.FormatChart)
	if model.renderMode != render.FormatChart {
		t.Fatalf("render mode = %q, want chart", model.renderMode)
	}
	if !strings.Contains(model.rendered, "+") {
		t.Fatalf("expected chart output, got:\n%s", model.rendered)
	}

	model.setRenderMode(render.FormatSparkline)
	if model.renderMode != render.FormatSparkline {
		t.Fatalf("render mode = %q, want sparkline", model.renderMode)
	}
	if !strings.Contains(model.rendered, "points=4") {
		t.Fatalf("expected sparkline output, got:\n%s", model.rendered)
	}
}

func TestEditingModeDoesNotStealSQLRunes(t *testing.T) {
	model := newTestModel(&fakeAdapter{})

	for _, key := range []tea.KeyMsg{
		{Type: tea.KeyRunes, Runes: []rune{'S'}},
		{Type: tea.KeyRunes, Runes: []rune{'1'}},
	} {
		updated, cmd := model.handleKey(key)
		model = updated.(Model)
		if cmd != nil {
			_ = cmd()
		}
	}

	if got := model.editor.Value(); got != "S1" {
		t.Fatalf("editor value = %q, want S1", got)
	}
	if !model.editor.Focused() {
		t.Fatal("expected editor to stay focused")
	}
}

func TestCommandModeSwitchesRendererAfterRun(t *testing.T) {
	model := newTestModel(&fakeAdapter{queryResult: seriesResult(), schemaSnapshot: cpuSnapshot()})
	model.editor.SetValue("select value from cpu")
	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlJ})
	model = updated.(Model)
	if model.editor.Focused() {
		t.Fatal("expected run to leave editor in command mode")
	}
	msg := cmd().(queryResultMsg)
	updated, _ = model.handleQueryResult(msg)
	model = updated.(Model)

	updated, _ = model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'3'}})
	model = updated.(Model)
	if model.renderMode != render.FormatChart {
		t.Fatalf("render mode = %q, want chart", model.renderMode)
	}
}

func TestCommandModeTogglesWatchAndRunsQuery(t *testing.T) {
	model := newTestModel(&fakeAdapter{queryResult: tableResult(), schemaSnapshot: cpuSnapshot()})
	model.editor.SetValue("select usage_idle from cpu")
	model.editor.Blur()

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'W'}})
	model = updated.(Model)
	if !model.watch {
		t.Fatal("expected watch to be enabled")
	}
	if !model.loading {
		t.Fatal("expected watch toggle to start a query")
	}
	if cmd == nil {
		t.Fatal("expected watch query command")
	}
	msg := cmd().(queryResultMsg)
	if !msg.fromWatch {
		t.Fatal("expected watch query result")
	}
}

func TestModelCompletesSelectFieldsBeforeFrom(t *testing.T) {
	model := newTestModel(&fakeAdapter{schemaSnapshot: cpuSnapshot()})
	model.editor.SetValue("select us")

	msg := model.completeCmd(model.editor.Value())().(completionMsg)
	model = model.handleCompletion(msg)
	if got := model.editor.Value(); got != "select usage_idle" {
		t.Fatalf("editor value = %q, want select usage_idle", got)
	}
}

func TestModelRecallsHistory(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.historyEntries = []history.Entry{
		{Query: "select * from cpu"},
		{Query: "select * from mem"},
	}

	model.recallHistory()
	if got := model.editor.Value(); got != "select * from cpu" {
		t.Fatalf("first recalled query = %q", got)
	}
	model.recallHistory()
	if got := model.editor.Value(); got != "select * from mem" {
		t.Fatalf("second recalled query = %q", got)
	}
}

func TestInferMeasurement(t *testing.T) {
	tests := map[string]string{
		"select mean(value) from cpu where host='a'":      "cpu",
		`select value from "autogen"."cpu" limit 10`:      "cpu",
		`select value from "retention policy"."cpu load"`: "cpu load",
		"show measurements":                               "",
	}
	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			if got := inferMeasurement(input); got != want {
				t.Fatalf("inferMeasurement() = %q, want %q", got, want)
			}
		})
	}
}

func newTestModel(fake *fakeAdapter) Model {
	executor := app.NewExecutor(
		app.NewSession(config.Effective{
			Adapter:         "fake",
			Database:        "metrics",
			RetentionPolicy: "autogen",
			Precision:       "rfc3339",
		}),
		fake,
	)
	return NewModel(executor, Options{
		Render:       render.Options{Format: render.FormatTable, Width: 60, MaxRows: 20, MaxSeries: 5},
		QueryTimeout: time.Second,
	})
}

func tableResult() result.Result {
	table := result.NewTable([]string{"time", "usage_idle"})
	table.AddRow("2026-07-05T00:00:00Z", 91.5)
	return result.FromTable(table)
}

func seriesResult() result.Result {
	return result.Result{
		Kind: result.KindSeries,
		Series: []result.Series{
			{
				Name: "cpu",
				Tags: map[string]string{"host": "a"},
				Points: []result.Point{
					{Time: time.Unix(0, 0), Value: 1},
					{Time: time.Unix(1, 0), Value: 3},
					{Time: time.Unix(2, 0), Value: 2},
					{Time: time.Unix(3, 0), Value: 5},
				},
			},
		},
		Metadata: result.Metadata{SeriesCount: 1, PointCount: 4},
	}
}

func cpuSnapshot() schema.Snapshot {
	return schema.Snapshot{
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Measurements: []schema.Measurement{
			{
				Name: "cpu",
				Fields: []schema.Field{
					{Name: "usage_idle", Type: "float"},
					{Name: "usage_user", Type: "float"},
				},
				Tags: []schema.Tag{{Name: "host"}, {Name: "region"}},
			},
		},
	}
}

type fakeAdapter struct {
	queryResult    result.Result
	queryErr       error
	schemaSnapshot schema.Snapshot
}

func (f *fakeAdapter) Name() string {
	return "fake"
}

func (f *fakeAdapter) Ping(ctx context.Context) error {
	return nil
}

func (f *fakeAdapter) Query(ctx context.Context, q query.Query) (result.Result, error) {
	if f.queryErr != nil {
		return result.Result{}, f.queryErr
	}
	if f.queryResult.Kind != "" {
		return f.queryResult, nil
	}
	return tableResult(), nil
}

func (f *fakeAdapter) ShowDatabases(ctx context.Context) ([]string, error) {
	return []string{"metrics"}, nil
}

func (f *fakeAdapter) ShowRetentionPolicies(ctx context.Context, db string) ([]adapter.RetentionPolicy, error) {
	return []adapter.RetentionPolicy{{Name: "autogen", Default: true}}, nil
}

func (f *fakeAdapter) ShowMeasurements(ctx context.Context, db, rp string) ([]string, error) {
	return []string{"cpu", "mem"}, nil
}

func (f *fakeAdapter) GetSchema(ctx context.Context, scope schema.Scope) (schema.Snapshot, error) {
	if len(f.schemaSnapshot.Measurements) > 0 {
		return f.schemaSnapshot, nil
	}
	return cpuSnapshot(), nil
}
