package tui

import (
	"context"
	"errors"
	"strconv"
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
		{Type: tea.KeyRunes, Runes: []rune{'W'}},
		{Type: tea.KeyRunes, Runes: []rune{'1'}},
		{Type: tea.KeyRunes, Runes: []rune{'2'}},
		{Type: tea.KeyRunes, Runes: []rune{'3'}},
	} {
		updated, cmd := model.handleKey(key)
		model = updated.(Model)
		if cmd != nil {
			_ = cmd()
		}
	}

	if got := model.editor.Value(); got != "SW123" {
		t.Fatalf("editor value = %q, want SW123", got)
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
	model.setMode(modeCommand)

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

func TestCommandModeSupportsJSONAndAutoRenderModes(t *testing.T) {
	model := newTestModel(&fakeAdapter{queryResult: seriesResult(), schemaSnapshot: cpuSnapshot()})
	model.lastResult = seriesResult()
	model.setMode(modeCommand)

	updated, _ := model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'4'}})
	model = updated.(Model)
	if model.renderMode != render.FormatJSON {
		t.Fatalf("render mode = %q, want json", model.renderMode)
	}
	if !strings.Contains(model.rendered, `"series"`) {
		t.Fatalf("expected json output, got:\n%s", model.rendered)
	}

	updated, _ = model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'0'}})
	model = updated.(Model)
	if model.renderMode != render.FormatAuto {
		t.Fatalf("render mode = %q, want auto", model.renderMode)
	}
	if model.renderer != render.FormatSparkline {
		t.Fatalf("renderer = %q, want sparkline", model.renderer)
	}
}

func TestResultModeScrollsWithoutEditing(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.resize(80, 18)
	model.resultView.SetContent(strings.Join([]string{
		"line 01", "line 02", "line 03", "line 04", "line 05",
		"line 06", "line 07", "line 08", "line 09", "line 10",
		"line 11", "line 12", "line 13", "line 14", "line 15",
	}, "\n"))
	model.setMode(modeResult)

	updated, _ := model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	model = updated.(Model)
	if model.resultView.YOffset == 0 {
		t.Fatal("expected result view to scroll down")
	}
	if got := model.editor.Value(); got != "" {
		t.Fatalf("editor value = %q, want empty", got)
	}

	updated, _ = model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'g'}})
	model = updated.(Model)
	if model.resultView.YOffset != 0 {
		t.Fatalf("result offset = %d, want top", model.resultView.YOffset)
	}
}

func TestCancelActiveQueryUsesQueryContext(t *testing.T) {
	model := newTestModel(&fakeAdapter{
		queryFunc: func(ctx context.Context, q query.Query) (result.Result, error) {
			if err := ctx.Err(); err != nil {
				return result.Result{}, err
			}
			return tableResult(), nil
		},
	})
	model.editor.SetValue("select usage_idle from cpu")

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlJ})
	model = updated.(Model)
	if !model.loading || model.activeCancel == nil {
		t.Fatal("expected active cancellable query")
	}

	updated, _ = model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlC})
	model = updated.(Model)
	if model.statusMessage != "query cancelling" {
		t.Fatalf("status = %q, want query cancelling", model.statusMessage)
	}

	msg := cmd().(queryResultMsg)
	if !errors.Is(msg.err, context.Canceled) {
		t.Fatalf("query err = %v, want context canceled", msg.err)
	}
	updated, _ = model.handleQueryResult(msg)
	model = updated.(Model)
	if model.loading {
		t.Fatal("expected cancelled query to finish loading state")
	}
	if model.statusMessage != "query cancelled" {
		t.Fatalf("status = %q, want query cancelled", model.statusMessage)
	}
}

func TestSecondCtrlCQuitsWhileCancellationIsPending(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.loading = true
	model.activeCancel = func() {}

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlC})
	model = updated.(Model)
	if cmd != nil {
		t.Fatal("first ctrl+c should only request cancellation")
	}
	if !model.cancelAsked {
		t.Fatal("expected cancellation to be marked as requested")
	}

	_, cmd = model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlC})
	if cmd == nil {
		t.Fatal("second ctrl+c should quit")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatal("expected quit message")
	}
}

func TestWatchTickDoesNotStealEditMode(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.watch = true
	model.lastQuery = "select usage_idle from cpu"
	model.editor.SetValue("select ")
	model.setMode(modeEdit)

	updated, cmd := model.Update(watchTickMsg(time.Now()))
	model = updated.(Model)
	if cmd == nil {
		t.Fatal("expected watch query command")
	}
	if model.mode != modeEdit {
		t.Fatalf("mode = %q, want edit", model.mode)
	}
	if !model.editor.Focused() {
		t.Fatal("expected editor to remain focused")
	}
	if !model.loading || !model.activeWatch {
		t.Fatal("expected watch query to be running")
	}
}

func TestWatchTickDoesNotStartConcurrentQuery(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.watch = true
	model.loading = true

	updated, cmd := model.Update(watchTickMsg(time.Now()))
	model = updated.(Model)
	if cmd != nil {
		t.Fatal("expected no command while query is already running")
	}
	if !model.loading {
		t.Fatal("expected loading to remain true")
	}
}

func TestWatchFailureKeepsLastSuccessfulResult(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.watch = true
	model.lastResult = tableResult()
	model.rerender()
	before := model.rendered

	updated, _ := model.handleQueryResult(queryResultMsg{
		query:     "select usage_idle from cpu",
		fromWatch: true,
		err:       errors.New("network down"),
	})
	model = updated.(Model)
	if model.rendered != before {
		t.Fatalf("rendered result changed after watch failure:\n%s", model.rendered)
	}
	if !strings.Contains(model.statusMessage, "watch failed") {
		t.Fatalf("status = %q, want watch failed", model.statusMessage)
	}
}

func TestWatchOffCancellationDoesNotRecordFailure(t *testing.T) {
	model := newTestModel(&fakeAdapter{
		queryFunc: func(ctx context.Context, q query.Query) (result.Result, error) {
			if err := ctx.Err(); err != nil {
				return result.Result{}, err
			}
			return tableResult(), nil
		},
	})
	model.lastResult = tableResult()
	model.rerender()
	before := model.rendered
	model.editor.SetValue("select usage_idle from cpu")
	model.setMode(modeCommand)

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'W'}})
	model = updated.(Model)
	if cmd == nil || !model.watch || !model.activeWatch {
		t.Fatal("expected watch refresh to be running")
	}

	updated, _ = model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'W'}})
	model = updated.(Model)
	if model.watch {
		t.Fatal("expected watch to be off")
	}
	if !model.cancelAsked {
		t.Fatal("expected watch cancellation to be requested")
	}

	msg := cmd().(queryResultMsg)
	if !errors.Is(msg.err, context.Canceled) {
		t.Fatalf("query err = %v, want context canceled", msg.err)
	}
	updated, _ = model.handleQueryResult(msg)
	model = updated.(Model)
	if model.statusMessage != "watch off" {
		t.Fatalf("status = %q, want watch off", model.statusMessage)
	}
	if model.lastErr != nil {
		t.Fatalf("lastErr = %v, want nil", model.lastErr)
	}
	if model.rendered != before {
		t.Fatalf("rendered result changed after watch cancellation:\n%s", model.rendered)
	}
}

func TestWatchIntervalCanBeAdjustedInCommandMode(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.watchInterval = 5 * time.Second
	model.setMode(modeCommand)

	updated, _ := model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'+'}})
	model = updated.(Model)
	if model.watchInterval != 6*time.Second {
		t.Fatalf("watch interval = %s, want 6s", model.watchInterval)
	}

	for i := 0; i < 10; i++ {
		updated, _ = model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'-'}})
		model = updated.(Model)
	}
	if model.watchInterval != minWatchInterval {
		t.Fatalf("watch interval = %s, want min %s", model.watchInterval, minWatchInterval)
	}
}

func TestViewIncludesResultAndWatchContext(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.lastResult = seriesResult()
	model.lastRefresh = time.Date(2026, 7, 6, 12, 30, 0, 0, time.UTC)
	model.watch = true
	model.rerender()
	model.resize(120, 30)

	view := model.View()
	for _, want := range []string{
		"Result [table]",
		"points 4",
		"series 1",
		"watch: on 5s",
		"format: table",
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("view does not contain %q:\n%s", want, view)
		}
	}
}

func TestViewRendersCommonTerminalSizes(t *testing.T) {
	for _, size := range []struct {
		width  int
		height int
	}{
		{80, 24},
		{120, 40},
		{160, 50},
	} {
		t.Run(strconv.Itoa(size.width)+"x"+strconv.Itoa(size.height), func(t *testing.T) {
			model := newTestModel(&fakeAdapter{})
			model.resize(size.width, size.height)
			view := model.View()
			if !strings.Contains(view, "Query") {
				t.Fatalf("view missing Query section:\n%s", view)
			}
			if !strings.Contains(view, "Result") {
				t.Fatalf("view missing Result section:\n%s", view)
			}
			if !strings.Contains(view, string(model.mode)) {
				t.Fatalf("view missing current mode %q:\n%s", model.mode, view)
			}
		})
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
	queryFunc      func(context.Context, query.Query) (result.Result, error)
	schemaSnapshot schema.Snapshot
}

func (f *fakeAdapter) Name() string {
	return "fake"
}

func (f *fakeAdapter) Ping(ctx context.Context) error {
	return nil
}

func (f *fakeAdapter) Query(ctx context.Context, q query.Query) (result.Result, error) {
	if f.queryFunc != nil {
		return f.queryFunc(ctx, q)
	}
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
