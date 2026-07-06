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
	model.lastQuery = "select value from cpu"
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
		"precision: rfc3339",
		"query: select value from cpu",
		"controls: S toggle, L refresh",
	} {
		if !strings.Contains(view, want) {
			t.Fatalf("view does not contain %q:\n%s", want, view)
		}
	}
}

func TestViewShowsLightSyntaxHintForSelectWithoutFrom(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.editor.SetValue("select usage_idle")
	model.resize(120, 30)

	view := model.View()
	if !strings.Contains(view, "syntax: SELECT needs FROM") {
		t.Fatalf("view missing syntax hint:\n%s", view)
	}

	model.editor.SetValue("select usage_idle from cpu")
	view = model.View()
	if strings.Contains(view, "syntax: SELECT needs FROM") {
		t.Fatalf("view still shows syntax hint for complete query:\n%s", view)
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
	if model.mode == modeCompletion {
		updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
		model = updated.(Model)
		if cmd != nil {
			_ = cmd()
		}
	}
	if got := model.editor.Value(); got != "select usage_idle" {
		t.Fatalf("editor value = %q, want select usage_idle", got)
	}
}

func TestCompletionMenuAcceptsSelectedCandidate(t *testing.T) {
	model := newTestModel(&fakeAdapter{schemaSnapshot: cpuSnapshot()})
	model.editor.SetValue("select us")

	msg := model.completeCmd(model.editor.Value())().(completionMsg)
	model = model.handleCompletion(msg)
	if model.mode != modeCompletion {
		t.Fatalf("mode = %q, want completion", model.mode)
	}
	if model.overlay.Kind != overlayCompletion {
		t.Fatalf("overlay kind = %q, want completion", model.overlay.Kind)
	}
	if len(model.overlay.Items) < 2 {
		t.Fatalf("expected multiple completion candidates, got %d", len(model.overlay.Items))
	}
	if got := model.editor.Value(); got != "select us" {
		t.Fatalf("editor value changed before accept: %q", got)
	}

	updated, _ := model.handleKey(tea.KeyMsg{Type: tea.KeyDown})
	model = updated.(Model)
	selected := model.overlay.Items[model.overlay.Selected].Value
	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
	model = updated.(Model)
	if cmd != nil {
		_ = cmd()
	}
	if model.mode != modeEdit || !model.editor.Focused() {
		t.Fatalf("mode/focus = %q/%v, want edit/focused", model.mode, model.editor.Focused())
	}
	if !strings.HasSuffix(model.editor.Value(), selected) {
		t.Fatalf("editor value = %q, want suffix %q", model.editor.Value(), selected)
	}
}

func TestCompletionMenuCancelKeepsEditorValue(t *testing.T) {
	model := newTestModel(&fakeAdapter{schemaSnapshot: cpuSnapshot()})
	model.editor.SetValue("select us")

	msg := model.completeCmd(model.editor.Value())().(completionMsg)
	model = model.handleCompletion(msg)
	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyEsc})
	model = updated.(Model)
	if cmd != nil {
		_ = cmd()
	}
	if model.mode != modeEdit {
		t.Fatalf("mode = %q, want edit", model.mode)
	}
	if got := model.editor.Value(); got != "select us" {
		t.Fatalf("editor value = %q, want unchanged", got)
	}
}

func TestStaleCompletionDoesNotClobberHistoryOverlay(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.editor.SetValue("select us")
	model.historyEntries = []history.Entry{{Query: "select * from cpu"}}

	updated, _ := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlR})
	model = updated.(Model)
	if model.mode != modeHistory {
		t.Fatalf("mode = %q, want history", model.mode)
	}

	model = model.handleCompletion(completionMsg{
		line:       "select us",
		prefix:     "us",
		candidates: []string{"usage_idle", "usage_user"},
	})
	if model.mode != modeHistory || model.overlay.Kind != overlayHistory {
		t.Fatalf("late completion changed mode/overlay to %q/%q", model.mode, model.overlay.Kind)
	}
	if got := model.editor.Value(); got != "select us" {
		t.Fatalf("editor value = %q, want unchanged", got)
	}
}

func TestStaleCompletionForChangedLineIsIgnored(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.editor.SetValue("select usage")

	model = model.handleCompletion(completionMsg{
		line:       "select us",
		prefix:     "us",
		candidates: []string{"usage_idle", "usage_user"},
	})
	if model.overlay.Active() {
		t.Fatalf("expected stale completion to be ignored, got overlay %q", model.overlay.Kind)
	}
	if got := model.editor.Value(); got != "select usage" {
		t.Fatalf("editor value = %q, want unchanged", got)
	}
}

func TestLateCompletionDoesNotOpenDuringOrAfterQuery(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.editor.SetValue("select us")

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlJ})
	model = updated.(Model)
	if cmd == nil || !model.loading {
		t.Fatal("expected running query")
	}

	model = model.handleCompletion(completionMsg{
		line:       "select us",
		prefix:     "us",
		candidates: []string{"usage_idle", "usage_user"},
	})
	if model.overlay.Active() || model.mode == modeCompletion {
		t.Fatalf("late completion opened overlay while loading: mode=%q overlay=%q", model.mode, model.overlay.Kind)
	}

	msg := cmd().(queryResultMsg)
	updated, _ = model.handleQueryResult(msg)
	model = updated.(Model)
	if model.mode != modeCommand {
		t.Fatalf("mode = %q, want command after query", model.mode)
	}

	model = model.handleCompletion(completionMsg{
		line:       "select us",
		prefix:     "us",
		candidates: []string{"usage_idle", "usage_user"},
	})
	if model.overlay.Active() || model.mode == modeCompletion {
		t.Fatalf("late completion opened overlay after query: mode=%q overlay=%q", model.mode, model.overlay.Kind)
	}
}

func TestClearEditorKeepsEditMode(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.editor.SetValue("select * from cpu")

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlL})
	model = updated.(Model)
	if cmd != nil {
		t.Fatal("expected clear editor to be synchronous")
	}
	if got := model.editor.Value(); got != "" {
		t.Fatalf("editor value = %q, want empty", got)
	}
	if model.mode != modeEdit || !model.editor.Focused() {
		t.Fatalf("mode/focus = %q/%v, want edit/focused", model.mode, model.editor.Focused())
	}
	if model.statusMessage != "editor cleared" {
		t.Fatalf("status = %q, want editor cleared", model.statusMessage)
	}
}

func TestCommandModeRefreshesCurrentSchema(t *testing.T) {
	model := newTestModel(&fakeAdapter{schemaSnapshot: cpuSnapshot()})
	model.schemaMeasurement = "cpu"
	model.schemaSnapshot = nil
	model.setMode(modeCommand)

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'L'}})
	model = updated.(Model)
	if cmd == nil {
		t.Fatal("expected schema load command")
	}
	if !model.schemaVisible || !model.schemaLoading {
		t.Fatalf("schema visible/loading = %v/%v, want true/true", model.schemaVisible, model.schemaLoading)
	}
	if model.statusMessage != "schema refresh: cpu" {
		t.Fatalf("status = %q, want schema refresh: cpu", model.statusMessage)
	}

	msg := cmd().(schemaLoadedMsg)
	model = model.handleSchema(msg)
	if model.schemaSnapshot == nil {
		t.Fatal("expected schema snapshot after refresh")
	}
}

func TestRefreshSchemaRequiresMeasurement(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.setMode(modeCommand)

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'L'}})
	model = updated.(Model)
	if cmd != nil {
		t.Fatal("expected no schema command without measurement")
	}
	if model.statusMessage != "schema refresh: no measurement" {
		t.Fatalf("status = %q, want no measurement", model.statusMessage)
	}
}

func TestHistoryPanelFiltersAndLoadsQuery(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.historyEntries = []history.Entry{
		{Time: time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC), Database: "metrics", RetentionPolicy: "autogen", Query: "select * from cpu"},
		{Time: time.Date(2026, 7, 6, 10, 1, 0, 0, time.UTC), Database: "metrics", RetentionPolicy: "autogen", Query: "select * from mem"},
	}

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlR})
	model = updated.(Model)
	if cmd != nil {
		t.Fatal("expected history panel to open synchronously")
	}
	if model.mode != modeHistory || model.overlay.Kind != overlayHistory {
		t.Fatalf("mode/overlay = %q/%q, want history/history", model.mode, model.overlay.Kind)
	}
	if len(model.overlay.Items) != 2 {
		t.Fatalf("history items = %d, want 2", len(model.overlay.Items))
	}

	for _, r := range []rune("mem") {
		updated, _ = model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		model = updated.(Model)
	}
	if model.overlay.Filter != "mem" {
		t.Fatalf("filter = %q, want mem", model.overlay.Filter)
	}
	if len(model.overlay.Items) != 1 {
		t.Fatalf("filtered items = %d, want 1", len(model.overlay.Items))
	}

	updated, cmd = model.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
	model = updated.(Model)
	if cmd != nil {
		_ = cmd()
	}
	if model.mode != modeEdit {
		t.Fatalf("mode = %q, want edit", model.mode)
	}
	if got := model.editor.Value(); got != "select * from mem" {
		t.Fatalf("editor value = %q, want mem query", got)
	}
	if model.statusMessage != "history loaded" {
		t.Fatalf("status = %q, want history loaded", model.statusMessage)
	}
}

func TestHistoryPanelCanFilterWithJAndK(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.historyEntries = []history.Entry{
		{Query: "select * from jvm"},
		{Query: "select * from cpu"},
	}

	updated, _ := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlR})
	model = updated.(Model)
	updated, _ = model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	model = updated.(Model)

	if model.overlay.Filter != "j" {
		t.Fatalf("filter = %q, want j", model.overlay.Filter)
	}
	if len(model.overlay.Items) != 1 || model.overlay.Items[0].Value != "select * from jvm" {
		t.Fatalf("filtered history items = %#v, want jvm only", model.overlay.Items)
	}
}

func TestHistoryLoadedFromResultReturnsToEditAndRefreshUsesEditor(t *testing.T) {
	var ran string
	model := newTestModel(&fakeAdapter{
		queryFunc: func(ctx context.Context, q query.Query) (result.Result, error) {
			ran = q.Raw
			return tableResult(), nil
		},
	})
	model.lastQuery = "select * from old"
	model.historyEntries = []history.Entry{{Query: "select * from mem"}}
	model.setMode(modeResult)

	updated, _ := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlR})
	model = updated.(Model)
	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
	model = updated.(Model)
	if cmd != nil {
		_ = cmd()
	}
	if model.mode != modeEdit || !model.editor.Focused() {
		t.Fatalf("mode/focus = %q/%v, want edit/focused", model.mode, model.editor.Focused())
	}

	updated, cmd = model.handleKey(tea.KeyMsg{Type: tea.KeyEsc})
	model = updated.(Model)
	if cmd != nil {
		_ = cmd()
	}
	updated, cmd = model.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'R'}})
	model = updated.(Model)
	if cmd == nil {
		t.Fatal("expected manual refresh command")
	}
	_ = cmd()
	if ran != "select * from mem" {
		t.Fatalf("ran query = %q, want loaded history query", ran)
	}
}

func TestOverlayReducesResultViewportHeight(t *testing.T) {
	model := newTestModel(&fakeAdapter{})
	model.resize(100, 30)
	before := model.resultView.Height
	model.historyEntries = []history.Entry{
		{Query: "select * from cpu"},
		{Query: "select * from mem"},
		{Query: "select * from disk"},
	}

	updated, _ := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlR})
	model = updated.(Model)
	if model.resultView.Height >= before {
		t.Fatalf("result height = %d, want less than %d while overlay is open", model.resultView.Height, before)
	}
}

func TestHistoryPanelEmptyShowsStatus(t *testing.T) {
	model := newTestModel(&fakeAdapter{})

	updated, cmd := model.handleKey(tea.KeyMsg{Type: tea.KeyCtrlR})
	model = updated.(Model)
	if cmd != nil {
		t.Fatal("expected empty history to be synchronous")
	}
	if model.mode != modeEdit {
		t.Fatalf("mode = %q, want edit", model.mode)
	}
	if model.statusMessage != "history: empty" {
		t.Fatalf("status = %q, want history empty", model.statusMessage)
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
