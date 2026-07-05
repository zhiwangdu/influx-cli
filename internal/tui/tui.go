package tui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/zhiwangdu/influx-cli/internal/app"
	"github.com/zhiwangdu/influx-cli/internal/history"
	"github.com/zhiwangdu/influx-cli/internal/render"
	"github.com/zhiwangdu/influx-cli/internal/result"
	"github.com/zhiwangdu/influx-cli/internal/schema"
)

const (
	defaultQueryTimeout  = 30 * time.Second
	defaultWatchInterval = 5 * time.Second
	defaultWidth         = 100
	defaultHeight        = 30
	queryEditorHeight    = 4
	contextPanelWidth    = 34
	wideLayoutWidth      = 96
)

type Options struct {
	Render        render.Options
	History       *history.Store
	QueryTimeout  time.Duration
	WatchInterval time.Duration
	Context       context.Context
}

type Model struct {
	ctx           context.Context
	executor      *app.Executor
	editor        textarea.Model
	resultView    viewport.Model
	renderOptions render.Options
	renderMode    string

	width  int
	height int

	lastResult result.Result
	rendered   string
	renderer   string
	lastQuery  string
	lastErr    error

	statusMessage string
	loading       bool
	watch         bool
	fullscreen    bool
	schemaVisible bool

	history        *history.Store
	historyEntries []history.Entry
	historyIndex   int

	queryTimeout  time.Duration
	watchInterval time.Duration

	schemaLoading     bool
	schemaMeasurement string
	schemaSnapshot    *schema.Snapshot
	schemaErr         error
}

type historyLoadedMsg struct {
	entries []history.Entry
	err     error
}

type queryResultMsg struct {
	query      string
	fromWatch  bool
	result     result.Result
	err        error
	persisted  *history.Entry
	historyErr error
}

type completionMsg struct {
	line       string
	prefix     string
	candidates []string
	err        error
}

type schemaLoadedMsg struct {
	measurement string
	snapshot    schema.Snapshot
	err         error
}

type watchTickMsg time.Time

func Run(ctx context.Context, executor *app.Executor, in io.Reader, out io.Writer, options Options) error {
	if executor == nil {
		return fmt.Errorf("executor is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	options.Context = ctx
	model := NewModel(executor, options)
	program := tea.NewProgram(
		model,
		tea.WithContext(ctx),
		tea.WithInput(in),
		tea.WithOutput(out),
		tea.WithAltScreen(),
	)
	_, err := program.Run()
	if errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil {
		return nil
	}
	return err
}

func NewModel(executor *app.Executor, options Options) Model {
	editor := textarea.New()
	editor.Placeholder = "select * from measurement limit 10"
	editor.Prompt = "> "
	editor.ShowLineNumbers = false
	editor.SetWidth(defaultWidth)
	editor.SetHeight(queryEditorHeight)
	editor.Focus()

	view := viewport.New(defaultWidth, 12)
	view.SetContent("no result yet")

	renderOptions := options.Render
	format, err := render.NormalizeFormat(renderOptions.Format)
	if err != nil {
		format = render.FormatTable
	}
	renderOptions.Format = format
	if renderOptions.Width <= 0 {
		renderOptions.Width = defaultWidth
	}
	if renderOptions.MaxRows <= 0 {
		renderOptions.MaxRows = 200
	}
	if renderOptions.MaxSeries <= 0 {
		renderOptions.MaxSeries = 5
	}

	queryTimeout := options.QueryTimeout
	if queryTimeout <= 0 {
		queryTimeout = defaultQueryTimeout
	}
	watchInterval := options.WatchInterval
	if watchInterval <= 0 {
		watchInterval = defaultWatchInterval
	}
	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}

	model := Model{
		ctx:           ctx,
		executor:      executor,
		editor:        editor,
		resultView:    view,
		renderOptions: renderOptions,
		renderMode:    format,
		width:         defaultWidth,
		height:        defaultHeight,
		rendered:      "no result yet",
		statusMessage: "ready",
		schemaVisible: true,
		history:       options.History,
		historyIndex:  -1,
		queryTimeout:  queryTimeout,
		watchInterval: watchInterval,
	}
	model.resize(defaultWidth, defaultHeight)
	return model
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(m.editor.Focus(), m.loadHistoryCmd())
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.resize(msg.Width, msg.Height)
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(msg)
	case historyLoadedMsg:
		if msg.err != nil {
			m.statusMessage = "history: " + oneLine(msg.err.Error())
			return m, nil
		}
		m.historyEntries = msg.entries
		m.historyIndex = -1
		return m, nil
	case queryResultMsg:
		return m.handleQueryResult(msg)
	case completionMsg:
		return m.handleCompletion(msg), nil
	case schemaLoadedMsg:
		return m.handleSchema(msg), nil
	case watchTickMsg:
		if !m.watch {
			return m, nil
		}
		if m.loading {
			return m, m.scheduleWatchCmd()
		}
		query := strings.TrimSpace(m.lastQuery)
		if query == "" {
			query = strings.TrimSpace(m.editor.Value())
		}
		if query == "" {
			return m, m.scheduleWatchCmd()
		}
		m.loading = true
		m.statusMessage = "watch refresh running"
		return m, m.runQueryCmd(query, true)
	default:
		updated, cmd := m.resultView.Update(msg)
		m.resultView = updated
		return m, cmd
	}
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		return m, tea.Quit
	case "ctrl+j", "ctrl+enter":
		return m.startQuery(strings.TrimSpace(m.editor.Value()), false)
	case "ctrl+r":
		m.recallHistory()
		return m, nil
	case "tab":
		if m.loading {
			m.statusMessage = "query is running"
			return m, nil
		}
		var focusCmd tea.Cmd
		if !m.editor.Focused() {
			focusCmd = m.editor.Focus()
		}
		line := m.editor.Value()
		return m, tea.Batch(focusCmd, m.completeCmd(line))
	case "esc":
		if m.editor.Focused() {
			m.editor.Blur()
			m.statusMessage = "command mode"
		} else {
			cmd := m.editor.Focus()
			m.statusMessage = "edit mode"
			return m, cmd
		}
		return m, nil
	}

	if m.editor.Focused() {
		updated, cmd := m.editor.Update(msg)
		m.editor = updated
		return m, cmd
	}

	switch msg.String() {
	case "1":
		m.setRenderMode(render.FormatTable)
		return m, nil
	case "2":
		m.setRenderMode(render.FormatSparkline)
		return m, nil
	case "3":
		m.setRenderMode(render.FormatChart)
		return m, nil
	case "s", "S":
		m.schemaVisible = !m.schemaVisible
		if m.schemaVisible && m.schemaSnapshot == nil && m.schemaMeasurement != "" {
			m.schemaLoading = true
			return m, m.loadSchemaCmd(m.schemaMeasurement)
		}
		return m, nil
	case "w", "W":
		m.watch = !m.watch
		if !m.watch {
			m.statusMessage = "watch off"
			return m, nil
		}
		query := strings.TrimSpace(m.editor.Value())
		if query == "" {
			m.statusMessage = "watch on; no query"
			return m, m.scheduleWatchCmd()
		}
		m.lastQuery = query
		if m.loading {
			m.statusMessage = "watch on"
			return m, m.scheduleWatchCmd()
		}
		m.loading = true
		m.statusMessage = "watch on"
		return m, m.runQueryCmd(query, true)
	case "f", "F":
		m.fullscreen = !m.fullscreen
		m.resize(m.width, m.height)
		return m, nil
	case "q", "Q":
		return m, tea.Quit
	}

	return m.focusEditor(msg)
}

func (m Model) focusEditor(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	focusCmd := m.editor.Focus()
	updated, updateCmd := m.editor.Update(msg)
	m.editor = updated
	return m, tea.Batch(focusCmd, updateCmd)
}

func (m Model) startQuery(query string, fromWatch bool) (tea.Model, tea.Cmd) {
	if query == "" {
		m.statusMessage = "empty query"
		return m, nil
	}
	if m.loading {
		m.statusMessage = "query is already running"
		return m, nil
	}
	m.loading = true
	m.lastQuery = query
	m.editor.Blur()
	m.statusMessage = "query running"
	return m, m.runQueryCmd(query, fromWatch)
}

func (m Model) runQueryCmd(query string, fromWatch bool) tea.Cmd {
	executor := m.executor
	timeout := m.queryTimeout
	baseCtx := m.ctx
	store := m.history
	return func() tea.Msg {
		ctx := baseCtx
		if ctx == nil {
			ctx = context.Background()
		}
		var cancel context.CancelFunc
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		res, err := executor.Execute(ctx, query)
		msg := queryResultMsg{
			query:     query,
			fromWatch: fromWatch,
			result:    res,
			err:       err,
		}
		if err != nil || fromWatch || !shouldPersistQuery(query) || store == nil {
			return msg
		}
		snapshot := executor.Session.Snapshot()
		entry := history.Entry{
			Database:        snapshot.Database,
			RetentionPolicy: snapshot.RP,
			Dialect:         string(snapshot.Dialect),
			Query:           query,
		}
		if historyErr := store.Append(entry); historyErr != nil {
			msg.historyErr = historyErr
			return msg
		}
		msg.persisted = &entry
		return msg
	}
}

func (m Model) handleQueryResult(msg queryResultMsg) (tea.Model, tea.Cmd) {
	m.loading = false
	m.lastQuery = msg.query
	m.lastErr = msg.err
	if msg.err != nil {
		m.rendered = "error: " + oneLine(msg.err.Error())
		m.resultView.SetContent(m.rendered)
		m.statusMessage = "query failed"
		if m.watch {
			return m, m.scheduleWatchCmd()
		}
		return m, nil
	}

	m.lastResult = msg.result
	if msg.result.Schema != nil {
		snapshot := *msg.result.Schema
		m.schemaSnapshot = &snapshot
		m.schemaMeasurement = firstSchemaMeasurement(snapshot)
		m.schemaErr = nil
		m.schemaLoading = false
	}
	m.rerender()
	m.statusMessage = "query ok"
	if msg.historyErr != nil {
		m.statusMessage = "history: " + oneLine(msg.historyErr.Error())
	}
	if msg.persisted != nil {
		m.historyEntries = append([]history.Entry{*msg.persisted}, m.historyEntries...)
		m.historyIndex = -1
	}

	var cmds []tea.Cmd
	if measurement := inferMeasurement(msg.query); measurement != "" {
		if m.schemaSnapshot != nil && findMeasurement(*m.schemaSnapshot, measurement).Name != "" {
			m.schemaMeasurement = measurement
			m.schemaLoading = false
			m.schemaErr = nil
		} else if measurement != m.schemaMeasurement || m.schemaSnapshot == nil {
			m.schemaMeasurement = measurement
			m.schemaLoading = true
			cmds = append(cmds, m.loadSchemaCmd(measurement))
		} else {
			m.schemaMeasurement = measurement
		}
	}
	if m.watch {
		cmds = append(cmds, m.scheduleWatchCmd())
	}
	return m, tea.Batch(cmds...)
}

func (m Model) completeCmd(line string) tea.Cmd {
	executor := m.executor
	timeout := m.queryTimeout
	baseCtx := m.ctx
	pos := len([]rune(line))
	return func() tea.Msg {
		ctx := baseCtx
		if ctx == nil {
			ctx = context.Background()
		}
		var cancel context.CancelFunc
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		completion, err := executor.Complete(ctx, line, pos)
		return completionMsg{
			line:       line,
			prefix:     completion.Prefix,
			candidates: completion.Candidates,
			err:        err,
		}
	}
}

func (m Model) handleCompletion(msg completionMsg) Model {
	if msg.err != nil {
		m.statusMessage = "completion: " + oneLine(msg.err.Error())
		return m
	}
	if len(msg.candidates) == 0 {
		m.statusMessage = "completion: no matches"
		return m
	}
	candidate := msg.candidates[0]
	value := replaceTrailingPrefix(m.editor.Value(), msg.prefix, candidate)
	m.editor.SetValue(value)
	m.editor.CursorEnd()
	if len(msg.candidates) == 1 {
		m.statusMessage = "completion: " + candidate
		return m
	}
	m.statusMessage = "completion: " + strings.Join(limitStrings(msg.candidates, 6), " ")
	return m
}

func (m Model) loadSchemaCmd(measurement string) tea.Cmd {
	executor := m.executor
	timeout := m.queryTimeout
	baseCtx := m.ctx
	return func() tea.Msg {
		ctx := baseCtx
		if ctx == nil {
			ctx = context.Background()
		}
		var cancel context.CancelFunc
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		snapshot, err := executor.Schema(ctx, measurement)
		return schemaLoadedMsg{measurement: measurement, snapshot: snapshot, err: err}
	}
}

func (m Model) handleSchema(msg schemaLoadedMsg) Model {
	if msg.measurement != m.schemaMeasurement {
		return m
	}
	m.schemaLoading = false
	m.schemaErr = msg.err
	if msg.err != nil {
		return m
	}
	snapshot := msg.snapshot
	m.schemaSnapshot = &snapshot
	return m
}

func (m Model) loadHistoryCmd() tea.Cmd {
	store := m.history
	if store == nil {
		return nil
	}
	return func() tea.Msg {
		entries, err := store.Search("", history.DefaultMaxEntries)
		return historyLoadedMsg{entries: entries, err: err}
	}
}

func (m Model) scheduleWatchCmd() tea.Cmd {
	if !m.watch {
		return nil
	}
	return tea.Tick(m.watchInterval, func(t time.Time) tea.Msg {
		return watchTickMsg(t)
	})
}

func (m *Model) recallHistory() {
	if len(m.historyEntries) == 0 {
		m.statusMessage = "history: empty"
		return
	}
	m.historyIndex++
	if m.historyIndex >= len(m.historyEntries) {
		m.historyIndex = 0
	}
	entry := m.historyEntries[m.historyIndex]
	m.editor.SetValue(entry.Query)
	m.editor.CursorEnd()
	m.statusMessage = fmt.Sprintf("history: %d/%d", m.historyIndex+1, len(m.historyEntries))
}

func (m *Model) setRenderMode(format string) {
	m.renderMode = format
	m.renderOptions.Format = format
	m.rerender()
	m.statusMessage = "format: " + format
}

func (m *Model) rerender() {
	if m.lastResult.Kind == "" {
		m.resultView.SetContent(m.rendered)
		return
	}
	output, renderer, err := render.Render(m.lastResult, m.renderOptions)
	if err != nil {
		m.lastErr = err
		m.rendered = "error: " + oneLine(err.Error())
		m.resultView.SetContent(m.rendered)
		return
	}
	if strings.TrimSpace(output) == "" {
		output = "no rows"
	}
	m.renderer = renderer
	m.rendered = output
	m.resultView.SetContent(output)
}

func shouldPersistQuery(query string) bool {
	return !strings.HasPrefix(strings.TrimSpace(query), ":")
}
