package tui

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

var (
	statusStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("15")).Background(lipgloss.Color("62"))
	titleStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("39"))
	dimStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
)

func (m Model) View() string {
	width := m.width
	if width <= 0 {
		width = defaultWidth
	}

	var builder strings.Builder
	builder.WriteString(m.statusView(width))
	builder.WriteByte('\n')
	builder.WriteString(divider(width))
	builder.WriteString("\n\n")

	if !m.fullscreen {
		builder.WriteString(titleStyle.Render("Query"))
		builder.WriteByte('\n')
		builder.WriteString(m.editor.View())
		builder.WriteString("\n\n")
	}

	if m.overlay.Active() {
		builder.WriteString(m.overlayView(width))
		builder.WriteString("\n\n")
	}

	resultPanel := m.resultPanel()
	if m.schemaVisible && width >= wideLayoutWidth && !m.fullscreen {
		builder.WriteString(lipgloss.JoinHorizontal(lipgloss.Top, resultPanel, strings.Repeat(" ", 2), m.contextPanel(contextPanelWidth)))
	} else {
		builder.WriteString(resultPanel)
		if m.schemaVisible && !m.fullscreen {
			builder.WriteString("\n\n")
			builder.WriteString(m.contextPanel(width))
		}
	}

	builder.WriteByte('\n')
	builder.WriteString(m.footerView(width))
	return builder.String()
}

func (m *Model) resize(width, height int) {
	if width <= 0 {
		width = defaultWidth
	}
	if height <= 0 {
		height = defaultHeight
	}
	m.width = width
	m.height = height

	editorWidth := width
	if editorWidth > 2 {
		editorWidth -= 2
	}
	m.editor.SetWidth(editorWidth)
	m.editor.SetHeight(queryEditorHeight)

	mainHeight := height - 12
	if m.fullscreen {
		mainHeight = height - 5
	}
	minMainHeight := 6
	minResultHeight := 4
	if overlayLines := m.overlayLineCount(); overlayLines > 0 {
		mainHeight -= overlayLines + 2
		minMainHeight = 3
		minResultHeight = 2
	}
	if mainHeight < minMainHeight {
		mainHeight = minMainHeight
	}

	resultWidth := width
	contextHeight := 0
	if m.schemaVisible && width >= wideLayoutWidth && !m.fullscreen {
		resultWidth = width - contextPanelWidth - 2
	} else if m.schemaVisible && !m.fullscreen {
		contextHeight = 7
		mainHeight -= contextHeight + 2
		if mainHeight < 6 {
			mainHeight = 6
		}
	}
	if resultWidth < 24 {
		resultWidth = 24
	}
	if contextHeight > 0 && mainHeight < 6 {
		mainHeight = 6
	}

	m.resultView.Width = resultWidth
	m.resultView.Height = mainHeight - 1
	if m.resultView.Height < minResultHeight {
		m.resultView.Height = minResultHeight
	}
	m.renderOptions.Width = resultWidth
	m.rerender()
}

func (m Model) statusView(width int) string {
	watch := "off"
	if m.watch {
		watch = "on/" + formatDuration(m.watchInterval)
	}
	state := m.statusMessage
	if m.loading {
		state = "running"
	}
	status := fmt.Sprintf("%s | format: %s | watch: %s | %s",
		m.executor.Session.StatusLine(),
		printValue(m.renderMode),
		watch,
		state,
	)
	status = truncateRunes(status, width)
	if !m.renderOptions.Color {
		return status
	}
	return statusStyle.Width(width).Render(status)
}

func (m Model) resultPanel() string {
	title := "Result"
	if m.renderer != "" {
		title += " [" + m.renderer + "]"
	}
	if summary := m.resultSummary(); summary != "" {
		title += " | " + summary
	}
	if latency := m.executor.Session.Snapshot().LastLatency; latency > 0 {
		title += " | latency " + formatDuration(latency)
	}
	if m.loading {
		title += " running"
	}
	return titleStyle.Render(truncateRunes(title, m.resultView.Width)) + "\n" + m.resultView.View()
}

func (m Model) contextPanel(width int) string {
	if width < 24 {
		width = 24
	}
	snapshot := m.executor.Session.Snapshot()
	lines := []string{
		titleStyle.Render("Context"),
		"db: " + printValue(snapshot.Database),
		"rp: " + printValue(snapshot.RP),
		"adapter: " + printValue(snapshot.AdapterName),
		"precision: " + printValue(snapshot.Precision),
		"measurement: " + printValue(m.schemaMeasurement),
		"query: " + printValue(m.querySummary()),
		"format: " + printValue(m.renderMode),
		"result: " + printValue(m.resultSummary()),
		"latency: " + formatDuration(snapshot.LastLatency),
		"watch: " + m.watchSummary(),
	}
	if timeRange := m.resultTimeRange(); timeRange != "" {
		lines = append(lines, "time: "+timeRange)
	}
	if hint := m.syntaxHint(); hint != "" {
		lines = append(lines, "syntax: "+hint)
	}
	if m.lastErr != nil {
		lines = append(lines, "last error: "+oneLine(m.lastErr.Error()))
	}
	lines = append(lines, m.schemaLines()...)
	lines = append(lines, "controls: S toggle, L refresh")
	return strings.Join(fitLines(lines, width), "\n")
}

func (m Model) schemaLines() []string {
	if m.schemaLoading {
		return []string{"schema: loading"}
	}
	if m.schemaErr != nil {
		return []string{"schema: " + oneLine(m.schemaErr.Error())}
	}
	if m.schemaSnapshot == nil {
		return []string{"schema: -"}
	}
	measurement := findMeasurement(*m.schemaSnapshot, m.schemaMeasurement)
	if measurement.Name == "" {
		return []string{"schema: -"}
	}
	fieldNames := make([]string, 0, len(measurement.Fields))
	for _, field := range measurement.Fields {
		if field.Type != "" {
			fieldNames = append(fieldNames, field.Name+":"+field.Type)
		} else {
			fieldNames = append(fieldNames, field.Name)
		}
	}
	tagNames := make([]string, 0, len(measurement.Tags))
	for _, tag := range measurement.Tags {
		tagNames = append(tagNames, tag.Name)
	}
	sort.Strings(fieldNames)
	sort.Strings(tagNames)
	return []string{
		fmt.Sprintf("schema: %d fields, %d tags", len(fieldNames), len(tagNames)),
		"fields: " + joinNames(fieldNames),
		"tags: " + joinNames(tagNames),
	}
}

func (m Model) footerView(width int) string {
	footer := string(m.mode) + " | Ctrl+J run | Ctrl+C cancel/quit | Ctrl+L clear | Ctrl+R history | Tab complete | Esc mode | Enter/V result | 0 auto | 1 table | 2 spark | 3 chart | 4 json | R refresh | +/- interval | S context | L schema refresh | W watch | F fullscreen | Q quit"
	footer = truncateRunes(footer, width)
	if !m.renderOptions.Color {
		return footer
	}
	return dimStyle.Render(footer)
}

func (m Model) overlayView(width int) string {
	if width < 24 {
		width = 24
	}
	title := m.overlay.Title
	if m.overlay.Kind == overlayHistory && m.overlay.Filter != "" {
		title += " filter: " + m.overlay.Filter
	}
	if len(m.overlay.Items) == 0 {
		lines := fitLines([]string{
			title,
			"no matches",
		}, width)
		lines[0] = titleStyle.Render(lines[0])
		return strings.Join(lines, "\n")
	}

	const maxVisible = 8
	selected := clampIndex(m.overlay.Selected, len(m.overlay.Items))
	start := selected - maxVisible/2
	if start < 0 {
		start = 0
	}
	if start+maxVisible > len(m.overlay.Items) {
		start = len(m.overlay.Items) - maxVisible
		if start < 0 {
			start = 0
		}
	}
	end := start + maxVisible
	if end > len(m.overlay.Items) {
		end = len(m.overlay.Items)
	}

	lines := []string{fmt.Sprintf("%s %d/%d", title, selected+1, len(m.overlay.Items))}
	for i := start; i < end; i++ {
		item := m.overlay.Items[i]
		marker := " "
		if i == selected {
			marker = ">"
		}
		line := marker + " " + item.Label
		if item.Detail != "" {
			line += "  " + item.Detail
		}
		lines = append(lines, line)
	}
	lines = fitLines(lines, width)
	lines[0] = titleStyle.Render(lines[0])
	return strings.Join(lines, "\n")
}

func (m Model) overlayLineCount() int {
	if !m.overlay.Active() {
		return 0
	}
	if len(m.overlay.Items) == 0 {
		return 2
	}
	if len(m.overlay.Items) > 8 {
		return 9
	}
	return len(m.overlay.Items) + 1
}

func (m Model) resultSummary() string {
	if m.lastResult.Kind == "" {
		return ""
	}
	rowCount := m.lastResult.Metadata.RowCount
	if rowCount == 0 && m.lastResult.Table != nil {
		rowCount = m.lastResult.Table.RowCount()
	}
	pointCount := m.lastResult.Metadata.PointCount
	if pointCount == 0 {
		for _, series := range m.lastResult.Series {
			pointCount += len(series.Points)
		}
	}
	seriesCount := m.lastResult.Metadata.SeriesCount
	if seriesCount == 0 && len(m.lastResult.Series) > 0 {
		seriesCount = len(m.lastResult.Series)
	}

	parts := make([]string, 0, 3)
	if rowCount > 0 || m.lastResult.Table != nil {
		if m.renderOptions.MaxRows > 0 && rowCount > m.renderOptions.MaxRows {
			parts = append(parts, fmt.Sprintf("rows %d shown %d", rowCount, m.renderOptions.MaxRows))
		} else {
			parts = append(parts, fmt.Sprintf("rows %d", rowCount))
		}
	}
	if pointCount > 0 {
		parts = append(parts, fmt.Sprintf("points %d", pointCount))
	}
	if seriesCount > 0 {
		parts = append(parts, fmt.Sprintf("series %d", seriesCount))
	}
	return strings.Join(parts, ", ")
}

func (m Model) querySummary() string {
	query := strings.TrimSpace(m.lastQuery)
	if query == "" {
		return ""
	}
	return oneLine(query)
}

func (m Model) syntaxHint() string {
	query := strings.TrimSpace(m.editor.Value())
	if query == "" {
		return ""
	}
	tokens := queryTokens(query)
	if len(tokens) == 0 {
		return ""
	}
	if !strings.EqualFold(tokens[0], "select") {
		return ""
	}
	for _, token := range tokens[1:] {
		if strings.EqualFold(token, "from") {
			return ""
		}
	}
	return "SELECT needs FROM"
}

func (m Model) resultTimeRange() string {
	var minTime time.Time
	var maxTime time.Time
	for _, series := range m.lastResult.Series {
		for _, point := range series.Points {
			if point.Time.IsZero() {
				continue
			}
			if minTime.IsZero() || point.Time.Before(minTime) {
				minTime = point.Time
			}
			if maxTime.IsZero() || point.Time.After(maxTime) {
				maxTime = point.Time
			}
		}
	}
	if minTime.IsZero() || maxTime.IsZero() {
		return ""
	}
	return minTime.Format(time.RFC3339) + " .. " + maxTime.Format(time.RFC3339)
}

func (m Model) watchSummary() string {
	if !m.watch {
		return "off"
	}
	summary := "on " + formatDuration(m.watchInterval)
	if !m.lastRefresh.IsZero() {
		summary += " last " + m.lastRefresh.Format("15:04:05")
	}
	if m.lastErr != nil {
		summary += " error " + oneLine(m.lastErr.Error())
	}
	return summary
}

func divider(width int) string {
	if width < 1 {
		width = 1
	}
	return strings.Repeat("-", width)
}

func fitLines(lines []string, width int) []string {
	out := make([]string, len(lines))
	for i, line := range lines {
		out[i] = truncateRunes(line, width)
	}
	return out
}

func joinNames(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	if len(values) > 6 {
		values = append(append([]string(nil), values[:6]...), "...")
	}
	return strings.Join(values, ", ")
}
