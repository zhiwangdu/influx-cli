package ingest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
)

const (
	defaultIQLStartDate = "2016-01-01"
	defaultIQLSeed      = int64(1)
)

type iqlScript struct {
	statements []iqlStatement
}

type iqlStatement interface {
	isIQLStatement()
}

type iqlSetStatement struct {
	name  string
	value string
}

func (iqlSetStatement) isIQLStatement() {}

type iqlInsertStatement struct {
	name             string
	template         string
	tagTemplateCount int
	count            int64
	tick             time.Duration
}

func (iqlInsertStatement) isIQLStatement() {}

type iqlQueryStatement struct {
	name  string
	count int
}

func (iqlQueryStatement) isIQLStatement() {}

type iqlWaitStatement struct{}

func (iqlWaitStatement) isIQLStatement() {}

type iqlRawStatement struct{}

func (iqlRawStatement) isIQLStatement() {}

type iqlLine struct {
	number int
	text   string
}

type iqlState struct {
	database             string
	retentionPolicy      string
	precision            string
	start                time.Time
	batchSize            int
	forceDatabase        bool
	forceRetentionPolicy bool
	forcePrecision       bool
	forceStart           bool
	forceBatchSize       bool
	allowEmptyDatabase   bool
}

func runIQL(ctx context.Context, writer adapter.LineProtocolWriter, options Options) (Summary, error) {
	if strings.TrimSpace(options.IQLFile) == "" {
		return Summary{}, errors.New("iql ingest requires --file")
	}
	body, err := os.ReadFile(options.IQLFile)
	if err != nil {
		return Summary{}, fmt.Errorf("read iql file %s: %w", options.IQLFile, err)
	}
	script, err := parseIQLScript(string(body))
	if err != nil {
		return Summary{}, err
	}

	state, err := newIQLState(options)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{
		Dataset:         DatasetIQL,
		Database:        state.database,
		RetentionPolicy: state.retentionPolicy,
		Precision:       state.precision,
		Measurement:     "iql",
		IQLFile:         options.IQLFile,
	}

	rng := rand.New(rand.NewSource(defaultIQLSeed))
	started := time.Now()
	for _, statement := range script.statements {
		select {
		case <-ctx.Done():
			return summary, ctx.Err()
		default:
		}

		switch stmt := statement.(type) {
		case iqlSetStatement:
			if err := applyIQLSet(&state, &summary, stmt); err != nil {
				return summary, err
			}
		case iqlInsertStatement:
			if err := runIQLInsert(ctx, writer, state, stmt, rng, &summary); err != nil {
				return summary, err
			}
		case iqlQueryStatement:
			if stmt.count > 0 {
				summary.IQLSkippedQuery += stmt.count
			} else {
				summary.IQLSkippedQuery++
			}
		case iqlRawStatement:
			summary.IQLSkippedRaw++
		case iqlWaitStatement:
		}
	}
	summary.Elapsed = time.Since(started)
	summary.Database = state.database
	summary.RetentionPolicy = state.retentionPolicy
	summary.Precision = state.precision
	if !summary.DataStartedAt.IsZero() && !summary.DataEndedAt.IsZero() {
		summary.StartedAt = summary.DataStartedAt
		summary.EndedAt = summary.DataEndedAt
		summary.Duration = summary.DataEndedAt.Sub(summary.DataStartedAt)
	} else {
		summary.StartedAt = state.start
		summary.EndedAt = state.start
	}
	return summary, nil
}

func newIQLState(options Options) (iqlState, error) {
	precision := strings.TrimSpace(options.Precision)
	if precision == "" || (!options.ForcePrecision && (precision == "rfc3339" || precision == "rfc3339nano")) {
		precision = "s"
	}
	normalizedPrecision, err := normalizePrecision(precision)
	if err != nil {
		return iqlState{}, err
	}
	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}
	if batchSize < 1 {
		return iqlState{}, errors.New("batch size must be greater than zero")
	}
	if batchSize > MaxBatchSize {
		return iqlState{}, fmt.Errorf("batch size must be less than or equal to %d", MaxBatchSize)
	}
	start := options.Start
	if start.IsZero() {
		start, err = parseIQLStartDate(defaultIQLStartDate)
		if err != nil {
			return iqlState{}, err
		}
	}
	return iqlState{
		database:             strings.TrimSpace(options.Database),
		retentionPolicy:      strings.TrimSpace(options.RetentionPolicy),
		precision:            normalizedPrecision,
		start:                start.UTC(),
		batchSize:            batchSize,
		forceDatabase:        options.ForceDatabase,
		forceRetentionPolicy: options.ForceRetentionPolicy,
		forcePrecision:       options.ForcePrecision,
		forceStart:           options.ForceStart,
		forceBatchSize:       options.ForceBatchSize,
		allowEmptyDatabase:   options.AllowEmptyDatabase,
	}, nil
}

func applyIQLSet(state *iqlState, summary *Summary, stmt iqlSetStatement) error {
	name := strings.ToLower(strings.TrimSpace(stmt.name))
	value := strings.TrimSpace(stmt.value)
	switch name {
	case "database":
		if !state.forceDatabase {
			state.database = value
		}
	case "retentionpolicy", "retention_policy", "rp":
		if !state.forceRetentionPolicy {
			state.retentionPolicy = value
		}
	case "precision":
		if !state.forcePrecision {
			precision, err := normalizePrecision(value)
			if err != nil {
				return err
			}
			state.precision = precision
		}
	case "startdate":
		if !state.forceStart {
			start, err := parseIQLStartDate(value)
			if err != nil {
				return err
			}
			state.start = start.UTC()
		}
	case "batchsize":
		if !state.forceBatchSize {
			batchSize, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("parse IQL BatchSize %q: %w", value, err)
			}
			if batchSize < 1 {
				return errors.New("IQL BatchSize must be greater than zero")
			}
			if batchSize > MaxBatchSize {
				return fmt.Errorf("IQL BatchSize must be less than or equal to %d", MaxBatchSize)
			}
			state.batchSize = batchSize
		}
	default:
		addIgnoredIQLSet(summary, stmt.name)
	}
	return nil
}

func addIgnoredIQLSet(summary *Summary, name string) {
	name = strings.TrimSpace(name)
	for _, existing := range summary.IQLIgnoredSets {
		if strings.EqualFold(existing, name) {
			return
		}
	}
	summary.IQLIgnoredSets = append(summary.IQLIgnoredSets, name)
}

func runIQLInsert(ctx context.Context, writer adapter.LineProtocolWriter, state iqlState, stmt iqlInsertStatement, rng *rand.Rand, summary *Summary) error {
	if strings.TrimSpace(state.database) == "" && !state.allowEmptyDatabase {
		return errors.New("database is required for iql ingest; set --db, configure a profile database, or include SET Database [...]")
	}
	compiled, err := compileIQLTemplate(stmt, rng)
	if err != nil {
		return err
	}
	clock := newIQLClock(state.start, stmt.tick, compiled.series)
	summary.IQLInserts++
	summary.RequestedPoints += stmt.count

	var buffer bytes.Buffer
	var pendingPoints int64
	for i := int64(0); i < stmt.count; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		timestamp := clock.Next()
		if summary.DataStartedAt.IsZero() || timestamp.Before(summary.DataStartedAt) {
			summary.DataStartedAt = timestamp
		}
		if summary.DataEndedAt.IsZero() || timestamp.After(summary.DataEndedAt) {
			summary.DataEndedAt = timestamp
		}
		buffer.WriteString(compiled.Line(i, encodeTimestamp(timestamp, state.precision)))
		buffer.WriteByte('\n')
		pendingPoints++
		if pendingPoints == int64(state.batchSize) {
			if err := writeIQLBatch(ctx, writer, state, buffer.Bytes()); err != nil {
				return err
			}
			summary.WrittenPoints += pendingPoints
			summary.Batches++
			pendingPoints = 0
			buffer.Reset()
		}
	}
	if buffer.Len() > 0 {
		if err := writeIQLBatch(ctx, writer, state, buffer.Bytes()); err != nil {
			return err
		}
		summary.WrittenPoints += pendingPoints
		summary.Batches++
	}
	return nil
}

func writeIQLBatch(ctx context.Context, writer adapter.LineProtocolWriter, state iqlState, body []byte) error {
	return writer.WriteLineProtocol(ctx, adapter.WriteRequest{
		Database:        state.database,
		RetentionPolicy: state.retentionPolicy,
		Precision:       state.precision,
		Body:            append([]byte(nil), body...),
	})
}

type iqlClock struct {
	current time.Time
	tick    time.Duration
	series  int64
	count   int64
}

func newIQLClock(start time.Time, tick time.Duration, series int64) *iqlClock {
	return &iqlClock{current: start.UTC(), tick: tick, series: series}
}

func (c *iqlClock) Next() time.Time {
	c.count++
	if c.count > c.series {
		c.current = c.current.Add(c.tick)
		c.count = 1
	}
	return c.current
}

func parseIQLScript(raw string) (iqlScript, error) {
	lines, err := scanIQLLines(raw)
	if err != nil {
		return iqlScript{}, err
	}
	var script iqlScript
	for i := 0; i < len(lines); {
		if lines[i].text == "" {
			i++
			continue
		}
		line := lines[i].text
		upper := strings.ToUpper(line)
		switch {
		case strings.HasPrefix(upper, "SET "):
			stmt, err := parseIQLSet(lines[i])
			if err != nil {
				return iqlScript{}, err
			}
			script.statements = append(script.statements, stmt)
			i++
		case strings.HasPrefix(upper, "USE "):
			stmt, err := parseIQLUse(lines[i])
			if err != nil {
				return iqlScript{}, err
			}
			script.statements = append(script.statements, stmt)
			i++
		case isIQLInsertHeader(upper):
			stmt, next, err := parseIQLInsert(lines, i)
			if err != nil {
				return iqlScript{}, err
			}
			script.statements = append(script.statements, stmt)
			i = next
		case isIQLQueryHeader(upper):
			stmt, next, err := parseIQLQuery(lines, i)
			if err != nil {
				return iqlScript{}, err
			}
			script.statements = append(script.statements, stmt)
			i = next
		case upper == "WAIT":
			script.statements = append(script.statements, iqlWaitStatement{})
			i++
		default:
			script.statements = append(script.statements, iqlRawStatement{})
			i++
		}
	}
	return script, nil
}

func scanIQLLines(raw string) ([]iqlLine, error) {
	scanner := bufio.NewScanner(strings.NewReader(raw))
	scanner.Buffer(make([]byte, 1024), 1024*1024)
	var lines []iqlLine
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, iqlLine{number: lineNumber, text: line})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func parseIQLSet(line iqlLine) (iqlSetStatement, error) {
	rest := strings.TrimSpace(line.text[len("SET "):])
	open := strings.Index(rest, "[")
	close := strings.LastIndex(rest, "]")
	if open < 1 || close <= open {
		return iqlSetStatement{}, fmt.Errorf("parse iql line %d: expected SET name [value]", line.number)
	}
	return iqlSetStatement{
		name:  strings.TrimSpace(rest[:open]),
		value: strings.TrimSpace(rest[open+1 : close]),
	}, nil
}

func parseIQLUse(line iqlLine) (iqlSetStatement, error) {
	parts := strings.Fields(line.text)
	if len(parts) != 2 {
		return iqlSetStatement{}, fmt.Errorf("parse iql line %d: expected USE database", line.number)
	}
	return iqlSetStatement{name: "Database", value: parts[1]}, nil
}

func isIQLInsertHeader(upper string) bool {
	return strings.HasPrefix(upper, "INSERT ") || strings.HasPrefix(upper, "GO INSERT ")
}

func parseIQLInsert(lines []iqlLine, start int) (iqlInsertStatement, int, error) {
	headerFields := strings.Fields(lines[start].text)
	if len(headerFields) < 2 {
		return iqlInsertStatement{}, start, fmt.Errorf("parse iql line %d: INSERT requires a name", lines[start].number)
	}
	nameIndex := 1
	if strings.EqualFold(headerFields[0], "GO") {
		if len(headerFields) < 3 {
			return iqlInsertStatement{}, start, fmt.Errorf("parse iql line %d: GO INSERT requires a name", lines[start].number)
		}
		nameIndex = 2
	}

	var content []iqlLine
	i := start + 1
	for ; i < len(lines); i++ {
		if lines[i].text == "" {
			continue
		}
		content = append(content, lines[i])
		if _, _, ok := parseIQLTimestampLine(lines[i].text); ok {
			i++
			break
		}
	}
	if len(content) < 3 {
		return iqlInsertStatement{}, start, fmt.Errorf("parse iql line %d: INSERT requires measurement/tags, fields, and timestamp lines", lines[start].number)
	}
	timestampLine := content[len(content)-1]
	count, tick, ok := parseIQLTimestampLine(timestampLine.text)
	if !ok {
		return iqlInsertStatement{}, start, fmt.Errorf("parse iql line %d: INSERT missing timestamp count and duration", lines[start].number)
	}
	if count < 1 {
		return iqlInsertStatement{}, start, fmt.Errorf("parse iql line %d: INSERT count must be greater than zero", timestampLine.number)
	}
	if tick <= 0 {
		return iqlInsertStatement{}, start, fmt.Errorf("parse iql line %d: INSERT duration must be greater than zero", timestampLine.number)
	}

	fields := content[len(content)-2].text
	var measurementTags strings.Builder
	for _, line := range content[:len(content)-2] {
		// IQL follows line protocol shape, so continuation lines are expected
		// to carry their own comma separators.
		measurementTags.WriteString(line.text)
	}
	mt := measurementTags.String()
	template := mt + " " + fields
	tagTemplateCount, err := countIQLTemplates(mt)
	if err != nil {
		return iqlInsertStatement{}, start, fmt.Errorf("parse iql line %d: %w", lines[start].number, err)
	}
	return iqlInsertStatement{
		name:             headerFields[nameIndex],
		template:         template,
		tagTemplateCount: tagTemplateCount,
		count:            count,
		tick:             tick,
	}, i, nil
}

func parseIQLTimestampLine(line string) (int64, time.Duration, bool) {
	parts := strings.Fields(line)
	if len(parts) != 2 {
		return 0, 0, false
	}
	count, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	duration, err := time.ParseDuration(parts[1])
	if err != nil {
		return 0, 0, false
	}
	return count, duration, true
}

func isIQLQueryHeader(upper string) bool {
	return strings.HasPrefix(upper, "QUERY ") || strings.HasPrefix(upper, "GO QUERY ")
}

func parseIQLQuery(lines []iqlLine, start int) (iqlQueryStatement, int, error) {
	headerFields := strings.Fields(lines[start].text)
	if len(headerFields) < 2 {
		return iqlQueryStatement{}, start, fmt.Errorf("parse iql line %d: QUERY requires a name", lines[start].number)
	}
	nameIndex := 1
	if strings.EqualFold(headerFields[0], "GO") {
		if len(headerFields) < 3 {
			return iqlQueryStatement{}, start, fmt.Errorf("parse iql line %d: GO QUERY requires a name", lines[start].number)
		}
		nameIndex = 2
	}
	stmt := iqlQueryStatement{name: headerFields[nameIndex], count: 1}
	i := start + 1
	for ; i < len(lines); i++ {
		if lines[i].text == "" {
			i++
			break
		}
		fields := strings.Fields(lines[i].text)
		if len(fields) == 2 && strings.EqualFold(fields[0], "DO") {
			count, err := strconv.Atoi(fields[1])
			if err != nil {
				return iqlQueryStatement{}, start, fmt.Errorf("parse iql line %d: parse DO count: %w", lines[i].number, err)
			}
			stmt.count = count
			i++
			break
		}
	}
	return stmt, i, nil
}

func countIQLTemplates(raw string) (int, error) {
	count := 0
	for i := 0; i < len(raw); i++ {
		if raw[i] != '[' {
			continue
		}
		end := strings.IndexByte(raw[i+1:], ']')
		if end < 0 {
			return 0, errors.New("unterminated template")
		}
		count++
		i += end + 1
	}
	return count, nil
}

func parseIQLStartDate(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if strings.EqualFold(raw, "now") {
		return time.Now().UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.UTC(), nil
	}
	parsed, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse IQL StartDate %q: %w", raw, err)
	}
	return parsed.UTC(), nil
}

type iqlCompiledTemplate struct {
	parts            []iqlTemplatePart
	tagValues        [][]string
	tagStrides       []int64
	fieldStringers   []func() string
	tagTemplateCount int
	series           int64
}

type iqlTemplatePart struct {
	literal       string
	stringerIndex int
}

type iqlValueTemplate struct {
	enum     []string
	dataType string
	fn       string
	argument int
	count    int64
}

func compileIQLTemplate(stmt iqlInsertStatement, rng *rand.Rand) (iqlCompiledTemplate, error) {
	parts, templates, err := parseIQLTemplate(stmt.template)
	if err != nil {
		return iqlCompiledTemplate{}, err
	}
	series := int64(1)
	tagValues := make([][]string, stmt.tagTemplateCount)
	tagStrides := make([]int64, stmt.tagTemplateCount)
	for i := 0; i < stmt.tagTemplateCount && i < len(templates); i++ {
		values, err := templates[i].finiteValues(rng)
		if err != nil {
			return iqlCompiledTemplate{}, err
		}
		if len(values) == 0 {
			return iqlCompiledTemplate{}, errors.New("IQL tag templates must have finite cardinality greater than zero")
		}
		if int64(len(values)) > maxInt64/series {
			return iqlCompiledTemplate{}, errors.New("IQL tag templates produce too many series")
		}
		tagValues[i] = values
		tagStrides[i] = series
		series *= int64(len(values))
	}
	fieldStringers := make([]func() string, len(templates))
	for i, template := range templates {
		if i < stmt.tagTemplateCount {
			continue
		}
		fieldStringers[i] = template.newStringer(series, rng)
	}
	return iqlCompiledTemplate{
		parts:            parts,
		tagValues:        tagValues,
		tagStrides:       tagStrides,
		fieldStringers:   fieldStringers,
		tagTemplateCount: stmt.tagTemplateCount,
		series:           series,
	}, nil
}

func (t iqlCompiledTemplate) Line(row int64, timestamp int64) string {
	var builder strings.Builder
	for _, part := range t.parts {
		if part.stringerIndex >= 0 {
			builder.WriteString(t.Value(part.stringerIndex, row))
			continue
		}
		builder.WriteString(part.literal)
	}
	builder.WriteByte(' ')
	builder.WriteString(strconv.FormatInt(timestamp, 10))
	return builder.String()
}

func (t iqlCompiledTemplate) Value(templateIndex int, row int64) string {
	if templateIndex < t.tagTemplateCount {
		values := t.tagValues[templateIndex]
		if len(values) == 0 {
			return ""
		}
		stride := t.tagStrides[templateIndex]
		if stride < 1 {
			stride = 1
		}
		return values[(row/stride)%int64(len(values))]
	}
	stringer := t.fieldStringers[templateIndex]
	if stringer == nil {
		return ""
	}
	return stringer()
}

func parseIQLTemplate(raw string) ([]iqlTemplatePart, []iqlValueTemplate, error) {
	parts := []iqlTemplatePart{}
	templates := []iqlValueTemplate{}
	for len(raw) > 0 {
		start := strings.IndexByte(raw, '[')
		if start < 0 {
			parts = append(parts, iqlTemplatePart{literal: raw, stringerIndex: -1})
			break
		}
		if start > 0 {
			parts = append(parts, iqlTemplatePart{literal: raw[:start], stringerIndex: -1})
		}
		end := strings.IndexByte(raw[start+1:], ']')
		if end < 0 {
			return nil, nil, errors.New("unterminated IQL template")
		}
		templateRaw := raw[start+1 : start+1+end]
		template, err := parseIQLValueTemplate(templateRaw)
		if err != nil {
			return nil, nil, err
		}
		parts = append(parts, iqlTemplatePart{stringerIndex: len(templates)})
		templates = append(templates, template)
		raw = raw[start+1+end+1:]
	}
	return parts, templates, nil
}

func parseIQLValueTemplate(raw string) (iqlValueTemplate, error) {
	raw = strings.TrimSpace(raw)
	if strings.Contains(raw, "|") {
		values := strings.Split(raw, "|")
		for i := range values {
			values[i] = strings.TrimSpace(values[i])
		}
		return iqlValueTemplate{enum: values}, nil
	}

	parts := strings.Fields(raw)
	if len(parts) != 3 {
		return iqlValueTemplate{}, fmt.Errorf("parse IQL template %q: expected enum or type fn(argument) count", raw)
	}
	fn, argument, err := parseIQLFunction(parts[1])
	if err != nil {
		return iqlValueTemplate{}, fmt.Errorf("parse IQL template %q: %w", raw, err)
	}
	count, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return iqlValueTemplate{}, fmt.Errorf("parse IQL template %q count: %w", raw, err)
	}
	if count < 0 {
		return iqlValueTemplate{}, fmt.Errorf("parse IQL template %q: count must be non-negative", raw)
	}
	return iqlValueTemplate{
		dataType: strings.ToLower(parts[0]),
		fn:       fn,
		argument: argument,
		count:    count,
	}, nil
}

func parseIQLFunction(raw string) (string, int, error) {
	open := strings.IndexByte(raw, '(')
	close := strings.LastIndexByte(raw, ')')
	if open <= 0 || close <= open {
		return "", 0, fmt.Errorf("expected function(argument), got %q", raw)
	}
	argument, err := strconv.Atoi(raw[open+1 : close])
	if err != nil {
		return "", 0, err
	}
	return strings.ToLower(raw[:open]), argument, nil
}

func (t iqlValueTemplate) numSeries() int64 {
	if len(t.enum) > 0 {
		return int64(len(t.enum))
	}
	return t.count
}

func (t iqlValueTemplate) finiteValues(rng *rand.Rand) ([]string, error) {
	count := t.numSeries()
	if count < 1 {
		return nil, nil
	}
	values := make([]string, count)
	stringer := t.newBaseStringer(rng)
	for i := range values {
		values[i] = stringer()
	}
	return values, nil
}

func (t iqlValueTemplate) newStringer(series int64, rng *rand.Rand) func() string {
	if len(t.enum) > 0 {
		index := 0
		return func() string {
			if len(t.enum) == 0 {
				return ""
			}
			value := t.enum[index]
			index = (index + 1) % len(t.enum)
			return value
		}
	}

	base := t.newBaseStringer(rng)
	if t.count != 0 {
		return cycleIQLStringer(t.count, base)
	}
	return nTimesIQLStringer(series, base)
}

func (t iqlValueTemplate) newBaseStringer(rng *rand.Rand) func() string {
	if len(t.enum) > 0 {
		index := 0
		return func() string {
			value := t.enum[index]
			index = (index + 1) % len(t.enum)
			return value
		}
	}
	switch t.dataType {
	case "int":
		return t.newIntStringer(rng)
	case "float":
		return t.newFloatStringer(rng)
	case "str", "string":
		return t.newStringStringer(rng)
	default:
		return func() string { return "IQL_TEMPLATE_ERROR" }
	}
}

func (t iqlValueTemplate) newIntStringer(rng *rand.Rand) func() string {
	switch t.fn {
	case "inc":
		value := t.argument
		return func() string {
			out := strconv.Itoa(value) + "i"
			value++
			return out
		}
	case "rand":
		return func() string {
			if t.argument <= 0 {
				return "0i"
			}
			return strconv.Itoa(rng.Intn(t.argument)) + "i"
		}
	default:
		return func() string { return "IQL_INT_ERROR" }
	}
}

func (t iqlValueTemplate) newFloatStringer(rng *rand.Rand) func() string {
	switch t.fn {
	case "inc":
		value := t.argument
		return func() string {
			out := strconv.Itoa(value)
			value++
			return out
		}
	case "rand":
		return func() string {
			if t.argument <= 0 {
				return "0"
			}
			return strconv.Itoa(rng.Intn(t.argument))
		}
	default:
		return func() string { return "IQL_FLOAT_ERROR" }
	}
}

func (t iqlValueTemplate) newStringStringer(rng *rand.Rand) func() string {
	switch t.fn {
	case "rand":
		return func() string {
			return deterministicIQLString(rng, t.argument)
		}
	default:
		return func() string { return "IQL_STR_ERROR" }
	}
}

func deterministicIQLString(rng *rand.Rand, n int) string {
	if n <= 0 {
		return ""
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	var builder strings.Builder
	for i := 0; i < n; i++ {
		builder.WriteByte(alphabet[rng.Intn(len(alphabet))])
	}
	return builder.String()
}

func cycleIQLStringer(n int64, fn func() string) func() string {
	if n <= 0 {
		return fn
	}
	index := int64(0)
	cache := make([]string, n)
	cache[0] = fn()
	return func() string {
		index++
		if index < n {
			cache[index] = fn()
		}
		return cache[(index-1)%n]
	}
}

func nTimesIQLStringer(n int64, fn func() string) func() string {
	index := int64(0)
	value := fn()
	return func() string {
		index++
		if index > n {
			value = fn()
			index = 1
		}
		return value
	}
}
