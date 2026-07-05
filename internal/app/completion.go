package app

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/zhiwangdu/influx-cli/internal/schema"
)

type Completion struct {
	Prefix     string
	Candidates []string
}

type schemaCandidateKind int

const (
	schemaFields schemaCandidateKind = iota
	schemaTags
	schemaFieldsAndTags
)

func (e *Executor) Complete(ctx context.Context, line string, pos int) (Completion, error) {
	if pos < 0 {
		pos = 0
	}
	runes := []rune(line)
	if pos > len(runes) {
		pos = len(runes)
	}
	before := string(runes[:pos])
	token := currentToken(before)
	trimmed := strings.TrimSpace(before)
	if strings.HasPrefix(trimmed, ":") {
		return e.completeMeta(ctx, trimmed, token)
	}
	return e.completeQuery(ctx, line, before, token)
}

func (e *Executor) RefreshSchemaCache() {
	if e.completionCache != nil {
		e.completionCache.Clear()
	}
}

func (e *Executor) completeMeta(ctx context.Context, before, token string) (Completion, error) {
	fields := strings.Fields(before)
	if len(fields) == 0 {
		return completionFor(token, metaCommands()), nil
	}
	command := strings.ToLower(fields[0])
	if len(fields) == 1 && strings.HasPrefix(token, ":") {
		return completionFor(token, metaCommands()), nil
	}

	switch command {
	case ":use":
		if len(fields) > 2 {
			return Completion{Prefix: token}, nil
		}
		if database, rpPrefix, ok := strings.Cut(token, "."); ok {
			policies, err := e.Adapter.ShowRetentionPolicies(ctx, database)
			if err != nil {
				return Completion{Prefix: token}, err
			}
			candidates := make([]string, 0, len(policies))
			for _, policy := range policies {
				candidates = append(candidates, database+"."+policy.Name)
			}
			return completionFor(database+"."+rpPrefix, candidates), nil
		}
		databases, err := e.Adapter.ShowDatabases(ctx)
		if err != nil {
			return Completion{Prefix: token}, err
		}
		return completionFor(token, databases), nil
	case ":db":
		if len(fields) > 2 {
			return Completion{Prefix: token}, nil
		}
		databases, err := e.Adapter.ShowDatabases(ctx)
		if err != nil {
			return Completion{Prefix: token}, err
		}
		return completionFor(token, databases), nil
	case ":rp":
		if len(fields) > 2 {
			return Completion{Prefix: token}, nil
		}
		policies, err := e.Adapter.ShowRetentionPolicies(ctx, e.Session.Database)
		if err != nil {
			return Completion{Prefix: token}, err
		}
		candidates := make([]string, 0, len(policies))
		for _, policy := range policies {
			candidates = append(candidates, policy.Name)
		}
		return completionFor(token, candidates), nil
	case ":schema", ":fields", ":tags":
		if len(fields) > 2 {
			return Completion{Prefix: token}, nil
		}
		measurements, err := e.measurements(ctx, e.Session.Database, e.Session.RP)
		if err != nil {
			return Completion{Prefix: token}, err
		}
		return completionFor(token, measurements), nil
	default:
		return Completion{Prefix: token}, nil
	}
}

func (e *Executor) completeQuery(ctx context.Context, line, before, token string) (Completion, error) {
	prev := previousWord(before, token)
	switch {
	case prev == "from":
		measurements, err := e.measurements(ctx, e.Session.Database, e.Session.RP)
		if err != nil {
			return Completion{Prefix: token}, err
		}
		return completionFor(token, measurements), nil
	}

	clause := queryClause(before, token)
	switch clause {
	case "where", "group":
		return e.completeSchemaCandidates(ctx, line, token, schemaTags, nil)
	case "select":
		if insideFunctionArgs(before) {
			return e.completeSchemaCandidates(ctx, line, token, schemaFields, nil)
		}
		return e.completeSchemaCandidates(ctx, line, token, schemaFieldsAndTags, selectClauseKeywords())
	default:
		return completionFor(token, queryKeywords()), nil
	}
}

func (e *Executor) completeSchemaCandidates(ctx context.Context, line, token string, kind schemaCandidateKind, extra []string) (Completion, error) {
	measurement := measurementFromQuery(line)
	if measurement == "" && strings.TrimSpace(e.Session.Database) == "" {
		if kind == schemaTags {
			return Completion{Prefix: token}, nil
		}
		candidates := append(queryKeywords(), extra...)
		return completionFor(token, candidates), nil
	}
	snapshot, err := e.measurementSchema(ctx, e.Session.Database, e.Session.RP, measurement)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Completion{Prefix: token}, nil
		}
		return Completion{Prefix: token}, err
	}
	candidates := schemaCandidates(snapshot, kind)
	candidates = append(candidates, extra...)
	return completionFor(token, candidates), nil
}

func schemaCandidates(snapshot schema.Snapshot, kind schemaCandidateKind) []string {
	var candidates []string
	for _, measurement := range snapshot.Measurements {
		if kind == schemaFields || kind == schemaFieldsAndTags {
			for _, field := range measurement.Fields {
				candidates = append(candidates, field.Name)
			}
		}
		if kind == schemaTags || kind == schemaFieldsAndTags {
			for _, tag := range measurement.Tags {
				candidates = append(candidates, tag.Name)
			}
		}
	}
	return candidates
}

func (e *Executor) measurements(ctx context.Context, db, rp string) ([]string, error) {
	key := cacheKey{Adapter: e.Session.AdapterName, Database: db, RP: rp}
	if values, ok := e.completionCache.Measurements(key); ok {
		return values, nil
	}
	values, err := e.Adapter.ShowMeasurements(ctx, db, rp)
	if err != nil {
		return nil, err
	}
	e.completionCache.SetMeasurements(key, values)
	return values, nil
}

func (e *Executor) measurementSchema(ctx context.Context, db, rp, measurement string) (schema.Snapshot, error) {
	key := schemaCacheKey{cacheKey: cacheKey{Adapter: e.Session.AdapterName, Database: db, RP: rp}, Measurement: measurement}
	if snapshot, ok := e.completionCache.Schema(key); ok {
		return snapshot, nil
	}
	snapshot, err := e.Adapter.GetSchema(ctx, schema.Scope{
		Database:        db,
		RetentionPolicy: rp,
		Measurement:     measurement,
	})
	if err != nil {
		return schema.Snapshot{}, err
	}
	e.completionCache.SetSchema(key, snapshot)
	return snapshot, nil
}

type completionCache struct {
	ttl          time.Duration
	now          func() time.Time
	mu           sync.Mutex
	measurements map[cacheKey]measurementCacheEntry
	schemas      map[schemaCacheKey]schemaCacheEntry
}

type cacheKey struct {
	Adapter  string
	Database string
	RP       string
}

type schemaCacheKey struct {
	cacheKey
	Measurement string
}

type measurementCacheEntry struct {
	Values    []string
	ExpiresAt time.Time
}

type schemaCacheEntry struct {
	Value     schema.Snapshot
	ExpiresAt time.Time
}

func newCompletionCache(ttl time.Duration) *completionCache {
	return &completionCache{
		ttl:          ttl,
		now:          time.Now,
		measurements: map[cacheKey]measurementCacheEntry{},
		schemas:      map[schemaCacheKey]schemaCacheEntry{},
	}
}

func (c *completionCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.measurements = map[cacheKey]measurementCacheEntry{}
	c.schemas = map[schemaCacheKey]schemaCacheEntry{}
}

func (c *completionCache) Measurements(key cacheKey) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.measurements[key]
	if !ok || c.now().After(entry.ExpiresAt) {
		return nil, false
	}
	return append([]string(nil), entry.Values...), true
}

func (c *completionCache) SetMeasurements(key cacheKey, values []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.measurements[key] = measurementCacheEntry{
		Values:    append([]string(nil), values...),
		ExpiresAt: c.now().Add(c.ttl),
	}
}

func (c *completionCache) Schema(key schemaCacheKey) (schema.Snapshot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.schemas[key]
	if !ok || c.now().After(entry.ExpiresAt) {
		return schema.Snapshot{}, false
	}
	return copySnapshot(entry.Value), true
}

func (c *completionCache) SetSchema(key schemaCacheKey, value schema.Snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[key] = schemaCacheEntry{
		Value:     copySnapshot(value),
		ExpiresAt: c.now().Add(c.ttl),
	}
}

func copySnapshot(snapshot schema.Snapshot) schema.Snapshot {
	out := snapshot
	out.Measurements = append([]schema.Measurement(nil), snapshot.Measurements...)
	for i, measurement := range out.Measurements {
		measurement.Fields = append([]schema.Field(nil), measurement.Fields...)
		measurement.Tags = append([]schema.Tag(nil), measurement.Tags...)
		out.Measurements[i] = measurement
	}
	return out
}

func completionFor(prefix string, candidates []string) Completion {
	seen := map[string]struct{}{}
	var out []string
	lowerPrefix := strings.ToLower(prefix)
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if !strings.HasPrefix(strings.ToLower(candidate), lowerPrefix) {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	sort.Strings(out)
	return Completion{Prefix: prefix, Candidates: out}
}

func metaCommands() []string {
	return []string{
		":use", ":db", ":rp", ":rps", ":dbs", ":measurements", ":msts",
		":fields", ":tags", ":schema", ":refresh", ":format", ":fmt", ":history", ":hist",
		":cancel", ":clear", ":status", ":help", ":q",
	}
}

func queryKeywords() []string {
	return []string{
		"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT",
		"SHOW", "MEASUREMENTS", "DATABASES", "RETENTION POLICIES",
		"AND", "OR", "time", "now()",
	}
}

func selectClauseKeywords() []string {
	return []string{"FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT"}
}

func currentToken(before string) string {
	runes := []rune(before)
	start := len(runes)
	for start > 0 && !isCompletionDelimiter(runes[start-1]) {
		start--
	}
	return string(runes[start:])
}

func previousWord(before, token string) string {
	trimmed := strings.TrimSpace(strings.TrimSuffix(before, token))
	words := strings.FieldsFunc(trimmed, isWordDelimiter)
	if len(words) == 0 {
		return ""
	}
	return strings.ToLower(words[len(words)-1])
}

func measurementFromQuery(line string) string {
	tokens := queryTokens(line)
	for i := 0; i < len(tokens)-1; i++ {
		if tokens[i].Depth == 0 && tokens[i+1].Depth == 0 && tokens[i].Lower == "from" {
			return cleanIdentifier(measurementReference(tokens[i+1:]))
		}
	}
	return ""
}

func measurementReference(tokens []queryToken) string {
	var builder strings.Builder
	for _, token := range tokens {
		if token.Depth != 0 || isClauseToken(token) {
			break
		}
		if token.Text == "." {
			builder.WriteString(".")
			continue
		}
		if builder.Len() > 0 && !strings.HasSuffix(builder.String(), ".") {
			break
		}
		builder.WriteString(token.Text)
	}
	return builder.String()
}

func isClauseToken(token queryToken) bool {
	if token.Quote {
		return false
	}
	switch token.Lower {
	case "where", "group", "order", "limit":
		return true
	default:
		return false
	}
}

type queryToken struct {
	Text  string
	Lower string
	Depth int
	Quote bool
}

func queryClause(before, token string) string {
	prefix := strings.TrimSuffix(before, token)
	tokens := queryTokens(prefix)
	clause := ""
	for i := 0; i < len(tokens); i++ {
		if tokens[i].Depth != 0 || tokens[i].Quote {
			continue
		}
		switch tokens[i].Lower {
		case "select":
			clause = "select"
		case "from":
			clause = "from"
		case "where":
			clause = "where"
		case "group":
			if nextTopLevelTokenIs(tokens, i+1, "by") {
				clause = "group"
			}
		case "order":
			if nextTopLevelTokenIs(tokens, i+1, "by") {
				clause = "order"
			}
		case "limit":
			clause = "limit"
		}
	}
	return clause
}

func nextTopLevelTokenIs(tokens []queryToken, start int, value string) bool {
	for i := start; i < len(tokens); i++ {
		if tokens[i].Depth != 0 || tokens[i].Quote {
			continue
		}
		return tokens[i].Lower == value
	}
	return false
}

func queryTokens(input string) []queryToken {
	runes := []rune(input)
	var tokens []queryToken
	depth := 0
	for i := 0; i < len(runes); {
		r := runes[i]
		if r == '\'' || r == '"' {
			token, next := quotedToken(runes, i, depth)
			if token.Text != "" {
				tokens = append(tokens, token)
			}
			i = next
			continue
		}
		switch r {
		case '(':
			depth++
			i++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			i++
			continue
		}
		if isWordDelimiter(r) {
			i++
			continue
		}
		start := i
		tokenDepth := depth
		for i < len(runes) && !isWordDelimiter(runes[i]) && runes[i] != '\'' && runes[i] != '"' {
			i++
		}
		text := string(runes[start:i])
		tokens = append(tokens, queryToken{Text: text, Lower: strings.ToLower(text), Depth: tokenDepth})
	}
	return tokens
}

func quotedToken(runes []rune, start, depth int) (queryToken, int) {
	quote := runes[start]
	var builder strings.Builder
	escaped := false
	i := start + 1
	for i < len(runes) {
		r := runes[i]
		if escaped {
			builder.WriteRune(r)
			escaped = false
			i++
			continue
		}
		if r == '\\' {
			escaped = true
			i++
			continue
		}
		if r == quote {
			i++
			break
		}
		builder.WriteRune(r)
		i++
	}
	text := builder.String()
	return queryToken{Text: text, Lower: strings.ToLower(text), Depth: depth, Quote: true}, i
}

func insideFunctionArgs(before string) bool {
	runes := []rune(before)
	var stack []bool
	var quote rune
	escaped := false
	for i, r := range runes {
		if quote != 0 {
			if escaped {
				escaped = false
			} else if r == '\\' {
				escaped = true
			} else if r == quote {
				quote = 0
			}
			continue
		}
		if r == '\'' || r == '"' {
			quote = r
			continue
		}
		switch r {
		case '(':
			stack = append(stack, hasIdentifierBefore(runes, i))
		case ')':
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
		}
	}
	return len(stack) > 0 && stack[len(stack)-1]
}

func hasIdentifierBefore(runes []rune, pos int) bool {
	i := pos - 1
	if i < 0 || !isIdentifierRune(runes[i]) {
		return false
	}
	for i >= 0 && isIdentifierRune(runes[i]) {
		i--
	}
	return true
}

func isIdentifierRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func cleanIdentifier(value string) string {
	value = strings.Trim(value, ` "'`)
	if dot := strings.LastIndex(value, "."); dot >= 0 {
		value = value[dot+1:]
	}
	return strings.Trim(value, ` "'`)
}

func isCompletionDelimiter(r rune) bool {
	return isWordDelimiter(r) && r != '.'
}

func isWordDelimiter(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\r', ',', '(', ')', '=', '<', '>', '+', '-', '*', '/', '%':
		return true
	default:
		return false
	}
}
