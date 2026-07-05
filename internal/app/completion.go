package app

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/schema"
)

type Completion struct {
	Prefix     string
	Candidates []string
}

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
	lowerBefore := strings.ToLower(before)
	prev, prevPrev := previousWords(before, token)
	switch {
	case prev == "from":
		measurements, err := e.measurements(ctx, e.Session.Database, e.Session.RP)
		if err != nil {
			return Completion{Prefix: token}, err
		}
		return completionFor(token, measurements), nil
	case prev == "where" || (prev == "by" && prevPrev == "group"):
		return e.completeTags(ctx, line, token)
	case strings.Contains(lowerBefore, "select") && !strings.Contains(lowerBefore, " from "):
		return e.completeFields(ctx, line, token)
	default:
		return completionFor(token, queryKeywords()), nil
	}
}

func (e *Executor) completeFields(ctx context.Context, line, token string) (Completion, error) {
	measurement := measurementFromQuery(line)
	if measurement == "" {
		return completionFor(token, queryKeywords()), nil
	}
	snapshot, err := e.measurementSchema(ctx, e.Session.Database, e.Session.RP, measurement)
	if err != nil {
		return Completion{Prefix: token}, err
	}
	var candidates []string
	for _, measurement := range snapshot.Measurements {
		for _, field := range measurement.Fields {
			candidates = append(candidates, field.Name)
		}
	}
	return completionFor(token, candidates), nil
}

func (e *Executor) completeTags(ctx context.Context, line, token string) (Completion, error) {
	measurement := measurementFromQuery(line)
	if measurement == "" {
		return Completion{Prefix: token}, nil
	}
	snapshot, err := e.measurementSchema(ctx, e.Session.Database, e.Session.RP, measurement)
	if err != nil {
		return Completion{Prefix: token}, err
	}
	var candidates []string
	for _, measurement := range snapshot.Measurements {
		for _, tag := range measurement.Tags {
			candidates = append(candidates, tag.Name)
		}
	}
	return completionFor(token, candidates), nil
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

func currentToken(before string) string {
	runes := []rune(before)
	start := len(runes)
	for start > 0 && !isCompletionDelimiter(runes[start-1]) {
		start--
	}
	return string(runes[start:])
}

func previousWords(before, token string) (string, string) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(before, token))
	words := strings.FieldsFunc(trimmed, isWordDelimiter)
	if len(words) == 0 {
		return "", ""
	}
	prev := strings.ToLower(words[len(words)-1])
	prevPrev := ""
	if len(words) > 1 {
		prevPrev = strings.ToLower(words[len(words)-2])
	}
	return prev, prevPrev
}

func measurementFromQuery(line string) string {
	words := strings.FieldsFunc(line, isWordDelimiter)
	for i := 0; i < len(words)-1; i++ {
		if strings.EqualFold(words[i], "from") {
			return cleanIdentifier(words[i+1])
		}
	}
	return ""
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
