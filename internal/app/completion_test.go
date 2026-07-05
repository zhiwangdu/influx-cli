package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/config"
	"github.com/zhiwangdu/influx-cli/internal/schema"
)

func TestCompleteMetaDatabaseAndRetentionPolicy(t *testing.T) {
	executor := newTestExecutor()

	dbCompletion, err := executor.Complete(context.Background(), ":use met", len(":use met"))
	if err != nil {
		t.Fatal(err)
	}
	if dbCompletion.Prefix != "met" || !reflect.DeepEqual(dbCompletion.Candidates, []string{"metrics"}) {
		t.Fatalf("db completion = %#v", dbCompletion)
	}

	rpCompletion, err := executor.Complete(context.Background(), ":use metrics.au", len(":use metrics.au"))
	if err != nil {
		t.Fatal(err)
	}
	if rpCompletion.Prefix != "metrics.au" || !reflect.DeepEqual(rpCompletion.Candidates, []string{"metrics.autogen"}) {
		t.Fatalf("rp completion = %#v", rpCompletion)
	}
}

func TestCompleteMetaCommandsIncludesLocalReplCommands(t *testing.T) {
	executor := newTestExecutor()

	completion, err := executor.Complete(context.Background(), ":ca", len(":ca"))
	if err != nil {
		t.Fatal(err)
	}
	if completion.Prefix != ":ca" || !reflect.DeepEqual(completion.Candidates, []string{":cancel"}) {
		t.Fatalf("meta completion = %#v", completion)
	}
}

func TestCompleteSchemaMetaCommandsUseMeasurements(t *testing.T) {
	executor := newTestExecutor()

	for _, line := range []string{":schema c", ":fields c", ":tags c"} {
		completion, err := executor.Complete(context.Background(), line, len(line))
		if err != nil {
			t.Fatal(err)
		}
		if completion.Prefix != "c" || !reflect.DeepEqual(completion.Candidates, []string{"cpu"}) {
			t.Fatalf("%s completion = %#v", line, completion)
		}
	}

	for _, line := range []string{":schema ", ":fields ", ":tags "} {
		completion, err := executor.Complete(context.Background(), line, len(line))
		if err != nil {
			t.Fatal(err)
		}
		if completion.Prefix != "" || !reflect.DeepEqual(completion.Candidates, []string{"cpu", "disk", "mem"}) {
			t.Fatalf("%s completion = %#v", line, completion)
		}
	}
}

func TestCompleteMeasurementFieldAndTagCandidates(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), fake)

	measurement, err := executor.Complete(context.Background(), "SELECT * FROM c", len("SELECT * FROM c"))
	if err != nil {
		t.Fatal(err)
	}
	if measurement.Prefix != "c" || !reflect.DeepEqual(measurement.Candidates, []string{"cpu"}) {
		t.Fatalf("measurement completion = %#v", measurement)
	}
	if fake.showMeasurementsCalls != 1 {
		t.Fatalf("showMeasurementsCalls = %d, want 1", fake.showMeasurementsCalls)
	}
	if _, err := executor.Complete(context.Background(), "SELECT * FROM c", len("SELECT * FROM c")); err != nil {
		t.Fatal(err)
	}
	if fake.showMeasurementsCalls != 1 {
		t.Fatalf("measurement completion did not use cache: %d calls", fake.showMeasurementsCalls)
	}

	line := "SELECT mean(us) FROM cpu"
	fieldPos := strings.Index(line, "us") + len("us")
	fields, err := executor.Complete(context.Background(), line, fieldPos)
	if err != nil {
		t.Fatal(err)
	}
	if fields.Prefix != "us" || !reflect.DeepEqual(fields.Candidates, []string{"usage_idle"}) {
		t.Fatalf("field completion = %#v", fields)
	}

	tags, err := executor.Complete(context.Background(), "SELECT mean(value) FROM cpu WHERE ho", len("SELECT mean(value) FROM cpu WHERE ho"))
	if err != nil {
		t.Fatal(err)
	}
	if tags.Prefix != "ho" || !reflect.DeepEqual(tags.Candidates, []string{"host"}) {
		t.Fatalf("tag completion = %#v", tags)
	}
	if fake.getSchemaCalls != 1 {
		t.Fatalf("getSchemaCalls = %d, want cached schema to be reused", fake.getSchemaCalls)
	}

	groupBy, err := executor.Complete(context.Background(), "SELECT mean(value) FROM cpu GROUP BY re", len("SELECT mean(value) FROM cpu GROUP BY re"))
	if err != nil {
		t.Fatal(err)
	}
	if groupBy.Prefix != "re" || !reflect.DeepEqual(groupBy.Candidates, []string{"region"}) {
		t.Fatalf("group by tag completion = %#v", groupBy)
	}
}

func TestCompleteSelectWithoutFromUsesDatabaseSchema(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), fake)

	field, err := executor.Complete(context.Background(), "SELECT us", len("SELECT us"))
	if err != nil {
		t.Fatal(err)
	}
	if field.Prefix != "us" || !reflect.DeepEqual(field.Candidates, []string{"usage_idle", "used_bytes", "used_percent"}) {
		t.Fatalf("db field completion = %#v", field)
	}

	tag, err := executor.Complete(context.Background(), "SELECT ho", len("SELECT ho"))
	if err != nil {
		t.Fatal(err)
	}
	if tag.Prefix != "ho" || !reflect.DeepEqual(tag.Candidates, []string{"host"}) {
		t.Fatalf("db tag completion = %#v", tag)
	}

	if fake.getSchemaCalls != 1 {
		t.Fatalf("getSchemaCalls = %d, want db schema cache reused", fake.getSchemaCalls)
	}
	if got := fake.schemaScopes[0].Measurement; got != "" {
		t.Fatalf("schema scope measurement = %q, want db-level schema", got)
	}
}

func TestCompleteSelectFunctionArgumentsUseFieldsOnly(t *testing.T) {
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), newFakeAdapter())

	field, err := executor.Complete(context.Background(), "SELECT mean(us", len("SELECT mean(us"))
	if err != nil {
		t.Fatal(err)
	}
	if field.Prefix != "us" || !reflect.DeepEqual(field.Candidates, []string{"usage_idle", "used_bytes", "used_percent"}) {
		t.Fatalf("function field completion = %#v", field)
	}

	tag, err := executor.Complete(context.Background(), "SELECT mean(ho", len("SELECT mean(ho"))
	if err != nil {
		t.Fatal(err)
	}
	if tag.Prefix != "ho" || len(tag.Candidates) != 0 {
		t.Fatalf("function tag completion = %#v, want no tag candidates", tag)
	}

	topLevel, err := executor.Complete(context.Background(), "SELECT mean(value), ho", len("SELECT mean(value), ho"))
	if err != nil {
		t.Fatal(err)
	}
	if topLevel.Prefix != "ho" || !reflect.DeepEqual(topLevel.Candidates, []string{"host"}) {
		t.Fatalf("top-level select completion = %#v", topLevel)
	}

	parenthesized, err := executor.Complete(context.Background(), "SELECT (ho", len("SELECT (ho"))
	if err != nil {
		t.Fatal(err)
	}
	if parenthesized.Prefix != "ho" || !reflect.DeepEqual(parenthesized.Candidates, []string{"host"}) {
		t.Fatalf("parenthesized select completion = %#v", parenthesized)
	}

	nested, err := executor.Complete(context.Background(), "SELECT max(mean(us", len("SELECT max(mean(us"))
	if err != nil {
		t.Fatal(err)
	}
	if nested.Prefix != "us" || !reflect.DeepEqual(nested.Candidates, []string{"usage_idle", "used_bytes", "used_percent"}) {
		t.Fatalf("nested function completion = %#v", nested)
	}
}

func TestCompleteSelectWithFromUsesMeasurementSchema(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), fake)

	field, err := executor.Complete(context.Background(), "SELECT us FROM cpu", len("SELECT us"))
	if err != nil {
		t.Fatal(err)
	}
	if field.Prefix != "us" || !reflect.DeepEqual(field.Candidates, []string{"usage_idle"}) {
		t.Fatalf("measurement field completion = %#v", field)
	}
	if fake.getSchemaCalls != 1 {
		t.Fatalf("getSchemaCalls = %d, want single measurement lookup", fake.getSchemaCalls)
	}
	if got := fake.schemaScopes[0].Measurement; got != "cpu" {
		t.Fatalf("schema scope measurement = %q, want cpu", got)
	}
}

func TestCompleteSelectWithQuotedFromUsesMeasurementSchema(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), fake)

	tag, err := executor.Complete(context.Background(), `SELECT value FROM "cpu" WHERE ho`, len(`SELECT value FROM "cpu" WHERE ho`))
	if err != nil {
		t.Fatal(err)
	}
	if tag.Prefix != "ho" || !reflect.DeepEqual(tag.Candidates, []string{"host"}) {
		t.Fatalf("quoted measurement tag completion = %#v", tag)
	}
	if fake.getSchemaCalls != 1 {
		t.Fatalf("getSchemaCalls = %d, want single measurement lookup", fake.getSchemaCalls)
	}
	if got := fake.schemaScopes[0].Measurement; got != "cpu" {
		t.Fatalf("schema scope measurement = %q, want cpu", got)
	}
}

func TestCompleteSelectWithQualifiedQuotedFromUsesLastMeasurementSegment(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), fake)

	line := `SELECT value FROM "metrics"."autogen"."cpu" WHERE re`
	tag, err := executor.Complete(context.Background(), line, len(line))
	if err != nil {
		t.Fatal(err)
	}
	if tag.Prefix != "re" || !reflect.DeepEqual(tag.Candidates, []string{"region"}) {
		t.Fatalf("qualified quoted measurement completion = %#v", tag)
	}
	if fake.getSchemaCalls != 1 {
		t.Fatalf("getSchemaCalls = %d, want single measurement lookup", fake.getSchemaCalls)
	}
	if got := fake.schemaScopes[0].Measurement; got != "cpu" {
		t.Fatalf("schema scope measurement = %q, want cpu", got)
	}
}

func TestCompleteWhereAndGroupByUseTagCandidates(t *testing.T) {
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), newFakeAdapter())

	for _, tc := range []struct {
		line string
		want []string
	}{
		{line: "SELECT value FROM cpu WHERE ho", want: []string{"host"}},
		{line: "SELECT value FROM cpu WHERE host = 'a' AND re", want: []string{"region"}},
		{line: "SELECT value FROM cpu WHERE host = 'a' OR re", want: []string{"region"}},
		{line: "SELECT value FROM cpu GROUP BY re", want: []string{"region"}},
		{line: "SELECT value WHERE pa", want: []string{"path"}},
	} {
		completion, err := executor.Complete(context.Background(), tc.line, len(tc.line))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(completion.Candidates, tc.want) {
			t.Fatalf("%s completion = %#v, want %#v", tc.line, completion, tc.want)
		}
	}
}

func TestCompleteSelectCanStillCompleteClauseKeywords(t *testing.T) {
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), newFakeAdapter())

	completion, err := executor.Complete(context.Background(), "SELECT mean(value) FRO", len("SELECT mean(value) FRO"))
	if err != nil {
		t.Fatal(err)
	}
	if completion.Prefix != "FRO" || !reflect.DeepEqual(completion.Candidates, []string{"FROM"}) {
		t.Fatalf("select keyword completion = %#v", completion)
	}
}

func TestCompleteSelectWithoutDatabaseFallsBackToKeywords(t *testing.T) {
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:   "fake",
		Precision: "rfc3339",
	}), newFakeAdapter())

	completion, err := executor.Complete(context.Background(), "SELECT FRO", len("SELECT FRO"))
	if err != nil {
		t.Fatal(err)
	}
	if completion.Prefix != "FRO" || !reflect.DeepEqual(completion.Candidates, []string{"FROM"}) {
		t.Fatalf("no-db completion = %#v, want FROM keyword", completion)
	}

	where, err := executor.Complete(context.Background(), "SELECT value WHERE ho", len("SELECT value WHERE ho"))
	if err != nil {
		t.Fatal(err)
	}
	if where.Prefix != "ho" || len(where.Candidates) != 0 {
		t.Fatalf("no-db where completion = %#v, want no candidates", where)
	}
}

func TestCompleteSchemaCacheRefreshClearsSchemaEntries(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), fake)

	if _, err := executor.Complete(context.Background(), "SELECT us", len("SELECT us")); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Complete(context.Background(), "SELECT us", len("SELECT us")); err != nil {
		t.Fatal(err)
	}
	if fake.getSchemaCalls != 1 {
		t.Fatalf("getSchemaCalls = %d, want cached db schema", fake.getSchemaCalls)
	}

	if _, err := executor.Execute(context.Background(), ":refresh schema"); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Complete(context.Background(), "SELECT us", len("SELECT us")); err != nil {
		t.Fatal(err)
	}
	if fake.getSchemaCalls != 2 {
		t.Fatalf("getSchemaCalls = %d, want schema cache refresh to force lookup", fake.getSchemaCalls)
	}
}

func TestCompleteSchemaCacheSeparatesDatabaseAndMeasurementScopes(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), fake)

	dbCompletion, err := executor.Complete(context.Background(), "SELECT us", len("SELECT us"))
	if err != nil {
		t.Fatal(err)
	}
	if dbCompletion.Prefix != "us" || !reflect.DeepEqual(dbCompletion.Candidates, []string{"usage_idle", "used_bytes", "used_percent"}) {
		t.Fatalf("db completion = %#v", dbCompletion)
	}

	measurementCompletion, err := executor.Complete(context.Background(), "SELECT us FROM cpu", len("SELECT us"))
	if err != nil {
		t.Fatal(err)
	}
	if measurementCompletion.Prefix != "us" || !reflect.DeepEqual(measurementCompletion.Candidates, []string{"usage_idle"}) {
		t.Fatalf("measurement completion = %#v", measurementCompletion)
	}

	if fake.getSchemaCalls != 2 {
		t.Fatalf("getSchemaCalls = %d, want db and measurement schema lookups", fake.getSchemaCalls)
	}
	if fake.schemaScopes[0].Measurement != "" || fake.schemaScopes[1].Measurement != "cpu" {
		t.Fatalf("schema scopes = %#v, want db-level then cpu", fake.schemaScopes)
	}
}

func TestCompleteSchemaErrorIsReturned(t *testing.T) {
	adapter := schemaErrorAdapter{fakeAdapter: newFakeAdapter(), err: errors.New("schema unavailable")}
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), adapter)

	completion, err := executor.Complete(context.Background(), "SELECT us", len("SELECT us"))
	if err == nil {
		t.Fatal("expected schema error")
	}
	if completion.Prefix != "us" || len(completion.Candidates) != 0 {
		t.Fatalf("completion = %#v, want prefix-only empty completion", completion)
	}
}

func TestCompleteSchemaCancellationReturnsEmptyCompletion(t *testing.T) {
	for _, schemaErr := range []error{context.Canceled, context.DeadlineExceeded} {
		adapter := schemaErrorAdapter{fakeAdapter: newFakeAdapter(), err: schemaErr}
		executor := NewExecutor(NewSession(config.Effective{
			Adapter:         "fake",
			Database:        "metrics",
			RetentionPolicy: "autogen",
			Precision:       "rfc3339",
		}), adapter)

		completion, err := executor.Complete(context.Background(), "SELECT us", len("SELECT us"))
		if err != nil {
			t.Fatalf("err = %v, want nil for %v", err, schemaErr)
		}
		if completion.Prefix != "us" || len(completion.Candidates) != 0 {
			t.Fatalf("completion = %#v, want prefix-only empty completion", completion)
		}
	}
}

func TestRefreshSchemaClearsCompletionCache(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	}), fake)

	if _, err := executor.Complete(context.Background(), "SELECT * FROM c", len("SELECT * FROM c")); err != nil {
		t.Fatal(err)
	}
	if fake.showMeasurementsCalls != 1 {
		t.Fatalf("showMeasurementsCalls = %d, want 1", fake.showMeasurementsCalls)
	}
	if _, err := executor.Execute(context.Background(), ":refresh schema"); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Complete(context.Background(), "SELECT * FROM c", len("SELECT * FROM c")); err != nil {
		t.Fatal(err)
	}
	if fake.showMeasurementsCalls != 2 {
		t.Fatalf("showMeasurementsCalls = %d, want cache refresh to force second call", fake.showMeasurementsCalls)
	}
}

type schemaErrorAdapter struct {
	*fakeAdapter
	err error
}

func (a schemaErrorAdapter) GetSchema(ctx context.Context, scope schema.Scope) (schema.Snapshot, error) {
	return schema.Snapshot{}, a.err
}

func TestCompletionCacheExpiresMeasurementsByTTL(t *testing.T) {
	now := time.Date(2026, 7, 5, 10, 0, 0, 0, time.UTC)
	cache := newCompletionCache(time.Minute)
	cache.now = func() time.Time {
		return now
	}
	key := cacheKey{Adapter: "fake", Database: "metrics", RP: "autogen"}

	cache.SetMeasurements(key, []string{"cpu"})
	if values, ok := cache.Measurements(key); !ok || !reflect.DeepEqual(values, []string{"cpu"}) {
		t.Fatalf("cache hit = %v values = %#v, want cpu", ok, values)
	}

	now = now.Add(time.Minute + time.Nanosecond)
	if values, ok := cache.Measurements(key); ok || values != nil {
		t.Fatalf("cache hit after expiry = %v values = %#v, want miss", ok, values)
	}
}
