package app

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/config"
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
