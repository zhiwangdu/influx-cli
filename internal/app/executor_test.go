package app

import (
	"context"
	"testing"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
	"github.com/zhiwangdu/influx-cli/internal/config"
	"github.com/zhiwangdu/influx-cli/internal/query"
	"github.com/zhiwangdu/influx-cli/internal/result"
	"github.com/zhiwangdu/influx-cli/internal/schema"
)

func TestExecutorMetaUseAndRPUpdateSession(t *testing.T) {
	executor := newTestExecutor()

	if _, err := executor.Execute(context.Background(), ":use metrics"); err != nil {
		t.Fatal(err)
	}
	snapshot := executor.Session.Snapshot()
	if snapshot.Database != "metrics" || snapshot.RP != "autogen" {
		t.Fatalf("context = %q/%q, want metrics/autogen", snapshot.Database, snapshot.RP)
	}

	if _, err := executor.Execute(context.Background(), ":db telegraf"); err != nil {
		t.Fatal(err)
	}
	snapshot = executor.Session.Snapshot()
	if snapshot.Database != "telegraf" || snapshot.RP != "raw" {
		t.Fatalf("context = %q/%q, want telegraf/raw", snapshot.Database, snapshot.RP)
	}

	if _, err := executor.Execute(context.Background(), ":use metrics.short"); err != nil {
		t.Fatal(err)
	}
	snapshot = executor.Session.Snapshot()
	if snapshot.Database != "metrics" || snapshot.RP != "short" {
		t.Fatalf("context = %q/%q, want metrics/short", snapshot.Database, snapshot.RP)
	}

	if _, err := executor.Execute(context.Background(), ":rp autogen"); err != nil {
		t.Fatal(err)
	}
	snapshot = executor.Session.Snapshot()
	if snapshot.RP != "autogen" {
		t.Fatalf("rp = %q, want autogen", snapshot.RP)
	}
}

func TestExecutorDBSMetaCommandUsesAdapter(t *testing.T) {
	executor := newTestExecutor()
	res, err := executor.Execute(context.Background(), ":dbs")
	if err != nil {
		t.Fatal(err)
	}
	if res.Table.RowCount() != 2 {
		t.Fatalf("rows = %d, want 2", res.Table.RowCount())
	}
}

func TestExecutorRPSMetaCommandShowsSingleDatabasePolicies(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}), fake)

	res, err := executor.Execute(context.Background(), ":rps metrics")
	if err != nil {
		t.Fatal(err)
	}
	if fake.showDatabasesCalls != 0 {
		t.Fatalf("showDatabasesCalls = %d, want 0", fake.showDatabasesCalls)
	}
	if got, want := res.Table.RowCount(), 2; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	assertRow(t, res.Table.Rows[0], "metrics", "autogen", true)
	assertRow(t, res.Table.Rows[1], "metrics", "short", false)
}

func TestExecutorRPSMetaCommandUsesCurrentDatabaseWhenSet(t *testing.T) {
	fake := newFakeAdapter()
	session := NewSession(config.Effective{Adapter: "fake", Database: "telegraf", Precision: "rfc3339"})
	executor := NewExecutor(session, fake)

	res, err := executor.Execute(context.Background(), ":rps")
	if err != nil {
		t.Fatal(err)
	}
	if fake.showDatabasesCalls != 0 {
		t.Fatalf("showDatabasesCalls = %d, want 0", fake.showDatabasesCalls)
	}
	if got, want := res.Table.RowCount(), 1; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	assertRow(t, res.Table.Rows[0], "telegraf", "raw", true)
}

func TestExecutorRPSMetaCommandShowsAllDatabasePolicies(t *testing.T) {
	fake := newFakeAdapter()
	executor := NewExecutor(NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"}), fake)

	res, err := executor.Execute(context.Background(), ":rps")
	if err != nil {
		t.Fatal(err)
	}
	if fake.showDatabasesCalls != 1 {
		t.Fatalf("showDatabasesCalls = %d, want 1", fake.showDatabasesCalls)
	}
	if got, want := res.Table.RowCount(), 3; got != want {
		t.Fatalf("rows = %d, want %d", got, want)
	}
	assertRow(t, res.Table.Rows[0], "metrics", "autogen", true)
	assertRow(t, res.Table.Rows[1], "metrics", "short", false)
	assertRow(t, res.Table.Rows[2], "telegraf", "raw", true)
}

func TestExecutorQueryInjectsSessionContext(t *testing.T) {
	fake := &fakeAdapter{}
	session := NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	})
	executor := NewExecutor(session, fake)

	if _, err := executor.Execute(context.Background(), "SHOW MEASUREMENTS"); err != nil {
		t.Fatal(err)
	}
	if fake.lastQuery.Database != "metrics" || fake.lastQuery.RP != "autogen" {
		t.Fatalf("query context = %q/%q, want metrics/autogen", fake.lastQuery.Database, fake.lastQuery.RP)
	}
}

func TestExecutorMeasurementsAliasUsesSameQuery(t *testing.T) {
	fake := newFakeAdapter()
	session := NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	})
	executor := NewExecutor(session, fake)

	if _, err := executor.Execute(context.Background(), ":msts"); err != nil {
		t.Fatal(err)
	}
	if fake.lastQuery.Raw != "SHOW MEASUREMENTS" {
		t.Fatalf("query = %q, want SHOW MEASUREMENTS", fake.lastQuery.Raw)
	}
	if fake.lastQuery.Database != "metrics" || fake.lastQuery.RP != "autogen" {
		t.Fatalf("query context = %q/%q, want metrics/autogen", fake.lastQuery.Database, fake.lastQuery.RP)
	}
}

func TestExecutorFieldsAndTagsCommandsUseMeasurementSchema(t *testing.T) {
	fake := newFakeAdapter()
	session := NewSession(config.Effective{
		Adapter:         "fake",
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
	})
	executor := NewExecutor(session, fake)

	fields, err := executor.Execute(context.Background(), ":fields cpu")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fields.Table.Columns, []string{"measurement", "field", "type"}; !equalStringSlices(got, want) {
		t.Fatalf("field columns = %#v, want %#v", got, want)
	}
	if got, want := fields.Table.RowCount(), 2; got != want {
		t.Fatalf("field rows = %d, want %d", got, want)
	}
	if fields.Table.Rows[0][0] != "cpu" || fields.Table.Rows[0][1] != "usage_idle" || fields.Table.Rows[0][2] != "float" {
		t.Fatalf("unexpected field row: %#v", fields.Table.Rows[0])
	}

	tags, err := executor.Execute(context.Background(), ":tags cpu")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tags.Table.Columns, []string{"measurement", "tag"}; !equalStringSlices(got, want) {
		t.Fatalf("tag columns = %#v, want %#v", got, want)
	}
	if got, want := tags.Table.RowCount(), 2; got != want {
		t.Fatalf("tag rows = %d, want %d", got, want)
	}
	if tags.Table.Rows[0][0] != "cpu" || tags.Table.Rows[0][1] != "host" {
		t.Fatalf("unexpected tag row: %#v", tags.Table.Rows[0])
	}
	if fake.getSchemaCalls != 2 {
		t.Fatalf("getSchemaCalls = %d, want 2", fake.getSchemaCalls)
	}
}

func TestExecutorFieldsAndTagsRejectInvalidMeasurementArity(t *testing.T) {
	executor := newTestExecutor()
	for _, command := range []string{":fields", ":fields cpu extra", ":tags", ":tags cpu extra"} {
		if _, err := executor.Execute(context.Background(), command); err == nil {
			t.Fatalf("%s succeeded with invalid measurement arity", command)
		}
	}
}

func newTestExecutor() *Executor {
	session := NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"})
	return NewExecutor(session, newFakeAdapter())
}

func newFakeAdapter() *fakeAdapter {
	return &fakeAdapter{
		retentionPolicies: map[string][]adapter.RetentionPolicy{
			"metrics": {
				{Name: "autogen", Duration: "0s", ShardGroupDuration: "168h0m0s", ReplicaN: "1", Default: true},
				{Name: "short", Duration: "24h0m0s", ShardGroupDuration: "1h0m0s", ReplicaN: "1"},
			},
			"telegraf": {
				{Name: "raw", Duration: "720h0m0s", ShardGroupDuration: "24h0m0s", ReplicaN: "1", Default: true},
			},
		},
	}
}

type fakeAdapter struct {
	lastQuery             query.Query
	retentionPolicies     map[string][]adapter.RetentionPolicy
	showDatabasesCalls    int
	showMeasurementsCalls int
	getSchemaCalls        int
	schemaScopes          []schema.Scope
}

func (f *fakeAdapter) Name() string {
	return "fake"
}

func (f *fakeAdapter) Ping(ctx context.Context) error {
	return nil
}

func (f *fakeAdapter) Query(ctx context.Context, q query.Query) (result.Result, error) {
	f.lastQuery = q
	table := result.NewTable([]string{"ok"})
	table.AddRow(true)
	return result.FromTable(table), nil
}

func (f *fakeAdapter) ShowDatabases(ctx context.Context) ([]string, error) {
	f.showDatabasesCalls++
	return []string{"metrics", "telegraf"}, nil
}

func (f *fakeAdapter) ShowRetentionPolicies(ctx context.Context, db string) ([]adapter.RetentionPolicy, error) {
	if policies, ok := f.retentionPolicies[db]; ok {
		return policies, nil
	}
	return []adapter.RetentionPolicy{{Name: "autogen", Duration: "0s", ShardGroupDuration: "168h0m0s", ReplicaN: "1", Default: true}}, nil
}

func (f *fakeAdapter) ShowMeasurements(ctx context.Context, db, rp string) ([]string, error) {
	f.showMeasurementsCalls++
	return []string{"cpu", "disk", "mem"}, nil
}

func (f *fakeAdapter) GetSchema(ctx context.Context, scope schema.Scope) (schema.Snapshot, error) {
	f.getSchemaCalls++
	f.schemaScopes = append(f.schemaScopes, scope)
	if scope.Measurement == "" {
		return schema.Snapshot{
			Database:        scope.Database,
			RetentionPolicy: scope.RetentionPolicy,
			Measurements: []schema.Measurement{
				{
					Name:   "cpu",
					Fields: []schema.Field{{Name: "usage_idle", Type: "float"}, {Name: "value", Type: "float"}},
					Tags:   []schema.Tag{{Name: "host"}, {Name: "region"}},
				},
				{
					Name:   "disk",
					Fields: []schema.Field{{Name: "used_bytes", Type: "integer"}},
					Tags:   []schema.Tag{{Name: "path"}},
				},
				{
					Name:   "mem",
					Fields: []schema.Field{{Name: "used_percent", Type: "float"}},
					Tags:   []schema.Tag{{Name: "host"}},
				},
			},
		}, nil
	}
	return schema.Snapshot{
		Database:        scope.Database,
		RetentionPolicy: scope.RetentionPolicy,
		Measurements: []schema.Measurement{
			{
				Name:   scope.Measurement,
				Fields: []schema.Field{{Name: "usage_idle", Type: "float"}, {Name: "value", Type: "float"}},
				Tags:   []schema.Tag{{Name: "host"}, {Name: "region"}},
			},
		},
	}, nil
}

func assertRow(t *testing.T, row []any, database, retentionPolicy string, isDefault bool) {
	t.Helper()
	if len(row) != 6 {
		t.Fatalf("row length = %d, want 6: %#v", len(row), row)
	}
	if row[0] != database || row[1] != retentionPolicy || row[5] != isDefault {
		t.Fatalf("row = %#v, want database=%q retention_policy=%q default=%v", row, database, retentionPolicy, isDefault)
	}
	if row[2] == "" || row[3] == "" || row[4] == "" {
		t.Fatalf("row missing retention policy details: %#v", row)
	}
}

func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
