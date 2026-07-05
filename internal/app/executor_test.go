package app

import (
	"context"
	"testing"

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
	if executor.Session.Database != "metrics" {
		t.Fatalf("database = %q, want metrics", executor.Session.Database)
	}

	if _, err := executor.Execute(context.Background(), ":db telegraf"); err != nil {
		t.Fatal(err)
	}
	if executor.Session.Database != "telegraf" {
		t.Fatalf("database = %q, want telegraf", executor.Session.Database)
	}

	if _, err := executor.Execute(context.Background(), ":rp autogen"); err != nil {
		t.Fatal(err)
	}
	if executor.Session.RP != "autogen" {
		t.Fatalf("rp = %q, want autogen", executor.Session.RP)
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

func newTestExecutor() *Executor {
	session := NewSession(config.Effective{Adapter: "fake", Precision: "rfc3339"})
	return NewExecutor(session, &fakeAdapter{})
}

type fakeAdapter struct {
	lastQuery query.Query
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
	return []string{"metrics", "telegraf"}, nil
}

func (f *fakeAdapter) ShowRetentionPolicies(ctx context.Context, db string) ([]string, error) {
	return []string{"autogen"}, nil
}

func (f *fakeAdapter) GetSchema(ctx context.Context, scope schema.Scope) (schema.Snapshot, error) {
	return schema.Snapshot{
		Database:        scope.Database,
		RetentionPolicy: scope.RetentionPolicy,
		Measurements: []schema.Measurement{
			{
				Name:   scope.Measurement,
				Fields: []schema.Field{{Name: "value", Type: "float"}},
				Tags:   []schema.Tag{{Name: "host"}},
			},
		},
	}, nil
}
