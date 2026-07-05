package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
	"github.com/zhiwangdu/influx-cli/internal/query"
	"github.com/zhiwangdu/influx-cli/internal/result"
	"github.com/zhiwangdu/influx-cli/internal/schema"
)

type Executor struct {
	Session *Session
	Adapter adapter.Adapter
}

func NewExecutor(session *Session, adapter adapter.Adapter) *Executor {
	return &Executor{Session: session, Adapter: adapter}
}

func (e *Executor) Execute(ctx context.Context, input string) (result.Result, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return result.Empty(), nil
	}

	start := time.Now()
	res, err := e.execute(ctx, trimmed)
	e.Session.update(time.Since(start), err)
	return res, err
}

func (e *Executor) execute(ctx context.Context, input string) (result.Result, error) {
	if strings.HasPrefix(input, ":") {
		return e.executeMeta(ctx, input)
	}

	q := query.New(input, e.Session.Database, e.Session.RP, e.Session.Precision)
	e.Session.Dialect = q.Dialect
	return e.Adapter.Query(ctx, q)
}

func (e *Executor) executeMeta(ctx context.Context, input string) (result.Result, error) {
	fields := strings.Fields(input)
	command := strings.ToLower(fields[0])

	switch command {
	case ":use":
		if len(fields) != 2 {
			return result.Result{}, fmt.Errorf("usage: :use <db>[.<rp>]")
		}
		database, retentionPolicy, explicitRP, err := parseUseTarget(fields[1])
		if err != nil {
			return result.Result{}, err
		}
		if !explicitRP {
			retentionPolicy, err = e.defaultRetentionPolicy(ctx, database)
			if err != nil {
				return result.Result{}, err
			}
		}
		e.Session.Database = database
		e.Session.RP = retentionPolicy
		return contextResult(database, retentionPolicy), nil
	case ":db":
		if len(fields) != 2 {
			return result.Result{}, fmt.Errorf("usage: :db <db>")
		}
		database := fields[1]
		retentionPolicy, err := e.defaultRetentionPolicy(ctx, database)
		if err != nil {
			return result.Result{}, err
		}
		e.Session.Database = database
		e.Session.RP = retentionPolicy
		return contextResult(database, retentionPolicy), nil
	case ":rp":
		if len(fields) != 2 {
			return result.Result{}, fmt.Errorf("usage: :rp <retention_policy>")
		}
		e.Session.RP = fields[1]
		return statusResult("retention_policy", fields[1]), nil
	case ":rps":
		if len(fields) > 2 {
			return result.Result{}, fmt.Errorf("usage: :rps [db]")
		}
		if len(fields) == 2 {
			return e.retentionPoliciesResult(ctx, []string{fields[1]})
		}
		if strings.TrimSpace(e.Session.Database) != "" {
			return e.retentionPoliciesResult(ctx, []string{e.Session.Database})
		}
		databases, err := e.Adapter.ShowDatabases(ctx)
		if err != nil {
			return result.Result{}, err
		}
		return e.retentionPoliciesResult(ctx, databases)
	case ":dbs":
		databases, err := e.Adapter.ShowDatabases(ctx)
		if err != nil {
			return result.Result{}, err
		}
		table := result.NewTable([]string{"name"})
		for _, database := range databases {
			table.AddRow(database)
		}
		return result.FromTable(table), nil
	case ":measurements", ":msts":
		q := query.New("SHOW MEASUREMENTS", e.Session.Database, e.Session.RP, e.Session.Precision)
		return e.Adapter.Query(ctx, q)
	case ":schema":
		if len(fields) != 2 {
			return result.Result{}, fmt.Errorf("usage: :schema <measurement>")
		}
		snapshot, err := e.Adapter.GetSchema(ctx, schema.Scope{
			Database:        e.Session.Database,
			RetentionPolicy: e.Session.RP,
			Measurement:     fields[1],
		})
		if err != nil {
			return result.Result{}, err
		}
		return result.SchemaResult(snapshot), nil
	case ":status":
		table := result.NewTable([]string{"key", "value"})
		table.AddRow("db", e.Session.Database)
		table.AddRow("rp", e.Session.RP)
		table.AddRow("mode", e.Session.Dialect)
		table.AddRow("adapter", e.Session.AdapterName)
		table.AddRow("latency", e.Session.LastLatency.String())
		if e.Session.LastError != nil {
			table.AddRow("last_error", e.Session.LastError.Error())
		} else {
			table.AddRow("last_error", "")
		}
		return result.FromTable(table), nil
	case ":help":
		return helpResult(), nil
	default:
		return result.Result{}, fmt.Errorf("unknown meta command %q; use :help", command)
	}
}

func statusResult(key, value string) result.Result {
	table := result.NewTable([]string{"key", "value"})
	table.AddRow(key, value)
	return result.FromTable(table)
}

func contextResult(database, retentionPolicy string) result.Result {
	table := result.NewTable([]string{"key", "value"})
	table.AddRow("database", database)
	table.AddRow("retention_policy", retentionPolicy)
	return result.FromTable(table)
}

func parseUseTarget(target string) (database string, retentionPolicy string, explicitRP bool, err error) {
	database = strings.TrimSpace(target)
	if database == "" {
		return "", "", false, fmt.Errorf("usage: :use <db>[.<rp>]")
	}
	dot := strings.LastIndex(database, ".")
	if dot < 0 {
		return database, "", false, nil
	}
	retentionPolicy = strings.TrimSpace(database[dot+1:])
	database = strings.TrimSpace(database[:dot])
	if database == "" || retentionPolicy == "" {
		return "", "", false, fmt.Errorf("usage: :use <db>[.<rp>]")
	}
	return database, retentionPolicy, true, nil
}

func (e *Executor) defaultRetentionPolicy(ctx context.Context, database string) (string, error) {
	policies, err := e.Adapter.ShowRetentionPolicies(ctx, database)
	if err != nil {
		return "", fmt.Errorf("show retention policies for %q: %w", database, err)
	}
	if len(policies) == 0 {
		return "", fmt.Errorf("database %q has no retention policies", database)
	}
	for _, policy := range policies {
		if policy.Default && policy.Name != "" {
			return policy.Name, nil
		}
	}
	if len(policies) == 1 && policies[0].Name != "" {
		return policies[0].Name, nil
	}
	return "", fmt.Errorf("default retention policy not found for database %q", database)
}

func (e *Executor) retentionPoliciesResult(ctx context.Context, databases []string) (result.Result, error) {
	table := result.NewTable([]string{"database", "retention_policy", "duration", "shard_group_duration", "replica_n", "default"})
	for _, database := range databases {
		policies, err := e.Adapter.ShowRetentionPolicies(ctx, database)
		if err != nil {
			return result.Result{}, fmt.Errorf("show retention policies for %q: %w", database, err)
		}
		for _, policy := range policies {
			table.AddRow(database, policy.Name, policy.Duration, policy.ShardGroupDuration, policy.ReplicaN, policy.Default)
		}
	}
	return result.FromTable(table), nil
}

func helpResult() result.Result {
	table := result.NewTable([]string{"command", "description"})
	table.AddRow(":use <db>[.<rp>]", "set database and optional retention policy")
	table.AddRow(":db <db>", "set database and resolve its default retention policy")
	table.AddRow(":rp <rp>", "set current retention policy")
	table.AddRow(":rps [db]", "show retention policy details for current, named, or all databases")
	table.AddRow(":dbs", "show databases")
	table.AddRow(":measurements", "show measurements in current database")
	table.AddRow(":msts", "alias for :measurements")
	table.AddRow(":schema <measurement>", "show field and tag keys for a measurement")
	table.AddRow(":status", "show current session status")
	table.AddRow(":help", "show commands")
	table.AddRow(":q", "quit")
	return result.FromTable(table)
}
