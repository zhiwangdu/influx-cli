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
			return result.Result{}, fmt.Errorf("usage: :use <db>")
		}
		e.Session.Database = fields[1]
		return statusResult("database", fields[1]), nil
	case ":rp":
		if len(fields) != 2 {
			return result.Result{}, fmt.Errorf("usage: :rp <retention_policy>")
		}
		e.Session.RP = fields[1]
		return statusResult("retention_policy", fields[1]), nil
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
	case ":measurements":
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

func helpResult() result.Result {
	table := result.NewTable([]string{"command", "description"})
	table.AddRow(":use <db>", "set current database")
	table.AddRow(":rp <rp>", "set current retention policy")
	table.AddRow(":dbs", "show databases")
	table.AddRow(":measurements", "show measurements in current database")
	table.AddRow(":schema <measurement>", "show field and tag keys for a measurement")
	table.AddRow(":status", "show current session status")
	table.AddRow(":help", "show commands")
	table.AddRow(":q", "quit")
	return result.FromTable(table)
}
