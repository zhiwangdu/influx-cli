package adapter

import (
	"context"
	"errors"

	"github.com/zhiwangdu/influx-cli/internal/query"
	"github.com/zhiwangdu/influx-cli/internal/result"
	"github.com/zhiwangdu/influx-cli/internal/schema"
)

var ErrNotSupported = errors.New("adapter does not support this operation")

type Adapter interface {
	Name() string
	Ping(ctx context.Context) error
	Query(ctx context.Context, q query.Query) (result.Result, error)
	ShowDatabases(ctx context.Context) ([]string, error)
	ShowRetentionPolicies(ctx context.Context, db string) ([]RetentionPolicy, error)
	GetSchema(ctx context.Context, scope schema.Scope) (schema.Snapshot, error)
}

type RetentionPolicy struct {
	Name               string
	Duration           string
	ShardGroupDuration string
	ReplicaN           string
	Default            bool
}
