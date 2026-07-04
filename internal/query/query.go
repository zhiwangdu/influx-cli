package query

import "strings"

type Dialect string

const (
	DialectInfluxQL Dialect = "influxql"
	DialectFlux     Dialect = "flux"
	DialectSQL      Dialect = "sql"
)

type Kind string

const (
	KindSelect  Kind = "select"
	KindShow    Kind = "show"
	KindMeta    Kind = "meta"
	KindExplain Kind = "explain"
	KindSchema  Kind = "schema"
	KindWatch   Kind = "watch"
)

type Query struct {
	Raw       string
	Dialect   Dialect
	Database  string
	RP        string
	Precision string
	Kind      Kind
}

func New(raw, database, rp, precision string) Query {
	trimmed := strings.TrimSpace(raw)
	return Query{
		Raw:       trimmed,
		Dialect:   DetectDialect(trimmed),
		Database:  database,
		RP:        rp,
		Precision: precision,
		Kind:      Classify(trimmed),
	}
}

func DetectDialect(raw string) Dialect {
	lower := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case strings.Contains(lower, "|>") || strings.Contains(lower, "from(bucket:"):
		return DialectFlux
	default:
		return DialectInfluxQL
	}
}

func Classify(raw string) Kind {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return KindSelect
	}
	if strings.HasPrefix(trimmed, ":") {
		if strings.HasPrefix(strings.ToLower(trimmed), ":schema") {
			return KindSchema
		}
		if strings.HasPrefix(strings.ToLower(trimmed), ":explain") {
			return KindExplain
		}
		return KindMeta
	}
	upper := strings.ToUpper(trimmed)
	switch {
	case strings.HasPrefix(upper, "SHOW "):
		return KindShow
	case strings.HasPrefix(upper, "EXPLAIN "):
		return KindExplain
	default:
		return KindSelect
	}
}
