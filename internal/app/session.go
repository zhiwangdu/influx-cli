package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/config"
	"github.com/zhiwangdu/influx-cli/internal/query"
)

type Session struct {
	AdapterName string
	Database    string
	RP          string
	Precision   string
	Dialect     query.Dialect
	LastLatency time.Duration
	LastError   error
}

func NewSession(effective config.Effective) *Session {
	return &Session{
		AdapterName: effective.Adapter,
		Database:    effective.Database,
		RP:          effective.RetentionPolicy,
		Precision:   effective.Precision,
		Dialect:     query.DialectInfluxQL,
	}
}

func (s *Session) StatusLine() string {
	status := "ok"
	if s.LastError != nil {
		status = "error: " + oneLine(s.LastError.Error())
	}
	return fmt.Sprintf("db: %s | rp: %s | mode: %s | latency: %s | %s",
		printValue(s.Database),
		printValue(s.RP),
		printValue(string(s.Dialect)),
		formatLatency(s.LastLatency),
		status,
	)
}

func (s *Session) Prompt() string {
	context := "influx"
	if s.Database != "" || s.RP != "" {
		context += "[" + printValue(s.Database) + "/" + printValue(s.RP) + "]"
	}
	return context + "> "
}

func (s *Session) update(latency time.Duration, err error) {
	s.LastLatency = latency
	s.LastError = err
}

func printValue(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func oneLine(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.Join(strings.Fields(value), " ")
	if len(value) > 160 {
		return value[:157] + "..."
	}
	return value
}

func formatLatency(latency time.Duration) string {
	if latency <= 0 {
		return "-"
	}
	if latency < time.Millisecond {
		return latency.String()
	}
	return latency.Truncate(time.Millisecond).String()
}
