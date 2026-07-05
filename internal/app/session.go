package app

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/config"
	"github.com/zhiwangdu/influx-cli/internal/query"
)

type Session struct {
	mu          sync.RWMutex
	adapterName string
	database    string
	rp          string
	precision   string
	dialect     query.Dialect
	lastLatency time.Duration
	lastError   error
}

type SessionSnapshot struct {
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
		adapterName: effective.Adapter,
		database:    effective.Database,
		rp:          effective.RetentionPolicy,
		precision:   effective.Precision,
		dialect:     query.DialectInfluxQL,
	}
}

func (s *Session) Snapshot() SessionSnapshot {
	if s == nil {
		return SessionSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return SessionSnapshot{
		AdapterName: s.adapterName,
		Database:    s.database,
		RP:          s.rp,
		Precision:   s.precision,
		Dialect:     s.dialect,
		LastLatency: s.lastLatency,
		LastError:   s.lastError,
	}
}

func (s *Session) SetAdapterName(adapterName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adapterName = adapterName
}

func (s *Session) SetContext(database, retentionPolicy string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.database = database
	s.rp = retentionPolicy
}

func (s *Session) SetRetentionPolicy(retentionPolicy string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rp = retentionPolicy
}

func (s *Session) SetDialect(dialect query.Dialect) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dialect = dialect
}

func (s *Session) StatusLine() string {
	snapshot := s.Snapshot()
	status := "ok"
	if snapshot.LastError != nil {
		status = "error: " + oneLine(snapshot.LastError.Error())
	}
	return fmt.Sprintf("db: %s | rp: %s | mode: %s | latency: %s | %s",
		printValue(snapshot.Database),
		printValue(snapshot.RP),
		printValue(string(snapshot.Dialect)),
		formatLatency(snapshot.LastLatency),
		status,
	)
}

func (s *Session) Prompt() string {
	snapshot := s.Snapshot()
	context := "influx"
	if snapshot.Database != "" || snapshot.RP != "" {
		context += "[" + printValue(snapshot.Database) + "/" + printValue(snapshot.RP) + "]"
	}
	return context + "> "
}

func (s *Session) update(latency time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastLatency = latency
	s.lastError = err
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
