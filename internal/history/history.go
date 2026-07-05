package history

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	DefaultLimit      = 20
	DefaultMaxEntries = 1000
)

type Entry struct {
	ID              int       `json:"-"`
	Time            time.Time `json:"time"`
	Database        string    `json:"database,omitempty"`
	RetentionPolicy string    `json:"retention_policy,omitempty"`
	Dialect         string    `json:"dialect,omitempty"`
	Query           string    `json:"query"`
}

type Options struct {
	MaxEntries   int
	CompactEvery int
	Now          func() time.Time
}

type Store struct {
	path                string
	maxEntries          int
	compactEvery        int
	appendsSinceCompact int
	now                 func() time.Time
}

func NewStore(path string, options Options) *Store {
	if strings.TrimSpace(path) == "" {
		path = DefaultPath()
	}
	maxEntries := options.MaxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultMaxEntries
	}
	compactEvery := options.CompactEvery
	if compactEvery <= 0 {
		compactEvery = 100
		if maxEntries <= DefaultLimit {
			compactEvery = 1
		}
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Store{
		path:         path,
		maxEntries:   maxEntries,
		compactEvery: compactEvery,
		now:          now,
	}
}

func DefaultPath() string {
	if stateHome := strings.TrimSpace(os.Getenv("XDG_STATE_HOME")); stateHome != "" {
		if filepath.IsAbs(stateHome) {
			return filepath.Join(stateHome, "influx-cli", "history.jsonl")
		}
	}
	home, err := os.UserHomeDir()
	if err == nil && home != "" {
		return filepath.Join(home, ".local", "state", "influx-cli", "history.jsonl")
	}
	cacheDir, err := os.UserCacheDir()
	if err == nil && cacheDir != "" {
		return filepath.Join(cacheDir, "influx-cli", "history.jsonl")
	}
	return "history.jsonl"
}

func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *Store) Append(entry Entry) error {
	if s == nil {
		return nil
	}
	entry.Query = strings.TrimSpace(entry.Query)
	if entry.Query == "" {
		return nil
	}
	if entry.Time.IsZero() {
		entry.Time = s.now().UTC()
	} else {
		entry.Time = entry.Time.UTC()
	}
	return s.withLock(func() error {
		return s.appendLocked(entry)
	})
}

func (s *Store) appendLocked(entry Entry) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create history directory: %w", err)
	}
	file, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open history file: %w", err)
	}
	_ = file.Chmod(0o600)
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(entry); err != nil {
		_ = file.Close()
		return fmt.Errorf("write history entry: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close history file: %w", err)
	}
	if s.maxEntries > 0 && s.shouldCompactLocked() {
		if err := s.compactLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) shouldCompactLocked() bool {
	s.appendsSinceCompact++
	if s.appendsSinceCompact >= s.compactEvery {
		s.appendsSinceCompact = 0
		return true
	}
	info, err := os.Stat(s.path)
	if err == nil && info.Size() > int64(s.maxEntries)*4096 {
		s.appendsSinceCompact = 0
		return true
	}
	return false
}

func (s *Store) Search(filter string, limit int) ([]Entry, error) {
	if s == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = DefaultLimit
	}
	if s.maxEntries > 0 && limit > s.maxEntries {
		limit = s.maxEntries
	}
	entries, err := s.readAll()
	if err != nil {
		return nil, err
	}

	normalizedFilter := strings.ToLower(strings.TrimSpace(filter))
	out := make([]Entry, 0, limit)
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if normalizedFilter != "" && !matches(entry, normalizedFilter) {
			continue
		}
		out = append(out, entry)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Store) compactLocked() error {
	entries, err := s.readAll()
	if err != nil {
		return err
	}
	if len(entries) <= s.maxEntries {
		return nil
	}
	return s.writeAll(entries[len(entries)-s.maxEntries:])
}

func (s *Store) readAll() ([]Entry, error) {
	file, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open history file: %w", err)
	}
	defer file.Close()

	var entries []Entry
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		entry.Query = strings.TrimSpace(entry.Query)
		if entry.Query == "" {
			continue
		}
		entry.ID = len(entries) + 1
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read history file: %w", err)
	}
	return entries, nil
}

func (s *Store) withLock(fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create history directory: %w", err)
	}
	lockPath := s.path + ".lock"
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open history lock: %w", err)
	}
	defer file.Close()
	_ = file.Chmod(0o600)
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock history file: %w", err)
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	return fn()
}

func (s *Store) writeAll(entries []Entry) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create history directory: %w", err)
	}
	tempPath := s.path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open history temp file: %w", err)
	}
	_ = file.Chmod(0o600)
	encoder := json.NewEncoder(file)
	for _, entry := range entries {
		entry.ID = 0
		if err := encoder.Encode(entry); err != nil {
			_ = file.Close()
			return fmt.Errorf("write history temp file: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close history temp file: %w", err)
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("replace history file: %w", err)
	}
	return nil
}

func matches(entry Entry, filter string) bool {
	return strings.Contains(strings.ToLower(entry.Query), filter) ||
		strings.Contains(strings.ToLower(entry.Database), filter) ||
		strings.Contains(strings.ToLower(entry.RetentionPolicy), filter) ||
		strings.Contains(strings.ToLower(entry.Dialect), filter)
}
