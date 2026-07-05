package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestIngestDryRunPrintsLineProtocolAndSummary(t *testing.T) {
	t.Setenv("INFLUX_CLI_PROFILE", "")
	t.Setenv("INFLUX_CLI_RENDER", "")
	t.Setenv("INFLUX_CLI_URL", "")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command := newRootCommand()
	command.SetOut(&stdout)
	command.SetErr(&stderr)
	command.SetArgs([]string{
		"--config", filepath.Join(t.TempDir(), "missing.yaml"),
		"--url", "http://127.0.0.1:8086",
		"ingest", "demo-cpu",
		"--rate", "2/s",
		"--duration", "1s",
		"--start", "2026-07-05T00:00:00Z",
		"--dry-run",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("stdout line count = %d, want 2:\n%s", len(lines), stdout.String())
	}
	if !strings.HasPrefix(lines[0], "demo_cpu,host=host-0000") {
		t.Fatalf("first line = %q, want demo_cpu line protocol", lines[0])
	}
	if !strings.HasSuffix(lines[0], " 1783209600000000000") || !strings.HasSuffix(lines[1], " 1783209600500000000") {
		t.Fatalf("unexpected fixed timestamps:\n%s", stdout.String())
	}
	summary := stderr.String()
	if !strings.Contains(summary, "ingest: generated") || !strings.Contains(summary, "points: 2") {
		t.Fatalf("summary missing generated/points fields:\n%s", summary)
	}
	if !strings.Contains(summary, "simulated_range: 2026-07-05T00:00:00Z to 2026-07-05T00:00:01Z") {
		t.Fatalf("summary missing fixed simulated range:\n%s", summary)
	}
}
