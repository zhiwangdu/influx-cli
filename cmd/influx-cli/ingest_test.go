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

func TestIngestStressBasicDryRun(t *testing.T) {
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
		"--precision", "s",
		"ingest", "stress-basic",
		"--point-count", "2",
		"--series-count", "2",
		"--tick", "10s",
		"--batch-size", "3",
		"--start", "2006-01-02T00:00:00Z",
		"--dry-run",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("stdout line count = %d, want 4:\n%s", len(lines), stdout.String())
	}
	if got, want := lines[0], "cpu,host=server-0,location=us-west value=0.000 1136160010"; got != want {
		t.Fatalf("first line = %q, want %q", got, want)
	}
	if got, want := lines[2], "cpu,host=server-0,location=us-west value=74.000 1136160020"; got != want {
		t.Fatalf("third line = %q, want %q", got, want)
	}
	summary := stderr.String()
	for _, want := range []string{
		"dataset: stress-basic",
		"point_count: 2",
		"series_count: 2",
		"tick: 10s",
		"points: 4",
		"batches: 2",
		"simulated_range: 2006-01-02T00:00:00Z to 2006-01-02T00:00:20Z",
		"data_range: 2006-01-02T00:00:10Z to 2006-01-02T00:00:20Z",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}
	if strings.Contains(summary, "rate:") {
		t.Fatalf("stress-basic summary should not include rate:\n%s", summary)
	}
}
