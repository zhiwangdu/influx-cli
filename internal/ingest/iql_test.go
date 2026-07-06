package ingest

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRunIQLGeneratesMockDataFromInsertStatements(t *testing.T) {
	iqlFile := writeTempIQL(t, `
SET Database [stress]
SET RetentionPolicy [autogen]
SET Precision [s]
SET StartDate [2006-01-02]
SET BatchSize [2]
SET WriteConcurrency [20]

GO INSERT cpu
cpu,
host=server-[float inc(0) 2],location=us-west
value=[int inc(10) 0]
3 10s

GO QUERY cpu
SELECT count(value) FROM cpu WHERE %t
DO 2

CREATE DATABASE ignored

WAIT
`)

	writer := &captureWriter{}
	summary, err := Run(context.Background(), writer, Options{
		Dataset: DatasetIQL,
		IQLFile: iqlFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.RequestedPoints != 3 || summary.WrittenPoints != 3 || summary.Batches != 2 {
		t.Fatalf("summary points/batches = %#v, want 3 points in 2 batches", summary)
	}
	if summary.Database != "stress" || summary.RetentionPolicy != "autogen" || summary.Precision != "s" {
		t.Fatalf("summary db/rp/precision = %q/%q/%q, want stress/autogen/s", summary.Database, summary.RetentionPolicy, summary.Precision)
	}
	if summary.IQLInserts != 1 || summary.IQLSkippedQuery != 2 || summary.IQLSkippedRaw != 1 {
		t.Fatalf("summary iql counts = %#v", summary)
	}
	if len(summary.IQLIgnoredSets) != 1 || summary.IQLIgnoredSets[0] != "WriteConcurrency" {
		t.Fatalf("ignored settings = %#v, want WriteConcurrency", summary.IQLIgnoredSets)
	}
	start := time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)
	if !summary.DataStartedAt.Equal(start) || !summary.DataEndedAt.Equal(start.Add(10*time.Second)) {
		t.Fatalf("data range = %s to %s, want first and last IQL timestamps", summary.DataStartedAt, summary.DataEndedAt)
	}

	if len(writer.requests) != 2 {
		t.Fatalf("write requests = %d, want 2", len(writer.requests))
	}
	if writer.requests[0].Database != "stress" || writer.requests[0].RetentionPolicy != "autogen" || writer.requests[0].Precision != "s" {
		t.Fatalf("first request metadata = %#v", writer.requests[0])
	}
	lines := strings.Split(strings.TrimSpace(writer.body()), "\n")
	wantLines := []string{
		"cpu,host=server-0,location=us-west value=10i 1136160000",
		"cpu,host=server-1,location=us-west value=10i 1136160000",
		"cpu,host=server-0,location=us-west value=11i 1136160010",
	}
	if strings.Join(lines, "\n") != strings.Join(wantLines, "\n") {
		t.Fatalf("generated lines:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(wantLines, "\n"))
	}
}

func TestRunIQLHonorsExplicitOverrides(t *testing.T) {
	iqlFile := writeTempIQL(t, `
SET Database [from_file]
SET Precision [s]
SET StartDate [2006-01-02]
SET BatchSize [1]

INSERT cpu
cpu,
host=server-[float inc(0) 1]
value=[float inc(1) 0]
2 10s
`)

	writer := &captureWriter{}
	start := time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)
	summary, err := Run(context.Background(), writer, Options{
		Dataset:        DatasetIQL,
		IQLFile:        iqlFile,
		Database:       "from_cli",
		Precision:      "ns",
		BatchSize:      2,
		Start:          start,
		ForceDatabase:  true,
		ForcePrecision: true,
		ForceStart:     true,
		ForceBatchSize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Database != "from_cli" || summary.Precision != "ns" || summary.Batches != 1 {
		t.Fatalf("summary did not honor CLI-like overrides: %#v", summary)
	}
	if !summary.DataStartedAt.Equal(start) {
		t.Fatalf("data start = %s, want override start %s", summary.DataStartedAt, start)
	}
	if writer.requests[0].Database != "from_cli" || writer.requests[0].Precision != "ns" {
		t.Fatalf("request metadata = %#v, want CLI-like overrides", writer.requests[0])
	}
}

func TestRunIQLUsesCartesianTagCardinalityForTimestamps(t *testing.T) {
	iqlFile := writeTempIQL(t, `
SET Database [stress]
SET Precision [s]
SET StartDate [2006-01-02]

INSERT cpu
cpu,
host=server-[float inc(0) 2],rack=rack-[float inc(0) 3]
value=[int inc(1) 0]
7 10s
`)

	writer := &captureWriter{}
	summary, err := Run(context.Background(), writer, Options{
		Dataset: DatasetIQL,
		IQLFile: iqlFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !summary.DataEndedAt.Equal(time.Date(2006, 1, 2, 0, 0, 10, 0, time.UTC)) {
		t.Fatalf("data end = %s, want timestamp to advance after 2*3 tag combinations", summary.DataEndedAt)
	}
	lines := strings.Split(strings.TrimSpace(writer.body()), "\n")
	wantLines := []string{
		"cpu,host=server-0,rack=rack-0 value=1i 1136160000",
		"cpu,host=server-1,rack=rack-0 value=1i 1136160000",
		"cpu,host=server-0,rack=rack-1 value=1i 1136160000",
		"cpu,host=server-1,rack=rack-1 value=1i 1136160000",
		"cpu,host=server-0,rack=rack-2 value=1i 1136160000",
		"cpu,host=server-1,rack=rack-2 value=1i 1136160000",
		"cpu,host=server-0,rack=rack-0 value=2i 1136160010",
	}
	if strings.Join(lines, "\n") != strings.Join(wantLines, "\n") {
		t.Fatalf("generated lines:\n%s\nwant:\n%s", strings.Join(lines, "\n"), strings.Join(wantLines, "\n"))
	}
}

func TestRunIQLRejectsUnboundedTagCardinality(t *testing.T) {
	iqlFile := writeTempIQL(t, `
SET Database [stress]

INSERT cpu
cpu,
host=server-[float inc(0) 0]
value=[int inc(1) 0]
1 10s
`)

	_, err := Run(context.Background(), &captureWriter{}, Options{
		Dataset: DatasetIQL,
		IQLFile: iqlFile,
	})
	if err == nil {
		t.Fatal("expected tag cardinality error")
	}
	if !strings.Contains(err.Error(), "finite cardinality") {
		t.Fatalf("error = %q, want finite cardinality error", err)
	}
}

func TestRunIQLRequiresDatabaseForWrites(t *testing.T) {
	iqlFile := writeTempIQL(t, `
INSERT cpu
cpu,
host=server-[float inc(0) 1]
value=[float inc(1) 0]
1 10s
`)

	_, err := Run(context.Background(), &captureWriter{}, Options{
		Dataset: DatasetIQL,
		IQLFile: iqlFile,
	})
	if err == nil {
		t.Fatal("expected database error")
	}
	if !strings.Contains(err.Error(), "database is required for iql ingest") {
		t.Fatalf("error = %q, want iql database error", err)
	}
}

func TestRunIQLWithoutInsertsUsesStartForSummaryRange(t *testing.T) {
	iqlFile := writeTempIQL(t, `
SET StartDate [2006-01-02]

QUERY cpu
SELECT count(value) FROM cpu
DO 3
`)

	summary, err := Run(context.Background(), &captureWriter{}, Options{
		Dataset:            DatasetIQL,
		IQLFile:            iqlFile,
		AllowEmptyDatabase: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)
	if !summary.StartedAt.Equal(start) || !summary.EndedAt.Equal(start) {
		t.Fatalf("summary range = %s to %s, want start fallback", summary.StartedAt, summary.EndedAt)
	}
	if summary.IQLSkippedQuery != 3 || summary.WrittenPoints != 0 {
		t.Fatalf("summary = %#v, want skipped query only", summary)
	}
}

func writeTempIQL(t *testing.T, content string) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "mock-*.iql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(strings.TrimSpace(content) + "\n"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return file.Name()
}
