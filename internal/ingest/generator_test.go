package ingest

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
)

func TestParseRate(t *testing.T) {
	tests := map[string]int{
		"100/s": 100,
		"10k/s": 10000,
		"1.5k":  1500,
		"2m/s":  2000000,
	}
	for raw, want := range tests {
		got, err := ParseRate(raw)
		if err != nil {
			t.Fatalf("ParseRate(%q) returned error: %v", raw, err)
		}
		if got != want {
			t.Fatalf("ParseRate(%q) = %d, want %d", raw, got, want)
		}
	}
}

func TestRunBatchesHighCardinalityDataset(t *testing.T) {
	writer := &captureWriter{}
	start := time.Date(2026, 7, 5, 1, 2, 3, 0, time.UTC)
	summary, err := Run(context.Background(), writer, Options{
		Dataset:         DatasetHighCardinality,
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Precision:       "rfc3339",
		RatePerSecond:   3,
		Duration:        2 * time.Second,
		BatchSize:       4,
		Hosts:           2,
		PIDs:            3,
		Start:           start,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.RequestedPoints != 6 || summary.WrittenPoints != 6 || summary.Batches != 2 {
		t.Fatalf("summary = %#v, want 6 points in 2 batches", summary)
	}
	if len(writer.requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(writer.requests))
	}
	if writer.requests[0].Database != "metrics" || writer.requests[0].RetentionPolicy != "autogen" || writer.requests[0].Precision != "ns" {
		t.Fatalf("first request metadata = %#v", writer.requests[0])
	}

	lines := strings.Split(strings.TrimSpace(writer.body()), "\n")
	if len(lines) != 6 {
		t.Fatalf("line count = %d, want 6", len(lines))
	}
	if !strings.Contains(lines[0], "high_cardinality,host=host-0000,pid=pid-00000000") {
		t.Fatalf("first line does not include expected first series: %q", lines[0])
	}
	if !strings.Contains(lines[5], "high_cardinality,host=host-0001,pid=pid-00000002") {
		t.Fatalf("last line does not include expected sixth series: %q", lines[5])
	}
}

func TestRunStressBasicDatasetMatchesInfluxStressShape(t *testing.T) {
	writer := &captureWriter{}
	start := time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)
	summary, err := Run(context.Background(), writer, Options{
		Dataset:     DatasetStressBasic,
		Database:    "stress",
		Precision:   "s",
		PointCount:  2,
		SeriesCount: 3,
		Tick:        10 * time.Second,
		BatchSize:   4,
		Start:       start,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.RequestedPoints != 6 || summary.WrittenPoints != 6 || summary.Batches != 2 {
		t.Fatalf("summary = %#v, want 6 points in 2 batches", summary)
	}
	if summary.Duration != 20*time.Second || summary.PointCount != 2 || summary.SeriesCount != 3 || summary.Tick != 10*time.Second {
		t.Fatalf("summary dimensions = %#v", summary)
	}
	if !summary.DataStartedAt.Equal(start.Add(10*time.Second)) || !summary.DataEndedAt.Equal(start.Add(20*time.Second)) {
		t.Fatalf("summary data range = %s to %s, want first generated point to last generated point", summary.DataStartedAt, summary.DataEndedAt)
	}

	lines := strings.Split(strings.TrimSpace(writer.body()), "\n")
	if len(lines) != 6 {
		t.Fatalf("line count = %d, want 6", len(lines))
	}
	firstTimestamp := start.Add(10 * time.Second).Unix()
	secondTimestamp := start.Add(20 * time.Second).Unix()
	if want := "cpu,host=server-0,location=us-west value=0.000 " + strconv.FormatInt(firstTimestamp, 10); lines[0] != want {
		t.Fatalf("first line = %q, want %q", lines[0], want)
	}
	if !strings.Contains(lines[2], "cpu,host=server-2,location=us-west") || !strings.HasSuffix(lines[2], " "+strconv.FormatInt(firstTimestamp, 10)) {
		t.Fatalf("third line has unexpected series/timestamp: %q", lines[2])
	}
	if !strings.Contains(lines[3], "cpu,host=server-0,location=us-west") || !strings.HasSuffix(lines[3], " "+strconv.FormatInt(secondTimestamp, 10)) {
		t.Fatalf("fourth line has unexpected series/timestamp: %q", lines[3])
	}
}

func TestOutOfOrderDatasetMovesConfiguredRowsBackward(t *testing.T) {
	plan, err := newPlan(Options{
		Dataset:       DatasetOutOfOrder,
		RatePerSecond: 10,
		Duration:      2 * time.Second,
		Hosts:         1,
		Ratio:         0.2,
		Start:         time.Unix(100, 0).UTC(),
	}, time.Unix(200, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}

	before := timestampFromLine(t, plan.line(9))
	outOfOrder := timestampFromLine(t, plan.line(10))
	if outOfOrder >= before {
		t.Fatalf("out-of-order timestamp = %d, previous = %d; want older timestamp", outOfOrder, before)
	}
}

func TestCoveringBlockDatasetRewritesEarlierWindow(t *testing.T) {
	plan, err := newPlan(Options{
		Dataset:       DatasetCoveringBlock,
		RatePerSecond: 10,
		Duration:      2 * time.Second,
		Hosts:         2,
		Start:         time.Unix(100, 0).UTC(),
	}, time.Unix(200, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}

	baseCount := plan.coveringBaseCount()
	lastBase := timestampFromLine(t, plan.line(baseCount-1))
	firstCover := timestampFromLine(t, plan.line(baseCount))
	if firstCover >= lastBase {
		t.Fatalf("first covering timestamp = %d, last base = %d; want earlier covering timestamp", firstCover, lastBase)
	}
}

func TestFormatLineEscapesLineProtocolParts(t *testing.T) {
	line := formatLine("demo cpu,main",
		[]pair{{key: "host name", value: "host=1,west"}},
		[]field{{key: "field name", value: "quoted \"value\""}},
		123,
	)
	want := `demo\ cpu\,main,host\ name=host\=1\,west field\ name="quoted \"value\"" 123`
	if line != want {
		t.Fatalf("line = %q, want %q", line, want)
	}
}

func TestNewPlanRejectsHugeBatchSize(t *testing.T) {
	_, err := newPlan(Options{
		Dataset:       DatasetDemoCPU,
		BatchSize:     MaxBatchSize + 1,
		RatePerSecond: 1,
		Duration:      time.Second,
	}, time.Unix(200, 0).UTC())
	if err == nil {
		t.Fatal("expected batch size error")
	}
	if !strings.Contains(err.Error(), "batch size") {
		t.Fatalf("error = %q, want batch size error", err)
	}
}

type captureWriter struct {
	requests []adapter.WriteRequest
}

func (w *captureWriter) WriteLineProtocol(ctx context.Context, request adapter.WriteRequest) error {
	request.Body = append([]byte(nil), request.Body...)
	w.requests = append(w.requests, request)
	return nil
}

func (w *captureWriter) body() string {
	var builder strings.Builder
	for _, request := range w.requests {
		builder.Write(request.Body)
	}
	return builder.String()
}

func timestampFromLine(t *testing.T, line string) int64 {
	t.Helper()
	parts := strings.Fields(line)
	if len(parts) == 0 {
		t.Fatalf("line has no fields: %q", line)
	}
	value, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		t.Fatalf("parse timestamp from %q: %v", line, err)
	}
	return value
}
