package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
)

type Dataset string

const (
	DatasetDemoCPU         Dataset = "demo-cpu"
	DatasetHighCardinality Dataset = "high-cardinality"
	DatasetOutOfOrder      Dataset = "out-of-order"
	DatasetCoveringBlock   Dataset = "covering-block"
)

const (
	defaultRatePerSecond = 100
	defaultDuration      = time.Minute
	defaultBatchSize     = 5000
	defaultHosts         = 10
	defaultPIDs          = 1000
	defaultPrecision     = "ns"
	MaxBatchSize         = 100000
)

type Options struct {
	Dataset         Dataset
	Database        string
	RetentionPolicy string
	Precision       string
	RatePerSecond   int
	Duration        time.Duration
	BatchSize       int
	Hosts           int
	PIDs            int
	Ratio           float64
	Measurement     string
	Start           time.Time
}

type Summary struct {
	Dataset         Dataset
	Database        string
	RetentionPolicy string
	Precision       string
	Measurement     string
	RatePerSecond   int
	Duration        time.Duration
	RequestedPoints int64
	WrittenPoints   int64
	Batches         int
	StartedAt       time.Time
	EndedAt         time.Time
	Elapsed         time.Duration
}

type WriterFunc func(ctx context.Context, request adapter.WriteRequest) error

func (f WriterFunc) WriteLineProtocol(ctx context.Context, request adapter.WriteRequest) error {
	return f(ctx, request)
}

func NewDryRunWriter(w io.Writer) adapter.LineProtocolWriter {
	return WriterFunc(func(ctx context.Context, request adapter.WriteRequest) error {
		if len(request.Body) == 0 {
			return nil
		}
		_, err := w.Write(request.Body)
		return err
	})
}

func Datasets() []Dataset {
	return []Dataset{
		DatasetDemoCPU,
		DatasetHighCardinality,
		DatasetOutOfOrder,
		DatasetCoveringBlock,
	}
}

func ParseDataset(raw string) (Dataset, error) {
	normalized := Dataset(strings.ToLower(strings.TrimSpace(raw)))
	switch normalized {
	case DatasetDemoCPU, DatasetHighCardinality, DatasetOutOfOrder, DatasetCoveringBlock:
		return normalized, nil
	default:
		return "", fmt.Errorf("unknown dataset %q; supported datasets: %s", raw, datasetList())
	}
}

func ParseRate(raw string) (int, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return 0, errors.New("rate is required")
	}
	normalized = strings.ReplaceAll(normalized, " ", "")
	normalized = strings.TrimSuffix(normalized, "/sec")
	normalized = strings.TrimSuffix(normalized, "/second")
	normalized = strings.TrimSuffix(normalized, "/s")
	normalized = strings.TrimSuffix(normalized, "ps")

	multiplier := 1.0
	switch {
	case strings.HasSuffix(normalized, "k"):
		multiplier = 1000
		normalized = strings.TrimSuffix(normalized, "k")
	case strings.HasSuffix(normalized, "m"):
		multiplier = 1000 * 1000
		normalized = strings.TrimSuffix(normalized, "m")
	}

	value, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0, fmt.Errorf("parse rate %q: %w", raw, err)
	}
	points := math.Round(value * multiplier)
	maxInt := float64(int(^uint(0) >> 1))
	if points < 1 || points > maxInt {
		return 0, fmt.Errorf("rate %q must resolve to a positive whole number of points per second", raw)
	}
	return int(points), nil
}

func Run(ctx context.Context, writer adapter.LineProtocolWriter, options Options) (Summary, error) {
	if writer == nil {
		return Summary{}, errors.New("line protocol writer is required")
	}
	plan, err := newPlan(options, time.Now())
	if err != nil {
		return Summary{}, err
	}

	started := time.Now()
	summary := plan.summary()
	var buffer bytes.Buffer
	for i := int64(0); i < plan.total; i++ {
		select {
		case <-ctx.Done():
			return summary, ctx.Err()
		default:
		}

		buffer.WriteString(plan.line(i))
		buffer.WriteByte('\n')
		if (i+1)%int64(plan.options.BatchSize) == 0 {
			if err := writeBatch(ctx, writer, plan, buffer.Bytes()); err != nil {
				return summary, err
			}
			summary.WrittenPoints += int64(plan.options.BatchSize)
			summary.Batches++
			buffer.Reset()
		}
	}
	if buffer.Len() > 0 {
		points := plan.total - summary.WrittenPoints
		if err := writeBatch(ctx, writer, plan, buffer.Bytes()); err != nil {
			return summary, err
		}
		summary.WrittenPoints += points
		summary.Batches++
	}
	summary.Elapsed = time.Since(started)
	return summary, nil
}

type plan struct {
	options     Options
	dataset     Dataset
	total       int64
	interval    time.Duration
	startedAt   time.Time
	endedAt     time.Time
	measurement string
	precision   string
}

type pair struct {
	key   string
	value string
}

type field struct {
	key   string
	value any
}

func newPlan(options Options, now time.Time) (plan, error) {
	dataset, err := ParseDataset(string(options.Dataset))
	if err != nil {
		return plan{}, err
	}
	if options.RatePerSecond == 0 {
		options.RatePerSecond = defaultRatePerSecond
	}
	if options.Duration == 0 {
		options.Duration = defaultDuration
	}
	if options.BatchSize == 0 {
		options.BatchSize = defaultBatchSize
	}
	if options.Hosts == 0 {
		options.Hosts = defaultHosts
	}
	if options.PIDs == 0 {
		options.PIDs = defaultPIDs
	}
	if options.Measurement == "" {
		options.Measurement = defaultMeasurement(dataset)
	}
	precision, err := normalizePrecision(options.Precision)
	if err != nil {
		return plan{}, err
	}
	options.Precision = precision

	if options.RatePerSecond < 1 {
		return plan{}, errors.New("rate must be greater than zero")
	}
	if options.Duration <= 0 {
		return plan{}, errors.New("duration must be greater than zero")
	}
	if options.BatchSize < 1 {
		return plan{}, errors.New("batch size must be greater than zero")
	}
	if options.BatchSize > MaxBatchSize {
		return plan{}, fmt.Errorf("batch size must be less than or equal to %d", MaxBatchSize)
	}
	if options.Hosts < 1 {
		return plan{}, errors.New("hosts must be greater than zero")
	}
	if options.PIDs < 1 {
		return plan{}, errors.New("pids must be greater than zero")
	}
	if options.Ratio < 0 || options.Ratio > 1 {
		return plan{}, errors.New("ratio must be between 0 and 1")
	}

	total := int64(math.Ceil(float64(options.RatePerSecond) * options.Duration.Seconds()))
	if total < 1 {
		total = 1
	}
	interval := options.Duration / time.Duration(total)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	startedAt := options.Start
	if startedAt.IsZero() {
		startedAt = now.Add(-options.Duration)
	}
	return plan{
		options:     options,
		dataset:     dataset,
		total:       total,
		interval:    interval,
		startedAt:   startedAt.UTC(),
		endedAt:     startedAt.Add(options.Duration).UTC(),
		measurement: options.Measurement,
		precision:   precision,
	}, nil
}

func (p plan) summary() Summary {
	return Summary{
		Dataset:         p.dataset,
		Database:        p.options.Database,
		RetentionPolicy: p.options.RetentionPolicy,
		Precision:       p.precision,
		Measurement:     p.measurement,
		RatePerSecond:   p.options.RatePerSecond,
		Duration:        p.options.Duration,
		RequestedPoints: p.total,
		StartedAt:       p.startedAt,
		EndedAt:         p.endedAt,
	}
}

func (p plan) line(i int64) string {
	switch p.dataset {
	case DatasetDemoCPU:
		return p.demoCPULine(i)
	case DatasetHighCardinality:
		return p.highCardinalityLine(i)
	case DatasetOutOfOrder:
		return p.outOfOrderLine(i)
	case DatasetCoveringBlock:
		return p.coveringBlockLine(i)
	default:
		return ""
	}
}

func (p plan) demoCPULine(i int64) string {
	hostIndex := i % int64(p.options.Hosts)
	user := 15 + 45*math.Abs(math.Sin(float64(i)/17))
	system := 5 + 12*math.Abs(math.Cos(float64(i)/29))
	idle := math.Max(0, 100-user-system)
	return formatLine(p.measurement,
		[]pair{
			{key: "host", value: hostName(hostIndex)},
			{key: "region", value: regionName(hostIndex)},
			{key: "cpu", value: "cpu-total"},
		},
		[]field{
			{key: "usage_user", value: user},
			{key: "usage_system", value: system},
			{key: "usage_idle", value: idle},
		},
		p.timestampAt(i),
	)
}

func (p plan) highCardinalityLine(i int64) string {
	hostIndex := i % int64(p.options.Hosts)
	pidIndex := (i / int64(p.options.Hosts)) % int64(p.options.PIDs)
	return formatLine(p.measurement,
		[]pair{
			{key: "host", value: hostName(hostIndex)},
			{key: "pid", value: pidName(pidIndex)},
			{key: "service", value: fmt.Sprintf("svc-%02d", pidIndex%64)},
			{key: "region", value: regionName(hostIndex)},
		},
		[]field{
			{key: "value", value: float64((i*37)%1000) / 10},
		},
		p.timestampAt(i),
	)
}

func (p plan) outOfOrderLine(i int64) string {
	hostIndex := i % int64(p.options.Hosts)
	timestampIndex := p.outOfOrderIndex(i)
	value := 50 + 30*math.Sin(float64(i)/11)
	return formatLine(p.measurement,
		[]pair{
			{key: "host", value: hostName(hostIndex)},
			{key: "region", value: regionName(hostIndex)},
		},
		[]field{
			{key: "usage_idle", value: value},
		},
		p.timestampAt(timestampIndex),
	)
}

func (p plan) coveringBlockLine(i int64) string {
	timestampIndex := i
	baseCount := p.coveringBaseCount()
	if i >= baseCount {
		window := baseCount / 5
		if window < 1 {
			window = 1
		}
		timestampIndex = (i - baseCount) % window
	}

	hostIndex := timestampIndex % int64(p.options.Hosts)
	value := 35 + float64((i*19)%500)/10
	return formatLine(p.measurement,
		[]pair{
			{key: "host", value: hostName(hostIndex)},
			{key: "region", value: regionName(hostIndex)},
		},
		[]field{
			{key: "usage_idle", value: value},
		},
		p.timestampAt(timestampIndex),
	)
}

func (p plan) outOfOrderIndex(i int64) int64 {
	if p.options.Ratio <= 0 {
		return i
	}
	every := int64(math.Round(1 / p.options.Ratio))
	if every < 1 {
		every = 1
	}
	offset := every * 2
	if offset < int64(p.options.RatePerSecond) {
		offset = int64(p.options.RatePerSecond)
	}
	if i >= offset && i%every == 0 {
		return i - offset
	}
	return i
}

func (p plan) coveringBaseCount() int64 {
	baseCount := p.total * 7 / 10
	if baseCount < 1 {
		return 1
	}
	if baseCount >= p.total {
		return p.total / 2
	}
	return baseCount
}

func (p plan) timestampAt(index int64) int64 {
	timestamp := p.startedAt.Add(time.Duration(index) * p.interval)
	return encodeTimestamp(timestamp, p.precision)
}

func writeBatch(ctx context.Context, writer adapter.LineProtocolWriter, plan plan, body []byte) error {
	request := adapter.WriteRequest{
		Database:        plan.options.Database,
		RetentionPolicy: plan.options.RetentionPolicy,
		Precision:       plan.precision,
		Body:            append([]byte(nil), body...),
	}
	return writer.WriteLineProtocol(ctx, request)
}

func normalizePrecision(raw string) (string, error) {
	switch normalized := strings.ToLower(strings.TrimSpace(raw)); normalized {
	case "", "rfc3339", "rfc3339nano", "n", "ns":
		return "ns", nil
	case "u", "us":
		return "us", nil
	case "ms", "s", "m", "h":
		return normalized, nil
	default:
		return "", fmt.Errorf("unsupported write precision %q", raw)
	}
}

func encodeTimestamp(timestamp time.Time, precision string) int64 {
	switch precision {
	case "h":
		return timestamp.Unix() / 3600
	case "m":
		return timestamp.Unix() / 60
	case "s":
		return timestamp.Unix()
	case "ms":
		return timestamp.UnixNano() / int64(time.Millisecond)
	case "us":
		return timestamp.UnixNano() / int64(time.Microsecond)
	default:
		return timestamp.UnixNano()
	}
}

func formatLine(measurement string, tags []pair, fields []field, timestamp int64) string {
	var builder strings.Builder
	builder.WriteString(escapeMeasurement(measurement))
	for _, tag := range tags {
		builder.WriteByte(',')
		builder.WriteString(escapeKey(tag.key))
		builder.WriteByte('=')
		builder.WriteString(escapeKey(tag.value))
	}
	builder.WriteByte(' ')
	for i, field := range fields {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(escapeKey(field.key))
		builder.WriteByte('=')
		builder.WriteString(formatFieldValue(field.value))
	}
	builder.WriteByte(' ')
	builder.WriteString(strconv.FormatInt(timestamp, 10))
	return builder.String()
}

func formatFieldValue(value any) string {
	switch typed := value.(type) {
	case float64:
		return strconv.FormatFloat(typed, 'f', 3, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', 3, 32)
	case int:
		return strconv.Itoa(typed) + "i"
	case int64:
		return strconv.FormatInt(typed, 10) + "i"
	case string:
		escaped := strings.ReplaceAll(typed, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `"`, `\"`)
		return `"` + escaped + `"`
	case bool:
		return strconv.FormatBool(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func escapeMeasurement(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, ",", `\,`)
	value = strings.ReplaceAll(value, " ", `\ `)
	return value
}

func escapeKey(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, ",", `\,`)
	value = strings.ReplaceAll(value, "=", `\=`)
	value = strings.ReplaceAll(value, " ", `\ `)
	return value
}

func hostName(index int64) string {
	return fmt.Sprintf("host-%04d", index)
}

func pidName(index int64) string {
	return fmt.Sprintf("pid-%08d", index)
}

func regionName(index int64) string {
	regions := []string{"us-east", "us-west", "eu-central", "ap-south"}
	return regions[index%int64(len(regions))]
}

func defaultMeasurement(dataset Dataset) string {
	switch dataset {
	case DatasetDemoCPU:
		return "demo_cpu"
	case DatasetHighCardinality:
		return "high_cardinality"
	case DatasetOutOfOrder:
		return "out_of_order_cpu"
	case DatasetCoveringBlock:
		return "covering_block_cpu"
	default:
		return "generated"
	}
}

func datasetList() string {
	names := Datasets()
	values := make([]string, 0, len(names))
	for _, dataset := range names {
		values = append(values, string(dataset))
	}
	return strings.Join(values, ", ")
}
