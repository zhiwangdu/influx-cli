package influxdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

type queryResponse struct {
	Results []statementResponse `json:"results"`
	Error   string              `json:"error"`
}

type statementResponse struct {
	StatementID int              `json:"statement_id"`
	Series      []seriesResponse `json:"series"`
	Error       string           `json:"error"`
}

type seriesResponse struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags"`
	Columns []string          `json:"columns"`
	Values  [][]any           `json:"values"`
}

func NormalizeResponse(body []byte, precision, source string) (result.Result, error) {
	response, err := decodeQueryResponse(body)
	if err != nil {
		return result.Result{}, err
	}

	table := buildTable(response.Results)
	series := buildSeries(response.Results, precision)
	metadata := result.Metadata{
		StatementCount: len(response.Results),
		RowCount:       table.RowCount(),
		PointCount:     countPoints(series),
		SeriesCount:    len(series),
		Source:         source,
	}

	kind := result.KindTable
	if len(series) > 0 {
		kind = result.KindSeries
	}
	if table.RowCount() == 0 && len(series) == 0 {
		kind = result.KindEmpty
	}

	return result.Result{
		Kind:     kind,
		Table:    table,
		Series:   series,
		Metadata: metadata,
	}, nil
}

func decodeQueryResponse(body []byte) (queryResponse, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()

	var response queryResponse
	if err := decoder.Decode(&response); err != nil {
		return queryResponse{}, fmt.Errorf("decode InfluxDB response: %w", err)
	}
	if response.Error != "" {
		return queryResponse{}, fmt.Errorf("influxdb query error: %s", response.Error)
	}

	for _, statement := range response.Results {
		if statement.Error != "" {
			return queryResponse{}, fmt.Errorf("influxdb statement %d error: %s", statement.StatementID, statement.Error)
		}
	}
	return response, nil
}

func buildTable(statements []statementResponse) *result.Table {
	includeStatement := len(statements) > 1
	includeSeries := false
	tagKeys := map[string]struct{}{}
	totalSeries := 0
	for _, statement := range statements {
		totalSeries += len(statement.Series)
		for _, series := range statement.Series {
			if len(series.Tags) > 0 {
				includeSeries = true
			}
			for key := range series.Tags {
				tagKeys[key] = struct{}{}
			}
		}
	}
	if totalSeries > 1 {
		includeSeries = true
	}

	sortedTagKeys := sortedKeys(tagKeys)
	columnSet := map[string]struct{}{}
	var columns []string
	addColumn := func(name string) {
		if _, ok := columnSet[name]; ok {
			return
		}
		columnSet[name] = struct{}{}
		columns = append(columns, name)
	}

	if includeStatement {
		addColumn("statement")
	}
	if includeSeries {
		addColumn("measurement")
	}
	for _, key := range sortedTagKeys {
		addColumn(key)
	}
	for _, statement := range statements {
		for _, series := range statement.Series {
			for _, column := range series.Columns {
				addColumn(column)
			}
		}
	}

	table := result.NewTable(columns)
	for _, statement := range statements {
		for _, series := range statement.Series {
			for _, values := range series.Values {
				rowMap := map[string]any{}
				if includeStatement {
					rowMap["statement"] = statement.StatementID
				}
				if includeSeries {
					rowMap["measurement"] = series.Name
				}
				for _, key := range sortedTagKeys {
					if value, ok := series.Tags[key]; ok {
						rowMap[key] = value
					}
				}
				for i, column := range series.Columns {
					if i < len(values) {
						rowMap[column] = values[i]
					}
				}
				row := make([]any, len(columns))
				for i, column := range columns {
					row[i] = rowMap[column]
				}
				table.AddRow(row...)
			}
		}
	}
	return table
}

func buildSeries(statements []statementResponse, precision string) []result.Series {
	var out []result.Series
	for _, statement := range statements {
		for _, responseSeries := range statement.Series {
			timeIndex := columnIndex(responseSeries.Columns, "time")
			if timeIndex < 0 {
				continue
			}
			for valueIndex, column := range responseSeries.Columns {
				if valueIndex == timeIndex {
					continue
				}
				points := make([]result.Point, 0, len(responseSeries.Values))
				for _, row := range responseSeries.Values {
					if timeIndex >= len(row) || valueIndex >= len(row) {
						continue
					}
					timestamp, ok := parseTimeValue(row[timeIndex], precision)
					if !ok {
						continue
					}
					value, ok := numericValue(row[valueIndex])
					if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
						continue
					}
					points = append(points, result.Point{Time: timestamp, Value: value})
				}
				if len(points) == 0 {
					continue
				}
				out = append(out, result.Series{
					Name:   seriesName(responseSeries.Name, column),
					Tags:   copyTags(responseSeries.Tags),
					Points: points,
				})
			}
		}
	}
	return out
}

func columnIndex(columns []string, target string) int {
	for i, column := range columns {
		if strings.EqualFold(column, target) {
			return i
		}
	}
	return -1
}

func parseTimeValue(value any, precision string) (time.Time, bool) {
	switch typed := value.(type) {
	case string:
		timestamp, err := time.Parse(time.RFC3339Nano, typed)
		return timestamp, err == nil
	case json.Number:
		return parseEpoch(typed.String(), precision)
	case float64:
		return parseEpoch(strconv.FormatFloat(typed, 'f', -1, 64), precision)
	case int64:
		return epochToTime(typed, precision), true
	case int:
		return epochToTime(int64(typed), precision), true
	default:
		return time.Time{}, false
	}
}

func parseEpoch(raw, precision string) (time.Time, bool) {
	if strings.ContainsAny(raw, ".eE") {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return time.Time{}, false
		}
		return epochFloatToTime(parsed, precision), true
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return epochToTime(parsed, precision), true
}

func epochFloatToTime(value float64, precision string) time.Time {
	switch strings.ToLower(precision) {
	case "h":
		return time.Unix(0, int64(math.Round(value*float64(time.Hour)))).UTC()
	case "m":
		return time.Unix(0, int64(math.Round(value*float64(time.Minute)))).UTC()
	case "s":
		return time.Unix(0, int64(math.Round(value*float64(time.Second)))).UTC()
	case "ms":
		return time.Unix(0, int64(math.Round(value*float64(time.Millisecond)))).UTC()
	case "u", "us":
		return time.Unix(0, int64(math.Round(value*float64(time.Microsecond)))).UTC()
	case "ns":
		return time.Unix(0, int64(math.Round(value))).UTC()
	default:
		return time.Unix(0, int64(math.Round(value))).UTC()
	}
}

func epochToTime(value int64, precision string) time.Time {
	switch strings.ToLower(precision) {
	case "h":
		return time.Unix(value*3600, 0).UTC()
	case "m":
		return time.Unix(value*60, 0).UTC()
	case "s":
		return time.Unix(value, 0).UTC()
	case "ms":
		return time.Unix(0, value*int64(time.Millisecond)).UTC()
	case "u", "us":
		return time.Unix(0, value*int64(time.Microsecond)).UTC()
	case "ns":
		return time.Unix(0, value).UTC()
	default:
		return time.Unix(0, value).UTC()
	}
}

func numericValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case float64:
		return typed, true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case nil:
		return 0, false
	default:
		return 0, false
	}
}

func seriesName(measurement, column string) string {
	if measurement == "" {
		return column
	}
	if column == "value" || column == "mean" {
		return measurement
	}
	return measurement + "." + column
}

func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for key, value := range tags {
		out[key] = value
	}
	return out
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func countPoints(series []result.Series) int {
	total := 0
	for _, item := range series {
		total += len(item.Points)
	}
	return total
}
