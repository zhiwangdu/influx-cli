package influxdb

import (
	"reflect"
	"testing"
	"time"

	"github.com/zhiwangdu/influx-cli/internal/result"
)

func TestNormalizeShowDatabasesAsTable(t *testing.T) {
	body := []byte(`{
		"results": [
			{
				"statement_id": 0,
				"series": [
					{"name": "databases", "columns": ["name"], "values": [["metrics"], ["telegraf"]]}
				]
			}
		]
	}`)

	res, err := NormalizeResponse(body, "rfc3339", "influxdb")
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != result.KindTable {
		t.Fatalf("kind = %s, want table", res.Kind)
	}
	if !reflect.DeepEqual(res.Table.Columns, []string{"name"}) {
		t.Fatalf("columns = %#v", res.Table.Columns)
	}
	if res.Table.RowCount() != 2 {
		t.Fatalf("rows = %d, want 2", res.Table.RowCount())
	}
	if len(res.Series) != 0 {
		t.Fatalf("series = %d, want 0", len(res.Series))
	}
}

func TestNormalizeSelectBuildsSeriesAndTable(t *testing.T) {
	body := []byte(`{
		"results": [
			{
				"statement_id": 0,
				"series": [
					{
						"name": "cpu",
						"tags": {"host": "a"},
						"columns": ["time", "mean"],
						"values": [
							["2026-07-04T00:00:00Z", 1.5],
							["2026-07-04T00:01:00Z", 2.5],
							["2026-07-04T00:02:00Z", null]
						]
					}
				]
			}
		]
	}`)

	res, err := NormalizeResponse(body, "rfc3339", "influxdb")
	if err != nil {
		t.Fatal(err)
	}
	if res.Kind != result.KindSeries {
		t.Fatalf("kind = %s, want series", res.Kind)
	}
	if res.Table.RowCount() != 3 {
		t.Fatalf("table rows = %d, want 3", res.Table.RowCount())
	}
	if len(res.Series) != 1 {
		t.Fatalf("series = %d, want 1", len(res.Series))
	}
	series := res.Series[0]
	if series.Name != "cpu" {
		t.Fatalf("series name = %q, want cpu", series.Name)
	}
	if series.Tags["host"] != "a" {
		t.Fatalf("series tags = %#v", series.Tags)
	}
	if len(series.Points) != 2 {
		t.Fatalf("points = %d, want 2", len(series.Points))
	}
	if series.Points[1].Value != 2.5 {
		t.Fatalf("point value = %v, want 2.5", series.Points[1].Value)
	}
}

func TestNormalizeMultipleUntaggedSeriesKeepsMeasurementColumn(t *testing.T) {
	body := []byte(`{
		"results": [
			{
				"statement_id": 0,
				"series": [
					{"name": "cpu", "columns": ["time", "value"], "values": [["2026-07-04T00:00:00Z", 1]]},
					{"name": "mem", "columns": ["time", "value"], "values": [["2026-07-04T00:00:00Z", 2]]}
				]
			}
		]
	}`)

	res, err := NormalizeResponse(body, "rfc3339", "influxdb")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Table.Columns, []string{"measurement", "time", "value"}) {
		t.Fatalf("columns = %#v", res.Table.Columns)
	}
	if res.Table.Rows[0][0] != "cpu" || res.Table.Rows[1][0] != "mem" {
		t.Fatalf("measurement values not preserved: %#v", res.Table.Rows)
	}
}

func TestNormalizeNumericEpochTime(t *testing.T) {
	body := []byte(`{
		"results": [
			{
				"statement_id": 0,
				"series": [
					{"name": "cpu", "columns": ["time", "value"], "values": [[1000000000, 42]]}
				]
			}
		]
	}`)

	res, err := NormalizeResponse(body, "ns", "influxdb")
	if err != nil {
		t.Fatal(err)
	}
	got := res.Series[0].Points[0].Time
	want := time.Unix(1, 0).UTC()
	if !got.Equal(want) {
		t.Fatalf("time = %s, want %s", got, want)
	}
}

func TestNormalizeScientificAndFractionalEpochTime(t *testing.T) {
	body := []byte(`{
		"results": [
			{
				"statement_id": 0,
				"series": [
					{"name": "cpu", "columns": ["time", "value"], "values": [[1e9, 42], [1.5, 43]]}
				]
			}
		]
	}`)

	res, err := NormalizeResponse(body, "s", "influxdb")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Series[0].Points) != 2 {
		t.Fatalf("points = %d, want 2", len(res.Series[0].Points))
	}
	if got, want := res.Series[0].Points[0].Time, time.Unix(1000000000, 0).UTC(); !got.Equal(want) {
		t.Fatalf("scientific time = %s, want %s", got, want)
	}
	if got, want := res.Series[0].Points[1].Time, time.Unix(1, 500000000).UTC(); !got.Equal(want) {
		t.Fatalf("fractional time = %s, want %s", got, want)
	}
}

func TestNormalizeReturnsStatementError(t *testing.T) {
	body := []byte(`{"results":[{"statement_id":0,"error":"syntax error"}]}`)
	_, err := NormalizeResponse(body, "rfc3339", "influxdb")
	if err == nil {
		t.Fatal("expected statement error")
	}
}
