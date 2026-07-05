package influxdb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/zhiwangdu/influx-cli/internal/schema"
)

func TestShowRetentionPoliciesPreservesDefaultFlag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			t.Fatalf("path = %q, want /query", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if got, want := r.Form.Get("q"), `SHOW RETENTION POLICIES ON "metrics"`; got != want {
			t.Fatalf("query = %q, want %q", got, want)
		}
		io.WriteString(w, `{
			"results": [
				{
					"statement_id": 0,
					"series": [
						{
							"name": "retention policies",
							"columns": ["name", "duration", "shardGroupDuration", "replicaN", "default"],
							"values": [
								["raw", "720h0m0s", "24h0m0s", 1, false],
								["autogen", "0s", "168h0m0s", 1, true]
							]
						}
					]
				}
			]
		}`)
	}))
	defer server.Close()

	adapter, err := New(Config{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	policies, err := adapter.ShowRetentionPolicies(context.Background(), "metrics")
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 2 {
		t.Fatalf("policies = %d, want 2", len(policies))
	}
	if policies[0].Name != "autogen" || policies[0].Duration != "0s" || policies[0].ShardGroupDuration != "168h0m0s" || policies[0].ReplicaN != "1" || !policies[0].Default {
		t.Fatalf("first policy = %#v, want autogen default", policies[0])
	}
	if policies[1].Name != "raw" || policies[1].Duration != "720h0m0s" || policies[1].ShardGroupDuration != "24h0m0s" || policies[1].ReplicaN != "1" || policies[1].Default {
		t.Fatalf("second policy = %#v, want raw non-default", policies[1])
	}
}

func TestShowMeasurementsUsesDatabaseAndRetentionPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			t.Fatalf("path = %q, want /query", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if got, want := r.Form.Get("q"), "SHOW MEASUREMENTS"; got != want {
			t.Fatalf("query = %q, want %q", got, want)
		}
		if got, want := r.Form.Get("db"), "metrics"; got != want {
			t.Fatalf("db = %q, want %q", got, want)
		}
		if got, want := r.Form.Get("rp"), "autogen"; got != want {
			t.Fatalf("rp = %q, want %q", got, want)
		}
		io.WriteString(w, `{
			"results": [
				{
					"statement_id": 0,
					"series": [
						{
							"name": "measurements",
							"columns": ["name"],
							"values": [["mem"], ["cpu"]]
						}
					]
				}
			]
		}`)
	}))
	defer server.Close()

	adapter, err := New(Config{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	measurements, err := adapter.ShowMeasurements(context.Background(), "metrics", "autogen")
	if err != nil {
		t.Fatal(err)
	}
	if len(measurements) != 2 || measurements[0] != "cpu" || measurements[1] != "mem" {
		t.Fatalf("measurements = %#v, want sorted cpu/mem", measurements)
	}
}

func TestGetSchemaUsesMeasurementScopedQueries(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			t.Fatalf("path = %q, want /query", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if got, want := r.Form.Get("db"), "metrics"; got != want {
			t.Fatalf("db = %q, want %q", got, want)
		}
		if got, want := r.Form.Get("rp"), "autogen"; got != want {
			t.Fatalf("rp = %q, want %q", got, want)
		}
		query := r.Form.Get("q")
		queries = append(queries, query)
		switch query {
		case `SHOW FIELD KEYS FROM "cpu"`:
			io.WriteString(w, `{
				"results": [
					{
						"statement_id": 0,
						"series": [
							{
								"name": "cpu",
								"columns": ["fieldKey", "fieldType"],
								"values": [["value", "float"], ["usage_idle", "float"]]
							}
						]
					}
				]
			}`)
		case `SHOW TAG KEYS FROM "cpu"`:
			io.WriteString(w, `{
				"results": [
					{
						"statement_id": 0,
						"series": [
							{
								"name": "cpu",
								"columns": ["tagKey"],
								"values": [["region"], ["host"]]
							}
						]
					}
				]
			}`)
		default:
			t.Fatalf("unexpected query %q", query)
		}
	}))
	defer server.Close()

	adapter, err := New(Config{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := adapter.GetSchema(context.Background(), schema.Scope{
		Database:        "metrics",
		RetentionPolicy: "autogen",
		Measurement:     "cpu",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(queries, []string{`SHOW FIELD KEYS FROM "cpu"`, `SHOW TAG KEYS FROM "cpu"`}) {
		t.Fatalf("queries = %#v", queries)
	}
	if len(snapshot.Measurements) != 1 || snapshot.Measurements[0].Name != "cpu" {
		t.Fatalf("measurements = %#v, want cpu", snapshot.Measurements)
	}
	if got, want := fieldNames(snapshot.Measurements[0]), []string{"usage_idle", "value"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fields = %#v, want %#v", got, want)
	}
	if got, want := tagNames(snapshot.Measurements[0]), []string{"host", "region"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tags = %#v, want %#v", got, want)
	}
}

func TestGetSchemaWithoutMeasurementUsesDatabaseWideQueries(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			t.Fatalf("path = %q, want /query", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		query := r.Form.Get("q")
		queries = append(queries, query)
		switch query {
		case "SHOW FIELD KEYS":
			io.WriteString(w, `{
				"results": [
					{
						"statement_id": 0,
						"series": [
							{
								"name": "mem",
								"columns": ["fieldKey", "fieldType"],
								"values": [["used_percent", "float"]]
							},
							{
								"name": "cpu",
								"columns": ["fieldKey", "fieldType"],
								"values": [["usage_idle", "float"], ["value", "float"]]
							}
						]
					}
				]
			}`)
		case "SHOW TAG KEYS":
			io.WriteString(w, `{
				"results": [
					{
						"statement_id": 0,
						"series": [
							{
								"name": "cpu",
								"columns": ["tagKey"],
								"values": [["host"], ["region"]]
							},
							{
								"name": "mem",
								"columns": ["tagKey"],
								"values": [["host"]]
							}
						]
					}
				]
			}`)
		default:
			t.Fatalf("unexpected query %q", query)
		}
	}))
	defer server.Close()

	adapter, err := New(Config{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := adapter.GetSchema(context.Background(), schema.Scope{
		Database:        "metrics",
		RetentionPolicy: "autogen",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(queries, []string{"SHOW FIELD KEYS", "SHOW TAG KEYS"}) {
		t.Fatalf("queries = %#v", queries)
	}
	if len(snapshot.Measurements) != 2 {
		t.Fatalf("measurements = %#v, want cpu and mem", snapshot.Measurements)
	}
	if snapshot.Measurements[0].Name != "cpu" || snapshot.Measurements[1].Name != "mem" {
		t.Fatalf("measurements not sorted: %#v", snapshot.Measurements)
	}
	if got, want := fieldNames(snapshot.Measurements[0]), []string{"usage_idle", "value"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cpu fields = %#v, want %#v", got, want)
	}
	if got, want := tagNames(snapshot.Measurements[0]), []string{"host", "region"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("cpu tags = %#v, want %#v", got, want)
	}
	if got, want := fieldNames(snapshot.Measurements[1]), []string{"used_percent"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mem fields = %#v, want %#v", got, want)
	}
	if got, want := tagNames(snapshot.Measurements[1]), []string{"host"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mem tags = %#v, want %#v", got, want)
	}
}

func fieldNames(measurement schema.Measurement) []string {
	names := make([]string, 0, len(measurement.Fields))
	for _, field := range measurement.Fields {
		names = append(names, field.Name)
	}
	return names
}

func tagNames(measurement schema.Measurement) []string {
	names := make([]string, 0, len(measurement.Tags))
	for _, tag := range measurement.Tags {
		names = append(names, tag.Name)
	}
	return names
}
