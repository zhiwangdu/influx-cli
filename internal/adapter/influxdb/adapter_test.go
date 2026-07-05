package influxdb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
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
