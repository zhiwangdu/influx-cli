package influxdb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zhiwangdu/influx-cli/internal/adapter"
)

func TestWriteLineProtocolPostsToWriteEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/write" {
			t.Fatalf("path = %q, want /write", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if got, want := r.URL.Query().Get("db"), "metrics"; got != want {
			t.Fatalf("db = %q, want %q", got, want)
		}
		if got, want := r.URL.Query().Get("rp"), "autogen"; got != want {
			t.Fatalf("rp = %q, want %q", got, want)
		}
		if got, want := r.URL.Query().Get("precision"), "n"; got != want {
			t.Fatalf("precision = %q, want %q", got, want)
		}
		user, password, ok := r.BasicAuth()
		if !ok || user != "u" || password != "p" {
			t.Fatalf("basic auth = %q/%q/%v, want u/p/true", user, password, ok)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(body), "cpu value=1 42"; got != want {
			t.Fatalf("body = %q, want %q", got, want)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	adapter, err := New(Config{
		URL:      server.URL,
		Username: "u",
		Password: "p",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.WriteLineProtocol(context.Background(), adapterpkgWriteRequest("metrics", "autogen", "ns", []byte("cpu value=1 42\n")))
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteLineProtocolUsesConfiguredDefaults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("db"), "defaultdb"; got != want {
			t.Fatalf("db = %q, want %q", got, want)
		}
		if got, want := r.URL.Query().Get("rp"), "raw"; got != want {
			t.Fatalf("rp = %q, want %q", got, want)
		}
		if precision := r.URL.Query().Get("precision"); precision != "" {
			t.Fatalf("precision = %q, want omitted for rfc3339", precision)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	adapter, err := New(Config{
		URL:             server.URL,
		Database:        "defaultdb",
		RetentionPolicy: "raw",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.WriteLineProtocol(context.Background(), adapterpkgWriteRequest("", "", "rfc3339", []byte("cpu value=1 42")))
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteLineProtocolReturnsHTTPStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad line protocol", http.StatusBadRequest)
	}))
	defer server.Close()

	adapter, err := New(Config{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.WriteLineProtocol(context.Background(), adapterpkgWriteRequest("metrics", "", "ns", []byte("bad")))
	if err == nil {
		t.Fatal("expected write error")
	}
	if !strings.Contains(err.Error(), "bad line protocol") {
		t.Fatalf("error = %q, want response body", err)
	}
}

func adapterpkgWriteRequest(database, rp, precision string, body []byte) adapter.WriteRequest {
	return adapter.WriteRequest{
		Database:        database,
		RetentionPolicy: rp,
		Precision:       precision,
		Body:            body,
	}
}
