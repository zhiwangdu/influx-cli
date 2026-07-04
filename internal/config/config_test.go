package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveMergesProfileEnvAndOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := []byte(`
profiles:
  local:
    adapter: influxdb
    url: http://127.0.0.1:8086
    username: local-user
    database: localdb
    retention_policy: autogen
  prod:
    adapter: opengemini
    url: http://prod.example:8086
    username: prod-user
    password: prod-secret
    token: prod-token
    database: proddb
    retention_policy: raw
    precision: ns
defaults:
  profile: local
  render: table
  timeout: 5s
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}

	env := mapEnv(map[string]string{
		"INFLUX_CLI_PROFILE":  "prod",
		"INFLUX_CLI_URL":      "http://env.example:8086",
		"INFLUX_CLI_PASSWORD": "env-secret",
	})

	effective, err := Resolve(path, Overrides{
		Database:        "override_db",
		RetentionPolicy: "override_rp",
		Render:          "sparkline",
		Timeout:         "12s",
	}, env)
	if err != nil {
		t.Fatal(err)
	}

	if effective.Profile != "prod" {
		t.Fatalf("profile = %q, want prod", effective.Profile)
	}
	if effective.Adapter != "opengemini" {
		t.Fatalf("adapter = %q, want opengemini", effective.Adapter)
	}
	if effective.URL != "http://env.example:8086" {
		t.Fatalf("url = %q, want env override", effective.URL)
	}
	if effective.Password != "env-secret" {
		t.Fatalf("password was not overridden by env")
	}
	if effective.Database != "override_db" || effective.RetentionPolicy != "override_rp" {
		t.Fatalf("db/rp overrides not applied: %q/%q", effective.Database, effective.RetentionPolicy)
	}
	if effective.Render != "sparkline" {
		t.Fatalf("render = %q, want sparkline", effective.Render)
	}
	if effective.Timeout.String() != "12s" {
		t.Fatalf("timeout = %s, want 12s", effective.Timeout)
	}

	redacted := strings.Join(effective.RedactedLines(), "\n")
	if strings.Contains(redacted, "env-secret") || strings.Contains(redacted, "prod-token") {
		t.Fatalf("redacted config leaked a secret:\n%s", redacted)
	}
}

func TestResolveUsesBuiltInDefaultsWhenConfigIsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	effective, err := Resolve(path, Overrides{}, mapEnv(nil))
	if err != nil {
		t.Fatal(err)
	}

	if effective.ConfigFound {
		t.Fatal("ConfigFound = true, want false")
	}
	if effective.Adapter != "influxdb" {
		t.Fatalf("adapter = %q, want influxdb", effective.Adapter)
	}
	if effective.URL != "http://127.0.0.1:8086" {
		t.Fatalf("url = %q, want local default", effective.URL)
	}
	if effective.Precision != "rfc3339" {
		t.Fatalf("precision = %q, want rfc3339", effective.Precision)
	}
	if effective.Timeout.String() != "10s" {
		t.Fatalf("timeout = %s, want 10s", effective.Timeout)
	}
}

func TestResolveErrorsForMissingProfileInExistingConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("profiles:\n  local:\n    url: http://localhost:8086\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Resolve(path, Overrides{Profile: "missing"}, mapEnv(nil))
	if err == nil {
		t.Fatal("expected missing profile error")
	}
}

func mapEnv(values map[string]string) EnvGetter {
	return func(key string) string {
		if values == nil {
			return ""
		}
		return values[key]
	}
}
