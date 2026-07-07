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
    host: local.example
    port: 8086
    username: local-user
    database: localdb
    retention_policy: autogen
  prod:
    adapter: opengemini
    host: prod.example
    port: 443
    ssl: true
    unsafeSsl: false
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
		"INFLUX_CLI_HOST":     "env.example",
		"INFLUX_CLI_PORT":     "8443",
		"INFLUX_CLI_SSL":      "true",
		"INFLUX_CLI_PASSWORD": "env-secret",
	})

	effective, err := Resolve(path, Overrides{
		Host:            "override.example",
		Port:            IntOverride{Value: 9443, Set: true},
		SSL:             BoolOverride{Value: false, Set: true},
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
	if effective.Host != "override.example" || effective.Port != 9443 {
		t.Fatalf("host/port = %q/%d, want override.example/9443", effective.Host, effective.Port)
	}
	if effective.SSL {
		t.Fatal("ssl = true, want CLI override false")
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
	for _, want := range []string{"host: override.example", "port: 9443", "ssl: false", "unsafeSsl: false"} {
		if !strings.Contains(redacted, want) {
			t.Fatalf("redacted config missing %q:\n%s", want, redacted)
		}
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
	if effective.Host != "localhost" {
		t.Fatalf("host = %q, want localhost", effective.Host)
	}
	if effective.Port != 8086 {
		t.Fatalf("port = %d, want 8086", effective.Port)
	}
	if effective.SSL || effective.UnsafeSSL {
		t.Fatalf("ssl/unsafeSsl = %v/%v, want false/false", effective.SSL, effective.UnsafeSSL)
	}
	if effective.Precision != "rfc3339" {
		t.Fatalf("precision = %q, want rfc3339", effective.Precision)
	}
	if effective.Render != "table" {
		t.Fatalf("render = %q, want table", effective.Render)
	}
	if effective.Timeout.String() != "10s" {
		t.Fatalf("timeout = %s, want 10s", effective.Timeout)
	}
}

func TestResolveAppliesConnectionEnvironment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	effective, err := Resolve(path, Overrides{}, mapEnv(map[string]string{
		"INFLUX_CLI_HOST":       "env.example",
		"INFLUX_CLI_PORT":       "443",
		"INFLUX_CLI_SSL":        "true",
		"INFLUX_CLI_UNSAFE_SSL": "true",
	}))
	if err != nil {
		t.Fatal(err)
	}

	if effective.Host != "env.example" {
		t.Fatalf("host = %q, want env.example", effective.Host)
	}
	if effective.Port != 443 {
		t.Fatalf("port = %d, want 443", effective.Port)
	}
	if !effective.SSL || !effective.UnsafeSSL {
		t.Fatalf("ssl/unsafeSsl = %v/%v, want true/true", effective.SSL, effective.UnsafeSSL)
	}
}

func TestResolveErrorsForMissingProfileInExistingConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("profiles:\n  local:\n    host: localhost\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Resolve(path, Overrides{Profile: "missing"}, mapEnv(nil))
	if err == nil {
		t.Fatal("expected missing profile error")
	}
}

func TestResolveErrorsForUnknownRenderFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := []byte(`
defaults:
  render: wide
`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Resolve(path, Overrides{}, mapEnv(nil))
	if err == nil {
		t.Fatal("expected unknown render format error")
	}
	if !strings.Contains(err.Error(), `unknown render format "wide"`) {
		t.Fatalf("error = %q, want unknown render format", err)
	}
}

func TestResolveErrorsForLegacyURLInConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("profiles:\n  local:\n    url: http://localhost:8086\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Resolve(path, Overrides{}, mapEnv(nil))
	if err == nil {
		t.Fatal("expected legacy url error")
	}
	if !strings.Contains(err.Error(), `uses removed field "url"`) {
		t.Fatalf("error = %q, want removed url field", err)
	}
}

func TestResolveErrorsForLegacyURLEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	_, err := Resolve(path, Overrides{}, mapEnv(map[string]string{
		"INFLUX_CLI_URL": "http://localhost:8086",
	}))
	if err == nil {
		t.Fatal("expected legacy INFLUX_CLI_URL error")
	}
	if !strings.Contains(err.Error(), "INFLUX_CLI_URL has been removed") {
		t.Fatalf("error = %q, want removed env error", err)
	}
}

func TestResolveErrorsForInvalidPortAndBoolEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	_, err := Resolve(path, Overrides{}, mapEnv(map[string]string{
		"INFLUX_CLI_PORT": "not-a-port",
	}))
	if err == nil || !strings.Contains(err.Error(), "parse INFLUX_CLI_PORT") {
		t.Fatalf("port error = %v, want parse INFLUX_CLI_PORT", err)
	}

	_, err = Resolve(path, Overrides{}, mapEnv(map[string]string{
		"INFLUX_CLI_SSL": "maybe",
	}))
	if err == nil || !strings.Contains(err.Error(), "parse INFLUX_CLI_SSL") {
		t.Fatalf("ssl error = %v, want parse INFLUX_CLI_SSL", err)
	}
}

func TestResolveErrorsForUnsafeSSLWithoutSSL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	_, err := Resolve(path, Overrides{
		UnsafeSSL: BoolOverride{Value: true, Set: true},
	}, mapEnv(nil))
	if err == nil {
		t.Fatal("expected unsafeSsl without ssl error")
	}
	if !strings.Contains(err.Error(), "unsafeSsl requires ssl") {
		t.Fatalf("error = %q, want unsafeSsl requires ssl", err)
	}
}

func TestResolveValidatesHostAndPort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	for _, tc := range []struct {
		name      string
		overrides Overrides
		want      string
	}{
		{
			name:      "scheme",
			overrides: Overrides{Host: "https://localhost"},
			want:      "scheme",
		},
		{
			name:      "embedded port",
			overrides: Overrides{Host: "localhost:8086"},
			want:      "use --port",
		},
		{
			name:      "bad port",
			overrides: Overrides{Port: IntOverride{Value: 0, Set: true}},
			want:      "port must be between",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Resolve(path, tc.overrides, mapEnv(nil))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestNormalizeHostSupportsIPv6AndRejectsPaths(t *testing.T) {
	for _, tc := range []struct {
		name string
		host string
		want string
	}{
		{name: "bare IPv6", host: "::1", want: "::1"},
		{name: "bracketed IPv6", host: "[::1]", want: "::1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeHost(tc.host)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("host = %q, want %q", got, tc.want)
			}
		})
	}

	if _, err := NormalizeHost("localhost/foo"); err == nil || !strings.Contains(err.Error(), "path") {
		t.Fatalf("path host error = %v, want path rejection", err)
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
