package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigShowPrintsInfluxStyleConnectionFlags(t *testing.T) {
	clearConnectionEnv(t)

	var stdout bytes.Buffer
	command := newRootCommand()
	command.SetOut(&stdout)
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", filepath.Join(t.TempDir(), "missing.yaml"),
		"--host", "db.example.com",
		"--port", "443",
		"--ssl",
		"--unsafeSsl",
		"config", "show",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	output := stdout.String()
	for _, want := range []string{
		"host: db.example.com",
		"port: 443",
		"ssl: true",
		"unsafeSsl: true",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("config show missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "url:") {
		t.Fatalf("config show should not include url:\n%s", output)
	}
}

func TestConfigShowCanOverrideSSLFalse(t *testing.T) {
	clearConnectionEnv(t)

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(`
profiles:
  local:
    host: db.example.com
    ssl: true
defaults:
  profile: local
`)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	command := newRootCommand()
	command.SetOut(&stdout)
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", path,
		"--ssl=false",
		"config", "show",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(stdout.String(), "ssl: false") {
		t.Fatalf("config show should include ssl false override:\n%s", stdout.String())
	}
}

func TestConfigShowPreservesProfilePortWhenPortFlagOmitted(t *testing.T) {
	clearConnectionEnv(t)

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(`
profiles:
  local:
    host: db.example.com
    port: 443
defaults:
  profile: local
`)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	command := newRootCommand()
	command.SetOut(&stdout)
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", path,
		"config", "show",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(stdout.String(), "port: 443") {
		t.Fatalf("config show should preserve profile port:\n%s", stdout.String())
	}
}

func TestConfigShowPortFlagOverridesProfileAndEnv(t *testing.T) {
	clearConnectionEnv(t)
	t.Setenv("INFLUX_CLI_PORT", "8443")

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(`
profiles:
  local:
    host: db.example.com
    port: 443
defaults:
  profile: local
`)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	command := newRootCommand()
	command.SetOut(&stdout)
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", path,
		"--port", "9443",
		"config", "show",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(stdout.String(), "port: 9443") {
		t.Fatalf("config show should use CLI port override:\n%s", stdout.String())
	}
}

func TestConfigShowErrorsForUnsafeSSLWithoutSSL(t *testing.T) {
	clearConnectionEnv(t)

	command := newRootCommand()
	command.SetOut(&bytes.Buffer{})
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", filepath.Join(t.TempDir(), "missing.yaml"),
		"--unsafeSsl",
		"config", "show",
	})

	err := command.Execute()
	if err == nil {
		t.Fatal("expected unsafeSsl without ssl error")
	}
	if !strings.Contains(err.Error(), "unsafeSsl requires ssl") {
		t.Fatalf("error = %q, want unsafeSsl requires ssl", err)
	}
}

func TestURLFlagIsRemoved(t *testing.T) {
	clearConnectionEnv(t)

	command := newRootCommand()
	command.SetOut(&bytes.Buffer{})
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", filepath.Join(t.TempDir(), "missing.yaml"),
		"--url", "http://localhost:8086",
		"config", "show",
	})

	err := command.Execute()
	if err == nil {
		t.Fatal("expected unknown --url flag error")
	}
	if !strings.Contains(err.Error(), "unknown flag: --url") {
		t.Fatalf("error = %q, want unknown --url flag", err)
	}
}

func clearConnectionEnv(t *testing.T) {
	t.Helper()
	t.Setenv("INFLUX_CLI_PROFILE", "")
	t.Setenv("INFLUX_CLI_HOST", "")
	t.Setenv("INFLUX_CLI_PORT", "")
	t.Setenv("INFLUX_CLI_SSL", "")
	t.Setenv("INFLUX_CLI_UNSAFE_SSL", "")
	t.Setenv("INFLUX_CLI_URL", "")
}
