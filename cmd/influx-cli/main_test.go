package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootCommandStartsREPLByDefault(t *testing.T) {
	clearConnectionEnv(t)

	var stdout bytes.Buffer
	command := newRootCommand()
	command.SetIn(strings.NewReader(":q\n"))
	command.SetOut(&stdout)
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", filepath.Join(t.TempDir(), "missing.yaml"),
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	output := stdout.String()
	if !strings.Contains(output, "Enter run") {
		t.Fatalf("default root command did not start REPL:\n%s", output)
	}
}

func TestRootCommandStartsREPLWithConnectionFlags(t *testing.T) {
	clearConnectionEnv(t)

	var stdout bytes.Buffer
	command := newRootCommand()
	command.SetIn(strings.NewReader(":q\n"))
	command.SetOut(&stdout)
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", filepath.Join(t.TempDir(), "missing.yaml"),
		"--host", "db.example.com",
		"--port", "443",
		"--ssl",
		"--unsafeSsl",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	output := stdout.String()
	if !strings.Contains(output, "Enter run") {
		t.Fatalf("root command with connection flags did not start REPL:\n%s", output)
	}
}

func TestExplicitREPLCommandStillStartsREPL(t *testing.T) {
	clearConnectionEnv(t)

	var stdout bytes.Buffer
	command := newRootCommand()
	command.SetIn(strings.NewReader(":q\n"))
	command.SetOut(&stdout)
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", filepath.Join(t.TempDir(), "missing.yaml"),
		"repl",
	})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}

	output := stdout.String()
	if !strings.Contains(output, "Enter run") {
		t.Fatalf("explicit repl command did not start REPL:\n%s", output)
	}
}

func TestRootCommandRejectsPositionalArgs(t *testing.T) {
	clearConnectionEnv(t)

	var stdout bytes.Buffer
	command := newRootCommand()
	command.SetIn(strings.NewReader(":q\n"))
	command.SetOut(&stdout)
	command.SetErr(&bytes.Buffer{})
	command.SetArgs([]string{
		"--config", filepath.Join(t.TempDir(), "missing.yaml"),
		"SHOW DATABASES",
	})

	err := command.Execute()
	if err == nil {
		t.Fatal("expected positional argument error")
	}
	if strings.Contains(stdout.String(), "Enter run") {
		t.Fatalf("root command should not start REPL when positional args are provided:\n%s", stdout.String())
	}
}
