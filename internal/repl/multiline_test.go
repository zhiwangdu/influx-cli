package repl

import "testing"

func TestStatementBufferContinuesWithTrailingBackslashUntilSemicolon(t *testing.T) {
	var buffer statementBuffer
	if statement, ready := buffer.Add(`SELECT mean(value) FROM cpu \`); ready || statement != "" {
		t.Fatalf("first line ready=%v statement=%q, want pending", ready, statement)
	}
	statement, ready := buffer.Add("WHERE time > now() - 1h;")
	if !ready {
		t.Fatal("second line was not ready")
	}
	if got, want := normalizeStatement(statement), "SELECT mean(value) FROM cpu\nWHERE time > now() - 1h"; got != want {
		t.Fatalf("statement = %q, want %q", got, want)
	}
}

func TestStatementBufferContinuesIncompleteSelect(t *testing.T) {
	var buffer statementBuffer
	if _, ready := buffer.Add("SELECT mean(value)"); ready {
		t.Fatal("SELECT without FROM should be pending")
	}
	statement, ready := buffer.Add("FROM cpu;")
	if !ready {
		t.Fatal("statement should be ready after FROM and semicolon")
	}
	if got, want := normalizeStatement(statement), "SELECT mean(value)\nFROM cpu"; got != want {
		t.Fatalf("statement = %q, want %q", got, want)
	}
}

func TestStatementBufferIgnoresSemicolonInsideQuote(t *testing.T) {
	var buffer statementBuffer
	if _, ready := buffer.Add("SELECT 'not;done'"); ready {
		t.Fatal("SELECT without FROM should be pending even with quoted semicolon")
	}
	statement, ready := buffer.Add("FROM cpu;")
	if !ready {
		t.Fatal("statement should be ready")
	}
	if got, want := normalizeStatement(statement), "SELECT 'not;done'\nFROM cpu"; got != want {
		t.Fatalf("statement = %q, want %q", got, want)
	}
}

func TestStatementBufferContinuesFluxPipeline(t *testing.T) {
	var buffer statementBuffer
	if _, ready := buffer.Add(`from(bucket: "metrics") |> `); ready {
		t.Fatal("flux pipeline should be pending")
	}
	statement, ready := buffer.Add(`range(start: -1h);`)
	if !ready {
		t.Fatal("statement should be ready")
	}
	if got, want := normalizeStatement(statement), "from(bucket: \"metrics\") |>\nrange(start: -1h)"; got != want {
		t.Fatalf("statement = %q, want %q", got, want)
	}
}

func TestStatementBufferRunsCompleteSingleLineWithoutSemicolon(t *testing.T) {
	var buffer statementBuffer
	statement, ready := buffer.Add("SELECT mean(value) FROM cpu")
	if !ready {
		t.Fatal("complete single-line query should run immediately")
	}
	if got, want := normalizeStatement(statement), "SELECT mean(value) FROM cpu"; got != want {
		t.Fatalf("statement = %q, want %q", got, want)
	}
}

func TestStatementBufferRunsTerminatedSelectWithoutFrom(t *testing.T) {
	var buffer statementBuffer
	statement, ready := buffer.Add("SELECT now();")
	if !ready {
		t.Fatal("terminated SELECT should run immediately")
	}
	if got, want := normalizeStatement(statement), "SELECT now()"; got != want {
		t.Fatalf("statement = %q, want %q", got, want)
	}
}
