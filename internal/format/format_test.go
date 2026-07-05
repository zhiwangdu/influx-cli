package format

import "testing"

func TestNormalizeDefaultsToTable(t *testing.T) {
	format, err := Normalize("")
	if err != nil {
		t.Fatal(err)
	}
	if format != Table {
		t.Fatalf("format = %q, want table", format)
	}
}

func TestNormalizeAcceptsKnownFormats(t *testing.T) {
	tests := map[string]string{
		"auto":      Auto,
		"table":     Table,
		"sparkline": Sparkline,
		"json":      JSON,
		" Auto ":    Auto,
		"JSON":      JSON,
	}

	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			format, err := Normalize(input)
			if err != nil {
				t.Fatal(err)
			}
			if format != want {
				t.Fatalf("format = %q, want %q", format, want)
			}
		})
	}
}

func TestNormalizeRejectsUnknownFormat(t *testing.T) {
	if _, err := Normalize("wide"); err == nil {
		t.Fatal("expected unknown format error")
	}
}
