package rules

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeRules writes content to a rules file in a fresh temp dir and returns its path.
func writeRules(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "rules.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing rules file: %v", err)
	}
	return path
}

func TestLoad_Valid(t *testing.T) {
	path := writeRules(t, `
rules:
- method: GET
  path: /api/hello
  limit: 5
  unit: minute
  on_limit: drop
- method: POST
  path: /api/messages
  limit: 10
  unit: second
  on_limit: queue
- method: DELETE
  path: /api/messages
  limit: 1
  unit: hour
`)

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := []Rule{
		{Method: "GET", Path: "/api/hello", Limit: 5, Unit: "minute", OnLimit: "drop"},
		{Method: "POST", Path: "/api/messages", Limit: 10, Unit: "second", OnLimit: "queue"},
		// on_limit omitted: allowed, means drop.
		{Method: "DELETE", Path: "/api/messages", Limit: 1, Unit: "hour", OnLimit: ""},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rules, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("rule %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestLoad_Empty(t *testing.T) {
	path := writeRules(t, "rules: []\n")

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d rules, want 0", len(got))
	}
}

func TestLoad_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name:    "bad method",
			content: "rules:\n- {method: PUT, path: /a, limit: 1, unit: second, on_limit: drop}\n",
			wantErr: `rule 0: invalid method "PUT"`,
		},
		{
			name:    "missing path",
			content: "rules:\n- {method: GET, limit: 1, unit: second, on_limit: drop}\n",
			wantErr: "rule 0: path must not be empty",
		},
		{
			name:    "bad unit",
			content: "rules:\n- {method: GET, path: /a, limit: 1, unit: minuet, on_limit: drop}\n",
			wantErr: `rule 0: invalid unit "minuet"`,
		},
		{
			name:    "zero limit",
			content: "rules:\n- {method: GET, path: /a, limit: 0, unit: second, on_limit: drop}\n",
			wantErr: "rule 0: limit must be more than 0",
		},
		{
			name:    "missing limit",
			content: "rules:\n- {method: GET, path: /a, unit: second, on_limit: drop}\n",
			wantErr: "rule 0: limit must be more than 0",
		},
		{
			name:    "bad on_limit",
			content: "rules:\n- {method: GET, path: /a, limit: 1, unit: second, on_limit: maybe}\n",
			wantErr: `rule 0: invalid on limit value "maybe"`,
		},
		{
			name: "reports the failing rule index",
			content: "rules:\n" +
				"- {method: GET, path: /a, limit: 1, unit: second, on_limit: drop}\n" +
				"- {method: GET, path: /b, limit: -1, unit: second, on_limit: drop}\n",
			wantErr: "rule 1:",
		},
		{
			name:    "malformed yaml",
			content: "rules:\n- method: GET\n  path: [unclosed\n",
			wantErr: "failed parsing the yaml file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeRules(t, tt.content)

			_, err := Load(path)
			if err == nil {
				t.Fatal("err = nil, want an error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("err = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("err = nil, want an error")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("err = %v, want it to wrap a not-exist error", err)
	}
}

func TestRule_Window(t *testing.T) {
	tests := []struct {
		unit string
		want time.Duration
	}{
		{"second", time.Second},
		{"minute", time.Minute},
		{"hour", time.Hour},
		{"day", 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.unit, func(t *testing.T) {
			r := Rule{Unit: tt.unit}
			got, err := r.Window()
			if err != nil {
				t.Fatalf("Window: %v", err)
			}
			if got != tt.want {
				t.Errorf("Window = %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("unknown unit", func(t *testing.T) {
		r := Rule{Unit: "week"}
		if _, err := r.Window(); err == nil {
			t.Error("err = nil, want an error")
		}
	})
}
