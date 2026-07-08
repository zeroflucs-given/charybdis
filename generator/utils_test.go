package generator

import (
	"strings"
	"testing"
)

func TestRedact(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		password string
	}{
		{
			name:     "simple password",
			input:    `ALTER ROLE some_role WITH PASSWORD = 'hunter2'`,
			password: "hunter2",
		},
		{
			name:     "case insensitive keyword",
			input:    `ALTER ROLE some_role WITH password = 'hunter2'`,
			password: "hunter2",
		},
		{
			name:     "trailing clause after password is preserved",
			input:    `ALTER ROLE some_role WITH PASSWORD = 'hunter2' AND LOGIN = true`,
			password: "hunter2",
		},
		{
			name:     "escaped single quote in password",
			input:    `ALTER ROLE some_role WITH PASSWORD = 'hunt''er2'`,
			password: "hunt''er2",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Redact(tc.input)

			if strings.Contains(got, tc.password) {
				t.Fatalf("redacted output still contains the password: %q", got)
			}
			if !strings.Contains(got, "<redacted>") {
				t.Fatalf("redacted output missing <redacted> marker: %q", got)
			}
		})
	}
}

func TestRedactPreservesTrailingClause(t *testing.T) {
	input := `ALTER ROLE some_role WITH PASSWORD = 'hunter2' AND LOGIN = true`
	got := Redact(input)

	if !strings.HasSuffix(got, "AND LOGIN = true") {
		t.Fatalf("expected trailing clause to survive redaction, got: %q", got)
	}
}
