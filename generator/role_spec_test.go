package generator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/metadata"
)

func TestCreateDDLForRole(t *testing.T) {
	tests := []struct {
		name     string
		username string
		options  []RoleOption
		want     []OpTest
		wantErr  error
	}{
		{
			name:     "basic add user",
			username: "foo",
			options:  nil,
			want: []OpTest{
				ExactMatchOpTest(metadata.DDLOperation{
					Description: "Create the role \"foo\" if it doesn't already exist",
					Command:     "CREATE ROLE IF NOT EXISTS foo",
				}),
			},
			wantErr: nil,
		},
		{
			name:     "empty user",
			username: "",
			options:  nil,
			want:     nil,
			wantErr:  ErrInvalidInput,
		},
		{
			name:     "with a password",
			username: "bar",
			options: []RoleOption{
				WithRolePassword("test-password"),
			},
			want: []OpTest{
				ExactMatchOpTest(metadata.DDLOperation{
					Description: "Create the role \"bar\" if it doesn't already exist",
					Command:     "CREATE ROLE IF NOT EXISTS bar",
				}),
				CommandMatchRegExOpTest(`^ALTER ROLE bar WITH PASSWORD = '.+'$`),
			},
			wantErr: nil,
		},
		{
			name:     "empty password",
			username: "baz",
			options: []RoleOption{
				WithRolePassword(""),
			},
			want:    nil,
			wantErr: ErrInvalidInput,
		},
		{
			name:     "password with single quotes",
			username: "gir",
			options: []RoleOption{
				WithRolePassword("don't"),
			},
			want: []OpTest{
				CommandMatchOpTest("CREATE ROLE IF NOT EXISTS gir"),
				CommandMatchRegExOpTest(`^ALTER ROLE gir WITH PASSWORD = '.+'$`),
			},
		},
		{
			name:     "service level",
			username: "gir",
			options: []RoleOption{
				WithRoleServiceLevel("batch"),
			},
			want: []OpTest{
				ExactMatchOpTest(metadata.DDLOperation{
					Description: `Create the role "gir" if it doesn't already exist`,
					Command:     "CREATE ROLE IF NOT EXISTS gir",
				}),
				CommandMatchOpTest("ATTACH SERVICE LEVEL 'batch' TO gir"),
			},
		},
		{
			name:     "all options",
			username: "gaz",
			options: []RoleOption{
				WithRolePassword("test-password"),
				WithRoleIsSuperuser(true),
				WithRoleIsLogin(true),
				WithRoleServiceLevel("foo"),
			},
			want: []OpTest{
				ExactMatchOpTest(metadata.DDLOperation{
					Description: "Create the role \"gaz\" if it doesn't already exist",
					Command:     "CREATE ROLE IF NOT EXISTS gaz",
				}),
				CommandMatchRegExOpTest(`^ALTER ROLE gaz WITH PASSWORD = '.+'$`),
				ExactMatchOpTest(metadata.DDLOperation{
					Description: "Set superuser permissions for \"gaz\"",
					Command:     "ALTER ROLE gaz WITH SUPERUSER = true",
				}),
				ExactMatchOpTest(metadata.DDLOperation{
					Description: "Set login permissions for \"gaz\"",
					Command:     "ALTER ROLE gaz WITH LOGIN = true",
				}),
				CommandMatchOpTest("ATTACH SERVICE LEVEL 'foo' TO gaz"),
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateDDLForRole(tt.username, tt.options...)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.Len(t, got, len(tt.want), "expected the number of commands returned to match the number of tests")
			for idx, testDDL := range tt.want {
				assert.NoError(t, testDDL(got[idx]))
			}

			t.Logf("statements: %v", got)
		})
	}
}
