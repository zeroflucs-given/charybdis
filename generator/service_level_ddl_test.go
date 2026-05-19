package generator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetServiceLevelExistenceDDL(t *testing.T) {
	tests := []struct {
		name      string
		level     string
		operation string
		want      []OpTest
		wantErr   error
	}{
		{
			name:      "create",
			level:     "foo",
			operation: "create",
			want: []OpTest{
				CommandMatchOpTest("CREATE SERVICE LEVEL IF NOT EXISTS 'foo'"),
			},
			wantErr: nil,
		},
		{
			name:      "drop",
			level:     "foo",
			operation: "drop",
			want: []OpTest{
				CommandMatchOpTest("DROP SERVICE LEVEL IF EXISTS 'foo'"),
			},
			wantErr: nil,
		},
		{
			name:      "unknown command",
			level:     "foo",
			operation: "delete",
			wantErr:   ErrUnknownOperation,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SetServiceLevelExistenceDDL(tt.level, tt.operation)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.want), "expected the number of commands returned to match the number of tests")

			for idx, testDDL := range tt.want {
				assert.NoError(t, testDDL(got[idx]))
			}
		})
	}
}

func TestUpdateServiceLevelDDL(t *testing.T) {
	tests := []struct {
		name    string
		level   string
		options []ServiceLevelOption
		want    []OpTest
		wantErr error
	}{
		{
			name:  "new shares",
			level: "foo",
			options: []ServiceLevelOption{
				WithShares(777),
			},
			want: []OpTest{
				CommandMatchOpTest("ALTER SERVICE LEVEL 'foo' WITH SHARES = 777"),
			},
			wantErr: nil,
		},
		{
			name:  "quotes",
			level: "foo's friend",
			options: []ServiceLevelOption{
				WithShares(900),
			},
			want: []OpTest{
				CommandMatchOpTest("ALTER SERVICE LEVEL 'foo''s friend' WITH SHARES = 900"),
			},
			wantErr: nil,
		},
		{
			name:  "too many shares",
			level: "foo",
			options: []ServiceLevelOption{
				WithShares(1777),
			},
			wantErr: ErrOutOfRange,
		},
		{
			name:  "default shares",
			level: "foo",
			options: []ServiceLevelOption{
				WithShares(0),
			},
			want: []OpTest{
				CommandMatchOpTest("ALTER SERVICE LEVEL 'foo' WITH SHARES = 1000"),
			},
			wantErr: nil,
		},
		{
			name:  "new timeout",
			level: "bar",
			options: []ServiceLevelOption{
				WithTimeout(10 * time.Second),
			},
			want: []OpTest{
				CommandMatchOpTest("ALTER SERVICE LEVEL 'bar' WITH timeout = 10000ms"),
			},
			wantErr: nil,
		},
		{
			name:  "too low timeout",
			level: "bar",
			options: []ServiceLevelOption{
				WithTimeout(0),
			},
			wantErr: ErrOutOfRange,
		},
		{
			name:  "new workload",
			level: "bar",
			options: []ServiceLevelOption{
				WithWorkloadType("interactive"),
			},
			want: []OpTest{
				CommandMatchOpTest("ALTER SERVICE LEVEL 'bar' WITH workload_type = interactive"),
			},
			wantErr: nil,
		},
		{
			name:  "unknown workload",
			level: "bar",
			options: []ServiceLevelOption{
				WithWorkloadType("spammer"),
			},
			wantErr: ErrOutOfRange,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UpdateServiceLevelDDL(tt.level, tt.options...)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.want), "expected the number of commands returned to match the number of tests")

			for idx, testDDL := range tt.want {
				assert.NoError(t, testDDL(got[idx]))
			}
		})
	}
}

func TestServiceLevelAttachmentDDL(t *testing.T) {
	tests := []struct {
		name      string
		level     string
		role      string
		operation string
		want      []OpTest
		wantErr   error
	}{
		{
			name:      "attach",
			level:     "baz",
			role:      "user",
			operation: "attach",
			want: []OpTest{
				CommandMatchOpTest("ATTACH SERVICE LEVEL 'baz' TO user"),
			},
		},
		{
			name:      "attach odds chars",
			level:     `'!^&@#%^&%#$\(`,
			role:      "user",
			operation: "attach",
			want: []OpTest{
				CommandMatchOpTest(`ATTACH SERVICE LEVEL '''!^&@#%^&%#$\(' TO user`),
			},
		},
		{
			name:      "detach",
			level:     "baz",
			role:      "user",
			operation: "detach",
			want: []OpTest{
				CommandMatchOpTest("DETACH SERVICE LEVEL 'baz' FROM user"),
			},
		},
		{
			name:      "unknown command",
			level:     "baz",
			role:      "user",
			operation: "splat",
			wantErr:   ErrUnknownOperation,
		},
		{
			name:      "bad user",
			level:     "baz",
			role:      "<bad>",
			operation: "attach",
			wantErr:   ErrInvalidInput,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ServiceLevelAttachmentDDL(tt.level, tt.role, tt.operation)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.want), "expected the number of commands returned to match the number of tests")

			for idx, testDDL := range tt.want {
				assert.NoError(t, testDDL(got[idx]))
			}
		})
	}
}
