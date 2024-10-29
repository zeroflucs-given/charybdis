package mapping

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectSubTypesFromType(t *testing.T) {

	type Two struct {
		X int `cql:"x"`
		Y int `cql:"y"`
	}

	type Six struct {
		N int `cql:"n"`
		M int `cql:"m"`
	}

	type One struct {
		A int    `cql:"a"`
		B string `cql:"b"`
		C *Two   `cql:"c"`
		D []*Six `cql:"d" cqltype:"notsix"`
	}

	var eg One

	typ := reflect.TypeOf(eg)
	got, err := CollectSubTypesFromType(typ)
	require.NoError(t, err)

	t.Logf("Got: %d subtypes\n", len(got))

	assert.Equal(t, 2, len(got))

	assert.Equal(t, "Two", got["two"].Name())
	assert.Equal(t, "Six", got["notsix"].Name())
}

func Test_getBaseTypeForScyllaTag(t *testing.T) {
	tests := []struct {
		name    string
		tag     string
		want    []string
		wantErr bool
	}{
		{
			name:    "primitive",
			tag:     "int",
			want:    []string{"int"},
			wantErr: false,
		},
		{
			name:    "list",
			tag:     "list<int>",
			want:    []string{"int"},
			wantErr: false,
		},
		{
			name:    "map",
			tag:     "map<text,int>",
			want:    []string{"text", "int"},
			wantErr: false,
		},
		{
			name:    "frozen list",
			tag:     "frozen<list<int>>",
			want:    []string{"int"},
			wantErr: false,
		},
		{
			name:    "frozen map",
			tag:     "frozen<map<text,int>>",
			want:    []string{"text", "int"},
			wantErr: false,
		},
		{
			name:    "mismatched brackets",
			tag:     "frozen<list<int>",
			wantErr: true,
		},
		{
			name:    "bad type",
			tag:     "int>",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getBaseTypesForScyllaTag(tt.tag)
			if tt.wantErr {
				require.Errorf(t, err, "expected error an no type, got type %s", got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
