package generator

import (
	"fmt"
)

type GrantObject string

const (
	GrantObjectKeyspace  GrantObject = "KEYSPACE"
	GrantObjectKeyspaces GrantObject = "ALL KEYSPACES"
	GrantObjectTable     GrantObject = "TABLE"
	GrantObjectRole      GrantObject = "ROLE"
	GrantObjectRoles     GrantObject = "ALL ROLES"
)

var validGrantObjects Set[GrantObject]

type GrantVerb string

const (
	GrantCreate               GrantVerb = "CREATE"
	GrantAlter                GrantVerb = "ALTER"
	GrantDrop                 GrantVerb = "DROP"
	GrantSelect               GrantVerb = "SELECT"
	GrantModify               GrantVerb = "MODIFY"
	GrantAuthorize            GrantVerb = "AUTHORIZE"
	GrantDescribe             GrantVerb = "DESCRIBE"
	GrantVectorSearchIndexing GrantVerb = "VECTOR_SEARCH_INDEXING"
)

var validGrantVerbs Set[GrantVerb]

func init() {
	validGrantVerbs = SetOf(
		GrantCreate,
		GrantAlter,
		GrantDrop,
		GrantSelect,
		GrantModify,
		GrantAuthorize,
		GrantDescribe,
		GrantVectorSearchIndexing,
	)
	validGrantObjects = SetOf(GrantObjectKeyspace)
}

func (g GrantVerb) String() string {
	return string(g)
}

func (g GrantVerb) Validate() error {
	if !validGrantVerbs.Has(g) {
		return fmt.Errorf("unknown grant verb %q", g)
	}
	return nil
}

func (o GrantObject) String() string {
	return string(o)
}

func (o GrantObject) Validate() error {
	if !validGrantObjects.Has(o) {
		return fmt.Errorf("unknown grant to object %q", o)
	}
	return nil
}
