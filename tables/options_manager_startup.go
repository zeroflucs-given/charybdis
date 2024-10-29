package tables

import (
	"context"

	"github.com/zeroflucs-given/charybdis/metadata"
)

// TableManagerStartupFnEx is a startup function called before the table-manager is deemed ready to use.
// This is an extended version of TableManagerStartupFn, allowing flexibility of adding additional options later
type TableManagerStartupFnEx func(ctx context.Context, keyspace string, options ...StartupOption) error

type StartupOption func(options *StartupOptions)

// StartupOptions includes data we can use oni table startup
type StartupOptions struct {
	table *metadata.TableSpecification
	view  *metadata.ViewSpecification
	types []*metadata.TypeSpecification
	ddl   []metadata.DDLOperation
}

func (o *StartupOptions) Table() *metadata.TableSpecification {
	return o.table
}

func (o *StartupOptions) View() *metadata.ViewSpecification {
	return o.view
}

func (o *StartupOptions) Types() []*metadata.TypeSpecification {
	return o.types
}

func (o *StartupOptions) AdditionalDDL() []metadata.DDLOperation {
	return o.ddl
}

// CollectStartupOptions creates a new initialised StartupOptions
func CollectStartupOptions(options []StartupOption) *StartupOptions {
	t := &StartupOptions{}
	for _, option := range options {
		option(t)
	}
	return t
}

func WithTableSpec(t *metadata.TableSpecification) StartupOption {
	return func(options *StartupOptions) {
		options.table = t
	}
}

func WithViewSpec(view *metadata.ViewSpecification) StartupOption {
	return func(options *StartupOptions) {
		options.view = view
	}
}

func WithTypeSpec(types ...*metadata.TypeSpecification) StartupOption {
	return func(options *StartupOptions) {
		options.types = append(options.types, types...)
	}
}

func WithAdditionalDDL(ddlOps ...metadata.DDLOperation) StartupOption {
	return func(options *StartupOptions) {
		options.ddl = append(options.ddl, ddlOps...)
	}
}
